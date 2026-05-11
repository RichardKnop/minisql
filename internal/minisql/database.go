package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

var (
	errUnrecognizedStatementType = errors.New("unrecognised statement type")
	errTableDoesNotExist         = errors.New("table does not exist")
	errTableAlreadyExists        = errors.New("table already exists")
	errIndexDoesNotExist         = errors.New("index does not exist")
	errIndexAlreadyExists        = errors.New("index already exists")
	errIndexOnJSONColumn         = errors.New("b-tree index on JSON column is not supported")
	errIndexMethodUnsupported    = errors.New("index method is parsed but not implemented yet")
)

// WALConfig bundles the Write-Ahead Log objects that NewDatabase needs.
// Pass nil when creating in-memory/test databases that do not require WAL.
type WALConfig struct {
	WAL                 *WAL
	Index               *WALIndex
	DBFile              DBFile
	CheckpointThreshold int
	WALWriteBufferSize  int // bytes to buffer before flushing; 0 = flush every commit
	Synchronous         SynchronousMode
}

// Database is the top-level embedded SQL database instance.
type Database struct {
	walDBFile      DBFile
	parser         Parser
	factory        PagerFactory
	saver          PageSaver
	lockedProvider TableProvider
	stmtCache      LRUCache[string]
	tables         map[string]*Table
	txManager      *TransactionManager
	dbLock         *sync.RWMutex
	walIndex       *WALIndex
	clock          clock
	logger         *zap.Logger
	wal            *WAL
	rowCounts      map[string]int64
	dbFilePath     string
	rowCountsMu    sync.RWMutex
	parallelScan   bool
	// referencedBy maps each parent table name to the list of FK constraints
	// from other (child) tables that reference it.  Built at startup and kept
	// in sync as tables are created/dropped.  Access is guarded by dbLock.
	referencedBy map[string][]inboundFK
	// foreignKeysEnabled controls whether FK constraints are enforced.
	// Default true; toggled by PRAGMA foreign_keys = on|off.
	foreignKeysEnabled bool
}

type clock func() Time

// NewDatabase creates a new database.
// walCfg wires in the Write-Ahead Log; pass nil for in-memory/test databases
// that do not require WAL (commits fall back to writing directly to the pager).
func NewDatabase(ctx context.Context, logger *zap.Logger, dbFilePath string, parser Parser, factory PagerFactory, saver PageSaver, walCfg *WALConfig, opts ...DatabaseOption) (*Database, error) {
	db := &Database{
		dbFilePath:         dbFilePath,
		parser:             parser,
		factory:            factory,
		saver:              saver,
		tables:             make(map[string]*Table),
		rowCounts:          make(map[string]int64),
		referencedBy:       make(map[string][]inboundFK),
		foreignKeysEnabled: true,
		dbLock:             new(sync.RWMutex),
		stmtCache:          lrucache.New[string](defaultMaxCachedStatements),
		logger:             logger,
		clock: func() Time {
			now := time.Now().UTC()
			return Time{
				Year:         int32(now.Year()),
				Month:        int8(now.Month()),
				Day:          int8(now.Day()),
				Hour:         int8(now.Hour()),
				Minutes:      int8(now.Minute()),
				Seconds:      int8(now.Second()),
				Microseconds: int32(now.Nanosecond() / 1000),
			}
		},
	}
	db.lockedProvider = &lockedTableProvider{db: db}

	db.txManager = NewTransactionManager(logger, dbFilePath, db.pagerFactory, saver, db)
	db.txManager.SetRowCountApplier(db.applyRowCountDeltas)

	if walCfg != nil {
		db.wal = walCfg.WAL
		db.walIndex = walCfg.Index
		db.walDBFile = walCfg.DBFile
		db.txManager.wal = walCfg.WAL
		db.txManager.walIndex = walCfg.Index
		db.txManager.checkpointThreshold = walCfg.CheckpointThreshold
		db.txManager.SetCheckpointFunc(func() error {
			return db.Checkpoint(context.Background())
		})
		saver.SetWALIndex(walCfg.Index)
		if walCfg.WAL != nil {
			walCfg.WAL.SetSynchronous(walCfg.Synchronous)
			walCfg.WAL.SetWriteBufferSize(walCfg.WALWriteBufferSize)
		}
	}

	for _, opt := range opts {
		opt(db)
	}

	if err := db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return db.init(ctx)
	}); err != nil {
		return nil, err
	}

	return db, nil
}

// GetTable retrieves a table by name in a thread-safe manner.
// CTE virtual tables registered in the context shadow real tables of the same name.
func (d *Database) GetTable(ctx context.Context, name string) (*Table, bool) {
	if t, ok := cteFromContext(ctx, name); ok {
		return t, true
	}
	d.dbLock.RLock()
	defer d.dbLock.RUnlock()
	table, exists := d.tables[name]
	if !exists {
		return nil, false
	}
	return table, true
}

// lockedTableProvider is a TableProvider wrapper that assumes locks are already held.
// Used internally when the database lock is already acquired.
type lockedTableProvider struct {
	db *Database
}

// GetTable retrieves a table by name without acquiring the database lock.
func (p *lockedTableProvider) GetTable(ctx context.Context, name string) (*Table, bool) {
	if t, ok := cteFromContext(ctx, name); ok {
		return t, true
	}
	table, ok := p.db.tables[name]
	if ok {
		return table, true
	}
	if tx := TxFromContext(ctx); tx != nil {
		// We could be in a trasaction and table is being created but tx is not yet committed
		for _, tableBeingCreated := range TxFromContext(ctx).DDLChanges.CreateTables {
			if tableBeingCreated.Name == name {
				return tableBeingCreated, true
			}
		}
	}
	return nil, false
}

// PrepareStatement parses and caches a SQL statement, returning the parsed Statement.
func (d *Database) PrepareStatement(ctx context.Context, query string) (Statement, error) {
	// Check cache first
	if stmt, ok := d.stmtCache.Get(query); ok {
		return stmt.(Statement), nil
	}

	// Parse the statement
	statements, err := d.parser.Parse(ctx, query)
	if err != nil {
		return Statement{}, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(statements) == 0 {
		return Statement{}, errors.New("no statements in query")
	}

	if len(statements) > 1 {
		return Statement{}, errors.New("multiple statements not supported in prepared statements")
	}

	stmt := statements[0]

	// Cache the parsed statement
	d.stmtCache.Put(query, stmt, true)

	return stmt, nil
}

// Close flushes and closes the underlying page storage.
func (d *Database) Close() error {
	// Passive checkpoint on close (mirrors SQLite behaviour): if there are
	// committed WAL frames that have not yet been written to the DB file, flush
	// them now so the DB file is a complete snapshot.  This limits WAL growth
	// across restarts and makes crash recovery trivially fast on the next open.
	// We log but do not fail on checkpoint error — the WAL is still valid and
	// will be replayed on the next open if the checkpoint is incomplete.
	if d.wal != nil && d.walDBFile != nil && d.wal.FrameCount() > 0 {
		if err := d.Checkpoint(context.Background()); err != nil {
			d.logger.Warn("checkpoint on close failed; WAL will be replayed on next open",
				zap.Error(err))
		}
	}

	// Close the WAL file before the DB file.
	if d.wal != nil {
		if err := d.wal.Close(); err != nil {
			d.logger.Warn("failed to close WAL on database close", zap.Error(err))
		}
		d.wal = nil
	}
	return d.saver.Close()
}

// Checkpoint checkpoints the WAL into the database file and truncates the WAL.
// It is a no-op when no WAL is configured.
//
// Checkpoint blocks new WAL writers until it completes.  Readers are not
// blocked: they continue to use the (now being reset) WAL index, which is
// fine because the pager cache still holds the correct pages and the DB file
// is being written with the same data.
func (d *Database) Checkpoint(_ context.Context) error {
	if d.wal == nil || d.walDBFile == nil {
		return nil
	}
	return d.txManager.CheckpointWAL(d.walDBFile)
}

// Reopen replaces the pager and transaction manager with fresh instances backed by the given factory and saver.
func (d *Database) Reopen(ctx context.Context, factory PagerFactory, saver PageSaver) error {
	d.factory = factory
	d.saver = saver
	d.tables = make(map[string]*Table)

	// Reset the row-count cache; init() will repopulate it via leaf walks.
	d.rowCountsMu.Lock()
	d.rowCounts = make(map[string]int64)
	d.rowCountsMu.Unlock()

	// Preserve WAL settings then create a fresh transaction manager for the new
	// file.  The old manager's global page versions are no longer valid after a
	// file swap, and its saver points to the closed old file.
	var (
		checkpointThreshold = d.txManager.checkpointThreshold
		checkpointFn        = d.txManager.checkpointFn
	)
	d.txManager = NewTransactionManager(d.logger, d.dbFilePath, d.pagerFactory, saver, d)
	d.txManager.SetRowCountApplier(d.applyRowCountDeltas)
	if d.wal != nil {
		d.txManager.wal = d.wal
		d.txManager.walIndex = d.walIndex
		d.txManager.checkpointThreshold = checkpointThreshold
		d.txManager.SetCheckpointFunc(checkpointFn)
	}

	// Use a fresh context so init runs in a brand-new transaction on the new
	// transaction manager, with no entanglement with any outer transaction.
	if err := d.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
		return d.init(ctx)
	}); err != nil {
		return err
	}
	return nil
}

// TODO - support composite values
func (d *Database) pagerFactory(ctx context.Context, tableName, indexName string) (Pager, error) {
	table, ok := d.lockedProvider.GetTable(ctx, tableName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errTableDoesNotExist, tableName)
	}
	if indexName == "" {
		return d.factory.ForTable(table.Columns), nil
	}

	columns, ok := table.IndexColumnsByIndexName(indexName)
	if !ok {
		if tx := TxFromContext(ctx); tx != nil {
			// We could be in a trasaction and index is being created but tx is not yet committed
			for _, secondaryIndex := range TxFromContext(ctx).DDLChanges.CreateIndexes {
				if secondaryIndex.Name == indexName {
					columns = secondaryIndex.Columns
				}
			}
		}
		if len(columns) == 0 {
			return nil, errIndexDoesNotExist
		}
	}

	unique := false
	if table.HasPrimaryKey() {
		unique = true
	} else {
		for _, uniqueIndex := range table.UniqueIndexes {
			if len(uniqueIndex.Columns) == 1 && uniqueIndex.Columns[0].Name == columns[0].Name {
				unique = true
				break
			}
		}
	}

	return d.factory.ForIndex(columns, unique), nil
}

// applyRowCountDeltas applies committed insert/delete row-count changes to the
// in-memory cache.  Only entries that already exist in rowCounts (i.e. user
// tables) are updated; system tables are not tracked and are silently ignored.
func (d *Database) applyRowCountDeltas(deltas map[string]int64) {
	d.rowCountsMu.Lock()
	for name, delta := range deltas {
		if _, tracked := d.rowCounts[name]; tracked {
			d.rowCounts[name] += delta
		}
	}
	d.rowCountsMu.Unlock()
}

// rowCountGetter returns a closure that reads the cached row count for
// tableName from this Database's rowCounts map.
func (d *Database) rowCountGetter(tableName string) func() int64 {
	return func() int64 {
		d.rowCountsMu.RLock()
		n := d.rowCounts[tableName]
		d.rowCountsMu.RUnlock()
		return n
	}
}

// initTableRowCount counts the rows in table via a B+ tree leaf walk and
// stores the result in d.rowCounts[tableName].  It also wires up the O(1)
// getter on the table so future COUNT(*) calls bypass the walk entirely.
func (d *Database) initTableRowCount(ctx context.Context, tableName string, table *Table) {
	// getRowCount is nil at this point, so countAllLeafWalk falls back to the
	// leaf walk — exactly what we want for the initial population.
	result, err := table.countAllLeafWalk(ctx)
	if err != nil {
		d.logger.Warn("failed to initialise row count; COUNT(*) will fall back to leaf walk",
			zap.String("table", tableName), zap.Error(err))
		return
	}
	var count int64
	if result.Rows.Next(ctx) {
		row := result.Rows.Row()
		if len(row.Values) > 0 {
			if n, ok := row.Values[0].Value.(int64); ok {
				count = n
			}
		}
	}
	d.rowCountsMu.Lock()
	d.rowCounts[tableName] = count
	d.rowCountsMu.Unlock()
	table.getRowCount = d.rowCountGetter(tableName)
}

// SaveDDLChanges applies committed DDL changes (table/index creates and drops) to the in-memory schema.
func (d *Database) SaveDDLChanges(ctx context.Context, changes DDLChanges) {
	if !changes.HasChanges() {
		return
	}

	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	for _, table := range changes.CreateTables {
		d.tables[table.Name] = table
		// New tables start with zero rows.
		tableName := table.Name
		d.rowCountsMu.Lock()
		d.rowCounts[tableName] = 0
		d.rowCountsMu.Unlock()
		table.getRowCount = d.rowCountGetter(tableName)
	}
	for _, tableName := range changes.DropTables {
		delete(d.tables, tableName)
		d.rowCountsMu.Lock()
		delete(d.rowCounts, tableName)
		d.rowCountsMu.Unlock()
	}
	for tableName, index := range changes.CreateIndexes {
		d.tables[tableName].SetSecondaryIndex(index)
	}
	for tableName, index := range changes.DropIndexes {
		d.tables[tableName].RemoveSecondaryIndex(index.Name)
	}
}

// ListTableNames lists names of all tables in the database
func (d *Database) ListTableNames(ctx context.Context) []string {
	d.dbLock.RLock()
	defer d.dbLock.RUnlock()

	tables := make([]string, 0, len(d.tables))
	for tableName := range d.tables {
		tables = append(tables, tableName)
	}
	return tables
}

// PrepareStatements parses SQL into a slice of Statement struct
func (d *Database) PrepareStatements(ctx context.Context, sql string) ([]Statement, error) {
	stmts, err := d.parser.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	return stmts, nil
}

// GetTransactionManager returns the transaction manager for this database
func (d *Database) GetTransactionManager() *TransactionManager {
	return d.txManager
}

// GetSaver returns the page saver for this database
func (d *Database) GetSaver() PageSaver {
	return d.saver
}

// GetDDLSaver returns the DDLSaver interface backed by this database.
func (d *Database) GetDDLSaver() DDLSaver {
	return d
}

// GetFileName returns the database file name
func (d *Database) GetFileName() string {
	return d.dbFilePath
}

// ExecuteStatement executes a single statement and returns the result
func (d *Database) ExecuteStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return StatementResult{}, errors.New("statement must be executed from within a transaction")
	}

	if !stmt.ReadOnly() && isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("cannot write to system table %s", stmt.TableName)
	}

	switch stmt.Kind {
	case Vacuum:
		// VACUUM manages its own locking and creates a fresh transaction
		// manager on completion, so it must not go through the normal DDL
		// path (which would deadlock by re-acquiring dbLock).
		return StatementResult{}, d.Vacuum(ctx)
	case Pragma:
		return d.executePragmaStatement(ctx, stmt)
	case Explain:
		return d.executeExplain(ctx, stmt)
	case CreateTable, DropTable, CreateIndex, DropIndex:
		return d.executeDDLStatement(ctx, stmt)
	case Insert, Select, Update, Delete:
		// WITH … SELECT — CTE statement. Route before resolveSubqueries because
		// the outer WHERE may reference CTE names that only become resolvable
		// after the CTE virtual tables are materialised.
		if stmt.Kind == Select && len(stmt.CTEs) > 0 {
			return d.executeCTESelect(ctx, stmt)
		}

		// Pre-evaluate any non-correlated scalar subqueries in the WHERE clause.
		if len(stmt.Conditions) > 0 {
			resolved, err := d.resolveSubqueries(ctx, stmt.Conditions)
			if err != nil {
				return StatementResult{}, err
			}
			stmt.Conditions = resolved
		}

		// SELECT with UNION / UNION ALL branches is handled by the union executor.
		if stmt.Kind == Select && len(stmt.Unions) > 0 {
			return d.executeUnion(ctx, stmt)
		}

		// SELECT … FROM (subquery) alias — derived table.
		if stmt.Kind == Select && stmt.FromSubquery != nil {
			return d.executeSelectFromDerivedTable(ctx, stmt)
		}

		table, ok := d.GetTable(ctx, stmt.TableName)
		if !ok {
			return StatementResult{}, fmt.Errorf("%w: %s", errTableDoesNotExist, stmt.TableName)
		}

		return d.executeTableStatement(ctx, table, stmt)
	}
	return StatementResult{}, errUnrecognizedStatementType
}

func (d *Database) init(ctx context.Context) error {
	var (
		mainTablePager = d.factory.ForTable(mainTableColumns)
		totalPages     = int(d.saver.TotalPages())
		rooPageIdx     = PageIndex(0)
	)

	d.logger.Sugar().With(
		"file_name", d.dbFilePath,
		"total_pages", totalPages,
	).Debug("initializing database")

	// Only initialise an empty database when BOTH the DB file has no pages AND
	// the WAL index has no data.  In WAL-only mode the DB file stays at 0 bytes
	// (writes are only in the WAL), so checking totalPages alone would
	// incorrectly re-initialise an existing database after a reopen.
	walEmpty := d.walIndex == nil || d.walIndex.Size() == 0
	if totalPages == 0 && walEmpty {
		if err := d.initEmptyDatabase(ctx, rooPageIdx, mainTablePager); err != nil {
			return err
		}
	}

	// Otherwise, main table already exists,
	// we need to read all existing tables from the schema table
	txPager := NewTransactionalPager(mainTablePager, d.txManager, SchemaTableName, "")
	txPager.table = SchemaTableName
	mainTable := NewTable(
		d.logger,
		txPager,
		d.txManager,
		SchemaTableName,
		mainTableColumns,
		rooPageIdx,
		d.lockedProvider,
	)
	d.tables[mainTable.Name] = mainTable

	schemas, err := d.listSchemas(ctx)
	if err != nil {
		return err
	}

	for _, schema := range schemas {
		switch schema.Type {
		case SchemaTable:
			if err := d.initTable(ctx, schema); err != nil {
				return err
			}
			// Populate row-count cache so COUNT(*) can be answered in O(1).
			if table, ok := d.tables[schema.Name]; ok {
				d.initTableRowCount(ctx, schema.Name, table)
			}
		case SchemaPrimaryKey:
			if err := d.initPrimaryKey(ctx, schema); err != nil {
				return err
			}
		case SchemaUniqueIndex:
			if err := d.initUniqueIndex(ctx, schema); err != nil {
				return err
			}
		case SchemaSecondaryIndex:
			if err := d.initSecondaryIndex(ctx, schema); err != nil {
				return err
			}
		case SchemaForeignKey:
			// FK schemas are processed in a second pass (see below).
		default:
			return fmt.Errorf("unrecognized schema type %d", schema.Type)
		}
	}

	// Second pass: build the referencedBy map from all loaded tables' FK lists
	// and wire up FK check callbacks.  This must happen after all tables are loaded
	// so that cross-table references can be resolved.
	d.rebuildFKState()

	// Use the lock-free variant: init is called either from NewDatabase
	// (single-threaded) or from Reopen (which holds dbLock.Lock()), so
	// calling the locking listStats here would deadlock.
	stats, err := d.listStatsNoLock(ctx, "")
	if err != nil {
		return err
	}
	for _, s := range stats {
		if s.IndexName == "" {
			continue
		}
		indexStats, err := parseIndexStats(s.StatValue)
		if err != nil {
			return err
		}
		d.tables[s.TableName].indexStats[s.IndexName] = indexStats
	}

	return nil
}

func (d *Database) listSchemas(ctx context.Context) ([]Schema, error) {
	mainTable := d.tables[SchemaTableName]
	schemaResults, err := mainTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: mainTableFields,
		Conditions: OneOrMore{
			{
				FieldIsNotEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte(mainTable.Name))), // skip main table itself
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var schemas []Schema
	for schemaResults.Rows.Next(ctx) {
		schemas = append(schemas, scanSchema(schemaResults.Rows.Row()))
	}
	if err := schemaResults.Rows.Err(); err != nil {
		return nil, err
	}

	return schemas, nil
}

func (d *Database) initEmptyDatabase(ctx context.Context, rooPageIdx PageIndex, mainTablePager Pager) error {
	d.logger.Sugar().With(
		"name", SchemaTableName,
		"root_page", rooPageIdx,
	).Debug("creating system schema table")

	// New database, need to create the main schema table
	mainTable := NewTable(
		d.logger,
		NewTransactionalPager(mainTablePager, d.txManager, SchemaTableName, ""),
		d.txManager,
		SchemaTableName,
		mainTableColumns,
		rooPageIdx,
		d.lockedProvider,
	)
	d.tables[SchemaTableName] = mainTable

	// And save record of itself
	_, err := mainTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields:    mainTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: int32(SchemaTable), Valid: true},
				{Value: NewTextPointer([]byte(mainTable.Name)), Valid: true}, // name
				{}, // tbl_name
				{Value: int32(mainTable.GetRootPageIdx()), Valid: true},    // root page
				{Value: NewTextPointer([]byte(MainTableSQL)), Valid: true}, // sql
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) initTable(ctx context.Context, schema Schema) error {
	// Parse and validate CREATE TABLE query is valid, this also parses any default values
	// and transforms them into TextPointer for text columns or TIme for timestamps.
	stmts, err := d.parser.Parse(ctx, schema.DDL)
	if err != nil {
		return err
	}
	if len(stmts) != 1 {
		return fmt.Errorf("expected one statement when loading table, got %d", len(stmts))
	}
	stmt := stmts[0]
	if err := stmt.Validate(nil); err != nil {
		return err
	}

	d.tables[stmt.TableName], err = d.tableFromSQL(ctx, schema)
	if err != nil {
		return err
	}

	d.logger.Sugar().With(
		"name", stmt.TableName,
		"root_page", schema.RootPage,
	).Debug("loaded table")

	return nil
}

func (d *Database) tableFromSQL(ctx context.Context, schema Schema) (*Table, error) {
	stmts, err := d.parser.Parse(ctx, schema.DDL)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected one statement when loading table, got %d", len(stmts))
	}
	stmt := stmts[0]
	if err := stmt.Validate(nil); err != nil {
		return nil, err
	}

	tp := NewTransactionalPager(
		d.factory.ForTable(stmt.Columns),
		d.txManager,
		stmt.TableName,
		"",
	)

	opts := []TableOption{}

	if stmt.PrimaryKey.Name != "" {
		opts = append(opts, WithPrimaryKey(NewPrimaryKey(
			stmt.PrimaryKey.Name,
			stmt.PrimaryKey.Columns,
			stmt.PrimaryKey.Autoincrement,
		)))
	}

	for _, uniqueIndex := range stmt.UniqueIndexes {
		opts = append(opts, WithUniqueIndex(uniqueIndex))
	}

	opts = append(opts, WithParallelScan(d.parallelScan))

	if len(stmt.ForeignKeys) > 0 {
		opts = append(opts, WithForeignKeys(stmt.ForeignKeys))
	}

	return NewTable(
		d.logger,
		tp,
		d.txManager,
		stmt.TableName,
		stmt.Columns,
		schema.RootPage,
		d.lockedProvider,
		opts...,
	), nil
}

func (d *Database) initPrimaryKey(ctx context.Context, schema Schema) error {
	// TODO - parse SQL once we store it for primary indexes? Right now it will be NULL

	table, ok := d.tables[schema.TableName]
	if !ok {
		return fmt.Errorf("table %s for primary key index %s does not exist", schema.TableName, schema.Name)
	}
	tp := NewTransactionalPager(
		d.factory.ForIndex(table.PrimaryKey.Columns, true),
		d.txManager,
		table.Name,
		schema.Name,
	)
	btreeIndex, err := table.newBTreeIndex(tp, schema.RootPage, table.PrimaryKey.Columns, table.PrimaryKey.Name, true)
	if err != nil {
		return err
	}

	// Set primary key BTree index on the table instance
	table.PrimaryKey.Index = btreeIndex

	d.logger.Sugar().With(
		"name", table.PrimaryKey.Name,
		"root_page", schema.RootPage,
	).Debug("loaded primary key index")

	return nil
}

func (d *Database) initUniqueIndex(ctx context.Context, schema Schema) error {
	// TODO - parse SQL once we store it for unique indexes? Right now it will be NULL

	table, ok := d.tables[schema.TableName]
	if !ok {
		return fmt.Errorf("table %s for unique index %s does not exist", schema.TableName, schema.Name)
	}
	uniqueIndex, ok := table.UniqueIndexes[schema.Name]
	if !ok {
		return fmt.Errorf("unique index %s does not exist on table %s", schema.Name, schema.TableName)
	}
	tp := NewTransactionalPager(
		d.factory.ForIndex(uniqueIndex.Columns, true),
		d.txManager,
		table.Name,
		schema.Name,
	)
	btreeIndex, err := table.newBTreeIndex(tp, schema.RootPage, uniqueIndex.Columns, uniqueIndex.Name, true)
	if err != nil {
		return err
	}

	// Set unique BTree index on the table instance
	uniqueIndex.Index = btreeIndex
	table.UniqueIndexes[schema.Name] = uniqueIndex

	d.logger.Sugar().With(
		"name", uniqueIndex.Name,
		"root_page", schema.RootPage,
	).Debug("loaded unique index")

	return nil
}

func (d *Database) initSecondaryIndex(ctx context.Context, schema Schema) error {
	table, ok := d.tables[schema.TableName]
	if !ok {
		return fmt.Errorf("table %s for secondary index %s does not exist", schema.TableName, schema.Name)
	}

	// Parse and validate CREATE INDEX statement to get indexed column
	stmts, err := d.parser.Parse(ctx, schema.DDL)
	if err != nil {
		return err
	}
	if len(stmts) != 1 {
		return fmt.Errorf("expected one statement when loading index, got %d", len(stmts))
	}
	stmt := stmts[0]
	if err := stmt.Validate(table); err != nil {
		return err
	}

	var indexColumn Column
	if stmt.IndexExpression != nil {
		kind := inferExprResultKind(stmt.IndexExpression, table.Columns)
		indexColumn = syntheticExprColumn(kind)
	} else {
		var ok bool
		indexColumn, ok = table.ColumnByName(stmt.Columns[0].Name)
		if !ok {
			return fmt.Errorf("column %s does not exist on table %s for secondary index %s", stmt.Columns[0].Name, schema.TableName, schema.Name)
		}
	}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:          schema.Name,
			Columns:       []Column{indexColumn},
			WhereClause:   stmt.IndexWhereClause,
			WhereCond:     stmt.Conditions,
			Expression:    stmt.IndexExpression,
			ExpressionSQL: stmt.IndexExpressionSQL,
			Tokenizer:     stmt.IndexTokenizer,
			Method:        stmt.IndexMethod,
		},
	}

	// Create and set BTree index instance
	tp := NewTransactionalPager(
		d.factory.ForIndex(secondaryIndex.Columns, false),
		d.txManager,
		table.Name,
		schema.Name,
	)
	btreeIndex, err := table.newBTreeIndex(tp, schema.RootPage, secondaryIndex.Columns, secondaryIndex.Name, false)
	if err != nil {
		return err
	}
	secondaryIndex.Index = btreeIndex

	table.SetSecondaryIndex(secondaryIndex)

	d.logger.Sugar().With(
		"name", schema.Name,
		"root_page", schema.RootPage,
	).Debug("loaded secondary index")

	return nil
}

func (d *Database) executeDDLStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
	var err error
	stmt, err = stmt.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	var table *Table
	if stmt.Kind == CreateIndex {
		// Table could only exist within this transaction so create it from the system table
		tableSchema, exists, err := d.checkSchemaExists(ctx, SchemaTable, stmt.TableName)
		if err != nil {
			return StatementResult{}, err
		}
		if !exists {
			return StatementResult{}, fmt.Errorf("%w: %s", errTableDoesNotExist, stmt.TableName)
		}
		table, err = d.tableFromSQL(ctx, tableSchema)
		if err != nil {
			return StatementResult{}, err
		}
	}

	if err := stmt.Validate(table); err != nil {
		return StatementResult{}, err
	}

	// Use lock to limit to only one write operation at a time
	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	switch stmt.Kind {
	case CreateTable:
		_, err := d.createTable(ctx, stmt)
		return StatementResult{}, err
	case DropTable:
		return StatementResult{}, d.dropTable(ctx, stmt.TableName)
	case CreateIndex:
		return StatementResult{}, d.createIndex(ctx, stmt, table)
	case DropIndex:
		return StatementResult{}, d.dropIndex(ctx, stmt)
	case Analyze:
		return StatementResult{}, d.Analyze(ctx, stmt.TableName)
	}

	return StatementResult{}, fmt.Errorf("unrecognized DDL statement type: %v", stmt.Kind)
}

func (d *Database) executeTableStatement(ctx context.Context, table *Table, stmt Statement) (StatementResult, error) {
	stmt.TableName = table.Name
	stmt.Columns = table.Columns

	var err error
	stmt, err = stmt.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	if err := stmt.Validate(table); err != nil {
		return StatementResult{}, err
	}

	if !stmt.ReadOnly() {
		// Use lock to limit to only one write operation at a time
		d.dbLock.Lock()
		defer d.dbLock.Unlock()
	}

	switch stmt.Kind {
	case Insert:
		return table.Insert(ctx, stmt)
	case Select:
		return table.Select(ctx, stmt)
	case Update:
		return table.Update(ctx, stmt)
	case Delete:
		return table.Delete(ctx, stmt)
	}

	return StatementResult{}, fmt.Errorf("unrecognized table statement type: %v", stmt.Kind)
}

// flattenUnionChain traverses a linked chain of UnionClause nodes (built by the parser)
// and returns two parallel slices: the SELECT statements in left-to-right order, and the
// All flag for each join between adjacent statements (len(alls) == len(stmts)-1).
func flattenUnionChain(stmt Statement) ([]Statement, []bool) {
	stmts := make([]Statement, 0, 4)
	alls := make([]bool, 0, 3)
	cur := stmt
	for {
		base := cur
		base.Unions = nil
		stmts = append(stmts, base)
		if len(cur.Unions) == 0 {
			break
		}
		alls = append(alls, cur.Unions[0].All)
		cur = cur.Unions[0].Stmt
	}
	return stmts, alls
}

// executeUnion handles SELECT … UNION [ALL] SELECT … chains.
// It executes each SELECT independently and merges the results:
//   - UNION ALL: concatenate all rows from both sides (duplicates kept).
//   - UNION:     concatenate then deduplicate (like DISTINCT across the union).
//
// Column metadata is taken from the first SELECT; all branches must produce the
// same number of columns (validated at execution time by the query engine).
func (d *Database) executeUnion(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmts, alls := flattenUnionChain(stmt)

	var allRows []Row
	var resultColumns []Column

	for i, s := range stmts {
		table, ok := d.GetTable(ctx, s.TableName)
		if !ok {
			return StatementResult{}, fmt.Errorf("%w: %s", errTableDoesNotExist, s.TableName)
		}

		result, err := d.executeTableStatement(ctx, table, s)
		if err != nil {
			return StatementResult{}, err
		}

		if i == 0 {
			resultColumns = result.Columns
		}

		// Drain the iterator for this branch.
		var branchRows []Row
		for result.Rows.Next(ctx) {
			branchRows = append(branchRows, result.Rows.Row())
		}
		if err := result.Rows.Err(); err != nil {
			return StatementResult{}, err
		}

		switch {
		case i == 0:
			// First branch: use its rows as the base.
			allRows = branchRows
		case alls[i-1]:
			// UNION ALL: append without deduplication.
			allRows = append(allRows, branchRows...)
		default:
			// UNION: append then deduplicate the entire running set.
			allRows = append(allRows, branchRows...)
			seen := make(map[string]struct{}, len(allRows))
			deduped := make([]Row, 0, len(allRows))
			for _, row := range allRows {
				key := row.rowDistinctKey()
				if _, dup := seen[key]; !dup {
					seen[key] = struct{}{}
					deduped = append(deduped, row)
				}
			}
			allRows = deduped
		}
	}

	idx := 0
	return StatementResult{
		Columns: resultColumns,
		Rows: NewIterator(func(ctx context.Context) (Row, error) {
			if idx >= len(allRows) {
				return Row{}, ErrNoMoreRows
			}
			row := allRows[idx]
			idx += 1
			return row, nil
		}),
	}, nil
}

// createTable creates a new table with a name and columns
func (d *Database) createTable(ctx context.Context, stmt Statement) (*Table, error) {
	tx := MustTxFromContext(ctx)

	// Table could only exist within this transaction so create it from the system table
	_, exists, err := d.checkSchemaExists(ctx, SchemaTable, stmt.TableName)
	if err != nil {
		return nil, err
	}
	if exists {
		if stmt.IfNotExists {
			return d.tables[stmt.TableName], nil
		}
		return nil, errTableAlreadyExists
	}

	d.logger.Sugar().With("name", stmt.TableName).Debug("creating table")

	txPager := NewTransactionalPager(
		d.factory.ForTable(stmt.Columns),
		d.txManager,
		stmt.TableName,
		"",
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}
	freePage.LeafNode = NewLeafNode()
	freePage.LeafNode.Header.IsRoot = true

	// Validate FK targets before creating the table.
	for _, fk := range stmt.ForeignKeys {
		if len(fk.TargetColumns) == 1 {
			targetCol := fk.TargetColumns[0]
			if fk.TargetTable == stmt.TableName {
				if !d.stmtHasIndexOnColumn(stmt, targetCol) {
					return nil, fmt.Errorf(
						"foreign key %q: column %q in table %q must be a primary key or unique index column",
						fk.Name, targetCol, fk.TargetTable,
					)
				}
				continue
			}
			parentTable, ok := d.tables[fk.TargetTable]
			if !ok {
				return nil, fmt.Errorf("foreign key %q references unknown table %q", fk.Name, fk.TargetTable)
			}
			if !d.tableHasIndexOnColumn(parentTable, targetCol) {
				return nil, fmt.Errorf(
					"foreign key %q: column %q in table %q must be a primary key or unique index column",
					fk.Name, targetCol, fk.TargetTable,
				)
			}
		} else {
			// Multi-column FK: target columns must form a primary key or unique constraint.
			if fk.TargetTable == stmt.TableName {
				if !d.stmtHasCompositeUniqueConstraint(stmt, fk.TargetColumns) {
					return nil, fmt.Errorf(
						"foreign key %q: columns (%s) in table %q must form a primary key or unique index",
						fk.Name, strings.Join(fk.TargetColumns, ", "), fk.TargetTable,
					)
				}
				continue
			}
			parentTable, ok := d.tables[fk.TargetTable]
			if !ok {
				return nil, fmt.Errorf("foreign key %q references unknown table %q", fk.Name, fk.TargetTable)
			}
			if !d.tableHasCompositeUniqueConstraint(parentTable, fk.TargetColumns) {
				return nil, fmt.Errorf(
					"foreign key %q: columns (%s) in table %q must form a primary key or unique index",
					fk.Name, strings.Join(fk.TargetColumns, ", "), fk.TargetTable,
				)
			}
		}
	}

	opts := []TableOption{}

	if stmt.PrimaryKey.Name != "" {
		opts = append(opts, WithPrimaryKey(NewPrimaryKey(
			stmt.PrimaryKey.Name,
			stmt.PrimaryKey.Columns,
			stmt.PrimaryKey.Autoincrement,
		)))
	}

	for _, uniqueIndex := range stmt.UniqueIndexes {
		opts = append(opts, WithUniqueIndex(uniqueIndex))
	}

	opts = append(opts, WithParallelScan(d.parallelScan))

	if len(stmt.ForeignKeys) > 0 {
		opts = append(opts, WithForeignKeys(stmt.ForeignKeys))
	}

	createdTable := NewTable(
		d.logger,
		txPager,
		d.txManager,
		stmt.TableName,
		stmt.Columns,
		freePage.Index,
		d.lockedProvider,
		opts...,
	)

	// Save table record into minisql_schema system table
	if err := d.insertSchema(ctx, Schema{
		Type:     SchemaTable,
		Name:     stmt.TableName,
		RootPage: freePage.Index,
		DDL:      stmt.DDL(),
	}); err != nil {
		return nil, err
	}

	if createdTable.HasPrimaryKey() {
		createdIndex, err := d.createPrimaryKey(ctx, createdTable, createdTable.PrimaryKey.Columns)
		if err != nil {
			return nil, err
		}
		// Set primary key index on the table instance
		createdTable.PrimaryKey.Index = createdIndex
	}

	for _, uniqueIndex := range createdTable.UniqueIndexes {
		createdIndex, err := d.createUniqueIndex(ctx, createdTable, uniqueIndex)
		if err != nil {
			return nil, err
		}
		// Set unique index on the table instance
		uniqueIndex.Index = createdIndex
		createdTable.UniqueIndexes[uniqueIndex.Name] = uniqueIndex
	}

	tx.DDLChanges = tx.DDLChanges.CreatedTable(createdTable)

	// Register any FK constraints in the referencedBy map and wire up callbacks.
	for _, fk := range createdTable.ForeignKeys {
		d.referencedBy[fk.TargetTable] = append(d.referencedBy[fk.TargetTable], inboundFK{
			ChildTable: createdTable.Name,
			FK:         fk,
		})
		// Update parent table's referencedColumns and re-wire its FK callbacks.
		// For self-referential FKs the parent IS the created table — handled below.
		if parentTable, ok := d.tables[fk.TargetTable]; ok {
			if parentTable.referencedColumns == nil {
				parentTable.referencedColumns = make(map[string]bool)
			}
			for _, col := range fk.TargetColumns {
				parentTable.referencedColumns[col] = true
			}
			d.wireFKCallbacks(parentTable)
		}
	}
	d.wireFKCallbacks(createdTable)

	return createdTable, nil
}

// dropTable drops a table and all its data
func (d *Database) dropTable(ctx context.Context, name string) error {
	tx := MustTxFromContext(ctx)

	// Table could only exist within this transaction so create it from the system table
	_, exists, err := d.checkSchemaExists(ctx, SchemaTable, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", errTableDoesNotExist, name)
	}
	tableToDelete := d.tables[name]

	d.logger.Sugar().With("name", tableToDelete.Name).Debug("dropping table")

	// Refuse drop if another table's FK references this table.
	if d.foreignKeysEnabled {
		if inbounds := d.referencedBy[name]; len(inbounds) > 0 {
			return fmt.Errorf("%w: referenced by %s.%v",
				minisqlErrors.ErrDropTableReferencedByFK, inbounds[0].ChildTable, inbounds[0].FK.Columns)
		}
	}

	// Remove outgoing FKs from the referencedBy map.
	for _, fk := range tableToDelete.ForeignKeys {
		d.removeFromReferencedBy(fk.TargetTable, name)
	}

	if err := d.deleteSchema(ctx, SchemaTable, tableToDelete.Name); err != nil {
		return err
	}
	if tableToDelete.HasPrimaryKey() {
		if err := d.deleteSchema(ctx, SchemaPrimaryKey, tableToDelete.PrimaryKey.Name); err != nil {
			return err
		}
	}
	for _, uniqueIndex := range tableToDelete.UniqueIndexes {
		if err := d.deleteSchema(ctx, SchemaUniqueIndex, uniqueIndex.Name); err != nil {
			return err
		}
	}
	for _, secondaryIndex := range tableToDelete.SecondaryIndexes {
		if err := d.deleteSchema(ctx, SchemaSecondaryIndex, secondaryIndex.Name); err != nil {
			return err
		}
	}

	// Free all table pages

	// First free pages for the table itself
	txPager := NewTransactionalPager(
		d.factory.ForTable(tableToDelete.Columns),
		d.txManager,
		tableToDelete.Name,
		"",
	)

	_ = tableToDelete.BFS(ctx, func(page *Page) {
		if err := txPager.AddFreePage(ctx, page.Index); err != nil {
			d.logger.Sugar().With(
				"page", page.Index,
				"error", err,
			).Error("failed to free page")
			return
		}
		d.logger.Sugar().With("page", page.Index).Debug("freed page")
	})
	// And then free pages for the primary key index if any
	if tableToDelete.HasPrimaryKey() {

		txPager := NewTransactionalPager(
			d.factory.ForIndex(tableToDelete.PrimaryKey.Columns, true),
			d.txManager,
			tableToDelete.Name,
			tableToDelete.PrimaryKey.Name,
		)

		_ = tableToDelete.PrimaryKey.Index.BFS(ctx, func(page *Page) {
			if err := txPager.AddFreePage(ctx, page.Index); err != nil {
				d.logger.Sugar().With(
					"page", page.Index,
					"error", err,
				).Error("failed to free primary key index page")
				return
			}
			d.logger.Sugar().With("page", page.Index).Debug("freed primary key index page")
		})
	}
	// And then free pages for unique indexes index if any
	for _, uniqueIndex := range tableToDelete.UniqueIndexes {

		txPager := NewTransactionalPager(
			d.factory.ForIndex(uniqueIndex.Columns, true),
			d.txManager,
			tableToDelete.Name,
			uniqueIndex.Name,
		)

		_ = uniqueIndex.Index.BFS(ctx, func(page *Page) {
			if err := txPager.AddFreePage(ctx, page.Index); err != nil {
				d.logger.Sugar().With(
					"page", page.Index,
					"error", err,
				).Error("failed to free unique index page")
				return
			}
			d.logger.Sugar().With("page", page.Index).Debug("freed unique index page")
		})
	}
	// And then free pages for secondary indexes index if any
	for _, secondaryIndex := range tableToDelete.SecondaryIndexes {

		txPager := NewTransactionalPager(
			d.factory.ForIndex(secondaryIndex.Columns, false),
			d.txManager,
			tableToDelete.Name,
			secondaryIndex.Name,
		)

		_ = secondaryIndex.Index.BFS(ctx, func(page *Page) {
			if err := txPager.AddFreePage(ctx, page.Index); err != nil {
				d.logger.Sugar().With(
					"page", page.Index,
					"error", err,
				).Error("failed to free secondary index page")
				return
			}
			d.logger.Sugar().With("page", page.Index).Debug("freed secondary index page")
		})
	}

	tx.DDLChanges = tx.DDLChanges.DroppedTable(tableToDelete.Name)

	return nil
}

func (d *Database) createIndex(ctx context.Context, stmt Statement, table *Table) error {
	tx := MustTxFromContext(ctx)

	_, exists, err := d.checkSchemaExists(ctx, SchemaSecondaryIndex, stmt.IndexName)
	if err != nil {
		return err
	}
	if exists {
		return errIndexAlreadyExists
	}

	if stmt.IndexMethod != IndexMethodBTree {
		return fmt.Errorf("%w: %s indexes", errIndexMethodUnsupported, stmt.IndexMethod)
	}

	// Resolve index columns from the table schema, or build a synthetic column for expression indexes.
	var indexColumns []Column
	if stmt.IndexExpression != nil {
		if !isImmutableExpr(stmt.IndexExpression) {
			return fmt.Errorf("expression index %s: expression must be immutable (no non-deterministic functions)", stmt.IndexName)
		}
		kind := inferExprResultKind(stmt.IndexExpression, table.Columns)
		indexColumns = []Column{syntheticExprColumn(kind)}
	} else {
		indexColumns = make([]Column, 0, len(stmt.Columns))
		for _, stmtCol := range stmt.Columns {
			col, ok := table.ColumnByName(stmtCol.Name)
			if !ok {
				return fmt.Errorf("column %s does not exist on table %s", stmtCol.Name, stmt.TableName)
			}
			if col.Kind == JSON {
				return fmt.Errorf("%w: column %q on table %q", errIndexOnJSONColumn, col.Name, stmt.TableName)
			}
			indexColumns = append(indexColumns, col)
		}
	}

	for _, info := range table.SecondaryIndexes {
		if info.Name == stmt.IndexName {
			if stmt.IfNotExists {
				return nil
			}
			return fmt.Errorf("index %s already exists on table %s", stmt.IndexName, stmt.TableName)
		}
	}

	d.logger.Sugar().With("name", stmt.IndexName, "table", stmt.TableName).Debug("creating index")

	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:          stmt.IndexName,
			Columns:       indexColumns,
			WhereClause:   stmt.IndexWhereClause,
			WhereCond:     stmt.Conditions,
			Expression:    stmt.IndexExpression,
			ExpressionSQL: stmt.IndexExpressionSQL,
			Tokenizer:     stmt.IndexTokenizer,
			Method:        stmt.IndexMethod,
		},
	}
	createdIndex, err := d.createSecondaryIndex(ctx, stmt, table, secondaryIndex)
	if err != nil {
		return err
	}
	secondaryIndex.Index = createdIndex

	// Scan table and populate index
	if err := d.populateIndex(ctx, table, secondaryIndex); err != nil {
		return err
	}

	tx.DDLChanges = tx.DDLChanges.CreatedIndex(table.Name, secondaryIndex)

	return nil
}

func (d *Database) populateIndex(ctx context.Context, table *Table, secondaryIndex SecondaryIndex) error {
	result, err := table.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(table.Columns...),
	})
	if err != nil {
		return err
	}

	for result.Rows.Next(ctx) {
		row := result.Rows.Row()

		// Partial index: skip rows that don't satisfy the WHERE predicate.
		if len(secondaryIndex.WhereCond) > 0 {
			ok, err := row.CheckOneOrMore(secondaryIndex.WhereCond)
			if err != nil {
				return fmt.Errorf("partial index %s where check: %w", secondaryIndex.Name, err)
			}
			if !ok {
				continue
			}
		}

		switch {
		case secondaryIndex.Expression != nil:
			// Expression index: evaluate expression against the row.
			key, ok, err := evalExprIndexKey(secondaryIndex.Expression, secondaryIndex.Columns[0], row)
			if err != nil {
				return fmt.Errorf("expression index %s eval: %w", secondaryIndex.Name, err)
			}
			if !ok {
				continue // NULL result — don't index this row
			}
			if err := secondaryIndex.Index.Insert(ctx, key, row.Key); err != nil {
				return err
			}
		case len(secondaryIndex.Columns) > 1:
			// Composite secondary index: build a CompositeKey from all index columns
			allValid := true
			keyValues := make([]any, 0, len(secondaryIndex.Columns))
			for _, col := range secondaryIndex.Columns {
				keyValue, ok := row.GetValue(col.Name)
				if !ok {
					return fmt.Errorf("column %s does not exist on row in table %s", col.Name, table.Name)
				}
				if !keyValue.Valid {
					allValid = false
					break
				}
				castedKeyValue, err := castKeyValue(col, keyValue.Value)
				if err != nil {
					return err
				}
				keyValues = append(keyValues, castedKeyValue)
			}
			if !allValid {
				continue // skip rows where any index column is NULL
			}
			ck := NewCompositeKey(secondaryIndex.Columns, keyValues...)
			if err := secondaryIndex.Index.Insert(ctx, ck, row.Key); err != nil {
				return err
			}
		default:
			// Single-column secondary index
			keyValue, ok := row.GetValue(secondaryIndex.Columns[0].Name)
			if !ok {
				return fmt.Errorf("column %s does not exist on row in table %s", secondaryIndex.Columns[0].Name, table.Name)
			}
			if !keyValue.Valid {
				continue // skip NULLs
			}
			castedKeyValue, err := castKeyValue(secondaryIndex.Columns[0], keyValue.Value)
			if err != nil {
				return err
			}
			if err := secondaryIndex.Index.Insert(ctx, castedKeyValue, row.Key); err != nil {
				return err
			}
		}
	}

	if err := result.Rows.Err(); err != nil {
		return err
	}

	return nil
}

func (d *Database) dropIndex(ctx context.Context, stmt Statement) error {
	tx := MustTxFromContext(ctx)

	schema, exists, err := d.checkSchemaExists(ctx, SchemaSecondaryIndex, stmt.IndexName)
	if err != nil {
		return err
	}
	if !exists {
		return errIndexDoesNotExist
	}
	stmts, err := d.parser.Parse(ctx, schema.DDL)
	if err != nil {
		return err
	}
	if len(stmts) != 1 {
		return fmt.Errorf("expected one statement when loading index, got %d", len(stmts))
	}

	// Table could only exist within this transaction so create it from the system table
	tableSchema, exists, err := d.checkSchemaExists(ctx, SchemaTable, schema.TableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", errTableDoesNotExist, schema.TableName)
	}
	table, err := d.tableFromSQL(ctx, tableSchema)
	if err != nil {
		return err
	}
	indexColumns := make([]Column, 0, len(stmts[0].Columns))
	for _, col := range stmts[0].Columns {
		indexColumn, ok := table.ColumnByName(col.Name)
		if !ok {
			return fmt.Errorf("column %s does not exist on table %s for secondary index %s", col.Name, schema.TableName, schema.Name)
		}
		indexColumns = append(indexColumns, indexColumn)
	}

	txPager := NewTransactionalPager(
		d.factory.ForIndex(indexColumns, true),
		d.txManager,
		table.Name,
		schema.Name,
	)

	btreeIndex, err := table.newBTreeIndex(txPager, schema.RootPage, indexColumns, schema.Name, false)
	if err != nil {
		return err
	}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    schema.Name,
			Columns: indexColumns,
		},
		Index: btreeIndex,
	}

	if err := d.deleteSchema(ctx, schema.Type, schema.Name); err != nil {
		return err
	}

	// Free pages for the index
	_ = btreeIndex.BFS(ctx, func(page *Page) {
		if err := txPager.AddFreePage(ctx, page.Index); err != nil {
			d.logger.Sugar().With(
				"page", page.Index,
				"error", err,
			).Error("failed to free secondary index page")
			return
		}
		d.logger.Sugar().With("page", page.Index).Debug("freed secondary index page")
	})

	tx.DDLChanges = tx.DDLChanges.DroppedIndex(table.Name, secondaryIndex)

	return nil
}

func (d *Database) createPrimaryKey(ctx context.Context, table *Table, columns []Column) (BTreeIndex, error) {
	d.logger.Sugar().With("columns", columns).Debug("creating primary key")

	txPager := NewTransactionalPager(
		d.factory.ForIndex(columns, true),
		d.txManager,
		table.Name,
		table.PrimaryKey.Name,
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := table.createBTreeIndex(txPager, freePage, columns, table.PrimaryKey.Name, true)
	if err != nil {
		return nil, err
	}

	if err := d.insertSchema(ctx, Schema{
		Type:      SchemaPrimaryKey,
		Name:      table.PrimaryKey.Name,
		TableName: table.Name,
		RootPage:  freePage.Index,
	}); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) createUniqueIndex(ctx context.Context, table *Table, uniqueIndex UniqueIndex) (BTreeIndex, error) {
	d.logger.Sugar().With("column", uniqueIndex.Columns[0].Name).Debug("creating unique index")

	txPager := NewTransactionalPager(
		d.factory.ForIndex(uniqueIndex.Columns, true),
		d.txManager,
		table.Name,
		uniqueIndex.Name,
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := table.createBTreeIndex(txPager, freePage, uniqueIndex.Columns, uniqueIndex.Name, true)
	if err != nil {
		return nil, err
	}

	if err := d.insertSchema(ctx, Schema{
		Type:      SchemaUniqueIndex,
		Name:      uniqueIndex.Name,
		TableName: table.Name,
		RootPage:  freePage.Index,
	}); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) createSecondaryIndex(ctx context.Context, stmt Statement, table *Table, secondaryIndex SecondaryIndex) (BTreeIndex, error) {
	d.logger.Sugar().With("column", secondaryIndex.Columns[0].Name).Debug("creating secondary index")

	txPager := NewTransactionalPager(
		d.factory.ForIndex(secondaryIndex.Columns, false),
		d.txManager,
		table.Name,
		secondaryIndex.Name,
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := table.createBTreeIndex(txPager, freePage, secondaryIndex.Columns, secondaryIndex.Name, false)
	if err != nil {
		return nil, err
	}

	if err := d.insertSchema(ctx, Schema{
		Type:      SchemaSecondaryIndex,
		Name:      secondaryIndex.Name,
		TableName: table.Name,
		RootPage:  freePage.Index,
		DDL:       stmt.DDL(),
	}); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) checkSchemaExists(ctx context.Context, schemaType SchemaType, name string) (Schema, bool, error) {
	schemaResults, err := d.tables[SchemaTableName].Select(ctx, Statement{
		Kind:   Select,
		Fields: mainTableFields,
		Conditions: OneOrMore{
			{
				FieldIsEqual(Field{Name: "type"}, OperandInteger, int64(schemaType)),
				FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte(name))),
			},
		},
	})
	if err != nil {
		return Schema{}, false, err
	}

	if !schemaResults.Rows.Next(ctx) {
		return Schema{}, false, nil
	}
	row := schemaResults.Rows.Row()
	if schemaResults.Rows.Next(ctx) {
		return Schema{}, false, fmt.Errorf("multiple schema entries found for name %s of type %d", name, schemaType)
	}
	if err := schemaResults.Rows.Err(); err != nil {
		return Schema{}, false, err
	}

	return scanSchema(row), true, nil
}

func (d *Database) insertSchema(ctx context.Context, schema Schema) error {
	mainTable := d.tables[SchemaTableName]
	_, err := mainTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields:    mainTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: int32(schema.Type), Valid: true},
				{Value: NewTextPointer([]byte(schema.Name)), Valid: true},
				{Value: NewTextPointer([]byte(schema.TableName)), Valid: schema.TableName != ""},
				{Value: int32(schema.RootPage), Valid: true},
				{Value: NewTextPointer([]byte(schema.DDL)), Valid: schema.DDL != ""},
			},
		},
	})
	return err
}

func (d *Database) deleteSchema(ctx context.Context, schemaType SchemaType, name string) error {
	mainTable := d.tables[SchemaTableName]
	result, err := mainTable.Delete(ctx, Statement{
		Kind: Delete,
		Conditions: OneOrMore{
			{
				FieldIsEqual(Field{Name: "type"}, OperandInteger, int64(schemaType)),
				FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte(name))),
			},
		},
	})
	if result.RowsAffected == 0 {
		return fmt.Errorf("failed to delete from main table: no such entry %s of type %d", name, schemaType)
	}
	return err
}

// rebuildFKState reconstructs referencedBy from all loaded user tables and wires
// up FK callbacks on each table.  Called once after all schemas are loaded at startup.
func (d *Database) rebuildFKState() {
	d.referencedBy = make(map[string][]inboundFK)
	for _, table := range d.tables {
		if isSystemTable(table.Name) {
			continue
		}
		for _, fk := range table.ForeignKeys {
			d.referencedBy[fk.TargetTable] = append(d.referencedBy[fk.TargetTable], inboundFK{
				ChildTable: table.Name,
				FK:         fk,
			})
		}
	}
	// Wire referencedColumns on each parent table so cursor.update can decide
	// whether a parent FK callback is necessary.
	refColsByTable := make(map[string]map[string]bool)
	for _, inbounds := range d.referencedBy {
		for _, inbound := range inbounds {
			if refColsByTable[inbound.FK.TargetTable] == nil {
				refColsByTable[inbound.FK.TargetTable] = make(map[string]bool)
			}
			for _, col := range inbound.FK.TargetColumns {
				refColsByTable[inbound.FK.TargetTable][col] = true
			}
		}
	}
	for tableName, cols := range refColsByTable {
		if t, ok := d.tables[tableName]; ok {
			t.referencedColumns = cols
		}
	}
	for _, table := range d.tables {
		if !isSystemTable(table.Name) {
			d.wireFKCallbacks(table)
		}
	}
}

// wireFKCallbacks attaches FK enforcement closures to a table.
// The closures capture d, so they always use the current FK state.
func (d *Database) wireFKCallbacks(table *Table) {
	if len(table.ForeignKeys) > 0 {
		t := table
		table.checkChildFK = func(ctx context.Context, row Row) error {
			return d.checkChildFK(ctx, t, row)
		}
	}
	if len(d.referencedBy[table.Name]) > 0 {
		t := table
		table.checkParentFK = func(ctx context.Context, row Row) error {
			return d.enforceParentFKOnDelete(ctx, t, row)
		}
		table.enforceParentFKOnUpdate = func(ctx context.Context, oldRow Row, newRow Row) error {
			return d.enforceParentFKOnUpdate(ctx, t, oldRow, newRow)
		}
	}
}

// stmtHasIndexOnColumn checks a CREATE TABLE statement (not yet persisted) for a
// primary key or unique constraint on colName.  Used for self-referential FK validation.
func (d *Database) stmtHasIndexOnColumn(stmt Statement, colName string) bool {
	if stmt.PrimaryKey.Name != "" {
		for _, col := range stmt.PrimaryKey.Columns {
			if col.Name == colName {
				return true
			}
		}
	}
	for _, ui := range stmt.UniqueIndexes {
		for _, col := range ui.Columns {
			if col.Name == colName {
				return true
			}
		}
	}
	return false
}

// tableHasIndexOnColumn returns true if the table has a primary key or unique
// index on the given (single) column.
func (d *Database) tableHasIndexOnColumn(table *Table, colName string) bool {
	if table.HasPrimaryKey() {
		for _, col := range table.PrimaryKey.Columns {
			if col.Name == colName {
				return true
			}
		}
	}
	for _, idx := range table.UniqueIndexes {
		for _, col := range idx.Columns {
			if col.Name == colName {
				return true
			}
		}
	}
	return false
}

// stmtHasCompositeUniqueConstraint returns true if the CREATE TABLE statement has a
// primary key or unique index whose column set exactly matches targetCols.
func (d *Database) stmtHasCompositeUniqueConstraint(stmt Statement, targetCols []string) bool {
	if columnsMatchSet(stmt.PrimaryKey.Columns, targetCols) {
		return true
	}
	for _, ui := range stmt.UniqueIndexes {
		if columnsMatchSet(ui.Columns, targetCols) {
			return true
		}
	}
	return false
}

// tableHasCompositeUniqueConstraint returns true if the table has a primary key or unique
// index whose column set exactly matches targetCols.
func (d *Database) tableHasCompositeUniqueConstraint(table *Table, targetCols []string) bool {
	if table.HasPrimaryKey() && columnsMatchSet(table.PrimaryKey.Columns, targetCols) {
		return true
	}
	for _, idx := range table.UniqueIndexes {
		if columnsMatchSet(idx.Columns, targetCols) {
			return true
		}
	}
	return false
}

// columnsMatchSet returns true if cols contains exactly the same names as targetNames
// (same count, same names, order-independent).
func columnsMatchSet(cols []Column, targetNames []string) bool {
	if len(cols) != len(targetNames) {
		return false
	}
	nameSet := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		nameSet[col.Name] = struct{}{}
	}
	for _, name := range targetNames {
		if _, ok := nameSet[name]; !ok {
			return false
		}
	}
	return true
}

// removeFromReferencedBy removes all inbound FK entries from childTable targeting parentTable.
func (d *Database) removeFromReferencedBy(parentTable, childTable string) {
	inbounds := d.referencedBy[parentTable]
	filtered := inbounds[:0]
	for _, ib := range inbounds {
		if ib.ChildTable != childTable {
			filtered = append(filtered, ib)
		}
	}
	if len(filtered) == 0 {
		delete(d.referencedBy, parentTable)
	} else {
		d.referencedBy[parentTable] = filtered
	}
}
