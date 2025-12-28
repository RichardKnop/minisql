package minisql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("unrecognised statement type")
	errTableDoesNotExist         = fmt.Errorf("table does not exist")
	errTableAlreadyExists        = fmt.Errorf("table already exists")
	errIndexDoesNotExist         = fmt.Errorf("index does not exist")
	errIndexAlreadyExists        = fmt.Errorf("index already exists")
)

type Parser interface {
	Parse(context.Context, string) ([]Statement, error)
}

type Database struct {
	dbFilePath string
	parser     Parser
	factory    PagerFactory
	saver      PageSaver
	txManager  *TransactionManager
	tables     map[string]*Table
	dbLock     *sync.RWMutex
	clock      clock
	logger     *zap.Logger
}

type clock func() Time

var ErrRecoveredFromJournal = fmt.Errorf("database recovered from journal on startup")

type DatabaseOption func(*Database)

func WithJournal(enabled bool) DatabaseOption {
	return func(d *Database) {
		d.txManager.journalEnabled = enabled
	}
}

// NewDatabase creates a new database
func NewDatabase(ctx context.Context, logger *zap.Logger, dbFilePath string, aParser Parser, aFactory PagerFactory, saver PageSaver, opts ...DatabaseOption) (*Database, error) {
	db := &Database{
		dbFilePath: dbFilePath,
		parser:     aParser,
		factory:    aFactory,
		saver:      saver,
		tables:     make(map[string]*Table),
		dbLock:     new(sync.RWMutex),
		logger:     logger,
		clock: func() Time {
			now := time.Now()
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

	db.txManager = NewTransactionManager(logger, dbFilePath, db.pagerFactory, saver, db)

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

func (d *Database) Close() error {
	return d.saver.Close()
}

func (d *Database) Reopen(ctx context.Context, factory PagerFactory, saver PageSaver) error {
	d.factory = factory
	d.saver = saver
	d.tables = make(map[string]*Table)

	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return d.init(ctx)
	}); err != nil {
		return err
	}
	return nil
}

func (d *Database) pagerFactory(ctx context.Context, tableName, indexName string) (Pager, error) {
	aTable, ok := d.tables[tableName]
	if !ok {
		if tx := TxFromContext(ctx); tx != nil {
			// We could be in a trasaction and table is being created but tx is not yet committed
			for _, tableBeingCreated := range TxFromContext(ctx).DDLChanges.CreateTables {
				if tableBeingCreated.Name == tableName {
					aTable = tableBeingCreated
				}
			}
		}
		if aTable == nil {
			return nil, errTableDoesNotExist
		}
	}
	if indexName == "" {
		return d.factory.ForTable(aTable.Columns), nil
	}

	aColumn, ok := aTable.IndexColumnByIndexName(indexName)
	if !ok {
		if tx := TxFromContext(ctx); tx != nil {
			// We could be in a trasaction and index is being created but tx is not yet committed
			for _, secondaryIndex := range TxFromContext(ctx).DDLChanges.CreateIndexes {
				if secondaryIndex.Name == indexName {
					aColumn = secondaryIndex.Column
				}
			}
		}
		if aColumn.Name == "" {
			return nil, errIndexDoesNotExist
		}
	}
	return d.factory.ForIndex(aColumn.Kind, aColumn.Unique || aColumn.PrimaryKey), nil
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

	if totalPages == 0 {
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
	)
	d.tables[mainTable.Name] = mainTable

	schemas, err := d.listSchemas(ctx)
	if err != nil {
		return err
	}

	for _, aSchema := range schemas {
		switch aSchema.Type {
		case SchemaTable:
			if err := d.initTable(ctx, aSchema); err != nil {
				return err
			}
		case SchemaPrimaryKey:
			if err := d.initPrimaryKey(ctx, aSchema); err != nil {
				return err
			}
		case SchemaUniqueIndex:
			if err := d.initUniqueIndex(ctx, aSchema); err != nil {
				return err
			}
		case SchemaSecondaryIndex:
			if err := d.initSecondaryIndex(ctx, aSchema); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized schema type %d", aSchema.Type)
		}
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
				FieldIsNotEqual("name", OperandQuotedString, NewTextPointer([]byte(mainTable.Name))), // skip main table itself
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
	).Debug("creating main schema table")

	txPager := NewTransactionalPager(mainTablePager, d.txManager, SchemaTableName, "")

	// New database, need to create the main schema table
	mainTable := NewTable(
		d.logger,
		txPager,
		d.txManager,
		SchemaTableName,
		mainTableColumns,
		rooPageIdx,
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
				{Value: int32(SchemaTable), Valid: true},                     // type (only 0 supported now)
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

func (d *Database) initTable(ctx context.Context, aSchema Schema) error {
	// Parse and validate CREATE TABLE query is valid, this also parses any default values
	// and transforms them into TextPointer for text columns or TIme for timestamps.
	stmts, err := d.parser.Parse(ctx, aSchema.DDL)
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

	d.tables[stmt.TableName], err = d.tableFromSQL(ctx, aSchema)
	if err != nil {
		return err
	}

	d.logger.Sugar().With(
		"name", stmt.TableName,
		"root_page", aSchema.RootPage,
	).Debug("loaded table")

	return nil
}

func (d *Database) tableFromSQL(ctx context.Context, aSchema Schema) (*Table, error) {
	stmts, err := d.parser.Parse(ctx, aSchema.DDL)
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

	return NewTable(
		d.logger,
		tp,
		d.txManager,
		stmt.TableName,
		stmt.Columns,
		aSchema.RootPage,
	), nil
}

func (d *Database) initPrimaryKey(ctx context.Context, aSchema Schema) error {
	// TODO - parse SQL once we store it for primary indexes? Right now it will be NULL

	aTable, ok := d.tables[aSchema.TableName]
	if !ok {
		return fmt.Errorf("table %s for primary key index %s does not exist", aSchema.TableName, aSchema.Name)
	}
	tp := NewTransactionalPager(
		d.factory.ForIndex(aTable.PrimaryKey.Column.Kind, true),
		d.txManager,
		aTable.Name,
		aSchema.Name,
	)
	btreeIndex, err := aTable.newBTreeIndex(tp, aSchema.RootPage, aTable.PrimaryKey.Column, aTable.PrimaryKey.Name, true)
	if err != nil {
		return err
	}

	// Set primary key BTree index on the table instance
	aTable.PrimaryKey.Index = btreeIndex

	d.logger.Sugar().With(
		"name", aTable.PrimaryKey.Name,
		"root_page", aSchema.RootPage,
	).Debug("loaded primary key index")

	return nil
}

func (d *Database) initUniqueIndex(ctx context.Context, aSchema Schema) error {
	// TODO - parse SQL once we store it for unique indexes? Right now it will be NULL

	aTable, ok := d.tables[aSchema.TableName]
	if !ok {
		return fmt.Errorf("table %s for unique index %s does not exist", aSchema.TableName, aSchema.Name)
	}
	uniqueIndex, ok := aTable.UniqueIndexes[aSchema.Name]
	if !ok {
		return fmt.Errorf("unique index %s does not exist on table %s", aSchema.Name, aSchema.TableName)
	}
	tp := NewTransactionalPager(
		d.factory.ForIndex(uniqueIndex.Column.Kind, true),
		d.txManager,
		aTable.Name,
		aSchema.Name,
	)
	btreeIndex, err := aTable.newBTreeIndex(tp, aSchema.RootPage, uniqueIndex.Column, uniqueIndex.Name, true)
	if err != nil {
		return err
	}

	// Set unique BTree index on the table instance
	uniqueIndex.Index = btreeIndex
	aTable.UniqueIndexes[aSchema.Name] = uniqueIndex

	d.logger.Sugar().With(
		"name", uniqueIndex.Name,
		"root_page", aSchema.RootPage,
	).Debug("loaded unique index")

	return nil
}

func (d *Database) initSecondaryIndex(ctx context.Context, aSchema Schema) error {

	aTable, ok := d.tables[aSchema.TableName]
	if !ok {
		return fmt.Errorf("table %s for secondary index %s does not exist", aSchema.TableName, aSchema.Name)
	}

	// Parse and validate CREATE INDEX statement to get indexed column
	stmts, err := d.parser.Parse(ctx, aSchema.DDL)
	if err != nil {
		return err
	}
	if len(stmts) != 1 {
		return fmt.Errorf("expected one statement when loading index, got %d", len(stmts))
	}
	stmt := stmts[0]
	if err := stmt.Validate(nil); err != nil {
		return err
	}

	indexColumn, ok := aTable.ColumnByName(stmt.Columns[0].Name)
	if !ok {
		return fmt.Errorf("column %s does not exist on table %s for secondary index %s", stmt.Columns[0].Name, aSchema.TableName, aSchema.Name)
	}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:   aSchema.Name,
			Column: indexColumn,
		},
	}

	// Create and set BTree index instance
	tp := NewTransactionalPager(
		d.factory.ForIndex(secondaryIndex.Column.Kind, false),
		d.txManager,
		aTable.Name,
		aSchema.Name,
	)
	btreeIndex, err := aTable.newBTreeIndex(tp, aSchema.RootPage, secondaryIndex.Column, secondaryIndex.Name, false)
	if err != nil {
		return err
	}

	aTable.SetSecondaryIndex(aSchema.Name, indexColumn, btreeIndex)

	d.logger.Sugar().With(
		"name", aSchema.Name,
		"root_page", aSchema.RootPage,
	).Debug("loaded secondary index")

	return nil
}

func (d *Database) SaveDDLChanges(ctx context.Context, changes DDLChanges) {
	if !changes.HasChanges() {
		return
	}

	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	for _, aTable := range changes.CreateTables {
		d.tables[aTable.Name] = aTable
	}
	for _, tableName := range changes.DropTables {
		delete(d.tables, tableName)
	}
	for tableName, index := range changes.CreateIndexes {
		d.tables[tableName].SetSecondaryIndex(index.Name, index.Column, index.Index)
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
		return StatementResult{}, fmt.Errorf("statement must be executed from within a transaction")
	}

	if !stmt.ReadOnly() && isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("cannot write to system table %s", SchemaTableName)
	}

	switch stmt.Kind {
	case CreateTable, DropTable, CreateIndex, DropIndex:
		return d.executeDDLStatement(ctx, stmt)
	case Insert, Select, Update, Delete:
		d.dbLock.RLock()
		aTable, ok := d.tables[stmt.TableName]
		if !ok {
			d.dbLock.RUnlock()
			return StatementResult{}, errTableDoesNotExist
		}
		d.dbLock.RUnlock()

		return d.executeTableStatement(ctx, aTable, stmt)
	}
	return StatementResult{}, errUnrecognizedStatementType
}

func (d *Database) executeDDLStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
	var err error
	stmt, err = stmt.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	if err := stmt.Validate(nil); err != nil {
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
		return StatementResult{}, d.createIndex(ctx, stmt)
	case DropIndex:
		return StatementResult{}, d.dropIndex(ctx, stmt)
	}

	return StatementResult{}, fmt.Errorf("unrecognized DDL statement type: %v", stmt.Kind)
}

func (d *Database) executeTableStatement(ctx context.Context, aTable *Table, stmt Statement) (StatementResult, error) {
	stmt.TableName = aTable.Name
	stmt.Columns = aTable.Columns

	var err error
	stmt, err = stmt.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	if err := stmt.Validate(aTable); err != nil {
		return StatementResult{}, err
	}

	if !stmt.ReadOnly() {
		// Use lock to limit to only one write operation at a time
		d.dbLock.Lock()
		defer d.dbLock.Unlock()
	}

	switch stmt.Kind {
	case Insert:
		return aTable.Insert(ctx, stmt)
	case Select:
		return aTable.Select(ctx, stmt)
	case Update:
		return aTable.Update(ctx, stmt)
	case Delete:
		return aTable.Delete(ctx, stmt)
	}

	return StatementResult{}, fmt.Errorf("unrecognized table statement type: %v", stmt.Kind)
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

	createdTable := NewTable(
		d.logger,
		txPager,
		d.txManager,
		stmt.TableName,
		stmt.Columns,
		freePage.Index,
	)

	// Save table record into minisql_schema system table
	if err := d.insertSchema(ctx, Schema{
		Type:     SchemaTable,
		Name:     stmt.TableName,
		RootPage: freePage.Index,
		DDL:      stmt.CreateTableDDL(),
	}); err != nil {
		return nil, err
	}

	if createdTable.HasPrimaryKey() {
		createdIndex, err := d.createPrimaryKey(ctx, createdTable, createdTable.PrimaryKey.Column)
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
		return errTableDoesNotExist
	}
	tableToDelete := d.tables[name]

	d.logger.Sugar().With("name", tableToDelete.Name).Debug("dropping table")

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

	tableToDelete.BFS(ctx, func(page *Page) {
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
			d.factory.ForIndex(tableToDelete.PrimaryKey.Column.Kind, true),
			d.txManager,
			tableToDelete.Name,
			tableToDelete.PrimaryKey.Name,
		)

		tableToDelete.PrimaryKey.Index.BFS(ctx, func(page *Page) {
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
			d.factory.ForIndex(uniqueIndex.Column.Kind, true),
			d.txManager,
			tableToDelete.Name,
			uniqueIndex.Name,
		)

		uniqueIndex.Index.BFS(ctx, func(page *Page) {
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
			d.factory.ForIndex(secondaryIndex.Column.Kind, false),
			d.txManager,
			tableToDelete.Name,
			secondaryIndex.Name,
		)

		secondaryIndex.Index.BFS(ctx, func(page *Page) {
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

func (d *Database) createIndex(ctx context.Context, stmt Statement) error {
	tx := MustTxFromContext(ctx)

	_, exists, err := d.checkSchemaExists(ctx, SchemaSecondaryIndex, stmt.IndexName)
	if err != nil {
		return err
	}
	if exists {
		return errIndexAlreadyExists
	}

	// Table could only exist within this transaction so create it from the system table
	aTableSchema, exists, err := d.checkSchemaExists(ctx, SchemaTable, stmt.TableName)
	if err != nil {
		return err
	}
	if !exists {
		return errTableDoesNotExist
	}
	aTable, err := d.tableFromSQL(ctx, aTableSchema)
	if err != nil {
		return err
	}

	aColumn, ok := aTable.ColumnByName(stmt.Columns[0].Name)
	if !ok {
		return fmt.Errorf("column %s does not exist on table %s", stmt.Columns[0].Name, stmt.TableName)
	}

	for _, info := range aTable.SecondaryIndexes {
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
			Name:   stmt.IndexName,
			Column: aColumn,
		},
	}
	createdIndex, err := d.createSecondaryIndex(ctx, stmt, aTable, secondaryIndex)
	if err != nil {
		return err
	}
	secondaryIndex.Index = createdIndex

	// Scan table and populate index
	if err := d.populateIndex(ctx, aTable, secondaryIndex); err != nil {
		return err
	}

	tx.DDLChanges = tx.DDLChanges.CreatedIndex(aTable.Name, secondaryIndex)

	return nil
}

func (d *Database) populateIndex(ctx context.Context, aTable *Table, secondaryIndex SecondaryIndex) error {

	aResult, err := aTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(aTable.Columns...),
	})
	if err != nil {
		return err
	}

	for aResult.Rows.Next(ctx) {
		aRow := aResult.Rows.Row()
		keyValue, ok := aRow.GetValue(secondaryIndex.Column.Name)
		if !ok {
			return fmt.Errorf("column %s does not exist on row in table %s", secondaryIndex.Column.Name, aTable.Name)
		}
		if !keyValue.Valid {
			continue // skip NULLs
		}
		castedKeyValue, err := castKeyValue(secondaryIndex.Column, keyValue.Value)
		if err != nil {
			return err
		}
		if err := secondaryIndex.Index.Insert(ctx, castedKeyValue, aRow.Key); err != nil {
			return err
		}
	}

	if err := aResult.Rows.Err(); err != nil {
		return err
	}

	return nil
}

func (d *Database) dropIndex(ctx context.Context, stmt Statement) error {
	tx := MustTxFromContext(ctx)

	aSchema, exists, err := d.checkSchemaExists(ctx, SchemaSecondaryIndex, stmt.IndexName)
	if err != nil {
		return err
	}
	if !exists {
		return errIndexDoesNotExist
	}
	stmts, err := d.parser.Parse(ctx, aSchema.DDL)
	if err != nil {
		return err
	}
	if len(stmts) != 1 {
		return fmt.Errorf("expected one statement when loading index, got %d", len(stmts))
	}

	// Table could only exist within this transaction so create it from the system table
	aTableSchema, exists, err := d.checkSchemaExists(ctx, SchemaTable, aSchema.TableName)
	if err != nil {
		return err
	}
	if !exists {
		return errTableDoesNotExist
	}
	aTable, err := d.tableFromSQL(ctx, aTableSchema)
	if err != nil {
		return err
	}
	indexColumn, ok := aTable.ColumnByName(stmts[0].Columns[0].Name)
	if !ok {
		return fmt.Errorf("column %s does not exist on table %s for secondary index %s", stmts[0].Columns[0].Name, aSchema.TableName, aSchema.Name)
	}

	txPager := NewTransactionalPager(
		d.factory.ForIndex(indexColumn.Kind, true),
		d.txManager,
		aTable.Name,
		aSchema.Name,
	)

	btreeIndex, err := aTable.newBTreeIndex(txPager, aSchema.RootPage, indexColumn, aSchema.Name, false)
	if err != nil {
		return err
	}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:   aSchema.Name,
			Column: indexColumn,
		},
		Index: btreeIndex,
	}

	if err := d.deleteSchema(ctx, aSchema.Type, aSchema.Name); err != nil {
		return err
	}

	// Free pages for the index
	btreeIndex.BFS(ctx, func(page *Page) {
		if err := txPager.AddFreePage(ctx, page.Index); err != nil {
			d.logger.Sugar().With(
				"page", page.Index,
				"error", err,
			).Error("failed to free secondary index page")
			return
		}
		d.logger.Sugar().With("page", page.Index).Debug("freed secondary index page")
	})

	tx.DDLChanges = tx.DDLChanges.DroppedIndex(aTable.Name, secondaryIndex)

	return nil
}

func (d *Database) createPrimaryKey(ctx context.Context, aTable *Table, aColumn Column) (BTreeIndex, error) {
	d.logger.Sugar().With("column", aColumn.Name).Debug("creating primary key")

	txPager := NewTransactionalPager(
		d.factory.ForIndex(aColumn.Kind, true),
		d.txManager,
		aTable.Name,
		aTable.PrimaryKey.Name,
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := aTable.createBTreeIndex(txPager, freePage, aTable.PrimaryKey.Column, aTable.PrimaryKey.Name, true)
	if err != nil {
		return nil, err
	}

	if err := d.insertSchema(ctx, Schema{
		Type:      SchemaPrimaryKey,
		Name:      aTable.PrimaryKey.Name,
		TableName: aTable.Name,
		RootPage:  freePage.Index,
	}); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) createUniqueIndex(ctx context.Context, aTable *Table, uniqueIndex UniqueIndex) (BTreeIndex, error) {
	d.logger.Sugar().With("column", uniqueIndex.Column.Name).Debug("creating unique index")

	txPager := NewTransactionalPager(
		d.factory.ForIndex(uniqueIndex.Column.Kind, true),
		d.txManager,
		aTable.Name,
		uniqueIndex.Name,
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := aTable.createBTreeIndex(txPager, freePage, uniqueIndex.Column, uniqueIndex.Name, true)
	if err != nil {
		return nil, err
	}

	if err := d.insertSchema(ctx, Schema{
		Type:      SchemaUniqueIndex,
		Name:      uniqueIndex.Name,
		TableName: aTable.Name,
		RootPage:  freePage.Index,
	}); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) createSecondaryIndex(ctx context.Context, stmt Statement, aTable *Table, secondaryIndex SecondaryIndex) (BTreeIndex, error) {
	d.logger.Sugar().With("column", secondaryIndex.Column.Name).Debug("creating secondary index")

	txPager := NewTransactionalPager(
		d.factory.ForIndex(secondaryIndex.Column.Kind, false),
		d.txManager,
		aTable.Name,
		secondaryIndex.Name,
	)

	freePage, err := txPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := aTable.createBTreeIndex(txPager, freePage, secondaryIndex.Column, secondaryIndex.Name, false)
	if err != nil {
		return nil, err
	}

	if err := d.insertSchema(ctx, Schema{
		Type:      SchemaSecondaryIndex,
		Name:      secondaryIndex.Name,
		TableName: aTable.Name,
		RootPage:  freePage.Index,
		DDL:       stmt.CreateIndexDDL(),
	}); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) checkSchemaExists(ctx context.Context, aType SchemaType, name string) (Schema, bool, error) {
	schemaResults, err := d.tables[SchemaTableName].Select(ctx, Statement{
		Kind:   Select,
		Fields: mainTableFields,
		Conditions: OneOrMore{
			{
				FieldIsEqual("type", OperandInteger, int64(aType)),
				FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte(name))),
			},
		},
	})
	if err != nil {
		return Schema{}, false, err
	}

	if !schemaResults.Rows.Next(ctx) {
		return Schema{}, false, nil
	}
	aRow := schemaResults.Rows.Row()
	if schemaResults.Rows.Next(ctx) {
		return Schema{}, false, fmt.Errorf("multiple schema entries found for name %s of type %d", name, aType)
	}
	if err := schemaResults.Rows.Err(); err != nil {
		return Schema{}, false, err
	}

	return scanSchema(aRow), true, nil
}

func (d *Database) insertSchema(ctx context.Context, aSchema Schema) error {
	mainTable := d.tables[SchemaTableName]
	_, err := mainTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields:    mainTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: int32(aSchema.Type), Valid: true},
				{Value: NewTextPointer([]byte(aSchema.Name)), Valid: true},
				{Value: NewTextPointer([]byte(aSchema.TableName)), Valid: aSchema.TableName != ""},
				{Value: int32(aSchema.RootPage), Valid: true},
				{Value: NewTextPointer([]byte(aSchema.DDL)), Valid: aSchema.DDL != ""},
			},
		},
	})
	return err
}

func (d *Database) deleteSchema(ctx context.Context, aType SchemaType, name string) error {
	mainTable := d.tables[SchemaTableName]
	aResult, err := mainTable.Delete(ctx, Statement{
		Kind: Delete,
		Conditions: OneOrMore{
			{
				FieldIsEqual("type", OperandInteger, int64(aType)),
				FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte(name))),
			},
		},
	})
	if aResult.RowsAffected == 0 {
		return fmt.Errorf("failed to delete from main table: no such entry %s of type %d", name, aType)
	}
	return err
}
