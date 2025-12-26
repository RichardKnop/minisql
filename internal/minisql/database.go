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
	Name      string
	parser    Parser
	factory   PagerFactory
	saver     PageSaver
	txManager *TransactionManager
	tables    map[string]*Table
	dbLock    *sync.RWMutex
	clock     clock
	logger    *zap.Logger
}

type clock func() Time

// NewDatabase creates a new database
func NewDatabase(ctx context.Context, logger *zap.Logger, name string, aParser Parser, factory PagerFactory, saver PageSaver) (*Database, error) {
	aDatabase := &Database{
		Name:      name,
		parser:    aParser,
		factory:   factory,
		saver:     saver,
		txManager: NewTransactionManager(logger),
		tables:    make(map[string]*Table),
		dbLock:    new(sync.RWMutex),
		logger:    logger,
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

	if err := aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aDatabase.init(ctx)
	}, TxCommitter{aDatabase.saver, aDatabase}); err != nil {
		return nil, err
	}

	return aDatabase, nil
}

func (d *Database) init(ctx context.Context) error {
	var (
		mainTablePager = d.factory.ForTable(mainTableColumns)
		totalPages     = int(d.saver.TotalPages())
		rooPageIdx     = PageIndex(0)
	)

	d.logger.Sugar().With(
		"name", d.Name,
		"total_pages", totalPages,
	).Debug("initializing database")

	if totalPages == 0 {
		if err := d.initEmptyDatabase(ctx, rooPageIdx, mainTablePager); err != nil {
			return err
		}
	}

	// Otherwise, main table already exists,
	// we need to read all existing tables from the schema table
	mainTable := NewTable(
		d.logger,
		NewTransactionalPager(mainTablePager, d.txManager),
		d.txManager,
		SchemaTableName,
		mainTableColumns,
		rooPageIdx,
	)
	d.tables[mainTable.Name] = mainTable

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
		return err
	}

	for schemaResults.Rows.Next(ctx) {
		aRow := schemaResults.Rows.Row()
		switch SchemaType(aRow.Values[0].Value.(int32)) {
		case SchemaTable:
			if err := d.initTable(ctx, aRow); err != nil {
				return err
			}
		case SchemaPrimaryKey:
			if err := d.initPrimaryKey(ctx, aRow); err != nil {
				return err
			}
		case SchemaUniqueIndex:
			if err := d.initUniqueIndex(ctx, aRow); err != nil {
				return err
			}
		case SchemaSecondaryIndex:
			if err := d.initSecondaryIndex(ctx, aRow); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized schema type %d", aRow.Values[0].Value.(int32))
		}
	}
	if err := schemaResults.Rows.Err(); err != nil {
		return err
	}

	return nil
}

func (d *Database) initEmptyDatabase(ctx context.Context, rooPageIdx PageIndex, mainTablePager Pager) error {
	d.logger.Sugar().With(
		"name", SchemaTableName,
		"root_page", rooPageIdx,
	).Debug("creating main schema table")

	// New database, need to create the main schema table
	mainTable := NewTable(
		d.logger,
		NewTransactionalPager(mainTablePager, d.txManager),
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

func (d *Database) initTable(ctx context.Context, aRow Row) error {
	var (
		rootPageIdx = PageIndex(aRow.Values[3].Value.(int32))
		sql         = aRow.Values[4].Value.(TextPointer).String()
	)

	// Parse and validate CREATE TABLE query is valid, this also parses any default values
	// and transforms them into TextPointer for text columns or TIme for timestamps.
	stmts, err := d.parser.Parse(ctx, sql)
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

	d.tables[stmt.TableName], err = d.tableFromSQL(ctx, sql, rootPageIdx)
	if err != nil {
		return err
	}

	d.logger.Sugar().With(
		"name", stmt.TableName,
		"root_page", rootPageIdx,
	).Debug("loaded table")

	return nil
}

func (d *Database) tableFromSQL(ctx context.Context, sql string, rootPageIdx PageIndex) (*Table, error) {
	stmts, err := d.parser.Parse(ctx, sql)
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

	return NewTable(
		d.logger,
		NewTransactionalPager(
			d.factory.ForTable(stmt.Columns),
			d.txManager,
		),
		d.txManager,
		stmt.TableName,
		stmt.Columns,
		rootPageIdx,
	), nil
}

func (d *Database) initPrimaryKey(ctx context.Context, aRow Row) error {
	// TODO - parse SQL once we store it for primary indexes? Right now it will be NULL
	var (
		name        = aRow.Values[1].Value.(TextPointer).String()
		tableName   = aRow.Values[2].Value.(TextPointer).String()
		rootPageIdx = PageIndex(aRow.Values[3].Value.(int32))
	)

	aTable, ok := d.tables[tableName]
	if !ok {
		return fmt.Errorf("table %s for primary key index %s does not exist", tableName, name)
	}
	indexPager := NewTransactionalPager(
		d.factory.ForIndex(aTable.PrimaryKey.Column.Kind, uint64(aTable.PrimaryKey.Column.Size), true),
		d.txManager,
	)
	btreeIndex, err := aTable.newBTreeIndex(indexPager, rootPageIdx, aTable.PrimaryKey.Column, aTable.PrimaryKey.Name, true)
	if err != nil {
		return err
	}

	// Set primary key BTree index on the table instance
	aTable.PrimaryKey.Index = btreeIndex

	d.logger.Sugar().With(
		"name", aTable.PrimaryKey.Name,
		"root_page", rootPageIdx,
	).Debug("loaded primary key index")

	return nil
}

func (d *Database) initUniqueIndex(ctx context.Context, aRow Row) error {
	// TODO - parse SQL once we store it for unique indexes? Right now it will be NULL
	var (
		name        = aRow.Values[1].Value.(TextPointer).String()
		tableName   = aRow.Values[2].Value.(TextPointer).String()
		rootPageIdx = PageIndex(aRow.Values[3].Value.(int32))
	)

	aTable, ok := d.tables[tableName]
	if !ok {
		return fmt.Errorf("table %s for unique index %s does not exist", tableName, name)
	}
	uniqueIndex, ok := aTable.UniqueIndexes[name]
	if !ok {
		return fmt.Errorf("unique index %s does not exist on table %s", name, tableName)
	}
	indexPager := NewTransactionalPager(
		d.factory.ForIndex(uniqueIndex.Column.Kind, uint64(uniqueIndex.Column.Size), true),
		d.txManager,
	)
	btreeIndex, err := aTable.newBTreeIndex(indexPager, rootPageIdx, uniqueIndex.Column, uniqueIndex.Name, true)
	if err != nil {
		return err
	}

	// Set unique BTree index on the table instance
	uniqueIndex.Index = btreeIndex
	aTable.UniqueIndexes[name] = uniqueIndex

	d.logger.Sugar().With(
		"name", uniqueIndex.Name,
		"root_page", rootPageIdx,
	).Debug("loaded unique index")

	return nil
}

func (d *Database) initSecondaryIndex(ctx context.Context, aRow Row) error {
	var (
		name        = aRow.Values[1].Value.(TextPointer).String()
		tableName   = aRow.Values[2].Value.(TextPointer).String()
		rootPageIdx = PageIndex(aRow.Values[3].Value.(int32))
		sql         = aRow.Values[4].Value.(TextPointer).String()
	)
	aTable, ok := d.tables[tableName]
	if !ok {
		return fmt.Errorf("table %s for secondary index %s does not exist", tableName, name)
	}

	// Parse and validate CREATE INDEX statement to get indexed column
	stmts, err := d.parser.Parse(ctx, sql)
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
		return fmt.Errorf("column %s does not exist on table %s for secondary index %s", stmt.Columns[0].Name, tableName, name)
	}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:   name,
			Column: indexColumn,
		},
	}

	// Create and set BTree index instance
	indexPager := NewTransactionalPager(
		d.factory.ForIndex(secondaryIndex.Column.Kind, uint64(secondaryIndex.Column.Size), true),
		d.txManager,
	)
	btreeIndex, err := aTable.newBTreeIndex(indexPager, rootPageIdx, secondaryIndex.Column, secondaryIndex.Name, false)
	if err != nil {
		return err
	}
	secondaryIndex.Index = btreeIndex
	aTable.SecondaryIndexes[name] = secondaryIndex

	d.logger.Sugar().With(
		"name", secondaryIndex.Name,
		"root_page", rootPageIdx,
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
		d.tables[tableName].SecondaryIndexes[index.Name] = index
	}
	for tableName, index := range changes.DropIndexes {
		delete(d.tables[tableName].SecondaryIndexes, index.Name)
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
		if stmt.ReadOnly() {
			// Allow concurrent reads
			d.dbLock.RLock()
			aTable, ok := d.tables[stmt.TableName]
			if !ok {
				d.dbLock.RUnlock()
				return StatementResult{}, errTableDoesNotExist
			}
			d.dbLock.RUnlock()
			return executeTableStatement(ctx, aTable, stmt, d.clock())
		}
		// Use lock to limit to only one write operation at a time
		d.dbLock.Lock()
		defer d.dbLock.Unlock()
		aTable, ok := d.tables[stmt.TableName]
		if !ok {
			return StatementResult{}, errTableDoesNotExist
		}
		return executeTableStatement(ctx, aTable, stmt, d.clock())
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

func executeTableStatement(ctx context.Context, aTable *Table, stmt Statement, now Time) (StatementResult, error) {
	stmt.TableName = aTable.Name
	stmt.Columns = aTable.Columns

	var err error
	stmt, err = stmt.Prepare(now)
	if err != nil {
		return StatementResult{}, err
	}

	if err := stmt.Validate(aTable); err != nil {
		return StatementResult{}, err
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

	tablePager := NewTransactionalPager(
		d.factory.ForTable(stmt.Columns),
		d.txManager,
	)
	freePage, err := tablePager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}
	freePage.LeafNode = NewLeafNode()
	freePage.LeafNode.Header.IsRoot = true

	createdTable := NewTable(
		d.logger,
		tablePager,
		d.txManager,
		stmt.TableName,
		stmt.Columns,
		freePage.Index,
	)

	// Save table record into minisql_schema system table
	if err := d.insertIntoSystemTable(ctx, SchemaTable, stmt.TableName, "", freePage.Index, stmt.CreateTableDDL()); err != nil {
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

	if err := d.deleteFromMainTable(ctx, SchemaTable, tableToDelete.Name); err != nil {
		return err
	}
	if tableToDelete.HasPrimaryKey() {
		if err := d.deleteFromMainTable(ctx, SchemaPrimaryKey, tableToDelete.PrimaryKey.Name); err != nil {
			return err
		}
	}
	for _, uniqueIndex := range tableToDelete.UniqueIndexes {
		if err := d.deleteFromMainTable(ctx, SchemaUniqueIndex, uniqueIndex.Name); err != nil {
			return err
		}
	}
	for _, secondaryIndex := range tableToDelete.SecondaryIndexes {
		if err := d.deleteFromMainTable(ctx, SchemaSecondaryIndex, secondaryIndex.Name); err != nil {
			return err
		}
	}

	// Free all table pages
	aPager := NewTransactionalPager(
		d.factory.ForTable(tableToDelete.Columns),
		d.txManager,
	)
	// First free pages for the table itself
	tableToDelete.BFS(ctx, func(page *Page) {
		if err := aPager.AddFreePage(ctx, page.Index); err != nil {
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
		tableToDelete.PrimaryKey.Index.BFS(ctx, func(page *Page) {
			if err := aPager.AddFreePage(ctx, page.Index); err != nil {
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
		uniqueIndex.Index.BFS(ctx, func(page *Page) {
			if err := aPager.AddFreePage(ctx, page.Index); err != nil {
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
		secondaryIndex.Index.BFS(ctx, func(page *Page) {
			if err := aPager.AddFreePage(ctx, page.Index); err != nil {
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
	aTable, err := d.tableFromSQL(ctx, aTableSchema.DDL, aTableSchema.RootPage)
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
	aTable, err := d.tableFromSQL(ctx, aTableSchema.DDL, aTableSchema.RootPage)
	if err != nil {
		return err
	}
	indexColumn, ok := aTable.ColumnByName(stmts[0].Columns[0].Name)
	if !ok {
		return fmt.Errorf("column %s does not exist on table %s for secondary index %s", stmts[0].Columns[0].Name, aSchema.TableName, aSchema.Name)
	}

	indexPager := NewTransactionalPager(
		d.factory.ForIndex(indexColumn.Kind, uint64(indexColumn.Size), true),
		d.txManager,
	)
	btreeIndex, err := aTable.newBTreeIndex(indexPager, aSchema.RootPage, indexColumn, aSchema.Name, false)
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

	if err := d.deleteFromMainTable(ctx, aSchema.Type, aSchema.Name); err != nil {
		return err
	}

	// Free pages for the index
	btreeIndex.BFS(ctx, func(page *Page) {
		if err := indexPager.AddFreePage(ctx, page.Index); err != nil {
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

	indexPager := NewTransactionalPager(
		d.factory.ForIndex(aColumn.Kind, uint64(aColumn.Size), true),
		d.txManager,
	)
	freePage, err := indexPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := aTable.createBTreeIndex(indexPager, freePage, aTable.PrimaryKey.Column, aTable.PrimaryKey.Name, true)
	if err != nil {
		return nil, err
	}

	if err := d.insertIntoSystemTable(ctx, SchemaPrimaryKey, aTable.PrimaryKey.Name, aTable.Name, freePage.Index, ""); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) createUniqueIndex(ctx context.Context, aTable *Table, uniqueIndex UniqueIndex) (BTreeIndex, error) {
	d.logger.Sugar().With("column", uniqueIndex.Column.Name).Debug("creating unique index")

	indexPager := NewTransactionalPager(
		d.factory.ForIndex(uniqueIndex.Column.Kind, uint64(uniqueIndex.Column.Size), true),
		d.txManager,
	)
	freePage, err := indexPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := aTable.createBTreeIndex(indexPager, freePage, uniqueIndex.Column, uniqueIndex.Name, true)
	if err != nil {
		return nil, err
	}

	if err := d.insertIntoSystemTable(ctx, SchemaUniqueIndex, uniqueIndex.Name, aTable.Name, freePage.Index, ""); err != nil {
		return nil, err
	}

	return createdIndex, nil
}

func (d *Database) createSecondaryIndex(ctx context.Context, stmt Statement, aTable *Table, secondaryIndex SecondaryIndex) (BTreeIndex, error) {
	d.logger.Sugar().With("column", secondaryIndex.Column.Name).Debug("creating secondary index")

	aPager := NewTransactionalPager(
		d.factory.ForTable(aTable.Columns),
		d.txManager,
	)
	freePage, err := aPager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}

	createdIndex, err := aTable.createBTreeIndex(aPager, freePage, secondaryIndex.Column, secondaryIndex.Name, false)
	if err != nil {
		return nil, err
	}

	if err := d.insertIntoSystemTable(ctx, SchemaSecondaryIndex, secondaryIndex.Name, aTable.Name, freePage.Index, stmt.CreateIndexDDL()); err != nil {
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
	if err := schemaResults.Rows.Err(); err != nil {
		return Schema{}, false, err
	}

	return scanSchema(aRow), true, nil
}

func (d *Database) insertIntoSystemTable(ctx context.Context, aType SchemaType, name, tableName string, rootIdx PageIndex, ddl string) error {
	mainTable := d.tables[SchemaTableName]
	_, err := mainTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields:    mainTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: int32(aType), Valid: true},
				{Value: NewTextPointer([]byte(name)), Valid: true},
				{Value: NewTextPointer([]byte(tableName)), Valid: tableName != ""},
				{Value: int32(rootIdx), Valid: true},
				{Value: NewTextPointer([]byte(ddl)), Valid: ddl != ""},
			},
		},
	})
	return err
}

func (d *Database) deleteFromMainTable(ctx context.Context, aType SchemaType, name string) error {
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
