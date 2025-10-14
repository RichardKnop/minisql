package minisql

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("unrecognised statement type")
	errTableDoesNotExist         = fmt.Errorf("table does not exist")
	errTableAlreadyExists        = fmt.Errorf("table already exists")
)

var (
	maximumSchemaSQL = PageSize - 6 - 8 - 8 - 8 - (4 + 255 + 4) - RootPageConfigSize
	mainTableColumns = []Column{
		{
			Kind:     Int4,
			Size:     4,
			Name:     "type",
			Nullable: false,
		},
		{
			Kind:     Varchar,
			Size:     255,
			Name:     "name",
			Nullable: false,
		},
		{
			Kind:     Int4,
			Size:     4,
			Name:     "root_page",
			Nullable: true,
		},
		{
			Kind:     Varchar,
			Size:     uint32(maximumSchemaSQL),
			Name:     "sql",
			Nullable: true,
		},
	}
)

var mainTableSQL = fmt.Sprintf(`create table "%s" (
	type int4 not null,
	table_name varchar(255) not null,
	root_page int4,
	sql varchar(2056)
);`, SchemaTableName)

type SchemaType int

const (
	SchemaTable SchemaType = iota + 1
	SchemaIndex
)

type Parser interface {
	Parse(context.Context, string) ([]Statement, error)
}

type Database struct {
	Name       string
	parser     Parser
	factory    PagerFactory
	flusher    PageFlusher
	tables     map[string]*Table
	dbLock     *sync.RWMutex
	tableLocks sync.Map
	logger     *zap.Logger
}

// NewDatabase creates a new database
func NewDatabase(ctx context.Context, logger *zap.Logger, name string, aParser Parser, factory PagerFactory, flusher PageFlusher) (*Database, error) {
	aDatabase := &Database{
		Name:       name,
		parser:     aParser,
		factory:    factory,
		flusher:    flusher,
		tables:     make(map[string]*Table),
		dbLock:     new(sync.RWMutex),
		tableLocks: sync.Map{},
		logger:     logger,
	}

	var (
		mainTablePager = factory.ForTable(Row{Columns: mainTableColumns}.Size())
		totalPages     = int(flusher.TotalPages())
		rooPageIdx     = uint32(0)
	)

	logger.Sugar().With(
		"name", name,
		"total_pages", totalPages,
	).Debug("initializing database")

	if totalPages == 0 {
		logger.Sugar().With(
			"name", SchemaTableName,
			"root_page", rooPageIdx,
		).Debug("creating main schema table")

		// New database, need to create the main schema table
		mainTable := NewTable(
			logger,
			SchemaTableName,
			mainTableColumns,
			mainTablePager,
			rooPageIdx,
		)
		aDatabase.tables[SchemaTableName] = mainTable

		// And save record of itself
		if err := mainTable.Insert(ctx, Statement{
			Kind:      Insert,
			TableName: mainTable.Name,
			Columns:   mainTable.Columns,
			Fields: []string{
				"type",
				"name",
				"root_page",
				"sql",
			},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(SchemaTable), Valid: true},           // type (only 0 supported now)
					{Value: mainTable.Name, Valid: true},               // name
					{Value: int32(mainTable.RootPageIdx), Valid: true}, // root page
					{Value: mainTableSQL, Valid: true},                 // sql
				},
			},
		}); err != nil {
			return nil, err
		}

		if err := mainTablePager.Flush(ctx, mainTable.RootPageIdx); err != nil {
			return nil, err
		}

		return aDatabase, nil
	}

	// Otherwise, main table already exists,
	// we need to read all existing tables from the schema table
	mainTable := NewTable(
		logger,
		SchemaTableName,
		mainTableColumns,
		mainTablePager,
		rooPageIdx,
	)
	aDatabase.tables[mainTable.Name] = mainTable

	aResult, err := mainTable.Select(ctx, Statement{
		Kind: Select,
		Fields: []string{
			"type",
			"name",
			"root_page",
			"sql",
		},
		Conditions: FieldIsNotIn("name", QuotedString, mainTable.Name), // skip self
	})
	if err != nil {
		return nil, err
	}

	aRow, err := aResult.Rows(ctx)
	for ; err == nil; aRow, err = aResult.Rows(ctx) {
		stmts, err := aParser.Parse(ctx, aRow.Values[3].Value.(string))
		if err != nil {
			return nil, err
		}
		if len(stmts) != 1 {
			return nil, fmt.Errorf("expected one statement when loading table, got %d", len(stmts))
		}
		stmt := stmts[0]
		aDatabase.tables[stmt.TableName] = NewTable(
			logger,
			stmt.TableName,
			stmt.Columns,
			factory.ForTable(Row{Columns: stmt.Columns}.Size()),
			uint32(aRow.Values[2].Value.(int32)),
		)

		logger.Sugar().With(
			"name", stmt.TableName,
			"root_page", uint32(aRow.Values[2].Value.(int32)),
		).Debug("loaded table")

		aDatabase.tableLocks.Store(stmt.TableName, new(sync.RWMutex))
	}

	return aDatabase, nil
}

func (d *Database) Close(ctx context.Context) error {
	for pageIdx := uint32(0); pageIdx < d.flusher.TotalPages(); pageIdx++ {
		d.logger.Sugar().With(
			"page", pageIdx,
		).Debug("flushing page to disk")
		if err := d.flusher.Flush(ctx, pageIdx); err != nil {
			return err
		}
	}

	return nil
}

// ListTableNames lists names of all tables in the database
func (d *Database) ListTableNames(ctx context.Context) []string {
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

// ExecuteStatement will eventually become virtual machine
func (d *Database) ExecuteStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
	switch stmt.Kind {
	case CreateTable:
		return d.executeCreateTable(ctx, stmt)
	case DropTable:
		return d.executeDropTable(ctx, stmt)
	case Insert:
		return d.executeInsert(ctx, stmt)
	case Select:
		return d.executeSelect(ctx, stmt)
	case Update:
		return d.executeUpdate(ctx, stmt)
	case Delete:
		return d.executeDelete(ctx, stmt)
	}
	return StatementResult{}, errUnrecognizedStatementType
}

// CreateTable creates a new table with a name and columns
func (d *Database) createTable(ctx context.Context, stmt Statement) (*Table, error) {
	if err := stmt.Validate((nil)); err != nil {
		return nil, err
	}

	if len(stmt.CreateTableDDL()) > maximumSchemaSQL {
		return nil, fmt.Errorf("table definition too long, maximum length is %d", maximumSchemaSQL)
	}

	var (
		name    = stmt.TableName
		columns = stmt.Columns
	)

	_, ok := d.tables[name]
	if ok {
		return nil, errTableAlreadyExists
	}

	d.logger.Sugar().With("name", name).Debug("creating table")

	// Save table record into minisql_schema system table
	mainTable := d.tables[SchemaTableName]
	if err := mainTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields: []string{
			"type",
			"name",
			"root_page",
			"sql",
		},
		Inserts: [][]OptionalValue{
			{
				{Value: int32(SchemaTable), Valid: true},    // type (only 0 supported now)
				{Value: name, Valid: true},                  // name
				{Valid: false},                              // update later, we don't know root page yet
				{Value: stmt.CreateTableDDL(), Valid: true}, // TODO - store actual SQL of the table
			},
		},
	}); err != nil {
		return nil, err
	}

	// Now let's create the actual table, inserting into the system table might have
	// caused a split and new page being created, so now we know what the root page
	// for the new table should be.
	rowSize := Row{Columns: columns}.Size()
	tablePager := d.factory.ForTable(rowSize)
	freePage, err := tablePager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}
	freePage.LeafNode = NewLeafNode(rowSize)
	createdTable := NewTable(
		d.logger,
		name,
		columns,
		tablePager,
		freePage.Index,
	)

	_, err = mainTable.Update(ctx, Statement{
		Kind:      Update,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Updates: map[string]OptionalValue{
			"root_page": {Value: int32(createdTable.RootPageIdx), Valid: true},
		},
		Conditions: FieldIsIn("name", QuotedString, name),
	})
	if err != nil {
		return nil, err
	}

	d.tableLocks.Store(name, new(sync.RWMutex))
	d.tables[name] = createdTable
	return d.tables[name], nil
}

// dropTable drops a table and all its data
func (d *Database) dropTable(ctx context.Context, name string) error {
	tableToDelete, ok := d.tables[name]
	if !ok {
		return errTableDoesNotExist
	}

	d.logger.Sugar().With("name", tableToDelete.Name).Debug("dropping table")

	mainTable := d.tables[SchemaTableName]
	_, err := mainTable.Delete(ctx, Statement{
		Kind:       Delete,
		Conditions: FieldIsIn("name", QuotedString, tableToDelete.Name),
	})
	if err != nil {
		return err
	}

	// Free all table pages
	tablePager := d.factory.ForTable(tableToDelete.RowSize)
	tableToDelete.BFS(func(page *Page) {
		if err := tablePager.AddFreePage(ctx, page.Index); err != nil {
			d.logger.Sugar().With(
				"page", page.Index,
				"error", err,
			).Error("failed to free page")
			return
		}
		d.logger.Sugar().With("page", page).Debug("freed page")
	})

	d.tableLocks.Delete(name)
	delete(d.tables, name)
	return nil
}

func (d *Database) executeCreateTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	// Only one CREATE/DROP TABLE operation can happen at a time
	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	_, err := d.createTable(ctx, stmt)
	return StatementResult{}, err
}

func (d *Database) executeDropTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("cannot drop system table %s", SchemaTableName)
	}

	// Only one CREATE/DROP TABLE operation can happen at a time
	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	// Only one go routine can read/write at a time by acquiring the lock.
	// This will guarantee that no other read/write operation is happening
	// on the table when we start the drop operation.
	tableLock, ok := d.tableLocks.Load(stmt.TableName)
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}
	tableLock.(*sync.RWMutex).Lock()
	defer tableLock.(*sync.RWMutex).Unlock()

	err := d.dropTable(ctx, stmt.TableName)
	return StatementResult{}, err
}

func (d *Database) executeInsert(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("inserts into system table %s are not allowed", SchemaTableName)
	}

	// Lock the table for reading to block any potential DROP TABLE operation
	// until this function returns.
	tableLock, ok := d.tableLocks.Load(stmt.TableName)
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}
	tableLock.(*sync.RWMutex).RLock()
	defer tableLock.(*sync.RWMutex).RUnlock()

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	if err := aTable.Insert(ctx, stmt); err != nil {
		return StatementResult{}, err
	}

	return StatementResult{RowsAffected: len(stmt.Inserts)}, nil
}

func (d *Database) executeSelect(ctx context.Context, stmt Statement) (StatementResult, error) {
	if !isSystemTable(stmt.TableName) {
		// Lock the table for reading to block any potential DROP TABLE operation
		// until this function returns.
		tableLock, ok := d.tableLocks.Load(stmt.TableName)
		if !ok {
			return StatementResult{}, errTableDoesNotExist
		}
		tableLock.(*sync.RWMutex).RLock()
		defer tableLock.(*sync.RWMutex).RUnlock()
	}

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Select(ctx, stmt)
}

func (d *Database) executeUpdate(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("updates to system table %s are not allowed", SchemaTableName)
	}

	// Lock the table for reading to block any potential DROP TABLE operation
	// until this function returns.
	tableLock, ok := d.tableLocks.Load(stmt.TableName)
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}
	tableLock.(*sync.RWMutex).RLock()
	defer tableLock.(*sync.RWMutex).RUnlock()

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Update(ctx, stmt)
}

func (d *Database) executeDelete(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("deletes from system table %s are not allowed", SchemaTableName)
	}

	// Lock the table for reading to block any potential DROP TABLE operation
	// until this function returns.
	tableLock, ok := d.tableLocks.Load(stmt.TableName)
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}
	tableLock.(*sync.RWMutex).RLock()
	defer tableLock.(*sync.RWMutex).RUnlock()

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Delete(ctx, stmt)
}

func isSystemTable(name string) bool {
	return name == SchemaTableName
}
