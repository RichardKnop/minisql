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
	maximumSchemaSQL = UsablePageSize - (4 + 255 + 4) - RootPageConfigSize
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
	Name      string
	parser    Parser
	factory   PagerFactory
	saver     PageSaver
	flusher   PageFlusher
	txManager *TransactionManager
	tables    map[string]*Table
	// TODO - populate
	primaryKeys map[string]any
	dbLock      *sync.RWMutex
	logger      *zap.Logger
}

// NewDatabase creates a new database
func NewDatabase(ctx context.Context, logger *zap.Logger, name string, aParser Parser, factory PagerFactory, saver PageSaver, flusher PageFlusher) (*Database, error) {
	aDatabase := &Database{
		Name:      name,
		parser:    aParser,
		factory:   factory,
		saver:     saver,
		flusher:   flusher,
		txManager: NewTransactionManager(),
		tables:    make(map[string]*Table),
		dbLock:    new(sync.RWMutex),
		logger:    logger,
	}

	if err := aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aDatabase.init(ctx)
	}, saver); err != nil {
		return nil, err
	}

	return aDatabase, nil
}

func (d *Database) init(ctx context.Context) error {
	var (
		mainTablePager = d.factory.ForTable(Row{Columns: mainTableColumns}.Size())
		totalPages     = int(d.flusher.TotalPages())
		rooPageIdx     = uint32(0)
	)

	d.logger.Sugar().With(
		"name", d.Name,
		"total_pages", totalPages,
	).Debug("initializing database")

	if totalPages == 0 {
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
			return err
		}

		if err := d.flusher.Flush(ctx, mainTable.RootPageIdx); err != nil {
			return err
		}

		return nil
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

	aResult, err := mainTable.Select(ctx, Statement{
		Kind: Select,
		Fields: []string{
			"type",
			"name",
			"root_page",
			"sql",
		},
		Conditions: OneOrMore{
			{
				FieldIsNotIn("name", OperandQuotedString, mainTable.Name), // skip main table itself
			},
		},
	})
	if err != nil {
		return err
	}

	aRow, err := aResult.Rows(ctx)
	for ; err == nil; aRow, err = aResult.Rows(ctx) {
		aType := SchemaType(aRow.Values[0].Value.(int32))
		if aType != SchemaTable {
			// TODO - support indexes later
			continue
		}

		stmts, err := d.parser.Parse(ctx, aRow.Values[3].Value.(string))
		if err != nil {
			return err
		}
		if len(stmts) != 1 {
			return fmt.Errorf("expected one statement when loading table, got %d", len(stmts))
		}
		stmt := stmts[0]
		d.tables[stmt.TableName] = NewTable(
			d.logger,
			NewTransactionalPager(
				d.factory.ForTable(Row{Columns: stmt.Columns}.Size()),
				d.txManager,
			),
			d.txManager,
			stmt.TableName,
			stmt.Columns,
			uint32(aRow.Values[2].Value.(int32)),
		)

		d.logger.Sugar().With(
			"name", stmt.TableName,
			"root_page", uint32(aRow.Values[2].Value.(int32)),
		).Debug("loaded table")
	}

	return nil
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

// executeStatement will eventually become virtual machine
func (d *Database) executeStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
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
	tablePager := NewTransactionalPager(
		d.factory.ForTable(rowSize),
		d.txManager,
	)
	freePage, err := tablePager.GetFreePage(ctx)
	if err != nil {
		return nil, err
	}
	freePage.LeafNode = NewLeafNode(rowSize)
	freePage.LeafNode.Header.IsRoot = true
	createdTable := NewTable(
		d.logger,
		tablePager,
		d.txManager,
		name,
		columns,
		freePage.Index,
	)

	_, err = mainTable.Update(ctx, Statement{
		Kind:      Update,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Updates: map[string]OptionalValue{
			"root_page": {Value: int32(createdTable.RootPageIdx), Valid: true},
		},
		Conditions: OneOrMore{
			{
				FieldIsIn("type", OperandInteger, int64(SchemaTable)),
				FieldIsIn("name", OperandQuotedString, name),
			},
		},
	})
	if err != nil {
		return nil, err
	}

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
		Kind: Delete,
		Conditions: OneOrMore{
			{
				FieldIsIn("type", OperandInteger, int64(SchemaTable)),
				FieldIsIn("name", OperandQuotedString, tableToDelete.Name),
			},
		},
	})
	if err != nil {
		return err
	}

	// Free all table pages
	tablePager := NewTransactionalPager(
		d.factory.ForTable(tableToDelete.RowSize),
		d.txManager,
	)
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

	delete(d.tables, name)
	return nil
}

func (d *Database) executeCreateTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	// Only one CREATE/DROP TABLE operation can happen at a time
	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := d.createTable(ctx, stmt)
		return err
	}, d.saver); err != nil {
		return StatementResult{}, err
	}

	return StatementResult{}, nil
}

func (d *Database) executeDropTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("cannot drop system table %s", SchemaTableName)
	}

	// Only one CREATE/DROP TABLE operation can happen at a time
	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return d.dropTable(ctx, stmt.TableName)
	}, d.saver); err != nil {
		return StatementResult{}, err
	}

	return StatementResult{}, nil
}

func (d *Database) executeInsert(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("inserts into system table %s are not allowed", SchemaTableName)
	}

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, stmt)
	}, d.saver); err != nil {
		return StatementResult{}, err
	}

	return StatementResult{RowsAffected: len(stmt.Inserts)}, nil
}

func (d *Database) executeSelect(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	var aResult StatementResult
	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		aResult, err = aTable.Select(ctx, stmt)
		return err
	}, d.saver); err != nil {
		return StatementResult{}, err
	}

	return aResult, nil
}

func (d *Database) executeUpdate(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("updates to system table %s are not allowed", SchemaTableName)
	}

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	var aResult StatementResult
	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		aResult, err = aTable.Update(ctx, stmt)
		return err
	}, d.saver); err != nil {
		return StatementResult{}, err
	}

	return aResult, nil
}

func (d *Database) executeDelete(ctx context.Context, stmt Statement) (StatementResult, error) {
	if isSystemTable(stmt.TableName) {
		return StatementResult{}, fmt.Errorf("deletes from system table %s are not allowed", SchemaTableName)
	}

	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	var aResult StatementResult
	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		aResult, err = aTable.Delete(ctx, stmt)
		return err
	}, d.saver); err != nil {
		return StatementResult{}, err
	}

	return aResult, nil
}

func (d *Database) BeginTransaction(ctx context.Context) (context.Context, *Transaction) {
	tx := d.txManager.BeginTransaction(ctx)
	return WithTransaction(ctx, tx), tx
}

func (d *Database) ExecuteInTransaction(ctx context.Context, statements ...Statement) ([]StatementResult, error) {
	var results []StatementResult
	if err := d.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, stmt := range statements {
			aResult, err := d.executeStatement(ctx, stmt)
			if err != nil {
				return fmt.Errorf("failed to execute statement %v: %w", stmt, err)
			}
			results = append(results, aResult)
		}
		return nil
	}, d.saver); err != nil {
		return nil, err
	}

	return results, nil
}

func isSystemTable(name string) bool {
	return name == SchemaTableName
}
