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
)`, SchemaTableName)

type SchemaType int

const (
	SchemaTable SchemaType = iota + 1
	SchemaIndex
)

type Parser interface {
	Parse(context.Context, string) (Statement, error)
}

type Database struct {
	Name      string
	parser    Parser
	pager     Pager
	tables    map[string]*Table
	writeLock *sync.RWMutex
	logger    *zap.Logger
}

// NewDatabase creates a new database
func NewDatabase(ctx context.Context, logger *zap.Logger, name string, aParser Parser, aPager Pager) (*Database, error) {
	aDatabase := &Database{
		Name:      name,
		parser:    aParser,
		pager:     aPager,
		tables:    make(map[string]*Table),
		writeLock: new(sync.RWMutex),
		logger:    logger,
	}

	var (
		totalPages = int(aPager.TotalPages())
		rooPageIdx = uint32(0)
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
			aPager,
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

		if err := aPager.Flush(ctx, mainTable.RootPageIdx); err != nil {
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
		aPager,
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
		stmt, err := aParser.Parse(ctx, aRow.Values[3].Value.(string))
		if err != nil {
			return nil, err
		}
		aDatabase.tables[stmt.TableName] = NewTable(
			logger,
			stmt.TableName,
			stmt.Columns,
			aPager,
			uint32(aRow.Values[2].Value.(int32)),
		)

		logger.Sugar().With(
			"name", stmt.TableName,
			"root_page", uint32(aRow.Values[2].Value.(int32)),
		).Debug("loaded table")
	}

	return aDatabase, nil
}

func (d *Database) Close(ctx context.Context) error {
	for pageIdx := uint32(0); pageIdx < d.pager.TotalPages(); pageIdx++ {
		d.logger.Sugar().With(
			"page", pageIdx,
		).Debug("flushing page to disk")
		if err := d.pager.Flush(ctx, pageIdx); err != nil {
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

// PrepareStatement parses SQL into a Statement struct
func (d *Database) PrepareStatement(ctx context.Context, sql string) (Statement, error) {
	stmt, err := d.parser.Parse(ctx, sql)
	if err != nil {
		return Statement{}, err
	}
	return stmt, nil
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
func (d *Database) CreateTable(ctx context.Context, stmt Statement) (*Table, error) {
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

	// Prevent concurrent create table operations to avoid conflicts
	d.writeLock.Lock()
	defer d.writeLock.Unlock()

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
	d.tables[name] = NewTable(
		d.logger,
		name,
		columns,
		d.pager,
		d.pager.TotalPages(),
	)

	_, err := mainTable.Update(ctx, Statement{
		Kind:      Update,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Updates: map[string]OptionalValue{
			"root_page": {Value: int32(d.tables[name].RootPageIdx), Valid: true},
		},
		Conditions: FieldIsIn("name", QuotedString, name),
	})
	if err != nil {
		return nil, err
	}

	return d.tables[name], nil
}

// CreateTable creates a new table with a name and columns
func (d *Database) DropTable(ctx context.Context, name string) error {
	_, ok := d.tables[name]
	if !ok {
		return errTableDoesNotExist
	}

	return fmt.Errorf("not implemented yet")

	//delete(d.tables, name)

	// TODO - delete pages

	// TODO - delete from main schema table
}

func (d *Database) executeCreateTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	_, err := d.CreateTable(ctx, stmt)
	return StatementResult{}, err
}

func (d *Database) executeDropTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	err := d.DropTable(ctx, stmt.TableName)
	return StatementResult{}, err
}

func (d *Database) executeInsert(ctx context.Context, stmt Statement) (StatementResult, error) {
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
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Select(ctx, stmt)
}

func (d *Database) executeUpdate(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Update(ctx, stmt)
}

func (d *Database) executeDelete(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Delete(ctx, stmt)
}
