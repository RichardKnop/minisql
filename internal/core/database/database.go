package database

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("unrecognised statement type")
	errTableDoesNotExist         = fmt.Errorf("table does not exist")
	errTableAlreadyExists        = fmt.Errorf("table already exists")
)

type Parser interface {
	Parse(context.Context, string) (minisql.Statement, error)
}

type Pager interface {
	GetPage(context.Context, *minisql.Table, uint32) (*minisql.Page, error)
	TotalPages() uint32
	Flush(context.Context, uint32, int64) error
}

var (
	mainTableColumns = []minisql.Column{
		{
			Kind:     minisql.Int4,
			Size:     4,
			Name:     "type",
			Nullable: false,
		},
		{
			Kind:     minisql.Varchar,
			Size:     255,
			Name:     "name",
			Nullable: false,
		},
		{
			Kind:     minisql.Int4,
			Size:     4,
			Name:     "root_page",
			Nullable: true,
		},
		{
			Kind:     minisql.Varchar,
			Size:     2056,
			Name:     "sql",
			Nullable: true,
		},
	}
)

var mainTableSQL = fmt.Sprintf(`create table %s (
	type int4 not null,
	table_name varchar(255) not null,
	root_page int4,
	sql varchar(2056)
)`, minisql.SchemaTableName)

type SchemaType int

const (
	SchemaTable SchemaType = iota + 1
	SchemaIndex
)

type Database struct {
	Name   string
	parser Parser
	pager  Pager
	tables map[string]*minisql.Table
	logger *zap.Logger
}

// New creates a new database
func New(ctx context.Context, logger *zap.Logger, name string, aParser Parser, aPager Pager) (*Database, error) {
	aDatabase := &Database{
		Name:   name,
		parser: aParser,
		pager:  aPager,
		tables: make(map[string]*minisql.Table),
		logger: logger,
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
			"name", minisql.SchemaTableName,
			"root_page", rooPageIdx,
		).Debug("creating main schema table")

		// New database, need to create the main schema table
		mainTable := minisql.NewTable(
			logger,
			minisql.SchemaTableName,
			mainTableColumns,
			aPager,
			rooPageIdx,
		)
		aDatabase.tables[minisql.SchemaTableName] = mainTable

		// And save record of itself
		if err := mainTable.Insert(ctx, minisql.Statement{
			Kind:      minisql.Insert,
			TableName: mainTable.Name,
			Columns:   mainTable.Columns,
			Fields: []string{
				"type",
				"name",
				"root_page",
				"sql",
			},
			Inserts: [][]minisql.OptionalValue{
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

		if err := aPager.Flush(ctx, mainTable.RootPageIdx, minisql.PageSize); err != nil {
			return nil, err
		}

		return aDatabase, nil
	}

	// Otherwise, main table already exists,
	// we need to read all existing tables from the schema table
	mainTable := minisql.NewTable(
		logger,
		minisql.SchemaTableName,
		mainTableColumns,
		aPager,
		rooPageIdx,
	)
	aDatabase.tables[mainTable.Name] = mainTable
	aResult, err := mainTable.Select(ctx, minisql.Statement{
		Kind:      minisql.Select,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields: []string{
			"type",
			"name",
			"root_page",
			"sql",
		},
		Conditions: minisql.FieldIsNotIn("name", minisql.QuotedString, mainTable.Name), // skip self
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
		aDatabase.tables[stmt.TableName] = minisql.NewTable(
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
		if err := d.pager.Flush(ctx, pageIdx, minisql.PageSize); err != nil {
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

// PrepareStatement parser SQL into a Statement struct
func (d *Database) PrepareStatement(ctx context.Context, sql string) (minisql.Statement, error) {
	stmt, err := d.parser.Parse(ctx, sql)
	if err != nil {
		return minisql.Statement{}, err
	}
	return stmt, nil
}

// ExecuteStatement will eventually become virtual machine
func (d *Database) ExecuteStatement(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	switch stmt.Kind {
	case minisql.CreateTable:
		return d.executeCreateTable(ctx, stmt)
	case minisql.DropTable:
		return d.executeDropTable(ctx, stmt)
	case minisql.Insert:
		return d.executeInsert(ctx, stmt)
	case minisql.Select:
		return d.executeSelect(ctx, stmt)
	case minisql.Update:
		return d.executeUpdate(ctx, stmt)
	case minisql.Delete:
		return d.executeDelete(ctx, stmt)
	}
	return minisql.StatementResult{}, errUnrecognizedStatementType
}

// CreateTable creates a new table with a name and columns
func (d *Database) CreateTable(ctx context.Context, name string, columns []minisql.Column) (*minisql.Table, error) {
	if len(columns) > minisql.MaxColumns {
		return nil, fmt.Errorf("maximum number of columns is %d", minisql.MaxColumns)
	}

	// TODO - check row size, currently no row overflowing a page is supported
	// so we need to return an error for such table DDLs

	_, ok := d.tables[name]
	if ok {
		return nil, errTableAlreadyExists
	}

	// Save table record into minisql_schema system table
	mainTable := d.tables[minisql.SchemaTableName]
	if err := mainTable.Insert(ctx, minisql.Statement{
		Kind:      minisql.Insert,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Fields: []string{
			"type",
			"name",
			"root_page",
			"sql",
		},
		Inserts: [][]minisql.OptionalValue{
			{
				{Value: int32(SchemaTable), Valid: true},              // type (only 0 supported now)
				{Value: name, Valid: true},                            // name
				{Valid: false},                                        // update later, we don't know root page yet
				{Value: "create table todo (todo int4)", Valid: true}, // TODO - store actual SQL of the table
			},
		},
	}); err != nil {
		return nil, err
	}

	// Now let's create the actual table, inserting into the system table might have
	// caused a split and new page being created, so now we know what the root page
	// for the new table should be.
	d.tables[name] = minisql.NewTable(
		d.logger,
		name,
		columns,
		d.pager,
		d.pager.TotalPages(),
	)

	_, err := mainTable.Update(ctx, minisql.Statement{
		Kind:      minisql.Update,
		TableName: mainTable.Name,
		Columns:   mainTable.Columns,
		Updates: map[string]minisql.OptionalValue{
			"root_page": {Value: int32(d.tables[name].RootPageIdx), Valid: true},
		},
		Conditions: minisql.FieldIsIn("name", minisql.QuotedString, name),
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
	delete(d.tables, name)

	// TODO - delete pages

	// TODO - delete from main schema table

	return nil
}

func (d *Database) executeCreateTable(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	_, err := d.CreateTable(ctx, stmt.TableName, stmt.Columns)
	return minisql.StatementResult{}, err
}

func (d *Database) executeDropTable(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	err := d.DropTable(ctx, stmt.TableName)
	return minisql.StatementResult{}, err
}

func (d *Database) executeInsert(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return minisql.StatementResult{}, errTableDoesNotExist
	}

	if err := aTable.Insert(ctx, stmt); err != nil {
		return minisql.StatementResult{}, err
	}

	return minisql.StatementResult{RowsAffected: len(stmt.Inserts)}, nil
}

func (d *Database) executeSelect(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return minisql.StatementResult{}, errTableDoesNotExist
	}

	return aTable.Select(ctx, stmt)
}

func (d *Database) executeUpdate(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return minisql.StatementResult{}, errTableDoesNotExist
	}

	return aTable.Update(ctx, stmt)
}

func (d *Database) executeDelete(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return minisql.StatementResult{}, errTableDoesNotExist
	}

	return aTable.Delete(ctx, stmt)
}
