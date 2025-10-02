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
	// ListPages() []*Page
	TotalPages() uint32
	Flush(context.Context, uint32, int64) error
}

var (
	schemaTableColumns = []minisql.Column{
		{
			Kind: minisql.Int4,
			Size: 4,
			Name: "type",
		},
		{
			Kind: minisql.Varchar,
			Size: 100,
			Name: "name",
		},
		{
			Kind: minisql.Varchar,
			Size: 100,
			Name: "table_name",
		},
		{
			Kind: minisql.Int8,
			Size: 8,
			Name: "root_page",
		},
		{
			Kind: minisql.Varchar,
			Size: 1000,
			Name: "sql",
		},
	}

	schemaTableSQL = `CREATE TABLE minisql_main (
		type INT4,
		name VARCHAR(100),
		table_name VARCHAR(100),
		root_page INT4,
		SQL VARCHAR(1000)
	)`
)

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

	logger.Sugar().With(
		"name", name,
		"total_pages",
		int(aPager.TotalPages()),
	).Debug("initializing database")

	// rooPageIdx := uint32(0)

	// // Get the root page
	// aRootPage, err := aDatabase.pager.GetPage(ctx, SchemaTableName, rooPageIdx)
	// if err != nil {
	// 	return nil, err
	// }

	// if aRootPage.nextOffset == 0 {
	// 	aTable, err := aDatabase.CreateTable(ctx, SchemaTableName, schemaTableColumns)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	// If this is a new database, insert its first row minisql_main's root page will be 0.
	// 	_, err = aTable.Insert(ctx, Statement{
	// 		Kind:      Insert,
	// 		TableName: aTable.Name,
	// 		Fields: []string{
	// 			"type",
	// 			"name",
	// 			"table_name",
	// 			"root_page",
	// 			"sql",
	// 		},
	// 		Inserts: [][]any{{
	// 			int32(SchemaTable), // type (only 0 supported now)
	// 			SchemaTableName,    // name
	// 			SchemaTableName,    // table name
	// 			rooPageIdx,         // root page
	// 			schemaTableSQL,     // sql
	// 		}},
	// 	})
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return aDatabase, nil
	// } else {
	// 	aDatabase.tables[SchemaTableName] = NewTable(SchemaTableName, schemaTableColumns, aPager, rooPageIdx)
	// }

	// // Otherwise we need to read all existing tables from the schema table
	// aTable := aDatabase.tables[SchemaTableName]
	// aResult, err := aTable.Select(ctx, Statement{
	// 	Kind:      Select,
	// 	TableName: aTable.Name,
	// 	Fields: []string{
	// 		"type",
	// 		"table_name",
	// 		"root_page",
	// 		"sql",
	// 	},
	// })
	// if err != nil {
	// 	return nil, err
	// }

	// aRow, err := aResult.Rows(ctx)
	// for err != ErrNoMoreRows {
	// 	if aRow.Values[1] == SchemaTableName {
	// 		continue
	// 	}

	// 	stmt, err := aParser.Parse(ctx, aRow.Values[3].(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	aDatabase.tables[stmt.TableName] = NewTable(
	// 		stmt.TableName,
	// 		stmt.Columns,
	// 		aPager,
	// 		aRow.Values[2].(uint32),
	// 	)

	// 	aRow, err = aResult.Rows(ctx)
	// }

	return aDatabase, nil
}

func (d *Database) CreateTestTable() {
	columns := []minisql.Column{
		{
			Kind: minisql.Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: minisql.Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: minisql.Int4,
			Size: 4,
			Name: "age",
		},
	}
	d.tables["foo"] = minisql.NewTable(d.logger, "foo", columns, d.pager, uint32(0))
}

func (d *Database) Close(ctx context.Context) error {
	if len(d.tables) > 1 {
		return fmt.Errorf("currently only single table is supported")
	}

	for pageIdx := uint32(0); pageIdx < d.pager.TotalPages(); pageIdx++ {
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
	if len(d.tables) == 1 {
		return nil, fmt.Errorf("currently only single table is supported")
	}

	// TODO - check row size, currently no row overflowing a page is supported
	// so we need to return an error for such table DDLs

	aTable, ok := d.tables[name]
	if ok {
		return aTable, errTableAlreadyExists
	}
	d.tables[name] = minisql.NewTable(d.logger, name, columns, d.pager, uint32(0))

	// TODO - insert into main schema table

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
	if len(stmt.Columns) > minisql.MaxColumns {
		return minisql.StatementResult{}, fmt.Errorf("maximum number of columns is %d", minisql.MaxColumns)
	}
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
