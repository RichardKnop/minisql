package minisql

import (
	"context"
	"fmt"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("unrecognised statement type")
)

type Parser interface {
	Parse(ctx context.Context, sql string) (Statement, error)
}

type Pager interface {
	GetPage(int64) (*Page, error)
}

type Database struct {
	Name   string
	parser Parser
	pager  Pager
	tables map[string]*Table
}

// NewDatabase creates a new database
// TODO - check if database already exists
func NewDatabase(name string, aParser Parser, aPager Pager) (*Database, error) {
	aDatabase := Database{
		Name:   name,
		parser: aParser,
		pager:  aPager,
		tables: make(map[string]*Table),
	}
	return &aDatabase, nil
}

// const (
// 	// Defines how much of the first partition is reserved
// 	// for database configuration
// 	configSize = 100
// 	// Name of the table that contains database schema
// 	mainSchemaTable = "minisql_main"
// )

// var (
// 	mainSchemaTableColumns = []Column{
// 		{
// 			Kind: Int4,
// 			Size: 4,
// 			Name: "type",
// 		},
// 		{
// 			Kind: Varchar,
// 			Size: 100,
// 			Name: "name",
// 		},
// 		{
// 			Kind: Varchar,
// 			Size: 100,
// 			Name: "table_name",
// 		},
// 		{
// 			Kind: Int8,
// 			Size: 8,
// 			Name: "root_page",
// 		},
// 		{
// 			Kind: Varchar,
// 			Size: 1000,
// 			Name: "sql",
// 		},
// 	}

// 	mainSchemaTableSQL = `CREATE TABLE minisql_main (
// 		type INT4,
// 		name VARCHAR(100),
// 		table_name VARCHAR(100),
// 		root_page INT4,
// 		SQL VARCHAR(1000)
// 	)`
// )

// type SchemaType int

// const (
// 	SchemaTable SchemaType = iota + 1
// 	SchemaIndex
// )

func (d *Database) Open(ctx context.Context) error {
	// TODO - open a file pointer

	// aTable, ok := d.tables[mainSchemaTable]
	// if !ok {
	// 	// If this is a new database, create minisql_main table and insert its first row
	// 	// minisql_main's root page will be 0.
	// 	aTable, err := d.CreateTable(ctx, mainSchemaTable, mainSchemaTableColumns)
	// 	if err != nil {
	// 		return err
	// 	}
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
	// 			int32(SchemaTable),
	// 			mainSchemaTable,
	// 			mainSchemaTable,
	// 			int64(0),
	// 			mainSchemaTableSQL,
	// 		}},
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	// Otherwise we need to register all tables
	// 	aResult, err := aTable.Select(ctx, Statement{
	// 		Kind:      Select,
	// 		TableName: aTable.Name,
	// 		Fields: []string{
	// 			"type",
	// 			"table_name",
	// 			"root_page",
	// 			"sql",
	// 		},
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}

	// 	aRow, err := aResult.Rows(ctx)
	// 	for err != ErrNoMoreRows {
	// 		if aRow.Values[1] == mainSchemaTable {
	// 			// TODO - implement
	// 		} else {
	// 			// TODO - implement
	// 		}
	// 		aRow, err = aResult.Rows(ctx)
	// 	}
	// }

	return nil
}

func (d *Database) Close() error {
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
