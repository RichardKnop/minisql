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

type Database struct {
	Name   string
	parser Parser
	tables map[string]*Table
}

// NewDatabase creates a new database
// TODO - check if database already exists
func NewDatabase(name string, aParser Parser) (*Database, error) {
	aDatabase := Database{
		Name:   name,
		parser: aParser,
		tables: make(map[string]*Table),
	}
	return &aDatabase, nil
}

const dbConfigSize = 100

// Configuration returns a byte array of length 100 which stores
// database configuiration, table meta data etc
func (d *Database) Configuration() [dbConfigSize]byte {
	// TODO - implement
	return [dbConfigSize]byte{}
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
