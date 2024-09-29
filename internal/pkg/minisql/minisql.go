package minisql

import (
	"context"
	"fmt"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("unrecognised statement type")
)

type Operator int

const (
	// Eq -> "="
	Eq Operator = iota + 1
	// Ne -> "!="
	Ne
	// Gt -> ">"
	Gt
	// Lt -> "<"
	Lt
	// Gte -> ">="
	Gte
	// Lte -> "<="
	Lte
)

type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
}

type StatementKind int

const (
	CreateTable StatementKind = iota + 1
	DropTable
	Insert
	Select
	Update
	Delete
)

type ColumnKind int

const (
	Int4 ColumnKind = iota + 1
	Int8
	Varchar
)

type Column struct {
	Kind ColumnKind
	Size int
	Name string
}

type Statement struct {
	Kind       StatementKind
	TableName  string
	Columns    []Column
	Fields     []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases    map[string]string
	Inserts    [][]any
	Updates    map[string]any
	Conditions []Condition
}

type StatementResult struct {
	RowsAffected int
}

type Database struct {
	Name   string
	tables map[string]Table
}

// NewDatabase creates a new database
// TODO - check if database already exists
func NewDatabase(name string) (Database, error) {
	aDatabase := Database{
		Name:   name,
		tables: make(map[string]Table),
	}
	return aDatabase, nil
}

// ListTableNames lists names of all tables in the database
func (d Database) ListTableNames(ctx context.Context) []string {
	tables := make([]string, 0, len(d.tables))
	for tableName := range d.tables {
		tables = append(tables, tableName)
	}
	return tables
}

// ExecuteStatement will eventually become virtual machine
func (d Database) ExecuteStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
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
