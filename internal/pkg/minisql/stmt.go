package minisql

import (
	"context"
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
	Columns    []Column // use for CREATE TABLE
	Fields     []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases    map[string]string
	Inserts    [][]any
	Updates    map[string]any
	Conditions []Condition // used for WHERE
}

type Iterator func(ctx context.Context) (Row, error)

type StatementResult struct {
	Rows         Iterator
	RowsAffected int
}
