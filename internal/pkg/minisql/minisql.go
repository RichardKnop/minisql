package minisql

import (
	"context"
	"fmt"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("unrecognised statement type")
	errMaximumPagesReached       = fmt.Errorf("maximum pages reached")
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
	Conditions []Condition
	Updates    map[string]string
	Inserts    [][]string
	Fields     []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases    map[string]string
	Columns    []Column
}

// Execute will eventually become virtual machine
func (s *Statement) Execute(ctx context.Context) error {
	switch s.Kind {
	case Insert:
		return s.executeInsert(ctx)
	case Select:
		return s.executeSelect(ctx)
	}
	return errUnrecognizedStatementType
}

func (stmt *Statement) executeInsert(ctx context.Context) error {
	fmt.Println("This is where we would do insert")
	return nil
}

func (stmt *Statement) executeSelect(ctx context.Context) error {
	fmt.Println("This is where we would do select")
	return nil
}

const (
	pageSize      = 4096 // 4 kilobytes
	tableMaxPages = 100  // temporary limit, TODO - remove later
)

type Database struct {
	tables map[string]Table
}

// NewDatabase creates a new database
// TODO - check if database already exists
func NewDatabase() (Database, error) {
	aDatabase := Database{
		tables: make(map[string]Table),
	}
	return aDatabase, nil
}

// CreateTable creates a new table with a name and columns
// TODO - check if table already exists
func (d *Database) CreateTable(ctx context.Context, name string, columns []Column) (Table, error) {
	aTable := Table{
		Name:    name,
		Columns: columns,
		Pages:   make(map[int]Page),
	}
	d.tables[name] = aTable
	return aTable, nil
}

type Page struct {
	Number int
	buf    [pageSize]byte
}

// NewPage returns a new page with a number (page numbers begin with 0 for the first page)
func NewPage(number int) Page {
	return Page{
		Number: number,
		buf:    [pageSize]byte{},
	}
}

type Table struct {
	Name    string
	Columns []Column
	Pages   map[int]Page
	numRows int
}

// RowSize calculates row size in bytes based on its columns
func (t Table) RowSize() int {
	return rowSize(t.Columns...)
}

// RowSlow calculates page and offset where row should go
func (t Table) RowSlot(rowNumber int) (Page, int, error) {
	rowSize := t.RowSize()
	rowsPerPage := pageSize / rowSize
	pageNumber := rowNumber / rowsPerPage

	if pageNumber > tableMaxPages-1 {
		return Page{}, 0, errMaximumPagesReached
	}

	aPage, ok := t.Pages[pageNumber]
	if !ok {
		aPage = NewPage(pageNumber)
		t.Pages[pageNumber] = aPage
	}

	rowOffset := rowNumber % rowsPerPage
	byteOffset := rowOffset * rowSize

	return aPage, byteOffset, nil
}
