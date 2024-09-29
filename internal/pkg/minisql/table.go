package minisql

import (
	"context"
	"fmt"
)

const (
	pageSize      = 4096 // 4 kilobytes
	tableMaxPages = 100  // temporary limit, TODO - remove later
)

var (
	errMaximumPagesReached = fmt.Errorf("maximum pages reached")
	errTableDoesNotExist   = fmt.Errorf("table does not exist")
	errTableAlreadyExists  = fmt.Errorf("table already exists")
)

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

// Insert inserts a row into the page
func (p Page) Insert(ctx context.Context, offset int, aRow Row) error {
	data, err := aRow.Marshal()
	if err != nil {
		return err
	}
	if offset+len(data) > len(p.buf) {
		return fmt.Errorf("error inserting %d bytes into page at offset %d, not enough space", len(data), offset)
	}
	for i, dataByte := range data {
		p.buf[offset+i] = dataByte
	}
	return nil
}

type Table struct {
	Name    string
	Columns []Column
	Pages   map[int]Page
	rowSize int
	numRows int
}

// RowSlot calculates page and offset where row should go
func (t Table) RowSlot(rowNumber int) (Page, int, error) {
	rowsPerPage := pageSize / t.rowSize
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
	byteOffset := rowOffset * t.rowSize

	return aPage, byteOffset, nil
}

// CreateTable creates a new table with a name and columns
func (d Database) CreateTable(ctx context.Context, name string, columns []Column) (Table, error) {
	aTable, ok := d.tables[name]
	if ok {
		return aTable, errTableAlreadyExists
	}
	d.tables[name] = Table{
		Name:    name,
		Columns: columns,
		Pages:   make(map[int]Page),
		rowSize: Row{Columns: columns}.Size(),
	}
	return d.tables[name], nil
}

// CreateTable creates a new table with a name and columns
func (d Database) DropTable(ctx context.Context, name string) error {
	_, ok := d.tables[name]
	if !ok {
		return errTableDoesNotExist
	}
	delete(d.tables, name)
	return nil
}

func (d Database) executeCreateTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	_, err := d.CreateTable(ctx, stmt.TableName, stmt.Columns)
	return StatementResult{}, err
}

func (d Database) executeDropTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	err := d.DropTable(ctx, stmt.TableName)
	return StatementResult{}, err
}
