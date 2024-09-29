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
	Number     int
	buf        [pageSize]byte
	nextOffset int
}

// NewPage returns a new page with a number (page numbers begin with 0 for the first page)
func NewPage(number int) *Page {
	return &Page{
		Number: number,
		buf:    [pageSize]byte{},
	}
}

// Insert inserts a row into the page
func (p *Page) Insert(ctx context.Context, offset int, aRow Row) error {
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
	p.nextOffset = offset + len(data)
	return nil
}

type Table struct {
	Name    string
	Columns []Column
	Pages   []*Page
	rowSize int
	numRows int
}

// Page retrieves the page by its number or creates a new page and returns it
func (t *Table) Page(pageNumber int) (*Page, error) {
	if pageNumber >= tableMaxPages {
		return nil, errMaximumPagesReached
	}
	if pageNumber >= len(t.Pages) {
		aPage := NewPage(pageNumber)
		t.Pages = append(t.Pages, aPage)
	}
	return t.Pages[pageNumber], nil
}

// RowSlot calculates page and offset where row should go
func (t *Table) RowSlot(rowNumber int) (int, int, error) {
	rowsPerPage := pageSize / t.rowSize
	pageNumber := rowNumber / rowsPerPage

	if pageNumber >= tableMaxPages {
		return 0, 0, errMaximumPagesReached
	}

	rowOffset := rowNumber % rowsPerPage
	byteOffset := rowOffset * t.rowSize

	return pageNumber, byteOffset, nil
}

// CreateTable creates a new table with a name and columns
func (d *Database) CreateTable(ctx context.Context, name string, columns []Column) (*Table, error) {
	aTable, ok := d.tables[name]
	if ok {
		return aTable, errTableAlreadyExists
	}
	d.tables[name] = &Table{
		Name:    name,
		Columns: columns,
		Pages:   make([]*Page, 0, tableMaxPages),
		rowSize: Row{Columns: columns}.Size(),
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
	return nil
}

func (d *Database) executeCreateTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	_, err := d.CreateTable(ctx, stmt.TableName, stmt.Columns)
	return StatementResult{}, err
}

func (d *Database) executeDropTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	err := d.DropTable(ctx, stmt.TableName)
	return StatementResult{}, err
}
