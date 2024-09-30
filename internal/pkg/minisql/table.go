package minisql

import (
	"context"
	"fmt"
)

var (
	errMaximumPagesReached = fmt.Errorf("maximum pages reached")
	errTableDoesNotExist   = fmt.Errorf("table does not exist")
	errTableAlreadyExists  = fmt.Errorf("table already exists")
)

type Table struct {
	Name        string
	Columns     []Column
	RootPageIdx uint32
	pager       Pager
	rowSize     uint32
	numRows     int
}

func NewTable(name string, columns []Column, pager Pager, rootPageIdx uint32) *Table {
	return &Table{
		Name:        name,
		Columns:     columns,
		RootPageIdx: rootPageIdx,
		pager:       pager,
		rowSize:     Row{Columns: columns}.Size(),
	}
}

// CreateTable creates a new table with a name and columns
func (d *Database) CreateTable(ctx context.Context, name string, columns []Column) (*Table, error) {
	if len(d.tables) == 1 {
		return nil, fmt.Errorf("currently only single table is supported")
	}

	aTable, ok := d.tables[name]
	if ok {
		return aTable, errTableAlreadyExists
	}
	d.tables[name] = NewTable(name, columns, d.pager, uint32(0))

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

func (d *Database) executeCreateTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	_, err := d.CreateTable(ctx, stmt.TableName, stmt.Columns)
	return StatementResult{}, err
}

func (d *Database) executeDropTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	err := d.DropTable(ctx, stmt.TableName)
	return StatementResult{}, err
}
