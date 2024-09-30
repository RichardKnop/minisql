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
	Name    string
	Columns []Column
	pager   Pager
	rowSize uint32
	numRows int
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
		pager:   d.pager,
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
