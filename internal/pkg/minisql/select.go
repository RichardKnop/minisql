package minisql

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNoMoreRows = errors.New("no more rows")
)

func (d *Database) executeSelect(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Select(ctx, stmt)
}

func (t *Table) Select(ctx context.Context, stmt Statement) (StatementResult, error) {
	return StatementResult{}, fmt.Errorf("not implemented")
	// var (
	// 	rowSize = t.RowSize
	// 	aCursor = TableStart(t)
	// )
	// aResult := StatementResult{
	// 	Rows: func(ctx context.Context) (Row, error) {
	// 		if aCursor.EndOfTable {
	// 			return Row{}, ErrNoMoreRows
	// 		}

	// 		pageIdx, offset, err := aCursor.Value()
	// 		if err != nil {
	// 			return Row{}, err
	// 		}
	// 		aPage, err := t.pager.GetPage(ctx, t.Name, pageIdx)
	// 		if err != nil {
	// 			return Row{}, err
	// 		}

	// 		aRow := NewRow(t.Columns)
	// 		if err := UnmarshalRow(aPage.buf[offset:int(offset+rowSize)], &aRow); err != nil {
	// 			return Row{}, err
	// 		}

	// 		aCursor.Advance()

	// 		return aRow, nil
	// 	},
	// }

	// return aResult, nil
}
