package minisql

import (
	"context"
	"errors"
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
	var (
		pageNumber = 0
		offset     = 0
		rowSize    = t.rowSize
	)
	aResult := StatementResult{
		Rows: func(ctx context.Context) (Row, error) {
			// If there is no more pages, return ErrNoMoreRows
			if pageNumber > len(t.Pages)-1 {
				return Row{}, ErrNoMoreRows
			}
			aPage := t.Pages[pageNumber]
			// If we are on the last page and there is no more data
			// (nextOffset marks where empty space in the page starts)
			// return ErrNoMoreRows
			if pageNumber == len(t.Pages)-1 && offset >= aPage.nextOffset {
				return Row{}, ErrNoMoreRows
			}
			aRow := NewRow(t.Columns)
			if err := UnmarshalRow(aPage.buf[offset:offset+rowSize], &aRow); err != nil {
				return Row{}, err
			}

			// If there is still enough space in the page for next row,
			// increase offset and return the row
			if pageSize-offset-1-rowSize >= rowSize {
				offset += rowSize
				return aRow, nil
			}

			// Otherwise we will go to the next page, starting at offset 0
			pageNumber += 1
			offset = 0

			return aRow, nil
		},
	}

	return aResult, nil
}
