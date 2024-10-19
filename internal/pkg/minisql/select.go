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
	aCursor, err := t.Seek(ctx, uint64(0))
	if err != nil {
		return StatementResult{}, err
	}
	aPage, err := t.pager.GetPage(ctx, t, aCursor.PageIdx)
	if err != nil {
		return StatementResult{}, err
	}
	aCursor.EndOfTable = aPage.LeafNode.Header.Cells == 0

	logger.Sugar().With(
		"page_index", int(aCursor.PageIdx),
		"cell_index", int(aCursor.CellIdx),
	).Debug("fetching rows from")

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan Row)
		limitedPipe    = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

	go func(out chan<- Row) {
		defer close(out)
		for aCursor.EndOfTable == false {
			aRow, err := aCursor.fetchRow(ctx)
			if err != nil {
				errorsPipe <- err
				return
			}

			select {
			case <-stopChan:
				return
			case out <- aRow:
				continue
			}
		}
	}(unfilteredPipe)

	// Filter rows according the WHERE conditions
	go func(in <-chan Row, out chan<- Row, conditions OneOrMore) {
		defer close(out)
		for aRow := range in {
			if len(conditions) == 0 {
				out <- aRow
				continue
			}
			filtered, err := isRowFiltered(conditions, aRow)
			if err != nil {
				errorsPipe <- err
				return
			}
			if !filtered {
				out <- aRow
			}
		}
	}(unfilteredPipe, filteredPipe, stmt.Conditions)

	// Count row count for LIMIT clause.
	var limit int64 // TODO - set limit from parser
	go func(in <-chan Row, out chan<- Row, limit int64) {
		defer close(out)
		defer close(stopChan)
		i := int64(0)
		for aRow := range in {
			i += 1
			if i > limit && limit > 0 {
				return
			}
			out <- aRow
		}
	}(filteredPipe, limitedPipe, limit)

	aResult := StatementResult{
		Columns: t.Columns,
		Rows: func(ctx context.Context) (Row, error) {
			select {
			case <-ctx.Done():
				return Row{}, fmt.Errorf("context done: %w", ctx.Err())
			case err := <-errorsPipe:
				return Row{}, err
			case aRow, open := <-limitedPipe:
				if !open {
					return Row{}, ErrNoMoreRows
				}

				return aRow, nil
			}
		},
	}

	return aResult, nil
}

func isRowFiltered(conditions OneOrMore, aRow Row) (filtered bool, err error) {
	// TODO - implement simple WHERE condition filtering
	// for _, aCondition := range conditions {
	// 	if aCondition.Operand1IsField {

	// 	}
	// }

	return false, nil
}
