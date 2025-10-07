package minisql

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNoMoreRows = errors.New("no more rows")
)

func (t *Table) Select(ctx context.Context, stmt Statement) (StatementResult, error) {
	aResult := StatementResult{
		Columns: t.Columns,
		Rows: func(ctx context.Context) (Row, error) {
			return Row{}, ErrNoMoreRows
		},
	}

	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		return aResult, err
	}
	aPage, err := t.pager.GetPage(ctx, aCursor.PageIdx, t.RowSize)
	if err != nil {
		return aResult, fmt.Errorf("select: %w", err)
	}
	aCursor.EndOfTable = aPage.LeafNode.Header.Cells == 0

	t.logger.Sugar().With(
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
		for !aCursor.EndOfTable {
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
			ok, err := aRow.CheckOneOrMore(conditions)
			if err != nil {
				errorsPipe <- err
				return
			}
			if ok {
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

	aResult.Rows = func(ctx context.Context) (Row, error) {
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
	}

	return aResult, nil
}
