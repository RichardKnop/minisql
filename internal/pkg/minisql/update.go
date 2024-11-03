package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Update(ctx context.Context, stmt Statement) (StatementResult, error) {
	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}
	aPage, err := t.pager.GetPage(ctx, t, aCursor.PageIdx)
	if err != nil {
		return StatementResult{}, err
	}
	aCursor.EndOfTable = aPage.LeafNode.Header.Cells == 0

	t.logger.Sugar().Debug("updating rows")

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

	go func(out chan<- Row) {
		defer close(out)
		for aCursor.EndOfTable == false {
			rowCursor := *aCursor
			aRow, err := aCursor.fetchRow(ctx)
			if err != nil {
				errorsPipe <- err
				return
			}
			aRow.cursor = rowCursor

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

	aResult := StatementResult{
		Columns: t.Columns,
	}

	go func(in <-chan Row) {
		defer close(stopChan)
		for aRow := range in {
			for name, value := range stmt.Updates {
				aRow.SetValue(name, value)
			}

			if err := aRow.cursor.update(ctx, &aRow); err != nil {
				errorsPipe <- err
				return
			}

			aResult.RowsAffected += 1
		}
	}(filteredPipe)

	select {
	case <-ctx.Done():
		return aResult, fmt.Errorf("context done: %w", ctx.Err())
	case err := <-errorsPipe:
		return aResult, err
	case <-stopChan:
		return aResult, nil
	}
}
