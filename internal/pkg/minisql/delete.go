package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) (StatementResult, error) {
	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Sugar().Debug("deleting rows")

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

	go func(out chan<- Row) {
		defer close(out)
		for !aCursor.EndOfTable {
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
		// When deleting multiple rows, we first collect them all
		// and then delete them one by one. This is to avoid fetchRow
		// in unfiltered pipe to skip rows that have been deleted while
		// scanning the table when doing multiple deletions.
		rows := make([]Row, 0, 100)
		for aRow := range in {
			rows = append(rows, aRow)
		}
		for _, aRow := range rows {
			if err := aRow.cursor.delete(ctx); err != nil {
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
