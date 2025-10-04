package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) (StatementResult, error) {
	if err := stmt.Validate(t); err != nil {
		return StatementResult{}, err
	}

	// Write lock limits concurrent writes to the table
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Sugar().Debug("deleting rows")

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan uint64)
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
	go func(in <-chan Row, out chan<- uint64, conditions OneOrMore) {
		defer close(out)
		for aRow := range in {
			if len(conditions) == 0 {
				out <- aRow.Key
				continue
			}
			ok, err := aRow.CheckOneOrMore(conditions)
			if err != nil {
				errorsPipe <- err
				return
			}
			if ok {
				out <- aRow.Key
			}
		}
	}(unfilteredPipe, filteredPipe, stmt.Conditions)

	aResult := StatementResult{
		Columns: t.Columns,
	}

	go func(in <-chan uint64) {
		defer close(stopChan)
		// When deleting multiple rows, we first collect them all
		// and then delete them one by one. This is to avoid fetchRow
		// in unfiltered pipe to skip rows that have been deleted while
		// scanning the table when doing multiple deletions.
		keys := make([]uint64, 0, 100)
		for aKey := range in {
			keys = append(keys, aKey)
		}
		for _, aKey := range keys {
			aCursor, err := t.Seek(ctx, aKey)
			if err != nil {
				errorsPipe <- err
				return
			}

			if err := aCursor.delete(ctx); err != nil {
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
