package minisql

import (
	"context"
	"fmt"
	"sync"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Validate(t); err != nil {
		return StatementResult{}, err
	}

	// Create query plan
	plan := t.PlanQuery(ctx, stmt)

	t.logger.Sugar().With("query type", "DELETE", "plan", plan).Debug("query plan")

	// Only fetch fields needed for WHERE conditions
	var selectedFields []Field
	for _, conditions := range stmt.Conditions {
		for _, cond := range conditions {
			if cond.Operand1.Type == OperandField {
				selectedFields = append(selectedFields, Field{Name: cond.Operand1.Value.(string)})
			}
			if cond.Operand2.Type == OperandField {
				selectedFields = append(selectedFields, Field{Name: cond.Operand2.Value.(string)})
			}
		}
	}

	var (
		filteredPipe = make(chan Row)
		errorsPipe   = make(chan error, 1)
		stopChan     = make(chan bool)
		wg           = new(sync.WaitGroup)
	)

	// Execute scans based on plan
	wg.Go(func() {
		if err := plan.Execute(ctx, t, selectedFields, filteredPipe); err != nil {
			errorsPipe <- err
		}
	})
	go func() {
		wg.Wait()
		close(filteredPipe)
	}()

	aResult := StatementResult{
		Columns: t.Columns,
	}

	go func(in <-chan Row) {
		defer close(stopChan)
		// When deleting multiple rows, we first collect them all
		// and then delete them one by one. This is to avoid fetchRow
		// in unfiltered pipe to skip rows that have been deleted while
		// scanning the table when doing multiple deletions.
		keys := make([]RowID, 0, 100)
		for aRow := range in {
			keys = append(keys, aRow.Key)
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
		t.logger.Sugar().Debugf("deleted %d rows", aResult.RowsAffected)
		return aResult, nil
	}
}
