package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Validate(t); err != nil {
		return StatementResult{}, err
	}

	// Create query plan
	plan := t.PlanQuery(ctx, stmt)

	t.logger.Sugar().With(plan.logArgs("query type", "DELETE")...).Debug("query plan")

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
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan RowID)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

	// Execute based on plan
	if plan.IsIndexPointScan() {
		// Use primary key index lookup
		go t.indexPointScan(ctx, plan, selectedFields, unfilteredPipe, errorsPipe)
	} else {
		// Sequential scan
		go t.sequentialScan(ctx, selectedFields, unfilteredPipe, errorsPipe)
	}

	// Filter rows according to the WHERE conditions. In case of an index scan,
	// any remaining filtering will happen here. In case of a sequential scan,
	// this will filter all rows.
	go func(in <-chan Row, out chan<- RowID) {
		defer close(out)
		for aRow := range in {
			ok, err := plan.FilterRow(aRow)
			if err != nil {
				errorsPipe <- err
				return
			}
			if ok {
				out <- aRow.Key
			}
		}
	}(unfilteredPipe, filteredPipe)

	aResult := StatementResult{
		Columns: t.Columns,
	}

	go func(in <-chan RowID) {
		defer close(stopChan)
		// When deleting multiple rows, we first collect them all
		// and then delete them one by one. This is to avoid fetchRow
		// in unfiltered pipe to skip rows that have been deleted while
		// scanning the table when doing multiple deletions.
		keys := make([]RowID, 0, 100)
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
		t.logger.Sugar().Debugf("deleted %d rows", aResult.RowsAffected)
		return aResult, nil
	}
}
