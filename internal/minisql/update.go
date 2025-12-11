package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Update(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Validate(t); err != nil {
		return StatementResult{}, err
	}

	// Create query plan
	plan := t.PlanQuery(ctx, stmt)

	t.logger.Sugar().With(plan.logArgs("query type", "UPDATE")...).Debug("query plan")

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

	// Execute based on plan
	if plan.IsIndexPointScan() {
		// Use primary key index lookup
		go t.indexPointScan(ctx, plan, fieldsFromColumns(t.Columns...), unfilteredPipe, errorsPipe)
	} else {
		// Sequential scan
		go t.sequentialScan(ctx, fieldsFromColumns(t.Columns...), unfilteredPipe, errorsPipe)
	}

	// Filter rows according to the WHERE conditions. In case of an index scan,
	// any remaining filtering will happen here. In case of a sequential scan,
	// this will filter all rows.
	go func(in <-chan Row, out chan<- Row) {
		defer close(out)
		for aRow := range in {
			ok, err := plan.FilterRow(aRow)
			if err != nil {
				errorsPipe <- err
				return
			}
			if ok {
				out <- aRow
			}
		}
	}(unfilteredPipe, filteredPipe)

	aResult := StatementResult{
		Columns: t.Columns,
	}

	go func(in <-chan Row) {
		defer close(stopChan)
		for aRow := range in {
			changed, err := aRow.cursor.update(ctx, stmt, &aRow)
			if err != nil {
				errorsPipe <- err
				return
			}

			if changed {
				aResult.RowsAffected += 1
			}
		}
	}(filteredPipe)

	select {
	case err := <-errorsPipe:
		return aResult, err
	case <-ctx.Done():
		return aResult, fmt.Errorf("context done: %w", ctx.Err())
	case <-stopChan:
		t.logger.Sugar().Debugf("updated %d rows", aResult.RowsAffected)
		return aResult, nil
	}
}
