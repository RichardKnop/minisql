package minisql

import (
	"context"
	"fmt"
	"sync"
)

func (t *Table) Update(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Validate(t); err != nil {
		return StatementResult{}, err
	}

	// Create query plan
	plan := t.PlanQuery(ctx, stmt)

	t.logger.Sugar().With("query type", "UPDATE", "plan", plan).Debug("query plan")

	var (
		filteredPipe   = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
		wg             = new(sync.WaitGroup)
		selectedFields = fieldsFromColumns(t.Columns...)
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
