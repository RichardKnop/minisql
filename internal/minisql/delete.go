package minisql

import (
	"context"
	"fmt"
	"sync"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if stmt.Kind != Delete {
		return StatementResult{}, fmt.Errorf("invalid statement kind for DELETE: %v", stmt.Kind)
	}

	// Create query plan
	plan, err := t.PlanQuery(ctx, stmt)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Sugar().With("query type", "DELETE", "plan", plan).Debug("query plan")

	// We need to select any fields used in WHERE conditions to filter rows to delete.
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
		// and then delete them one by one. This is to avoid conflict
		// with rows that are still being read because delete can cause
		// nodes to be split or merged which can cause cells to move around.
		rows := make([]Row, 0, 100)
		for aRow := range in {
			rows = append(rows, aRow)
		}
		for _, aRow := range rows {
			// Row locations can change after each delete, so we seek again for each key
			// to make sure we have the correct cursor.
			aCursor, err := t.Seek(ctx, aRow.Key)
			if err != nil {
				errorsPipe <- err
				return
			}

			if len(selectedFields) < len(t.Columns) {
				// Load full row before delete, this is so we have all indexed values available
				// for proper index cleanup as well as any overflow data that needs to be freed.
				fullRow, err := aCursor.fetchRow(ctx, false, fieldsFromColumns(t.Columns...)...)
				if err != nil {
					errorsPipe <- err
					return
				}
				aRow = fullRow
			}

			if err := aCursor.delete(ctx, aRow); err != nil {
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
