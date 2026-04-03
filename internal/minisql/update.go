package minisql

import (
	"context"
	"fmt"
	"sync"
)

// Update executes an UPDATE statement on the table and returns the result.
func (t *Table) Update(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if stmt.Kind != Update {
		return StatementResult{}, fmt.Errorf("invalid statement kind for UPDATE: %v", stmt.Kind)
	}

	// Create query plan
	plan, err := t.PlanQuery(ctx, stmt)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Sugar().With("query type", "UPDATE", "plan", plan).Debug("query plan")

	var (
		filteredPipe   = make(chan Row)
		chunkPipe      = make(chan []Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
		wg             = new(sync.WaitGroup)
		selectedFields = fieldsFromColumns(t.Columns...)
	)

	// Execute scans based on plan
	wg.Go(func() {
		if err := plan.Execute(ctx, t.provider, selectedFields, filteredPipe); err != nil {
			errorsPipe <- err
		}
	})
	go func() {
		wg.Wait()
		close(filteredPipe)
	}()

	result := StatementResult{
		Columns: t.Columns,
	}

	// Instead of collecting all rows from pipe and then updating, we can first check, if the
	// update affects indexed columns. If not, and row size does not increase, we can update rows
	// in place as we read them from the pipe. This can significantly improve performance for
	// large updates. In case indexed values are changing or row size increases, we have to collect
	// all rows and then update them after that to avoid issues with changing row locations.
	go func(in <-chan Row, out chan<- []Row) {
		defer close(out)
		cantUpdateInPlace := make([]Row, 0, 10)
		for row := range in {
			size := row.Size()
			// We will calculate new size after update
			newSize := size

			indexChanges := false
			for colName, newValue := range stmt.Updates {
				col, _ := stmt.ColumnByName(colName)
				oldValue, _ := row.GetValue(colName)

				if t.HasIndexOnColumn(colName) && !compareValue(col.Kind, oldValue, newValue) {
					// Updating indexed column, can't update in place
					indexChanges = true
					break
				}

				switch {
				case col.Kind.IsText():
					if oldValue.Valid {
						newSize -= uint64(oldValue.Value.(TextPointer).Size())
					}
					if newValue.Valid {
						newSize += uint64(newValue.Value.(TextPointer).Size())
					}
					continue
				case !oldValue.Valid && newValue.Valid:
					// NULL -> NOT NULL
					newSize += uint64(col.Size)
				case oldValue.Valid && !newValue.Valid:
					// NOT NULL -> NULL
					newSize -= uint64(col.Size)
				}
			}

			// If we are not changing indexed values and row size does not increase,
			// we can update in place, just send it to output channel directly.
			if !indexChanges && newSize <= size {
				// Can update in place
				out <- []Row{row}
				continue
			}

			// Otherwise collect rows and send them all once reading from filtered pipe is done.
			cantUpdateInPlace = append(cantUpdateInPlace, row)
		}
		if len(cantUpdateInPlace) > 0 {
			out <- cantUpdateInPlace
		}
	}(filteredPipe, chunkPipe)

	go func(in <-chan []Row) {
		defer close(stopChan)
		for rowChunk := range in {
			for _, row := range rowChunk {
				// Row locations can change after each update in case row grows larger
				// and causes a page split (for example setting NULL values),
				// so we seek again for each key to make sure we have the correct cursor.
				cursor, err := t.Seek(ctx, row.Key)
				if err != nil {
					errorsPipe <- err
					return
				}

				changed, err := cursor.update(ctx, stmt, row)
				if err != nil {
					errorsPipe <- err
					return
				}

				if changed {
					result.RowsAffected += 1
				}
			}
		}
	}(chunkPipe)

	select {
	case err := <-errorsPipe:
		return result, err
	case <-ctx.Done():
		return result, fmt.Errorf("context done: %w", ctx.Err())
	case <-stopChan:
		t.logger.Sugar().Debugf("updated %d rows", result.RowsAffected)
		return result, nil
	}
}
