package minisql

import (
	"context"
	"fmt"
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

	selectedFields := fieldsFromColumns(t.Columns...)

	result := StatementResult{
		Columns: t.Columns,
	}

	// Instead of collecting all rows and then updating, we check whether the update
	// affects indexed columns. If not, and the row size does not increase, we update
	// rows in-place as we encounter them. Otherwise we collect all rows first and
	// update them afterwards to avoid conflicts with B-tree structural changes.
	var cantUpdateInPlace []Row

	if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		size := row.Size()
		newSize := size

		indexChanges := false
		for colName, newValue := range stmt.Updates {
			col, _ := stmt.ColumnByName(colName)
			oldValue, _ := row.GetValue(colName)

			// Expression values are evaluated at execution time — their actual value
			// and size are unknown statically, so we must use the full update path.
			if _, isExpr := newValue.Value.(*Expr); isExpr {
				indexChanges = true
				break
			}

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

		if !indexChanges && newSize <= size {
			// Can update in place immediately
			cursor, err := t.Seek(ctx, row.Key)
			if err != nil {
				return err
			}
			changed, err := cursor.update(ctx, stmt, row)
			if err != nil {
				return err
			}
			if changed {
				result.RowsAffected++
			}
			return nil
		}

		// Cannot update in place — collect and defer.
		cantUpdateInPlace = append(cantUpdateInPlace, row)
		return nil
	}); err != nil {
		return result, err
	}

	// Apply deferred updates for rows that couldn't be updated in place.
	for _, row := range cantUpdateInPlace {
		cursor, err := t.Seek(ctx, row.Key)
		if err != nil {
			return result, err
		}

		changed, err := cursor.update(ctx, stmt, row)
		if err != nil {
			return result, err
		}

		if changed {
			result.RowsAffected++
		}
	}

	t.logger.Sugar().Debugf("updated %d rows", result.RowsAffected)
	return result, nil
}
