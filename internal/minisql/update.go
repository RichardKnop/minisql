package minisql

import (
	"context"
	"fmt"

	"go.uber.org/zap"
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

	if ce := t.logger.Check(zap.DebugLevel, "query plan"); ce != nil {
		ce.Write(zap.String("query type", "UPDATE"), zap.Any("plan", plan))
	}

	selectedFields := t.allFields

	result := StatementResult{
		Columns: t.Columns,
	}

	// Instead of collecting all rows and then updating, we check whether the update
	// affects indexed columns. If not, and the row size does not increase, we update
	// rows in-place as we encounter them. Otherwise we collect all rows first and
	// update them afterwards to avoid conflicts with B-tree structural changes.
	type pendingRow struct {
		row  Row
		stmt Statement // may carry per-row correlated SET values
	}
	var (
		cantUpdateInPlace []pendingRow
		updatedKeys       []RowID // tracked only when RETURNING is requested
	)

	// Pre-computed correlated SET subquery values (built in resolveSetSubqueries).
	corrUpdates, hasCorrUpdates := correlatedSetUpdatesFromContext(ctx)

	// mergeCorrelatedUpdates returns a statement with per-row correlated SET values
	// merged in.  If there are none, it returns stmt unchanged.
	mergeCorrelatedUpdates := func(row Row) Statement {
		if !hasCorrUpdates {
			return stmt
		}
		rowMap, ok := corrUpdates[row.Key]
		if !ok || len(rowMap) == 0 {
			return stmt
		}
		merged := make(map[string]OptionalValue, len(stmt.Updates)+len(rowMap))
		for k, v := range stmt.Updates {
			merged[k] = v
		}
		for k, v := range rowMap {
			merged[k] = v
		}
		s := stmt
		s.Updates = merged
		return s
	}

	if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		rowStmt := mergeCorrelatedUpdates(row)

		size := row.Size()
		newSize := size

		indexChanges := false
		for colName, newValue := range rowStmt.Updates {
			col, _ := rowStmt.ColumnByName(colName)
			oldValue, _ := row.GetValue(colName)

			// Expression values and correlated subquery placeholders are resolved at
			// execution time — their actual value and size are unknown statically,
			// so we must use the full update path.
			if _, isExpr := newValue.Value.(*Expr); isExpr {
				indexChanges = true
				break
			}
			if _, isSub := newValue.Value.(*Statement); isSub {
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
			changed, err := cursor.update(ctx, rowStmt, row)
			if err != nil {
				return err
			}
			if changed {
				result.RowsAffected += 1
				if len(stmt.ReturningFields) > 0 {
					updatedKeys = append(updatedKeys, row.Key)
				}
			}
			return nil
		}

		// Cannot update in place — collect and defer.
		cantUpdateInPlace = append(cantUpdateInPlace, pendingRow{row: row, stmt: rowStmt})
		return nil
	}); err != nil {
		return result, err
	}

	// Apply deferred updates for rows that couldn't be updated in place.
	for _, pending := range cantUpdateInPlace {
		cursor, err := t.Seek(ctx, pending.row.Key)
		if err != nil {
			return result, err
		}

		changed, err := cursor.update(ctx, pending.stmt, pending.row)
		if err != nil {
			return result, err
		}

		if changed {
			result.RowsAffected += 1
			if len(stmt.ReturningFields) > 0 {
				updatedKeys = append(updatedKeys, pending.row.Key)
			}
		}
	}

	if ce := t.logger.Check(zap.DebugLevel, "updated rows"); ce != nil {
		ce.Write(zap.Int("count", result.RowsAffected))
	}

	if len(stmt.ReturningFields) > 0 {
		returningRows := make([]Row, 0, len(updatedKeys))
		for _, key := range updatedKeys {
			cursor, err := t.Seek(ctx, key)
			if err != nil {
				return result, err
			}
			row, err := cursor.fetchRow(ctx, false, t.allFields...)
			if err != nil {
				return result, err
			}
			projected, err := projectReturning(row, stmt.ReturningFields)
			if err != nil {
				return result, err
			}
			returningRows = append(returningRows, projected)
		}
		result.Columns = returningColumns(stmt.ReturningFields, t.Columns)
		result.Rows = NewSliceIterator(returningRows)
	}

	return result, nil
}
