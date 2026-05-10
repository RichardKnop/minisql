package minisql

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// Delete executes a DELETE statement against the table, removing all rows that
// match the WHERE clause and maintaining every affected index. Returns the
// number of rows deleted and, when a RETURNING clause is present, the deleted rows.
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

	t.logger.Debug("query plan", zap.String("query type", "DELETE"), zap.Any("plan", plan))

	// Always select all columns so the full row is available for index cleanup on delete.
	selectedFields := fieldsFromColumns(t.Columns...)

	result := StatementResult{
		Columns: t.Columns,
	}

	// Collect all rows first, then delete. We must collect before deleting because
	// a delete can cause B-tree node splits/merges that move cells around, which
	// would corrupt an in-progress scan.
	var rows []Row
	if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		rows = append(rows, row)
		return nil
	}); err != nil {
		return result, err
	}

	for _, row := range rows {
		if t.checkParentFK != nil {
			if err := t.checkParentFK(ctx, row); err != nil {
				return result, err
			}
		}

		// Row locations can change after each delete, so we seek again for each key
		// to make sure we have the correct cursor.
		cursor, err := t.Seek(ctx, row.Key)
		if err != nil {
			return result, err
		}

		if len(selectedFields) < len(t.Columns) {
			// Load full row before delete, this is so we have all indexed values available
			// for proper index cleanup as well as any overflow data that needs to be freed.
			fullRow, err := cursor.fetchRow(ctx, false, fieldsFromColumns(t.Columns...)...)
			if err != nil {
				return result, err
			}
			row = fullRow
		}

		if err := cursor.delete(ctx, row); err != nil {
			return result, err
		}

		result.RowsAffected += 1
	}

	t.logger.Debug("deleted rows", zap.Int("count", result.RowsAffected))

	// Update the in-memory row-count cache (only for user tables).
	if t.getRowCount != nil && result.RowsAffected > 0 {
		if tx := TxFromContext(ctx); tx != nil {
			tx.AddRowCountDelta(t.Name, -int64(result.RowsAffected))
		}
	}

	if len(stmt.ReturningFields) > 0 {
		returningRows := make([]Row, 0, len(rows))
		for _, row := range rows {
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
