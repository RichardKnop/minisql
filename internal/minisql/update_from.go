package minisql

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

type ctxKeyUpdateFromRows struct{}

// contextWithUpdateFromRows stores pre-materialised UPDATE FROM rows in ctx.
func contextWithUpdateFromRows(ctx context.Context, rows []Row) context.Context {
	return context.WithValue(ctx, ctxKeyUpdateFromRows{}, rows)
}

// updateFromRowsFromContext retrieves pre-materialised rows stored by contextWithUpdateFromRows.
func updateFromRowsFromContext(ctx context.Context) ([]Row, bool) {
	rows, ok := ctx.Value(ctxKeyUpdateFromRows{}).([]Row)
	return rows, ok
}

// executeUpdateFrom implements PostgreSQL-style UPDATE … FROM:
//
//	UPDATE t1 [AS alias] SET col = expr FROM t2 [AS alias] WHERE join_cond
//
// It materialises all FROM-source rows, scans the target table without WHERE
// filtering (so that cross-table conditions can be evaluated), builds a merged
// row for each (target, from) pair, evaluates the full WHERE clause against
// that merged row, resolves any *Expr SET values against the merged row, then
// applies the update. Each target row may match at most one FROM row.
func (d *Database) executeUpdateFrom(ctx context.Context, stmt Statement) (StatementResult, error) {
	targetTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, minisqlErrors.ErrNoSuchTable{Name: stmt.TableName}
	}

	// FROM rows are pre-materialised in ExecuteStatement (before the write lock
	// is acquired) to prevent re-entrant dbLock acquisition.  If somehow we
	// arrive here without pre-materialised rows (e.g. in a test calling
	// executeUpdateFrom directly), fall back to materialising now.
	fromRows, ok2 := updateFromRowsFromContext(ctx)
	if !ok2 {
		var err error
		fromRows, err = d.materialiseFromSource(ctx, stmt)
		if err != nil {
			return StatementResult{}, err
		}
	}

	targetAlias := stmt.TableAlias
	if targetAlias == "" {
		targetAlias = stmt.TableName
	}

	// Scan all target rows without WHERE filtering (conditions span both tables).
	scanStmt := stmt
	scanStmt.Conditions = nil
	scanStmt.TableName = targetTable.Name
	scanStmt.Columns = targetTable.Columns

	plan, err := targetTable.PlanQuery(ctx, scanStmt)
	if err != nil {
		return StatementResult{}, err
	}

	if ce := targetTable.logger.Check(zap.DebugLevel, "query plan"); ce != nil {
		ce.Write(zap.String("query type", "UPDATE FROM"), zap.Any("plan", plan))
	}

	allFields := fieldsFromColumns(targetTable.Columns...)

	type pendingUpdate struct {
		row  Row
		stmt Statement // *Expr values pre-resolved against the merged row
	}
	var pending []pendingUpdate

	if err := plan.Execute(ctx, targetTable.provider, allFields, func(targetRow Row) error {
		// Find the unique FROM row that matches the WHERE clause.
		var matched *Row
		mergedRow := Row{}
		for _, fromRow := range fromRows {
			mr := buildUpdateFromMergedRow(targetRow, targetAlias, fromRow)
			ok, err := mr.CheckOneOrMore(stmt.Conditions)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if matched != nil {
				return fmt.Errorf("UPDATE FROM: target row (key=%d) matched more than one FROM row", targetRow.Key)
			}
			fr := fromRow
			matched = &fr
			mergedRow = mr
		}
		if matched == nil {
			return nil // no matching FROM row — target row unchanged
		}

		// Pre-resolve *Expr SET values against the merged row so that
		// cursor.update (which only sees the target row) can apply them.
		resolvedStmt, err := resolveUpdateFromExprs(stmt, mergedRow)
		if err != nil {
			return err
		}
		pending = append(pending, pendingUpdate{row: targetRow, stmt: resolvedStmt})
		return nil
	}); err != nil {
		return StatementResult{}, err
	}

	result := StatementResult{Columns: targetTable.Columns}

	var updatedKeys []RowID
	for _, pu := range pending {
		cursor, err := targetTable.Seek(ctx, pu.row.Key)
		if err != nil {
			return result, err
		}
		changed, err := cursor.update(ctx, pu.stmt, pu.row)
		if err != nil {
			return result, err
		}
		if changed {
			result.RowsAffected += 1
			if len(stmt.ReturningFields) > 0 {
				updatedKeys = append(updatedKeys, pu.row.Key)
			}
		}
	}

	if len(stmt.ReturningFields) > 0 {
		returningRows := make([]Row, 0, len(updatedKeys))
		for _, key := range updatedKeys {
			row, err := targetTable.rowByRowID(ctx, key, allFields...)
			if err != nil {
				return result, err
			}
			returningRows = append(returningRows, row)
		}
		if err := applyReturning(&result, returningRows, stmt.ReturningFields, targetTable.Columns); err != nil {
			return result, err
		}
	}

	if ce := targetTable.logger.Check(zap.DebugLevel, "updated rows (UPDATE FROM)"); ce != nil {
		ce.Write(zap.Int("count", result.RowsAffected))
	}
	return result, nil
}

// materialiseFromSource fetches all rows from the UPDATE FROM source (table or
// subquery) and returns them with column names prefixed by the FROM alias.
func (d *Database) materialiseFromSource(ctx context.Context, stmt Statement) ([]Row, error) {
	fromAlias := stmt.UpdateFromAlias

	if stmt.UpdateFromSubquery != nil {
		result, err := d.ExecuteStatement(ctx, *stmt.UpdateFromSubquery)
		if err != nil {
			return nil, fmt.Errorf("UPDATE FROM subquery: %w", err)
		}
		rows, err := materializeResultRows(ctx, result)
		if err != nil {
			return nil, err
		}
		for i, row := range rows {
			rows[i] = prefixRowColumns(row, fromAlias)
		}
		return rows, nil
	}

	fromTable, ok := d.tables[stmt.UpdateFromTable]
	if !ok {
		return nil, minisqlErrors.ErrNoSuchTable{Name: stmt.UpdateFromTable}
	}
	if fromAlias == "" {
		fromAlias = stmt.UpdateFromTable
	}

	selectAll := Statement{
		Kind:      Select,
		TableName: fromTable.Name,
		Fields:    fieldsFromColumns(fromTable.Columns...),
	}
	result, err := fromTable.Select(ctx, selectAll)
	if err != nil {
		return nil, fmt.Errorf("UPDATE FROM table %q: %w", stmt.UpdateFromTable, err)
	}
	rows, err := materializeResultRows(ctx, result)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		rows[i] = prefixRowColumns(row, fromAlias)
	}
	return rows, nil
}

// buildUpdateFromMergedRow combines a target row and an already alias-prefixed
// FROM row into a single row for WHERE evaluation and SET expression resolution.
//
// Target columns are present under both their plain name (for unqualified refs
// like "dept_id") and their alias-qualified name (e.g. "e.dept_id").
// FROM columns are present only under their alias-qualified name (e.g. "d.id").
func buildUpdateFromMergedRow(targetRow Row, targetAlias string, prefixedFromRow Row) Row {
	n := len(targetRow.Columns)*2 + len(prefixedFromRow.Columns)
	cols := make([]Column, 0, n)
	vals := make([]OptionalValue, 0, n)

	for i, col := range targetRow.Columns {
		// Plain name for unqualified column references.
		cols = append(cols, col)
		vals = append(vals, targetRow.Values[i])
		// Alias-prefixed name for qualified references like "e.dept_id".
		qualified := col
		qualified.Name = targetAlias + "." + col.Name
		cols = append(cols, qualified)
		vals = append(vals, targetRow.Values[i])
	}

	cols = append(cols, prefixedFromRow.Columns...)
	vals = append(vals, prefixedFromRow.Values...)

	return NewRowWithValues(cols, vals)
}

// prefixRowColumns returns a copy of row with every column name prefixed by
// alias + ".".  Used to prepare FROM-source rows for merged-row construction.
func prefixRowColumns(row Row, alias string) Row {
	if alias == "" {
		return row
	}
	cols := make([]Column, len(row.Columns))
	for i, col := range row.Columns {
		cols[i] = col
		cols[i].Name = alias + "." + col.Name
	}
	return NewRowWithValues(cols, row.Values)
}

// resolveUpdateFromExprs returns a copy of stmt with all *Expr UPDATE values
// evaluated against mergedRow.  Non-expression values are kept as-is.
// This lets cursor.update operate on a target-only row without needing
// access to FROM columns.
func resolveUpdateFromExprs(stmt Statement, mergedRow Row) (Statement, error) {
	if len(stmt.Updates) == 0 {
		return stmt, nil
	}
	resolved := make(map[string]OptionalValue, len(stmt.Updates))
	for colName, val := range stmt.Updates {
		if expr, ok := val.Value.(*Expr); ok {
			result, err := expr.Eval(mergedRow)
			if err != nil {
				return stmt, fmt.Errorf("evaluating SET expression for column %q: %w", colName, err)
			}
			if result == nil {
				resolved[colName] = OptionalValue{Valid: false}
			} else {
				resolved[colName] = OptionalValue{Value: result, Valid: true}
			}
		} else {
			resolved[colName] = val
		}
	}
	stmt.Updates = resolved
	return stmt, nil
}
