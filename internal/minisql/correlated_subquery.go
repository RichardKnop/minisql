package minisql

import (
	"context"
	"fmt"
)

// ctxKeyCorrelatedSetUpdates is the context key for pre-computed per-row SET values.
type ctxKeyCorrelatedSetUpdates struct{}

// correlatedSetUpdates maps RowID → (column name → pre-computed value).
// Built before the write lock is acquired and consumed inside Table.Update.
type correlatedSetUpdates map[RowID]map[string]OptionalValue

func contextWithCorrelatedSetUpdates(ctx context.Context, u correlatedSetUpdates) context.Context {
	return context.WithValue(ctx, ctxKeyCorrelatedSetUpdates{}, u)
}

func correlatedSetUpdatesFromContext(ctx context.Context) (correlatedSetUpdates, bool) {
	v, ok := ctx.Value(ctxKeyCorrelatedSetUpdates{}).(correlatedSetUpdates)
	return v, ok
}

// resolveSetSubqueries inspects stmt.Updates for *Statement values (subqueries in
// SET).  Non-correlated subqueries are executed once and replaced with their scalar
// result.  Correlated subqueries (whose WHERE conditions reference outer-table
// columns) require per-row execution: this function does a read scan of the target
// table, executes each correlated subquery once per matching row, and stores the
// results in the returned context so that Table.Update can consume them.
//
// Must be called BEFORE the write lock is acquired because subquery execution itself
// calls ExecuteStatement → GetTable → RLock.
func (d *Database) resolveSetSubqueries(ctx context.Context, stmt *Statement) (context.Context, error) {
	// Fast path: no subquery values in SET.
	hasSubquery := false
	for _, val := range stmt.Updates {
		if val.IsStatement() {
			hasSubquery = true
			break
		}
	}
	if !hasSubquery {
		return ctx, nil
	}

	targetTable, ok := d.tables[stmt.TableName]
	if !ok {
		// Table not found — will fail later with a proper error.
		return ctx, nil
	}

	outerAlias := stmt.TableAlias
	if outerAlias == "" {
		outerAlias = stmt.TableName
	}

	type subEntry struct {
		colName   string
		inner     Statement
		correlated bool
	}
	var entries []subEntry

	for colName, val := range stmt.Updates {
		if !val.IsStatement() {
			continue
		}
		inner := val.AsStatement()
		corr := isCorrelatedSetSubquery(*inner, targetTable.Columns, stmt.TableName, outerAlias)
		entries = append(entries, subEntry{colName, *inner, corr})
	}

	// Handle non-correlated subqueries first: execute once and substitute.
	for _, e := range entries {
		if e.correlated {
			continue
		}
		var val OptionalValue
		if err := d.txManager.ExecuteReadOnlyTransaction(ctx, func(roCtx context.Context) error {
			var err error
			val, err = d.executeScalarSetSubquery(roCtx, e.inner)
			return err
		}); err != nil {
			return ctx, fmt.Errorf("SET subquery for column %q: %w", e.colName, err)
		}
		stmt.Updates[e.colName] = val
	}

	// Collect correlated entries.
	var corrEntries []subEntry
	for _, e := range entries {
		if e.correlated {
			corrEntries = append(corrEntries, e)
		}
	}
	if len(corrEntries) == 0 {
		return ctx, nil
	}

	// Read scan: collect all target rows that match the WHERE clause.
	// We re-use the same conditions as the UPDATE to avoid computing new values
	// for rows that won't actually be updated.
	scanStmt := Statement{
		Kind:       Select,
		TableName:  stmt.TableName,
		TableAlias: outerAlias,
		Columns:    targetTable.Columns,
		Fields:     fieldsFromColumns(targetTable.Columns...),
		Conditions: stmt.Conditions,
	}

	precomputed := make(correlatedSetUpdates)
	scanErr := d.txManager.ExecuteReadOnlyTransaction(ctx, func(roCtx context.Context) error {
		selectResult, err := targetTable.Select(roCtx, scanStmt)
		if err != nil {
			return fmt.Errorf("correlated SET subquery: scanning target table: %w", err)
		}

		for selectResult.Rows.Next(roCtx) {
			outerRow := selectResult.Rows.Row()
			rowUpdates := make(map[string]OptionalValue, len(corrEntries))
			for _, e := range corrEntries {
				bound := bindOuterRowToStatement(e.inner, outerRow, stmt.TableName, outerAlias)
				val, err := d.executeScalarSetSubquery(roCtx, bound)
				if err != nil {
					return fmt.Errorf("correlated SET subquery for column %q: %w", e.colName, err)
				}
				rowUpdates[e.colName] = val
			}
			precomputed[outerRow.Key] = rowUpdates
		}
		return selectResult.Rows.Err()
	})
	if scanErr != nil {
		return ctx, fmt.Errorf("correlated SET subquery: %w", scanErr)
	}

	// Leave *Statement placeholders in stmt.Updates so that validateUpdate does
	// not see an empty Updates map.  Table.Update will overlay the pre-computed
	// values before cursor.update is called; cursor.update guards against any
	// *Statement that slips through.

	return contextWithCorrelatedSetUpdates(ctx, precomputed), nil
}

// executeScalarSetSubquery runs stmt and returns the first column of the first row.
// Zero rows → NULL; more than one row → error.
func (d *Database) executeScalarSetSubquery(ctx context.Context, stmt Statement) (OptionalValue, error) {
	result, err := d.ExecuteStatement(ctx, stmt)
	if err != nil {
		return OptionalValue{}, err
	}
	if len(result.Columns) != 1 {
		return OptionalValue{}, fmt.Errorf("subquery used as expression must return exactly one column, got %d", len(result.Columns))
	}
	if !result.Rows.Next(ctx) {
		if err := result.Rows.Err(); err != nil {
			return OptionalValue{}, err
		}
		return MakeNull(), nil // NULL
	}
	row := result.Rows.Row()
	// Guard: more than one row is an error.
	if result.Rows.Next(ctx) {
		return OptionalValue{}, fmt.Errorf("more than one row returned by a subquery used as an expression")
	}
	if err := result.Rows.Err(); err != nil {
		return OptionalValue{}, err
	}
	if len(row.Values) == 0 || row.Values[0].IsNull() {
		return MakeNull(), nil
	}
	return row.Values[0], nil
}

// isCorrelatedSetSubquery reports whether inner's WHERE conditions contain any
// reference to an outer-table column, identified by outerTableName or outerAlias
// as an AliasPrefix, or as an unqualified name matching a column in outerCols.
func isCorrelatedSetSubquery(inner Statement, outerCols []Column, outerTableName, outerAlias string) bool {
	outerColSet := make(map[string]bool, len(outerCols))
	for _, col := range outerCols {
		outerColSet[col.Name] = true
	}
	return conditionsHaveOuterRef(inner.Conditions, outerColSet, outerTableName, outerAlias)
}

func conditionsHaveOuterRef(conds OneOrMore, outerColSet map[string]bool, outerTableName, outerAlias string) bool {
	for _, group := range conds {
		for _, cond := range group {
			for _, op := range [2]Operand{cond.Operand1, cond.Operand2} {
				if op.Type != OperandField {
					continue
				}
				f, ok := op.Value.(Field)
				if !ok {
					continue
				}
				if f.AliasPrefix == outerTableName || (outerAlias != outerTableName && f.AliasPrefix == outerAlias) {
					return true
				}
				// Unqualified name that matches an outer column.
				if f.AliasPrefix == "" && outerColSet[f.Name] {
					return true
				}
			}
		}
	}
	return false
}

// bindOuterRowToStatement returns a copy of inner with all outer-table column
// references in the WHERE conditions replaced by the concrete values from outerRow.
func bindOuterRowToStatement(inner Statement, outerRow Row, outerTableName, outerAlias string) Statement {
	bound := inner
	bound.Conditions = bindOuterRowToConditions(inner.Conditions, outerRow, outerTableName, outerAlias)
	return bound
}

func bindOuterRowToConditions(conds OneOrMore, outerRow Row, outerTableName, outerAlias string) OneOrMore {
	result := make(OneOrMore, len(conds))
	for gi, group := range conds {
		newGroup := make([]Condition, len(group))
		for ci, cond := range group {
			newCond := cond
			newCond.Operand1 = bindOuterOperand(cond.Operand1, outerRow, outerTableName, outerAlias)
			newCond.Operand2 = bindOuterOperand(cond.Operand2, outerRow, outerTableName, outerAlias)
			newGroup[ci] = newCond
		}
		result[gi] = newGroup
	}
	return result
}

func bindOuterOperand(op Operand, outerRow Row, outerTableName, outerAlias string) Operand {
	if op.Type != OperandField {
		return op
	}
	f, ok := op.Value.(Field)
	if !ok {
		return op
	}

	isOuterRef := f.AliasPrefix == outerTableName ||
		(outerAlias != outerTableName && f.AliasPrefix == outerAlias)

	if !isOuterRef {
		return op
	}

	val, ok := outerRow.GetValue(f.Name)
	if !ok {
		return op
	}
	return operandFromOptionalValue(val)
}

// operandFromOptionalValue converts a concrete row value to the matching Operand.
func operandFromOptionalValue(val OptionalValue) Operand {
	if val.IsNull() {
		return Operand{Type: OperandNull}
	}
	switch val.Kind() {
	case ovalInt8:
		return Operand{Type: OperandInteger, Value: val.AsInt8()}
	case ovalInt4:
		return Operand{Type: OperandInteger, Value: int64(val.AsInt4())}
	case ovalDouble:
		return Operand{Type: OperandFloat, Value: val.AsDouble()}
	case ovalReal:
		return Operand{Type: OperandFloat, Value: float64(val.AsReal())}
	case ovalBoolean:
		return Operand{Type: OperandBoolean, Value: val.AsBool()}
	case ovalVarchar, ovalText, ovalJSON:
		return Operand{Type: OperandQuotedString, Value: string(val.AsTextPointer().Data)}
	case ovalTimestamp:
		return Operand{Type: OperandQuotedString, Value: val.AsTimestamp()}
	default:
		return Operand{Type: OperandNull}
	}
}
