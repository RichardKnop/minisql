package minisql

import (
	"context"
	"fmt"
)

// resolveSubqueries walks stmt.Conditions, finds any OperandSubquery operands,
// executes the subquery using the same transaction already in ctx, and replaces
// each OperandSubquery with a concrete scalar or list value so that downstream
// condition evaluation can proceed without any knowledge of subqueries.
//
// For scalar operators (=, !=, <, <=, >, >=) the subquery must return exactly
// one column and at most one row.  Zero rows resolves to NULL.
// For IN / NOT IN the subquery must return exactly one column; all rows are
// collected into an OperandList.
func (d *Database) resolveSubqueries(ctx context.Context, conditions OneOrMore) (OneOrMore, error) {
	for i, condGroup := range conditions {
		for j, cond := range condGroup {
			if cond.Operand2.Type != OperandSubquery {
				continue
			}
			subStmt := *cond.Operand2.Value.(*Statement)

			result, err := d.ExecuteStatement(ctx, subStmt)
			if err != nil {
				return nil, fmt.Errorf("subquery: %w", err)
			}
			if len(result.Columns) != 1 {
				return nil, fmt.Errorf("subquery must return exactly one column, got %d", len(result.Columns))
			}

			switch cond.Operator {
			case In, NotIn:
				seen := make(map[any]struct{})
				var values []any
				for result.Rows.Next(ctx) {
					row := result.Rows.Row()
					if len(row.Values) > 0 && row.Values[0].Valid {
						v := row.Values[0].Value
						if _, dup := seen[v]; !dup {
							seen[v] = struct{}{}
							values = append(values, v)
						}
					}
				}
				if err := result.Rows.Err(); err != nil {
					return nil, fmt.Errorf("subquery: reading rows: %w", err)
				}
				if values == nil {
					values = []any{}
				}
				cond.Operand2 = Operand{Type: OperandList, Value: values}

			default:
				// Scalar subquery: at most one row expected.
				if !result.Rows.Next(ctx) {
					if err := result.Rows.Err(); err != nil {
						return nil, fmt.Errorf("subquery: reading row: %w", err)
					}
					// Zero rows → NULL.
					cond.Operand2 = Operand{Type: OperandNull}
					conditions[i][j] = cond
					continue
				}
				row := result.Rows.Row()
				// Guard against more than one row.
				if result.Rows.Next(ctx) {
					return nil, fmt.Errorf("scalar subquery returned more than one row")
				}
				if err := result.Rows.Err(); err != nil {
					return nil, fmt.Errorf("subquery: reading row: %w", err)
				}
				if len(row.Values) == 0 || !row.Values[0].Valid {
					cond.Operand2 = Operand{Type: OperandNull}
				} else {
					val := row.Values[0].Value
					cond.Operand2 = Operand{
						Type:  scalarOperandType(val),
						Value: val,
					}
				}
			}
			conditions[i][j] = cond
		}
	}
	return conditions, nil
}

// scalarOperandType returns the OperandType for a concrete Go value that came
// from a subquery result row.  TimestampMicros is mapped to OperandQuotedString
// because the condition evaluator already handles TextPointer/TimestampMicros
// comparisons at the column-type level.
func scalarOperandType(v any) OperandType {
	switch v.(type) {
	case int64, int32:
		return OperandInteger
	case float64, float32:
		return OperandFloat
	case bool:
		return OperandBoolean
	case TimestampMicros:
		return OperandQuotedString
	default:
		return OperandQuotedString
	}
}
