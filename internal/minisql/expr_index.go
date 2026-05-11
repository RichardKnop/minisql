package minisql

import (
	"fmt"
	"reflect"
)

// syntheticExprColumn returns a Column to use as the key column for an expression index.
// Text is promoted to Varchar (same wire format; ForIndex handles Varchar → indexPager[string]).
func syntheticExprColumn(kind ColumnKind) Column {
	if kind == Text || kind == JSON {
		kind = Varchar
	}
	return Column{Name: "__expr__", Kind: kind}
}

// exprSourceColumns returns the names of all table columns referenced in expr.
func exprSourceColumns(expr *Expr) []string {
	if expr == nil {
		return nil
	}
	seen := make(map[string]bool)
	var cols []string
	collectExprColumns(expr, seen, &cols)
	return cols
}

func collectExprColumns(expr *Expr, seen map[string]bool, cols *[]string) {
	if expr == nil {
		return
	}
	if expr.Column != "" && !seen[expr.Column] {
		seen[expr.Column] = true
		*cols = append(*cols, expr.Column)
	}
	for _, arg := range expr.Args {
		collectExprColumns(arg, seen, cols)
	}
	collectExprColumns(expr.Left, seen, cols)
	collectExprColumns(expr.Right, seen, cols)
	collectExprColumns(expr.CastExpr, seen, cols)
	collectExprColumns(expr.CaseInput, seen, cols)
	collectExprColumns(expr.CaseElse, seen, cols)
	for _, cw := range expr.CaseClauses {
		collectExprColumns(cw.When, seen, cols)
		collectExprColumns(cw.Then, seen, cols)
	}
}

// inferExprResultKind infers the B-tree key ColumnKind for an expression index result.
// The caller passes the full table column list so column references can be resolved.
func inferExprResultKind(expr *Expr, tableCols []Column) ColumnKind {
	if expr == nil {
		return Text
	}
	if expr.CastExpr != nil {
		return expr.CastTargetType
	}
	if expr.FuncName != "" {
		switch expr.FuncName {
		case "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "SUBSTR", "REPLACE", "CONCAT":
			return Varchar
		case "MATCH":
			return Boolean
		case "TS_RANK":
			return Double
		case "LENGTH", "MOD":
			return Int8
		case "EXTRACT", "DATE_PART":
			return Int8
		case "ABS":
			if len(expr.Args) == 1 {
				return inferExprResultKind(expr.Args[0], tableCols)
			}
			return Double
		case "FLOOR", "CEIL", "ROUND":
			if len(expr.Args) >= 1 {
				argKind := inferExprResultKind(expr.Args[0], tableCols)
				if argKind == Int8 || argKind == Int4 {
					return Int8
				}
			}
			return Double
		case "DATE_TRUNC", "TO_TIMESTAMP":
			return Timestamp
		default:
			return Text
		}
	}
	if expr.Op == JSONArrowArrow || expr.Op == JSONArrow {
		return Text
	}
	if expr.Left != nil && expr.Op != 0 {
		leftKind := inferExprResultKind(expr.Left, tableCols)
		rightKind := inferExprResultKind(expr.Right, tableCols)
		if isIntegerKind(leftKind) && isIntegerKind(rightKind) {
			return Int8
		}
		return Double
	}
	if expr.Column != "" {
		for _, col := range tableCols {
			if col.Name == expr.Column {
				return col.Kind
			}
		}
		return Text
	}
	if expr.Literal != nil {
		switch expr.Literal.(type) {
		case int64:
			return Int8
		case float64:
			return Double
		case bool:
			return Boolean
		case TextPointer:
			return Varchar
		}
	}
	return Text
}

func isIntegerKind(k ColumnKind) bool {
	return k == Int8 || k == Int4
}

// isImmutableExpr reports whether the expression contains only deterministic sub-expressions.
// NOW() is the only non-deterministic function currently implemented.
func isImmutableExpr(expr *Expr) bool {
	if expr == nil {
		return true
	}
	if expr.FuncName == "NOW" {
		return false
	}
	for _, arg := range expr.Args {
		if !isImmutableExpr(arg) {
			return false
		}
	}
	if !isImmutableExpr(expr.Left) {
		return false
	}
	if !isImmutableExpr(expr.Right) {
		return false
	}
	if !isImmutableExpr(expr.CastExpr) {
		return false
	}
	if !isImmutableExpr(expr.CaseInput) {
		return false
	}
	if !isImmutableExpr(expr.CaseElse) {
		return false
	}
	for _, cw := range expr.CaseClauses {
		if !isImmutableExpr(cw.Then) {
			return false
		}
		if !isImmutableExpr(cw.When) {
			return false
		}
	}
	return true
}

// evalExprIndexKey evaluates the expression against row and returns the B-tree key value.
// Returns (nil, false, nil) when the expression evaluates to NULL (row should not be indexed).
func evalExprIndexKey(expr *Expr, syntheticCol Column, row Row) (any, bool, error) {
	val, err := expr.Eval(row)
	if err != nil {
		return nil, false, fmt.Errorf("expression index eval: %w", err)
	}
	if val == nil {
		return nil, false, nil
	}
	key, err := castKeyValue(syntheticCol, val)
	if err != nil {
		return nil, false, fmt.Errorf("expression index key cast: %w", err)
	}
	return key, true, nil
}

// exprEqual reports whether two expression trees are structurally identical
// using deep value comparison.
func exprEqual(a, b *Expr) bool {
	return reflect.DeepEqual(a, b)
}
