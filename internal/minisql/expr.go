package minisql

import (
	"fmt"
	"strings"
)

// ArithOp identifies the arithmetic operator in a binary Expr node.
type ArithOp int

// ArithOp constants.
const (
	ArithAdd ArithOp = iota + 1 // +
	ArithSub                    // -
	ArithMul                    // *
	ArithDiv                    // /
)

func (op ArithOp) String() string {
	switch op {
	case ArithAdd:
		return "+"
	case ArithSub:
		return "-"
	case ArithMul:
		return "*"
	case ArithDiv:
		return "/"
	default:
		return "?"
	}
}

// Expr is an arithmetic expression tree node.
// Exactly one interpretation is active (checked in priority order):
//   - FuncName != "":          a built-in function call (Args holds the arguments)
//   - IsNull:                  an explicit NULL literal
//   - Column != "":            a column reference (read value from the row)
//   - Literal != nil:          a scalar literal (int64, float64, bool, TextPointer)
//   - Left != nil && Op != 0:  a binary arithmetic operation
type Expr struct {
	FuncName string  // built-in function name, e.g. "COALESCE", "NULLIF"
	Args     []*Expr // function arguments (used when FuncName != "")
	IsNull   bool    // true when this node represents an explicit SQL NULL literal
	Column   string  // column reference, may include alias prefix ("u.price")
	Literal  any     // int64, float64, bool, or TextPointer
	Left     *Expr
	Right    *Expr
	Op       ArithOp
}

// String returns a human-readable representation suitable for use as a default column name.
func (e *Expr) String() string {
	return e.str(false)
}

func (e *Expr) str(nested bool) string {
	if e == nil {
		return ""
	}
	if e.FuncName != "" {
		argStrs := make([]string, len(e.Args))
		for i, arg := range e.Args {
			argStrs[i] = arg.str(false)
		}
		return e.FuncName + "(" + strings.Join(argStrs, ", ") + ")"
	}
	if e.IsNull {
		return "NULL"
	}
	if e.Column != "" {
		return e.Column
	}
	if e.Literal != nil {
		return fmt.Sprintf("%v", e.Literal)
	}
	inner := e.Left.str(true) + " " + e.Op.String() + " " + e.Right.str(true)
	if nested {
		return "(" + inner + ")"
	}
	return inner
}

// Columns returns all column names referenced by this expression.
func (e *Expr) Columns() []string {
	if e == nil {
		return nil
	}
	if e.FuncName != "" {
		var cols []string
		for _, arg := range e.Args {
			cols = append(cols, arg.Columns()...)
		}
		return cols
	}
	if e.IsNull {
		return nil
	}
	if e.Column != "" {
		return []string{e.Column}
	}
	if e.Literal != nil {
		return nil
	}
	left := e.Left.Columns()
	right := e.Right.Columns()
	return append(left, right...)
}

// Eval evaluates the expression against a row, returning a numeric result.
// NULL propagates: any NULL operand produces a nil result.
// Returns int64 when both operands are int64 (except division, which always returns float64).
// Returns float64 when either operand is float64.
func (e *Expr) Eval(row Row) (any, error) {
	if e == nil {
		return nil, nil
	}

	// Function call
	if e.FuncName != "" {
		return e.evalFunc(row)
	}

	// Explicit NULL literal
	if e.IsNull {
		return nil, nil
	}

	// Column reference
	if e.Column != "" {
		// Support alias.col lookup (used in JOIN projections)
		name := e.Column
		v, ok := row.GetValue(name)
		if !ok {
			// Try stripping alias prefix for plain column ref
			if dot := strings.LastIndex(name, "."); dot >= 0 {
				v, ok = row.GetValue(name[dot+1:])
			}
			if !ok {
				return nil, fmt.Errorf("column %q not found in row", e.Column)
			}
		}
		if !v.Valid {
			return nil, nil // NULL propagates
		}
		return v.Value, nil
	}

	// Numeric literal
	if e.Literal != nil {
		return e.Literal, nil
	}

	// Binary expression
	leftVal, err := e.Left.Eval(row)
	if err != nil {
		return nil, err
	}
	rightVal, err := e.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	// NULL propagation
	if leftVal == nil || rightVal == nil {
		return nil, nil
	}

	lf, err := toFloat64(leftVal)
	if err != nil {
		return nil, fmt.Errorf("left operand of %s: %w", e.Op, err)
	}
	rf, err := toFloat64(rightVal)
	if err != nil {
		return nil, fmt.Errorf("right operand of %s: %w", e.Op, err)
	}

	// Try integer arithmetic when both sides are integral (avoids float64 rounding).
	li, leftIsInt := toInt64(leftVal)
	ri, rightIsInt := toInt64(rightVal)

	switch e.Op {
	case ArithAdd:
		if leftIsInt && rightIsInt {
			return li + ri, nil
		}
		return lf + rf, nil
	case ArithSub:
		if leftIsInt && rightIsInt {
			return li - ri, nil
		}
		return lf - rf, nil
	case ArithMul:
		if leftIsInt && rightIsInt {
			return li * ri, nil
		}
		return lf * rf, nil
	case ArithDiv:
		if rf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lf / rf, nil
	default:
		return nil, fmt.Errorf("unknown arithmetic operator %d", e.Op)
	}
}

// evalFunc evaluates a built-in function call against the given row.
func (e *Expr) evalFunc(row Row) (any, error) {
	switch e.FuncName {
	case "COALESCE":
		if len(e.Args) == 0 {
			return nil, fmt.Errorf("COALESCE requires at least 1 argument")
		}
		for _, arg := range e.Args {
			val, err := arg.Eval(row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil // all arguments were NULL
	case "NULLIF":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("NULLIF requires exactly 2 arguments, got %d", len(e.Args))
		}
		a, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if a == nil {
			// For example: NULLIF(NULL, x) = NULL
			return nil, nil
		}
		b, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if equalAny(a, b) {
			return nil, nil // equal → return NULL
		}
		return a, nil
	default:
		return nil, fmt.Errorf("unknown function %q", e.FuncName)
	}
}

// toInt64 returns an int64 if v is an integer type (int32 or int64), otherwise false.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	case int32:
		return float64(n), nil
	case float32:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("cannot use %T as a numeric operand", v)
	}
}
