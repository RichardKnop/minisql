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
// Exactly one interpretation is active:
//   - Column != "":           a column reference (read value from the row)
//   - Literal != nil:         a numeric literal (int64 or float64)
//   - Left != nil && Op != 0: a binary operation
type Expr struct {
	Column  string // column reference, may include alias prefix ("u.price")
	Literal any    // int64 or float64
	Left    *Expr
	Right   *Expr
	Op      ArithOp
}

// String returns a human-readable representation suitable for use as a default column name.
func (e *Expr) String() string {
	return e.str(false)
}

func (e *Expr) str(nested bool) string {
	if e == nil {
		return ""
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
