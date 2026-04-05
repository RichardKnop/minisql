package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rowWithInt(name string, val int64) Row {
	return NewRowWithValues(
		[]Column{{Name: name, Kind: Int8}},
		[]OptionalValue{{Value: val, Valid: true}},
	)
}

func rowWithFloat(name string, val float64) Row {
	return NewRowWithValues(
		[]Column{{Name: name, Kind: Double}},
		[]OptionalValue{{Value: val, Valid: true}},
	)
}

func rowWithNull(name string) Row {
	return NewRowWithValues(
		[]Column{{Name: name, Kind: Int8}},
		[]OptionalValue{{Valid: false}},
	)
}

func TestExpr_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "price", (&Expr{Column: "price"}).String())
	assert.Equal(t, "42", (&Expr{Literal: int64(42)}).String())
	assert.Equal(t, "3.14", (&Expr{Literal: float64(3.14)}).String())
	// Top-level binary: no outer parens
	assert.Equal(t, "price + 1", (&Expr{
		Left:  &Expr{Column: "price"},
		Right: &Expr{Literal: int64(1)},
		Op:    ArithAdd,
	}).String())
	// Nested left side: inner expr gets parens
	assert.Equal(t, "(a * b) + c", (&Expr{
		Left: &Expr{
			Left:  &Expr{Column: "a"},
			Right: &Expr{Column: "b"},
			Op:    ArithMul,
		},
		Right: &Expr{Column: "c"},
		Op:    ArithAdd,
	}).String())
	assert.Equal(t, "", (*Expr)(nil).String())
}

func TestExpr_Columns(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"price"}, (&Expr{Column: "price"}).Columns())
	assert.Nil(t, (&Expr{Literal: int64(1)}).Columns())
	assert.Equal(t, []string{"a", "b"}, (&Expr{
		Left:  &Expr{Column: "a"},
		Right: &Expr{Column: "b"},
		Op:    ArithAdd,
	}).Columns())
	// nested: a + b * c → [a, b, c]
	assert.Equal(t, []string{"a", "b", "c"}, (&Expr{
		Left: &Expr{Column: "a"},
		Right: &Expr{
			Left:  &Expr{Column: "b"},
			Right: &Expr{Column: "c"},
			Op:    ArithMul,
		},
		Op: ArithAdd,
	}).Columns())
}

func TestExpr_Eval_Literal(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)

	res, err := (&Expr{Literal: int64(7)}).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(7), res)

	res, err = (&Expr{Literal: float64(3.5)}).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, float64(3.5), res)
}

func TestExpr_Eval_ColumnRef(t *testing.T) {
	t.Parallel()

	row := rowWithInt("count", 10)

	res, err := (&Expr{Column: "count"}).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(10), res)
}

func TestExpr_Eval_ColumnRef_NullPropagates(t *testing.T) {
	t.Parallel()

	row := rowWithNull("count")
	res, err := (&Expr{Column: "count"}).Eval(row)
	require.NoError(t, err)
	assert.Nil(t, res)
}

func TestExpr_Eval_ColumnNotFound(t *testing.T) {
	t.Parallel()

	row := rowWithInt("count", 10)
	_, err := (&Expr{Column: "missing"}).Eval(row)
	assert.ErrorContains(t, err, "not found")
}

func TestExpr_Eval_IntegerArithmetic(t *testing.T) {
	t.Parallel()

	row := rowWithInt("n", 10)

	expr := func(op ArithOp, rhs int64) *Expr {
		return &Expr{
			Left:  &Expr{Column: "n"},
			Right: &Expr{Literal: rhs},
			Op:    op,
		}
	}

	res, err := expr(ArithAdd, 5).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(15), res)

	res, err = expr(ArithSub, 3).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(7), res)

	res, err = expr(ArithMul, 4).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(40), res)

	// Division always produces float64
	res, err = expr(ArithDiv, 4).Eval(row)
	require.NoError(t, err)
	assert.Equal(t, float64(2.5), res)
}

func TestExpr_Eval_FloatArithmetic(t *testing.T) {
	t.Parallel()

	row := rowWithFloat("price", 100.0)

	res, err := (&Expr{
		Left:  &Expr{Column: "price"},
		Right: &Expr{Literal: float64(1.1)},
		Op:    ArithMul,
	}).Eval(row)
	require.NoError(t, err)
	assert.InDelta(t, float64(110.0), res, 0.0001)
}

func TestExpr_Eval_NullPropagation(t *testing.T) {
	t.Parallel()

	row := rowWithNull("n")

	res, err := (&Expr{
		Left:  &Expr{Column: "n"},
		Right: &Expr{Literal: int64(5)},
		Op:    ArithAdd,
	}).Eval(row)
	require.NoError(t, err)
	assert.Nil(t, res, "NULL operand should produce NULL result")
}

func TestExpr_Eval_DivisionByZero(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)

	_, err := (&Expr{
		Left:  &Expr{Literal: int64(10)},
		Right: &Expr{Literal: int64(0)},
		Op:    ArithDiv,
	}).Eval(row)
	assert.ErrorContains(t, err, "division by zero")
}

func TestExpr_Eval_Nested(t *testing.T) {
	t.Parallel()

	// a + b * c where a=2, b=3, c=4 → 2 + (3*4) = 14
	row := NewRowWithValues(
		[]Column{
			{Name: "a", Kind: Int8},
			{Name: "b", Kind: Int8},
			{Name: "c", Kind: Int8},
		},
		[]OptionalValue{
			{Value: int64(2), Valid: true},
			{Value: int64(3), Valid: true},
			{Value: int64(4), Valid: true},
		},
	)

	expr := &Expr{
		Left: &Expr{Column: "a"},
		Right: &Expr{
			Left:  &Expr{Column: "b"},
			Right: &Expr{Column: "c"},
			Op:    ArithMul,
		},
		Op: ArithAdd,
	}

	res, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(14), res)
}

func TestExpr_Eval_NilExpr(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)
	res, err := (*Expr)(nil).Eval(row)
	require.NoError(t, err)
	assert.Nil(t, res)
}
