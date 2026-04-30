package minisql

import (
	"testing"
	"time"

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

func TestExpr_Eval_NullLiteral(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)
	res, err := (&Expr{IsNull: true}).Eval(row)
	require.NoError(t, err)
	assert.Nil(t, res)
}

func TestExpr_String_FuncCall(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "COALESCE(a, b)", (&Expr{
		FuncName: "COALESCE",
		Args:     []*Expr{{Column: "a"}, {Column: "b"}},
	}).String())
	assert.Equal(t, "NULLIF(x, 0)", (&Expr{
		FuncName: "NULLIF",
		Args:     []*Expr{{Column: "x"}, {Literal: int64(0)}},
	}).String())
	assert.Equal(t, "NULL", (&Expr{IsNull: true}).String())
}

func TestExpr_Columns_FuncCall(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		FuncName: "COALESCE",
		Args:     []*Expr{{Column: "a"}, {Column: "b"}, {Literal: int64(0)}},
	}
	assert.Equal(t, []string{"a", "b"}, expr.Columns())
}

func TestExpr_Eval_COALESCE(t *testing.T) {
	t.Parallel()

	t.Run("returns first non-null", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(
			[]Column{{Name: "a", Kind: Int8}, {Name: "b", Kind: Int8}},
			[]OptionalValue{{Valid: false}, {Value: int64(42), Valid: true}},
		)
		res, err := (&Expr{
			FuncName: "COALESCE",
			Args:     []*Expr{{Column: "a"}, {Column: "b"}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(42), res)
	})

	t.Run("returns first arg when non-null", func(t *testing.T) {
		t.Parallel()
		row := rowWithInt("x", 7)
		res, err := (&Expr{
			FuncName: "COALESCE",
			Args:     []*Expr{{Column: "x"}, {Literal: int64(99)}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(7), res)
	})

	t.Run("all null returns null", func(t *testing.T) {
		t.Parallel()
		row := rowWithNull("x")
		res, err := (&Expr{
			FuncName: "COALESCE",
			Args:     []*Expr{{Column: "x"}, {IsNull: true}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("literal fallback", func(t *testing.T) {
		t.Parallel()
		row := rowWithNull("x")
		res, err := (&Expr{
			FuncName: "COALESCE",
			Args:     []*Expr{{Column: "x"}, {Literal: int64(0)}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(0), res)
	})

	t.Run("error when no args", func(t *testing.T) {
		t.Parallel()
		row := NewRow(nil)
		_, err := (&Expr{FuncName: "COALESCE", Args: nil}).Eval(row)
		assert.ErrorContains(t, err, "at least 1 argument")
	})
}

func TestExpr_Eval_NULLIF(t *testing.T) {
	t.Parallel()

	t.Run("returns null when equal", func(t *testing.T) {
		t.Parallel()
		row := rowWithInt("x", 5)
		res, err := (&Expr{
			FuncName: "NULLIF",
			Args:     []*Expr{{Column: "x"}, {Literal: int64(5)}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("returns first when not equal", func(t *testing.T) {
		t.Parallel()
		row := rowWithInt("x", 5)
		res, err := (&Expr{
			FuncName: "NULLIF",
			Args:     []*Expr{{Column: "x"}, {Literal: int64(0)}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(5), res)
	})

	t.Run("first arg null returns null", func(t *testing.T) {
		t.Parallel()
		row := rowWithNull("x")
		res, err := (&Expr{
			FuncName: "NULLIF",
			Args:     []*Expr{{Column: "x"}, {Literal: int64(0)}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("second arg null never equal", func(t *testing.T) {
		t.Parallel()
		row := rowWithInt("x", 5)
		res, err := (&Expr{
			FuncName: "NULLIF",
			Args:     []*Expr{{Column: "x"}, {IsNull: true}},
		}).Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(5), res) // NULL != 5 → return 5
	})

	t.Run("error when wrong arg count", func(t *testing.T) {
		t.Parallel()
		row := NewRow(nil)
		_, err := (&Expr{FuncName: "NULLIF", Args: []*Expr{{Literal: int64(1)}}}).Eval(row)
		assert.ErrorContains(t, err, "exactly 2 arguments")
	})
}

// rowWithText returns a single-column row containing a TEXT value.
func rowWithText(name, val string) Row {
	return NewRowWithValues(
		[]Column{{Name: name, Kind: Text}},
		[]OptionalValue{{Value: NewTextPointer([]byte(val)), Valid: true}},
	)
}

// textExpr builds a literal TextPointer expression.
func textExpr(s string) *Expr {
	return &Expr{Literal: NewTextPointer([]byte(s))}
}

// evalText is a test helper: evaluates expr, asserts no error and returns the
// string content of the resulting TextPointer.
func evalText(t *testing.T, e *Expr, row Row) string {
	t.Helper()
	v, err := e.Eval(row)
	require.NoError(t, err)
	tp, ok := v.(TextPointer)
	require.True(t, ok, "expected TextPointer result, got %T", v)
	return string(tp.Data)
}

func TestExpr_Eval_UPPER_LOWER(t *testing.T) {
	t.Parallel()

	row := rowWithText("name", "Hello World")

	t.Run("UPPER", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "UPPER", Args: []*Expr{{Column: "name"}}}
		assert.Equal(t, "HELLO WORLD", evalText(t, e, row))
	})

	t.Run("LOWER", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		assert.Equal(t, "hello world", evalText(t, e, row))
	})

	t.Run("UPPER null propagates", func(t *testing.T) {
		t.Parallel()
		nullRow := NewRowWithValues([]Column{{Name: "name", Kind: Text}}, []OptionalValue{{Valid: false}})
		v, err := (&Expr{FuncName: "UPPER", Args: []*Expr{{Column: "name"}}}).Eval(nullRow)
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("UPPER on literal", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "UPPER", Args: []*Expr{textExpr("hello")}}
		assert.Equal(t, "HELLO", evalText(t, e, NewRow(nil)))
	})

	t.Run("wrong arg count", func(t *testing.T) {
		t.Parallel()
		_, err := (&Expr{FuncName: "UPPER", Args: nil}).Eval(NewRow(nil))
		assert.ErrorContains(t, err, "exactly 1 argument")
	})
}

func TestExpr_Eval_TRIM(t *testing.T) {
	t.Parallel()

	t.Run("TRIM whitespace", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "TRIM", Args: []*Expr{textExpr("  hello  ")}}
		assert.Equal(t, "hello", evalText(t, e, NewRow(nil)))
	})

	t.Run("LTRIM whitespace", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "LTRIM", Args: []*Expr{textExpr("  hello  ")}}
		assert.Equal(t, "hello  ", evalText(t, e, NewRow(nil)))
	})

	t.Run("RTRIM whitespace", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "RTRIM", Args: []*Expr{textExpr("  hello  ")}}
		assert.Equal(t, "  hello", evalText(t, e, NewRow(nil)))
	})

	t.Run("TRIM custom cutset", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "TRIM", Args: []*Expr{textExpr("xxhelloxx"), textExpr("x")}}
		assert.Equal(t, "hello", evalText(t, e, NewRow(nil)))
	})

	t.Run("TRIM null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "TRIM", Args: []*Expr{{IsNull: true}}}).Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("already trimmed string is unchanged", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "TRIM", Args: []*Expr{textExpr("hello")}}
		assert.Equal(t, "hello", evalText(t, e, NewRow(nil)))
	})
}

func TestExpr_Eval_LENGTH(t *testing.T) {
	t.Parallel()

	t.Run("byte length of ASCII string", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "LENGTH", Args: []*Expr{textExpr("hello")}}
		v, err := e.Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Equal(t, int64(5), v)
	})

	t.Run("empty string has length 0", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "LENGTH", Args: []*Expr{textExpr("")}}
		v, err := e.Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Equal(t, int64(0), v)
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "LENGTH", Args: []*Expr{{IsNull: true}}}).Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("from column", func(t *testing.T) {
		t.Parallel()
		row := rowWithText("s", "abcde")
		e := &Expr{FuncName: "LENGTH", Args: []*Expr{{Column: "s"}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(5), v)
	})
}

func TestExpr_Eval_SUBSTR(t *testing.T) {
	t.Parallel()

	t.Run("from start to end", func(t *testing.T) {
		t.Parallel()
		// SUBSTR('hello', 2) = 'ello'
		e := &Expr{FuncName: "SUBSTR", Args: []*Expr{textExpr("hello"), {Literal: int64(2)}}}
		assert.Equal(t, "ello", evalText(t, e, NewRow(nil)))
	})

	t.Run("with length", func(t *testing.T) {
		t.Parallel()
		// SUBSTR('hello', 2, 3) = 'ell'
		e := &Expr{FuncName: "SUBSTR", Args: []*Expr{textExpr("hello"), {Literal: int64(2)}, {Literal: int64(3)}}}
		assert.Equal(t, "ell", evalText(t, e, NewRow(nil)))
	})

	t.Run("start at 1", func(t *testing.T) {
		t.Parallel()
		// SUBSTR('hello', 1, 3) = 'hel'
		e := &Expr{FuncName: "SUBSTR", Args: []*Expr{textExpr("hello"), {Literal: int64(1)}, {Literal: int64(3)}}}
		assert.Equal(t, "hel", evalText(t, e, NewRow(nil)))
	})

	t.Run("start beyond end returns empty", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "SUBSTR", Args: []*Expr{textExpr("hi"), {Literal: int64(10)}}}
		assert.Equal(t, "", evalText(t, e, NewRow(nil)))
	})

	t.Run("length exceeds string returns remainder", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "SUBSTR", Args: []*Expr{textExpr("hello"), {Literal: int64(3)}, {Literal: int64(100)}}}
		assert.Equal(t, "llo", evalText(t, e, NewRow(nil)))
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "SUBSTR", Args: []*Expr{{IsNull: true}, {Literal: int64(1)}}}).Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

func TestExpr_Eval_REPLACE(t *testing.T) {
	t.Parallel()

	t.Run("replaces all occurrences", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "REPLACE", Args: []*Expr{textExpr("aabbaa"), textExpr("aa"), textExpr("x")}}
		assert.Equal(t, "xbbx", evalText(t, e, NewRow(nil)))
	})

	t.Run("no match returns original", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "REPLACE", Args: []*Expr{textExpr("hello"), textExpr("z"), textExpr("x")}}
		assert.Equal(t, "hello", evalText(t, e, NewRow(nil)))
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "REPLACE", Args: []*Expr{{IsNull: true}, textExpr("a"), textExpr("b")}}).Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

func TestExpr_Eval_CONCAT(t *testing.T) {
	t.Parallel()

	t.Run("concatenates strings", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "CONCAT", Args: []*Expr{textExpr("hello"), textExpr(", "), textExpr("world")}}
		assert.Equal(t, "hello, world", evalText(t, e, NewRow(nil)))
	})

	t.Run("skips null args", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "CONCAT", Args: []*Expr{textExpr("a"), {IsNull: true}, textExpr("b")}}
		assert.Equal(t, "ab", evalText(t, e, NewRow(nil)))
	})

	t.Run("all null returns empty string", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "CONCAT", Args: []*Expr{{IsNull: true}, {IsNull: true}}}
		assert.Equal(t, "", evalText(t, e, NewRow(nil)))
	})

	t.Run("from columns", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(
			[]Column{{Name: "first", Kind: Text}, {Name: "last", Kind: Text}},
			[]OptionalValue{
				{Value: NewTextPointer([]byte("John")), Valid: true},
				{Value: NewTextPointer([]byte("Doe")), Valid: true},
			},
		)
		e := &Expr{FuncName: "CONCAT", Args: []*Expr{{Column: "first"}, textExpr(" "), {Column: "last"}}}
		assert.Equal(t, "John Doe", evalText(t, e, row))
	})
}

func TestExpr_Eval_StringFunctions_NestInArithmetic(t *testing.T) {
	t.Parallel()

	// LENGTH('hello') + 1 = 6
	e := &Expr{
		Left:  &Expr{FuncName: "LENGTH", Args: []*Expr{textExpr("hello")}},
		Right: &Expr{Literal: int64(1)},
		Op:    ArithAdd,
	}
	v, err := e.Eval(NewRow(nil))
	require.NoError(t, err)
	assert.Equal(t, int64(6), v)
}

func TestExpr_Eval_ABS(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)

	cases := []struct {
		arg  any
		want any
		name string
	}{
		{name: "positive int64", arg: int64(5), want: int64(5)},
		{name: "negative int64", arg: int64(-5), want: int64(5)},
		{name: "zero int64", arg: int64(0), want: int64(0)},
		{name: "positive float64", arg: float64(3.14), want: float64(3.14)},
		{name: "negative float64", arg: float64(-3.14), want: float64(3.14)},
		{name: "positive int32", arg: int32(7), want: int32(7)},
		{name: "negative int32", arg: int32(-7), want: int32(7)},
		{name: "positive float32", arg: float32(1.5), want: float32(1.5)},
		{name: "negative float32", arg: float32(-1.5), want: float32(1.5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &Expr{FuncName: "ABS", Args: []*Expr{{Literal: tc.arg}}}
			v, err := e.Eval(row)
			require.NoError(t, err)
			assert.Equal(t, tc.want, v)
		})
	}

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "ABS", Args: []*Expr{{IsNull: true}}}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("wrong arg count", func(t *testing.T) {
		t.Parallel()
		_, err := (&Expr{FuncName: "ABS", Args: nil}).Eval(row)
		assert.ErrorContains(t, err, "exactly 1 argument")
	})
}

func TestExpr_Eval_FLOOR_CEIL(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)

	t.Run("FLOOR float", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "FLOOR", Args: []*Expr{{Literal: float64(3.9)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, float64(3), v)
	})

	t.Run("CEIL float", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "CEIL", Args: []*Expr{{Literal: float64(3.1)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, float64(4), v)
	})

	t.Run("FLOOR integer is unchanged", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "FLOOR", Args: []*Expr{{Literal: int64(5)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(5), v)
	})

	t.Run("CEIL integer is unchanged", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "CEIL", Args: []*Expr{{Literal: int64(5)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(5), v)
	})

	t.Run("FLOOR negative float", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "FLOOR", Args: []*Expr{{Literal: float64(-2.3)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, float64(-3), v)
	})

	t.Run("CEIL negative float", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "CEIL", Args: []*Expr{{Literal: float64(-2.7)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, float64(-2), v)
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "FLOOR", Args: []*Expr{{IsNull: true}}}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

func TestExpr_Eval_ROUND(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)

	t.Run("round to nearest integer", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "ROUND", Args: []*Expr{{Literal: float64(3.5)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, float64(4), v)
	})

	t.Run("round down", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "ROUND", Args: []*Expr{{Literal: float64(3.4)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, float64(3), v)
	})

	t.Run("round to 2 decimal places", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "ROUND", Args: []*Expr{{Literal: float64(3.14159)}, {Literal: int64(2)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.InDelta(t, float64(3.14), v, 1e-9)
	})

	t.Run("integer input is unchanged", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "ROUND", Args: []*Expr{{Literal: int64(7)}, {Literal: int64(2)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(7), v)
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "ROUND", Args: []*Expr{{IsNull: true}}}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("wrong arg count", func(t *testing.T) {
		t.Parallel()
		_, err := (&Expr{FuncName: "ROUND", Args: nil}).Eval(row)
		assert.ErrorContains(t, err, "1 or 2 arguments")
	})
}

func TestExpr_Eval_MOD(t *testing.T) {
	t.Parallel()

	row := NewRow(nil)

	t.Run("integer modulo", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "MOD", Args: []*Expr{{Literal: int64(10)}, {Literal: int64(3)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(1), v)
	})

	t.Run("exact division gives zero", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "MOD", Args: []*Expr{{Literal: int64(9)}, {Literal: int64(3)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.Equal(t, int64(0), v)
	})

	t.Run("float modulo", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "MOD", Args: []*Expr{{Literal: float64(10.5)}, {Literal: float64(3.0)}}}
		v, err := e.Eval(row)
		require.NoError(t, err)
		assert.InDelta(t, float64(1.5), v, 1e-9)
	})

	t.Run("division by zero errors", func(t *testing.T) {
		t.Parallel()
		_, err := (&Expr{FuncName: "MOD", Args: []*Expr{{Literal: int64(5)}, {Literal: int64(0)}}}).Eval(row)
		assert.ErrorContains(t, err, "division by zero")
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "MOD", Args: []*Expr{{IsNull: true}, {Literal: int64(3)}}}).Eval(row)
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("wrong arg count", func(t *testing.T) {
		t.Parallel()
		_, err := (&Expr{FuncName: "MOD", Args: []*Expr{{Literal: int64(1)}}}).Eval(row)
		assert.ErrorContains(t, err, "exactly 2 arguments")
	})
}

// rowWithTimestamp returns a single-column row containing a TIMESTAMP value.
func rowWithTimestamp(name string, ts TimestampMicros) Row {
	return NewRowWithValues(
		[]Column{{Name: name, Kind: Timestamp}},
		[]OptionalValue{{Value: ts, Valid: true}},
	)
}

func TestExpr_Eval_NOW(t *testing.T) {
	t.Parallel()

	// Truncate to microsecond precision: Time stores only microseconds, so GoTime()
	// always returns a time whose nanosecond field is a multiple of 1000.  A
	// nanosecond-precision `before` (e.g. .123456789) would compare as after the
	// truncated result (.123456000), causing a spurious "returned time before call"
	// failure.
	before := time.Now().UTC().Truncate(time.Microsecond)
	v, err := (&Expr{FuncName: "NOW"}).Eval(NewRow(nil))
	after := time.Now().UTC()

	require.NoError(t, err)
	ts, ok := v.(TimestampMicros)
	require.True(t, ok, "expected TimestampMicros result, got %T", v)

	// The returned timestamp should be between before and after.
	got := FromMicroseconds(int64(ts)).GoTime()
	assert.False(t, got.Before(before), "NOW() returned time before call")
	assert.False(t, got.After(after), "NOW() returned time after call")
}

func TestExpr_Eval_DATE_TRUNC(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestampMicros("2024-06-15 14:32:45.123456")
	row := rowWithTimestamp("ts", ts)

	cases := []struct {
		unit string
		want TimestampMicros
	}{
		{"year", MustParseTimestampMicros("2024-01-01 00:00:00")},
		{"month", MustParseTimestampMicros("2024-06-01 00:00:00")},
		{"day", MustParseTimestampMicros("2024-06-15 00:00:00")},
		{"hour", MustParseTimestampMicros("2024-06-15 14:00:00")},
		{"minute", MustParseTimestampMicros("2024-06-15 14:32:00")},
		{"second", MustParseTimestampMicros("2024-06-15 14:32:45")},
	}
	for _, tc := range cases {
		t.Run(tc.unit, func(t *testing.T) {
			t.Parallel()
			e := &Expr{
				FuncName: "DATE_TRUNC",
				Args:     []*Expr{textExpr(tc.unit), {Column: "ts"}},
			}
			v, err := e.Eval(row)
			require.NoError(t, err)
			assert.Equal(t, tc.want, v)
		})
	}

	t.Run("unknown unit errors", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "DATE_TRUNC", Args: []*Expr{textExpr("week"), {Column: "ts"}}}
		_, err := e.Eval(row)
		assert.ErrorContains(t, err, "unknown unit")
	})

	t.Run("null timestamp propagates", func(t *testing.T) {
		t.Parallel()
		nullRow := NewRowWithValues([]Column{{Name: "ts", Kind: Timestamp}}, []OptionalValue{{Valid: false}})
		e := &Expr{FuncName: "DATE_TRUNC", Args: []*Expr{textExpr("day"), {Column: "ts"}}}
		v, err := e.Eval(nullRow)
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

func TestExpr_Eval_EXTRACT(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestampMicros("2024-06-15 14:32:45.123456")
	row := rowWithTimestamp("ts", ts)

	cases := []struct {
		field string
		want  int64
	}{
		{"year", 2024},
		{"month", 6},
		{"day", 15},
		{"hour", 14},
		{"minute", 32},
		{"second", 45},
		{"microsecond", 123456},
	}
	for _, tc := range cases {
		t.Run("EXTRACT "+tc.field, func(t *testing.T) {
			t.Parallel()
			e := &Expr{FuncName: "EXTRACT", Args: []*Expr{textExpr(tc.field), {Column: "ts"}}}
			v, err := e.Eval(row)
			require.NoError(t, err)
			assert.Equal(t, tc.want, v)
		})
		t.Run("DATE_PART "+tc.field, func(t *testing.T) {
			t.Parallel()
			e := &Expr{FuncName: "DATE_PART", Args: []*Expr{textExpr(tc.field), {Column: "ts"}}}
			v, err := e.Eval(row)
			require.NoError(t, err)
			assert.Equal(t, tc.want, v)
		})
	}

	t.Run("unknown field errors", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "EXTRACT", Args: []*Expr{textExpr("quarter"), {Column: "ts"}}}
		_, err := e.Eval(row)
		assert.ErrorContains(t, err, "unknown field")
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		nullRow := NewRowWithValues([]Column{{Name: "ts", Kind: Timestamp}}, []OptionalValue{{Valid: false}})
		e := &Expr{FuncName: "EXTRACT", Args: []*Expr{textExpr("year"), {Column: "ts"}}}
		v, err := e.Eval(nullRow)
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

func TestExpr_Eval_TO_TIMESTAMP(t *testing.T) {
	t.Parallel()

	t.Run("parses valid timestamp string", func(t *testing.T) {
		t.Parallel()
		e := &Expr{FuncName: "TO_TIMESTAMP", Args: []*Expr{textExpr("2024-03-20 10:15:30")}}
		v, err := e.Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Equal(t, MustParseTimestampMicros("2024-03-20 10:15:30"), v)
	})

	t.Run("null propagates", func(t *testing.T) {
		t.Parallel()
		v, err := (&Expr{FuncName: "TO_TIMESTAMP", Args: []*Expr{{IsNull: true}}}).Eval(NewRow(nil))
		require.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("invalid string errors", func(t *testing.T) {
		t.Parallel()
		_, err := (&Expr{FuncName: "TO_TIMESTAMP", Args: []*Expr{textExpr("not-a-date")}}).Eval(NewRow(nil))
		assert.ErrorContains(t, err, "TO_TIMESTAMP")
	})
}

// ── CASE WHEN ────────────────────────────────────────────────────────────────

// caseRow builds a row with an int8 column "score" and a text column "status".
func caseRow(score int64, status string) Row {
	return NewRowWithValues(
		[]Column{
			{Name: "score", Kind: Int8},
			{Name: "status", Kind: Text},
		},
		[]OptionalValue{
			{Value: score, Valid: true},
			{Value: NewTextPointer([]byte(status)), Valid: true},
		},
	)
}

func TestExpr_Eval_CaseWhen_Searched(t *testing.T) {
	t.Parallel()

	// CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END
	caseExpr := &Expr{
		CaseClauses: []CaseWhen{
			{
				Cond: &ConditionNode{Leaf: &Condition{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
					Operator: Gte,
					Operand2: Operand{Type: OperandInteger, Value: int64(90)},
				}},
				Then: textExpr("A"),
			},
			{
				Cond: &ConditionNode{Leaf: &Condition{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
					Operator: Gte,
					Operand2: Operand{Type: OperandInteger, Value: int64(70)},
				}},
				Then: textExpr("B"),
			},
		},
		CaseElse: textExpr("C"),
	}

	t.Run("first clause matches", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "A", evalText(t, caseExpr, caseRow(95, "")))
	})

	t.Run("second clause matches", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "B", evalText(t, caseExpr, caseRow(75, "")))
	})

	t.Run("else branch", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "C", evalText(t, caseExpr, caseRow(50, "")))
	})
}

func TestExpr_Eval_CaseWhen_SearchedNoElse(t *testing.T) {
	t.Parallel()

	// CASE WHEN score >= 90 THEN 'A' END  (no ELSE → returns NULL when unmatched)
	caseExpr := &Expr{
		CaseClauses: []CaseWhen{
			{
				Cond: &ConditionNode{Leaf: &Condition{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
					Operator: Gte,
					Operand2: Operand{Type: OperandInteger, Value: int64(90)},
				}},
				Then: textExpr("A"),
			},
		},
	}

	v, err := caseExpr.Eval(caseRow(50, ""))
	require.NoError(t, err)
	assert.Nil(t, v, "unmatched CASE without ELSE should return NULL")
}

func TestExpr_Eval_CaseWhen_Simple(t *testing.T) {
	t.Parallel()

	// CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END
	caseExpr := &Expr{
		CaseInput: &Expr{Column: "status"},
		CaseClauses: []CaseWhen{
			{When: textExpr("active"), Then: &Expr{Literal: int64(1)}},
			{When: textExpr("pending"), Then: &Expr{Literal: int64(2)}},
		},
		CaseElse: &Expr{Literal: int64(0)},
	}

	t.Run("first value matches", func(t *testing.T) {
		t.Parallel()
		v, err := caseExpr.Eval(caseRow(0, "active"))
		require.NoError(t, err)
		assert.Equal(t, int64(1), v)
	})

	t.Run("second value matches", func(t *testing.T) {
		t.Parallel()
		v, err := caseExpr.Eval(caseRow(0, "pending"))
		require.NoError(t, err)
		assert.Equal(t, int64(2), v)
	})

	t.Run("else branch", func(t *testing.T) {
		t.Parallel()
		v, err := caseExpr.Eval(caseRow(0, "inactive"))
		require.NoError(t, err)
		assert.Equal(t, int64(0), v)
	})
}

func TestExpr_Eval_CaseWhen_String(t *testing.T) {
	t.Parallel()

	// Searched CASE: CASE WHEN score >= 90 THEN A ELSE C END
	e := &Expr{
		CaseClauses: []CaseWhen{
			{
				Cond: &ConditionNode{Leaf: &Condition{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
					Operator: Gte,
					Operand2: Operand{Type: OperandInteger, Value: int64(90)},
				}},
				Then: textExpr("A"),
			},
		},
		CaseElse: textExpr("C"),
	}
	assert.Equal(t, "CASE WHEN score >= 90 THEN A ELSE C END", e.String())

	// Simple CASE: CASE status WHEN active THEN 1 END
	e2 := &Expr{
		CaseInput: &Expr{Column: "status"},
		CaseClauses: []CaseWhen{
			{When: textExpr("active"), Then: &Expr{Literal: int64(1)}},
		},
	}
	assert.Equal(t, "CASE status WHEN active THEN 1 END", e2.String())
}

func TestExpr_Eval_CaseWhen_Columns(t *testing.T) {
	t.Parallel()

	// CASE WHEN score >= 90 THEN grade ELSE 'F' END
	e := &Expr{
		CaseClauses: []CaseWhen{
			{
				Cond: &ConditionNode{Leaf: &Condition{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
					Operator: Gte,
					Operand2: Operand{Type: OperandInteger, Value: int64(90)},
				}},
				Then: &Expr{Column: "grade"},
			},
		},
		CaseElse: textExpr("F"),
	}
	assert.Equal(t, []string{"score", "grade"}, e.Columns())
}
