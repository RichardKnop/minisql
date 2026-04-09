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
