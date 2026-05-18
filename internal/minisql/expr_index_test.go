package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyntheticExprColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    ColumnKind
		wantKind ColumnKind
	}{
		{Int8, Int8},
		{Int4, Int4},
		{Double, Double},
		{Boolean, Boolean},
		{Varchar, Varchar},
		{Timestamp, Timestamp},
		// Text and JSON are promoted to Varchar
		{Text, Varchar},
		{JSON, Varchar},
	}

	for _, tt := range tests {
		col := syntheticExprColumn(tt.input)
		assert.Equal(t, "__expr__", col.Name)
		assert.Equal(t, tt.wantKind, col.Kind)
	}
}

func TestExprSourceColumns(t *testing.T) {
	t.Parallel()

	t.Run("nil expr", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, exprSourceColumns(nil))
	})

	t.Run("single column", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{Column: "price"}
		assert.Equal(t, []string{"price"}, exprSourceColumns(expr))
	})

	t.Run("arithmetic with two columns", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			Left:  &Expr{Column: "price"},
			Right: &Expr{Column: "quantity"},
			Op:    ArithMul,
		}
		cols := exprSourceColumns(expr)
		assert.ElementsMatch(t, []string{"price", "quantity"}, cols)
	})

	t.Run("duplicate columns deduplicated", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			Left:  &Expr{Column: "price"},
			Right: &Expr{Column: "price"},
			Op:    ArithAdd,
		}
		assert.Equal(t, []string{"price"}, exprSourceColumns(expr))
	})

	t.Run("function with column arg", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			FuncName: "LOWER",
			Args:     []*Expr{{Column: "name"}},
		}
		assert.Equal(t, []string{"name"}, exprSourceColumns(expr))
	})

	t.Run("cast expr", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{CastExpr: &Expr{Column: "amount"}, CastTargetType: Int8}
		assert.Equal(t, []string{"amount"}, exprSourceColumns(expr))
	})

	t.Run("case expr with columns", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			CaseInput: &Expr{Column: "status"},
			CaseClauses: []CaseWhen{
				{When: &Expr{Literal: int64(1)}, Then: &Expr{Column: "price"}},
			},
			CaseElse: &Expr{Column: "discount"},
		}
		cols := exprSourceColumns(expr)
		assert.ElementsMatch(t, []string{"status", "price", "discount"}, cols)
	})

	t.Run("literal only no columns", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{Literal: int64(42)}
		assert.Empty(t, exprSourceColumns(expr))
	})
}

func TestInferExprResultKind(t *testing.T) {
	t.Parallel()

	tableCols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "price", Kind: Double},
		{Name: "name", Kind: Varchar},
		{Name: "active", Kind: Boolean},
		{Name: "ts", Kind: Timestamp},
	}

	t.Run("nil expr returns Text", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Text, inferExprResultKind(nil, tableCols))
	})

	t.Run("cast returns target type", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{CastExpr: &Expr{Column: "name"}, CastTargetType: Int8}
		assert.Equal(t, Int8, inferExprResultKind(expr, tableCols))
	})

	t.Run("string functions return Varchar", func(t *testing.T) {
		t.Parallel()
		for _, fn := range []string{"LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "SUBSTR", "REPLACE", "CONCAT"} {
			expr := &Expr{FuncName: fn, Args: []*Expr{{Column: "name"}}}
			assert.Equal(t, Varchar, inferExprResultKind(expr, tableCols), "fn=%s", fn)
		}
	})

	t.Run("LENGTH and MOD return Int8", func(t *testing.T) {
		t.Parallel()
		for _, fn := range []string{"LENGTH", "MOD"} {
			expr := &Expr{FuncName: fn, Args: []*Expr{{Column: "name"}}}
			assert.Equal(t, Int8, inferExprResultKind(expr, tableCols), "fn=%s", fn)
		}
	})

	t.Run("EXTRACT and DATE_PART return Int8", func(t *testing.T) {
		t.Parallel()
		for _, fn := range []string{"EXTRACT", "DATE_PART"} {
			expr := &Expr{FuncName: fn, Args: []*Expr{
				{Literal: NewTextPointer([]byte("year"))},
				{Column: "ts"},
			}}
			assert.Equal(t, Int8, inferExprResultKind(expr, tableCols), "fn=%s", fn)
		}
	})

	t.Run("ABS with int arg returns int", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "ABS", Args: []*Expr{{Column: "id"}}}
		assert.Equal(t, Int8, inferExprResultKind(expr, tableCols))
	})

	t.Run("ABS with float arg returns Double", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "ABS", Args: []*Expr{{Column: "price"}}}
		assert.Equal(t, Double, inferExprResultKind(expr, tableCols))
	})

	t.Run("ABS with no args returns Double", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "ABS"}
		assert.Equal(t, Double, inferExprResultKind(expr, tableCols))
	})

	t.Run("FLOOR/CEIL/ROUND with int arg returns Int8", func(t *testing.T) {
		t.Parallel()
		for _, fn := range []string{"FLOOR", "CEIL", "ROUND"} {
			expr := &Expr{FuncName: fn, Args: []*Expr{{Column: "id"}}}
			assert.Equal(t, Int8, inferExprResultKind(expr, tableCols), "fn=%s", fn)
		}
	})

	t.Run("FLOOR/CEIL/ROUND with float arg returns Double", func(t *testing.T) {
		t.Parallel()
		for _, fn := range []string{"FLOOR", "CEIL", "ROUND"} {
			expr := &Expr{FuncName: fn, Args: []*Expr{{Column: "price"}}}
			assert.Equal(t, Double, inferExprResultKind(expr, tableCols), "fn=%s", fn)
		}
	})

	t.Run("FLOOR/CEIL/ROUND with no args returns Double", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "FLOOR"}
		assert.Equal(t, Double, inferExprResultKind(expr, tableCols))
	})

	t.Run("DATE_TRUNC and TO_TIMESTAMP return Timestamp", func(t *testing.T) {
		t.Parallel()
		for _, fn := range []string{"DATE_TRUNC", "TO_TIMESTAMP"} {
			expr := &Expr{FuncName: fn, Args: []*Expr{{Column: "ts"}}}
			assert.Equal(t, Timestamp, inferExprResultKind(expr, tableCols), "fn=%s", fn)
		}
	})

	t.Run("unknown function returns Text", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "UNKNOWN_FUNC", Args: []*Expr{{Column: "name"}}}
		assert.Equal(t, Text, inferExprResultKind(expr, tableCols))
	})

	t.Run("JSON arrow operators return Text", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{Left: &Expr{Column: "data"}, Right: &Expr{Literal: NewTextPointer([]byte("key"))}, Op: JSONArrow}
		assert.Equal(t, Text, inferExprResultKind(expr, tableCols))
		expr2 := &Expr{Left: &Expr{Column: "data"}, Right: &Expr{Literal: NewTextPointer([]byte("key"))}, Op: JSONArrowArrow}
		assert.Equal(t, Text, inferExprResultKind(expr2, tableCols))
	})

	t.Run("arithmetic with two ints returns Int8", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			Left:  &Expr{Column: "id"},
			Right: &Expr{Literal: int64(2)},
			Op:    ArithMul,
		}
		assert.Equal(t, Int8, inferExprResultKind(expr, tableCols))
	})

	t.Run("arithmetic with float returns Double", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			Left:  &Expr{Column: "id"},
			Right: &Expr{Column: "price"},
			Op:    ArithMul,
		}
		assert.Equal(t, Double, inferExprResultKind(expr, tableCols))
	})

	t.Run("known column returns column kind", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Int8, inferExprResultKind(&Expr{Column: "id"}, tableCols))
		assert.Equal(t, Double, inferExprResultKind(&Expr{Column: "price"}, tableCols))
		assert.Equal(t, Varchar, inferExprResultKind(&Expr{Column: "name"}, tableCols))
		assert.Equal(t, Boolean, inferExprResultKind(&Expr{Column: "active"}, tableCols))
		assert.Equal(t, Timestamp, inferExprResultKind(&Expr{Column: "ts"}, tableCols))
	})

	t.Run("unknown column returns Text", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Text, inferExprResultKind(&Expr{Column: "nonexistent"}, tableCols))
	})

	t.Run("int64 literal returns Int8", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Int8, inferExprResultKind(&Expr{Literal: int64(42)}, tableCols))
	})

	t.Run("float64 literal returns Double", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Double, inferExprResultKind(&Expr{Literal: float64(3.14)}, tableCols))
	})

	t.Run("bool literal returns Boolean", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Boolean, inferExprResultKind(&Expr{Literal: true}, tableCols))
	})

	t.Run("TextPointer literal returns Varchar", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, Varchar, inferExprResultKind(&Expr{Literal: NewTextPointer([]byte("hello"))}, tableCols))
	})

	t.Run("unknown literal type returns Text", func(t *testing.T) {
		t.Parallel()
		// An Expr with a non-nil but unrecognised literal type (e.g. a byte slice)
		// falls through to return Text.
		expr := &Expr{Literal: []byte("raw")}
		assert.Equal(t, Text, inferExprResultKind(expr, tableCols))
	})

	t.Run("Int4 column counts as integer for arithmetic", func(t *testing.T) {
		t.Parallel()
		cols := []Column{{Name: "qty", Kind: Int4}}
		expr := &Expr{
			Left:  &Expr{Column: "qty"},
			Right: &Expr{Literal: int64(1)},
			Op:    ArithAdd,
		}
		assert.Equal(t, Int8, inferExprResultKind(expr, cols))
	})
}

func TestIsIntegerKind(t *testing.T) {
	t.Parallel()

	assert.True(t, isIntegerKind(Int8))
	assert.True(t, isIntegerKind(Int4))
	assert.False(t, isIntegerKind(Double))
	assert.False(t, isIntegerKind(Real))
	assert.False(t, isIntegerKind(Boolean))
	assert.False(t, isIntegerKind(Varchar))
	assert.False(t, isIntegerKind(Text))
	assert.False(t, isIntegerKind(Timestamp))
}

func TestIsImmutableExpr(t *testing.T) {
	t.Parallel()

	t.Run("nil is immutable", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isImmutableExpr(nil))
	})

	t.Run("literal is immutable", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isImmutableExpr(&Expr{Literal: int64(1)}))
	})

	t.Run("column ref is immutable", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isImmutableExpr(&Expr{Column: "price"}))
	})

	t.Run("NOW() is not immutable", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isImmutableExpr(&Expr{FuncName: "NOW"}))
	})

	t.Run("LOWER(col) is immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		assert.True(t, isImmutableExpr(expr))
	})

	t.Run("function with NOW() arg is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "DATE_TRUNC", Args: []*Expr{
			{Literal: NewTextPointer([]byte("day"))},
			{FuncName: "NOW"},
		}}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("arithmetic is immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{Left: &Expr{Column: "price"}, Right: &Expr{Literal: int64(2)}, Op: ArithMul}
		assert.True(t, isImmutableExpr(expr))
	})

	t.Run("arithmetic with NOW() in left is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{Left: &Expr{FuncName: "NOW"}, Right: &Expr{Literal: int64(1)}, Op: ArithAdd}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("arithmetic with NOW() in right is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{Left: &Expr{Literal: int64(1)}, Right: &Expr{FuncName: "NOW"}, Op: ArithAdd}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("cast of NOW() is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{CastExpr: &Expr{FuncName: "NOW"}, CastTargetType: Timestamp}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("CASE with immutable branches is immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			CaseInput: &Expr{Column: "status"},
			CaseClauses: []CaseWhen{
				{When: &Expr{Literal: int64(1)}, Then: &Expr{Column: "price"}},
			},
			CaseElse: &Expr{Literal: int64(0)},
		}
		assert.True(t, isImmutableExpr(expr))
	})

	t.Run("CASE with NOW() in input is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			CaseInput:   &Expr{FuncName: "NOW"},
			CaseClauses: []CaseWhen{{When: &Expr{Literal: int64(1)}, Then: &Expr{Literal: int64(2)}}},
		}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("CASE with NOW() in else is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			CaseClauses: []CaseWhen{{When: &Expr{Literal: int64(1)}, Then: &Expr{Literal: int64(2)}}},
			CaseElse:    &Expr{FuncName: "NOW"},
		}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("CASE with NOW() in WHEN is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			CaseClauses: []CaseWhen{{When: &Expr{FuncName: "NOW"}, Then: &Expr{Literal: int64(2)}}},
		}
		assert.False(t, isImmutableExpr(expr))
	})

	t.Run("CASE with NOW() in THEN is not immutable", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{
			CaseClauses: []CaseWhen{{When: &Expr{Literal: int64(1)}, Then: &Expr{FuncName: "NOW"}}},
		}
		assert.False(t, isImmutableExpr(expr))
	})
}

func TestEvalExprIndexKey(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "price", Kind: Double},
		{Name: "name", Kind: Varchar},
		{Name: "qty", Kind: Int8},
	}
	row := NewRowWithValues(cols, []OptionalValue{
		MakeDouble(float64(9.99)),
		MakeVarchar(NewTextPointer([]byte("widget"))),
		MakeInt8(int64(5)),
	})

	t.Run("string column lower", func(t *testing.T) {
		t.Parallel()
		expr := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		synCol := syntheticExprColumn(Varchar)
		key, ok, err := evalExprIndexKey(expr, synCol, row)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "widget", key)
	})

	t.Run("arithmetic expression", func(t *testing.T) {
		t.Parallel()
		// price * qty → 9.99 * 5 = 49.95
		expr := &Expr{
			Left:  &Expr{Column: "price"},
			Right: &Expr{Column: "qty"},
			Op:    ArithMul,
		}
		synCol := syntheticExprColumn(Double)
		key, ok, err := evalExprIndexKey(expr, synCol, row)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.InDelta(t, 49.95, key, 0.001)
	})

	t.Run("NULL column not indexed", func(t *testing.T) {
		t.Parallel()
		nullCols := []Column{{Name: "name", Kind: Varchar}}
		nullRow := NewRowWithValues(nullCols, []OptionalValue{MakeNull()})
		expr := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		synCol := syntheticExprColumn(Varchar)
		key, ok, err := evalExprIndexKey(expr, synCol, nullRow)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, key)
	})
}

func TestExprEqual(t *testing.T) {
	t.Parallel()

	t.Run("both nil", func(t *testing.T) {
		t.Parallel()
		assert.True(t, exprEqual(nil, nil))
	})

	t.Run("one nil", func(t *testing.T) {
		t.Parallel()
		assert.False(t, exprEqual(nil, &Expr{Column: "x"}))
		assert.False(t, exprEqual(&Expr{Column: "x"}, nil))
	})

	t.Run("identical column refs", func(t *testing.T) {
		t.Parallel()
		assert.True(t, exprEqual(&Expr{Column: "price"}, &Expr{Column: "price"}))
	})

	t.Run("different column names", func(t *testing.T) {
		t.Parallel()
		assert.False(t, exprEqual(&Expr{Column: "price"}, &Expr{Column: "qty"}))
	})

	t.Run("identical function exprs", func(t *testing.T) {
		t.Parallel()
		a := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		b := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		assert.True(t, exprEqual(a, b))
	})

	t.Run("different function names", func(t *testing.T) {
		t.Parallel()
		a := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		b := &Expr{FuncName: "UPPER", Args: []*Expr{{Column: "name"}}}
		assert.False(t, exprEqual(a, b))
	})

	t.Run("identical arithmetic", func(t *testing.T) {
		t.Parallel()
		a := &Expr{Left: &Expr{Column: "price"}, Right: &Expr{Literal: int64(2)}, Op: ArithMul}
		b := &Expr{Left: &Expr{Column: "price"}, Right: &Expr{Literal: int64(2)}, Op: ArithMul}
		assert.True(t, exprEqual(a, b))
	})

	t.Run("different operators", func(t *testing.T) {
		t.Parallel()
		a := &Expr{Left: &Expr{Column: "price"}, Right: &Expr{Literal: int64(2)}, Op: ArithMul}
		b := &Expr{Left: &Expr{Column: "price"}, Right: &Expr{Literal: int64(2)}, Op: ArithAdd}
		assert.False(t, exprEqual(a, b))
	})
}
