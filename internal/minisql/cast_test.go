package minisql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── toInt64FromString ──────────────────────────────────────────────────────────

func TestToInt64FromString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    string
		expected int64
		ok       bool
	}{
		{"123", 123, true},
		{"-5", -5, true},
		{"+10", 10, true},
		{"42abc", 42, true},  // leading digits only
		{"  7  ", 7, true},   // whitespace trimmed
		{"abc", 0, false},    // no leading digits
		{"", 0, false},       // empty string
		{"-", 0, false},      // sign only
		{"+", 0, false},      // sign only
	}
	for _, tc := range cases {
		n, ok := toInt64FromString(tc.input)
		assert.Equal(t, tc.ok, ok, "input=%q", tc.input)
		if tc.ok {
			assert.Equal(t, tc.expected, n, "input=%q", tc.input)
		}
	}
}

// ── toFloat64FromString ────────────────────────────────────────────────────────

func TestToFloat64FromString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    string
		expected float64
		ok       bool
	}{
		{"3.14", 3.14, true},
		{"-2.5", -2.5, true},
		{"1e3", 1000, true},
		{"3.7x", 3.7, true},   // trailing non-numeric ignored
		{"  9  ", 9, true},    // whitespace trimmed
		{"abc", 0, false},     // no leading digits
		{"", 0, false},        // empty string
	}
	for _, tc := range cases {
		f, ok := toFloat64FromString(tc.input)
		assert.Equal(t, tc.ok, ok, "input=%q", tc.input)
		if tc.ok {
			assert.InDelta(t, tc.expected, f, 1e-9, "input=%q", tc.input)
		}
	}
}

// ── castToBool ────────────────────────────────────────────────────────────────

func TestCastToBool(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    any
		expected bool
	}{
		{true, true},
		{false, false},
		{int64(1), true},
		{int64(0), false},
		{int64(-3), true},
		{int32(1), true},
		{int32(0), false},
		{float64(0.5), true},
		{float64(0.0), false},
		{float32(1.0), true},
		{NewTextPointer([]byte("1")), true},
		{NewTextPointer([]byte("0")), false},
		{NewTextPointer([]byte("abc")), false}, // no leading digits → false
	}
	for _, tc := range cases {
		got, err := castToBool(tc.input)
		require.NoError(t, err, "input=%v", tc.input)
		assert.Equal(t, tc.expected, got, "input=%v", tc.input)
	}
}

// ── castToInt64 ────────────────────────────────────────────────────────────────

func TestCastToInt64(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    any
		expected int64
	}{
		{int64(7), 7},
		{int32(3), 3},
		{float64(9.9), 9},   // truncate toward zero
		{float64(-2.7), -2}, // truncate toward zero
		{float32(4.1), 4},
		{true, 1},
		{false, 0},
		{NewTextPointer([]byte("123abc")), 123},  // leading digits
		{NewTextPointer([]byte("3.9")), 3},       // leading float digits → truncate
		{NewTextPointer([]byte("abc")), 0},       // no leading digits → 0
	}
	for _, tc := range cases {
		got, err := castToInt64(tc.input)
		require.NoError(t, err, "input=%v", tc.input)
		assert.Equal(t, tc.expected, got, "input=%v", tc.input)
	}
}

// ── castToFloat64 ─────────────────────────────────────────────────────────────

func TestCastToFloat64(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    any
		expected float64
	}{
		{float64(3.14), 3.14},
		{float32(1.5), 1.5},
		{int64(5), 5.0},
		{int32(2), 2.0},
		{true, 1.0},
		{false, 0.0},
		{NewTextPointer([]byte("2.71x")), 2.71}, // leading float
		{NewTextPointer([]byte("abc")), 0.0},    // no leading digits → 0.0 (SQLite semantics)
	}
	for _, tc := range cases {
		got, err := castToFloat64(tc.input)
		require.NoError(t, err, "input=%v", tc.input)
		assert.InDelta(t, tc.expected, got, 1e-5, "input=%v", tc.input)
	}
}

// ── castToTextPointer ─────────────────────────────────────────────────────────

func TestCastToTextPointer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    any
		expected string
	}{
		{NewTextPointer([]byte("hello")), "hello"},
		{int64(42), "42"},
		{int32(7), "7"},
		{float64(3.14), "3.14"},
		{true, "1"},
		{false, "0"},
	}
	for _, tc := range cases {
		got, err := castToTextPointer(tc.input)
		require.NoError(t, err, "input=%v", tc.input)
		assert.Equal(t, tc.expected, string(got.Data), "input=%v", tc.input)
	}
}

func rowWithBool(name string, val bool) Row {
	return NewRowWithValues(
		[]Column{{Name: name, Kind: Boolean}},
		[]OptionalValue{{Value: val, Valid: true}},
	)
}

func TestEvalCast_NullPropagates(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Column: "x"},
		CastTargetType: Int8,
	}
	row := rowWithNull("x")
	val, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestEvalCast_FloatToInt8(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: float64(3.9)},
		CastTargetType: Int8,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), val) // truncate toward zero
}

func TestEvalCast_FloatToInt8_Negative(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: float64(-2.7)},
		CastTargetType: Int8,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, int64(-2), val)
}

func TestEvalCast_IntToDouble(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: int64(5)},
		CastTargetType: Double,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, float64(5), val)
}

func TestEvalCast_IntToText(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: int64(42)},
		CastTargetType: Text,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, NewTextPointer([]byte("42")), val)
}

func TestEvalCast_FloatToText(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: float64(3.14)},
		CastTargetType: Text,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, NewTextPointer([]byte("3.14")), val)
}

func TestEvalCast_TextToInt8_LeadingDigits(t *testing.T) {
	t.Parallel()

	row := rowWithText("s", "123abc")
	expr := &Expr{
		CastExpr:       &Expr{Column: "s"},
		CastTargetType: Int8,
	}
	val, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(123), val)
}

func TestEvalCast_TextToInt8_NoDigits(t *testing.T) {
	t.Parallel()

	row := rowWithText("s", "abc")
	expr := &Expr{
		CastExpr:       &Expr{Column: "s"},
		CastTargetType: Int8,
	}
	val, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestEvalCast_TextToDouble_LeadingFloat(t *testing.T) {
	t.Parallel()

	row := rowWithText("s", "3.7x")
	expr := &Expr{
		CastExpr:       &Expr{Column: "s"},
		CastTargetType: Double,
	}
	val, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, float64(3.7), val)
}

func TestEvalCast_BoolToInt8(t *testing.T) {
	t.Parallel()

	row := rowWithBool("b", true)
	expr := &Expr{
		CastExpr:       &Expr{Column: "b"},
		CastTargetType: Int8,
	}
	val, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)
}

func TestEvalCast_IntToBool(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    int64
		expected bool
	}{
		{0, false},
		{1, true},
		{-5, true},
	}
	for _, tc := range cases {
		expr := &Expr{
			CastExpr:       &Expr{Literal: tc.input},
			CastTargetType: Boolean,
		}
		val, err := expr.Eval(Row{})
		require.NoError(t, err)
		assert.Equal(t, tc.expected, val)
	}
}

func TestEvalCast_ColumnRef(t *testing.T) {
	t.Parallel()

	row := rowWithFloat("price", 9.99)
	expr := &Expr{
		CastExpr:       &Expr{Column: "price"},
		CastTargetType: Int8,
	}
	val, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(9), val)
}

func TestEvalCast_Int4(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: float64(7.8)},
		CastTargetType: Int4,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, int32(7), val)
}

func TestEvalCast_Int4_Overflow(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: int64(math.MaxInt32 + 1)},
		CastTargetType: Int4,
	}
	_, err := expr.Eval(Row{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows INT4")
}

func TestEvalCast_Int4_Underflow(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: int64(math.MinInt32 - 1)},
		CastTargetType: Int4,
	}
	_, err := expr.Eval(Row{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows INT4")
}

func TestEvalCast_Varchar(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Literal: int64(99)},
		CastTargetType: Varchar,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, NewTextPointer([]byte("99")), val)
}

func TestEvalCast_Str(t *testing.T) {
	t.Parallel()

	expr := &Expr{
		CastExpr:       &Expr{Column: "price"},
		CastTargetType: Int8,
	}
	assert.Equal(t, "CAST(price AS int8)", expr.String())
}
