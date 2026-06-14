package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// naturalSortExpr builds an Expr that evaluates NATURAL_SORT(colName).
func naturalSortExpr(colName string) *Expr {
	return &Expr{
		FuncName: "NATURAL_SORT",
		Args:     []*Expr{{Column: colName}},
	}
}

func strRow(colName, value string) Row {
	return NewRowWithValues(
		[]Column{{Name: colName, Kind: Varchar, Size: 64}},
		[]OptionalValue{{Value: NewTextPointer([]byte(value)), Valid: true}},
	)
}

func rowStrVal(row Row, colName string) string {
	v, _ := row.GetValue(colName)
	tp, _ := v.Value.(TextPointer)
	return string(tp.Data)
}

// ── evalOrderByValue ─────────────────────────────────────────────────────────

func TestEvalOrderByValue_ColumnLookup(t *testing.T) {
	t.Parallel()

	row := strRow("name", "alice")
	clause := OrderBy{Field: Field{Name: "name"}, Direction: Asc}

	val, found, err := evalOrderByValue(clause, row)

	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, val.Valid)
	tp, ok := val.Value.(TextPointer)
	require.True(t, ok)
	assert.Equal(t, "alice", string(tp.Data))
}

func TestEvalOrderByValue_ColumnNotFound(t *testing.T) {
	t.Parallel()

	row := strRow("name", "alice")
	clause := OrderBy{Field: Field{Name: "missing"}, Direction: Asc}

	_, found, err := evalOrderByValue(clause, row)

	require.NoError(t, err)
	assert.False(t, found)
}

func TestEvalOrderByValue_ExprEval(t *testing.T) {
	t.Parallel()

	row := strRow("ver", "1.10.0")
	clause := OrderBy{Field: Field{Expr: naturalSortExpr("ver")}, Direction: Asc}

	val, found, err := evalOrderByValue(clause, row)

	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, val.Valid)
	tp, ok := val.Value.(TextPointer)
	require.True(t, ok)
	assert.Equal(t, "00000000000000000001.00000000000000000010.00000000000000000000", string(tp.Data))
}

func TestEvalOrderByValue_ExprReturnsNull(t *testing.T) {
	t.Parallel()

	row := NewRowWithValues(
		[]Column{{Name: "ver", Kind: Varchar, Size: 64}},
		[]OptionalValue{{Valid: false}}, // NULL value
	)
	clause := OrderBy{Field: Field{Expr: naturalSortExpr("ver")}, Direction: Asc}

	val, found, err := evalOrderByValue(clause, row)

	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, val.Valid)
}

// ── sortRows with expression ORDER BY ────────────────────────────────────────

func TestSortRows_ExprOrderBy(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "ver", Kind: Varchar, Size: 64}}
	mkRow := func(v string) Row {
		return NewRowWithValues(cols, []OptionalValue{{Value: NewTextPointer([]byte(v)), Valid: true}})
	}

	rows := []Row{mkRow("1.10.0"), mkRow("1.2.0"), mkRow("1.9.0")}

	table := &Table{}
	err := table.sortRows(rows, []OrderBy{
		{Field: Field{Expr: naturalSortExpr("ver")}, Direction: Asc},
	})
	require.NoError(t, err)

	want := []string{"1.2.0", "1.9.0", "1.10.0"}
	for i, row := range rows {
		assert.Equal(t, want[i], rowStrVal(row, "ver"), "position %d", i)
	}
}

func TestSortRows_ExprOrderByDesc(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "ver", Kind: Varchar, Size: 64}}
	mkRow := func(v string) Row {
		return NewRowWithValues(cols, []OptionalValue{{Value: NewTextPointer([]byte(v)), Valid: true}})
	}

	rows := []Row{mkRow("1.2.0"), mkRow("1.10.0"), mkRow("1.9.0")}

	table := &Table{}
	err := table.sortRows(rows, []OrderBy{
		{Field: Field{Expr: naturalSortExpr("ver")}, Direction: Desc},
	})
	require.NoError(t, err)

	want := []string{"1.10.0", "1.9.0", "1.2.0"}
	for i, row := range rows {
		assert.Equal(t, want[i], rowStrVal(row, "ver"), "position %d", i)
	}
}

// ── rowHeap with expression ORDER BY ─────────────────────────────────────────

func TestRowHeap_ExprOrderBy(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "ver", Kind: Varchar, Size: 64}}
	mkRow := func(v string) Row {
		return NewRowWithValues(cols, []OptionalValue{{Value: NewTextPointer([]byte(v)), Valid: true}})
	}

	orderBy := []OrderBy{
		{Field: Field{Expr: naturalSortExpr("ver")}, Direction: Asc},
	}
	h := newRowHeap(orderBy, 2)

	for _, v := range []string{"1.10.0", "1.2.0", "1.9.0"} {
		h.PushRow(mkRow(v))
	}

	result := h.ExtractSorted()
	require.Len(t, result, 2)

	want := []string{"1.2.0", "1.9.0"}
	for i, row := range result {
		assert.Equal(t, want[i], rowStrVal(row, "ver"), "position %d", i)
	}
}
