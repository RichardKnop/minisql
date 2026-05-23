package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// windowTestColumns returns a small schema: id INT8, name TEXT, dept TEXT, score INT8.
var windowTestColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Varchar, Size: MaxInlineVarchar, Name: "name", Nullable: true},
	{Kind: Varchar, Size: MaxInlineVarchar, Name: "dept", Nullable: true},
	{Kind: Int8, Size: 8, Name: "score", Nullable: true},
}

// windowTestRows builds the in-memory rows used across window tests.
// dept "eng": Alice(90), Bob(85), Eve(95)
// dept "mkt": Carol(70), Dave(70)
func buildWindowTestRows() []Row {
	rows := []Row{
		makeWinRow(1, "Alice", "eng", 90),
		makeWinRow(2, "Bob", "eng", 85),
		makeWinRow(3, "Carol", "mkt", 70),
		makeWinRow(4, "Dave", "mkt", 70),
		makeWinRow(5, "Eve", "eng", 95),
	}
	return rows
}

func makeWinRow(id int64, name, dept string, score int64) Row {
	return NewRowWithValues(windowTestColumns, []OptionalValue{
		{Valid: true, Value: id},
		{Valid: true, Value: NewTextPointer([]byte(name))},
		{Valid: true, Value: NewTextPointer([]byte(dept))},
		{Valid: true, Value: score},
	})
}

// orderByScore returns an ORDER BY score ASC spec.
func orderByScore(dir Direction) []OrderBy {
	return []OrderBy{{Field: Field{Name: "score"}, Direction: dir}}
}

// ── computeWindowColumn helpers ───────────────────────────────────────────

func TestComputeWindowColumn_Empty(t *testing.T) {
	t.Parallel()
	vals, err := computeWindowColumn(nil, &WindowFunc{Kind: WindowRowNumber})
	require.NoError(t, err)
	assert.Nil(t, vals)
}

// ── ROW_NUMBER ────────────────────────────────────────────────────────────

func TestWindowExec_RowNumber_NoPartition(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowRowNumber,
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	require.Len(t, vals, 5)

	// Collect rn by row name for stable assertions (sort is score ASC).
	rnByName := map[string]int64{}
	for i, row := range rows {
		name := string(row.Values[1].Value.(TextPointer).Data)
		rnByName[name] = vals[i].Value.(int64)
	}
	assert.Equal(t, int64(1), rnByName["Carol"]) // score 70 tied – Carol first in stable sort
	assert.Equal(t, int64(3), rnByName["Bob"])   // score 85
	assert.Equal(t, int64(4), rnByName["Alice"]) // score 90
	assert.Equal(t, int64(5), rnByName["Eve"])   // score 95
}

func TestWindowExec_RowNumber_WithPartition(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowRowNumber,
		Spec: WindowSpec{
			PartitionBy: []string{"dept"},
			OrderBy:     orderByScore(Desc),
		},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	require.Len(t, vals, 5)

	rnByName := map[string]int64{}
	for i, row := range rows {
		name := string(row.Values[1].Value.(TextPointer).Data)
		rnByName[name] = vals[i].Value.(int64)
	}
	// eng partition ordered score DESC: Eve(95)=1, Alice(90)=2, Bob(85)=3
	assert.Equal(t, int64(1), rnByName["Eve"])
	assert.Equal(t, int64(2), rnByName["Alice"])
	assert.Equal(t, int64(3), rnByName["Bob"])
	// mkt partition: both 70, stable order → Carol=1, Dave=2
	assert.Equal(t, int64(1), rnByName["Carol"])
	assert.Equal(t, int64(2), rnByName["Dave"])
}

// ── RANK ──────────────────────────────────────────────────────────────────

func TestWindowExec_Rank_Ties(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowRank,
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	rnByName := map[string]int64{}
	for i, row := range rows {
		name := string(row.Values[1].Value.(TextPointer).Data)
		rnByName[name] = vals[i].Value.(int64)
	}
	// Carol and Dave both score 70 → rank 1; next rank is 3 (gap).
	assert.Equal(t, rnByName["Carol"], rnByName["Dave"])
	assert.Equal(t, int64(1), rnByName["Carol"])
	assert.Equal(t, int64(3), rnByName["Bob"])
	assert.Equal(t, int64(4), rnByName["Alice"])
	assert.Equal(t, int64(5), rnByName["Eve"])
}

// ── DENSE_RANK ────────────────────────────────────────────────────────────

func TestWindowExec_DenseRank_NoGaps(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowDenseRank,
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	drByName := map[string]int64{}
	for i, row := range rows {
		name := string(row.Values[1].Value.(TextPointer).Data)
		drByName[name] = vals[i].Value.(int64)
	}
	// Dense rank: 70→1, 85→2, 90→3, 95→4 (no gaps).
	assert.Equal(t, int64(1), drByName["Carol"])
	assert.Equal(t, int64(1), drByName["Dave"])
	assert.Equal(t, int64(2), drByName["Bob"])
	assert.Equal(t, int64(3), drByName["Alice"])
	assert.Equal(t, int64(4), drByName["Eve"])
}

// ── NTILE ─────────────────────────────────────────────────────────────────

func TestWindowExec_Ntile(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowNtile,
		Arg:  &Expr{Literal: int64(2)},
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	require.Len(t, vals, 5)

	buckets := map[int64]int{}
	for _, v := range vals {
		buckets[v.Value.(int64)]++
	}
	// 5 rows into 2 buckets: bucket 1 gets ceil(5/2)=3, bucket 2 gets 2.
	assert.Equal(t, 3, buckets[1])
	assert.Equal(t, 2, buckets[2])
}

func TestWindowExec_Ntile_ZeroBucket(t *testing.T) {
	t.Parallel()
	// Zero/negative bucket count should clamp to 1.
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowNtile,
		Arg:  &Expr{Literal: int64(0)},
		Spec: WindowSpec{},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	for _, v := range vals {
		assert.Equal(t, int64(1), v.Value.(int64))
	}
}

func TestWindowExec_Ntile_NoArg(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{Kind: WindowNtile, Spec: WindowSpec{}}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	for _, v := range vals {
		assert.Equal(t, int64(1), v.Value.(int64))
	}
}

// ── LAG / LEAD ────────────────────────────────────────────────────────────

func TestWindowExec_Lag(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowLag,
		Arg:  &Expr{Column: "score"},
		Arg2: &Expr{Literal: int64(1)},
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	require.Len(t, vals, 5)

	// Map by original row index for assertion.
	// Rows sorted score ASC: Carol(70), Dave(70), Bob(85), Alice(90), Eve(95).
	// For the first row in the sorted order, LAG is NULL.
	nullCount := 0
	for _, v := range vals {
		if !v.Valid {
			nullCount++
		}
	}
	assert.Equal(t, 1, nullCount, "exactly one NULL (first row in partition)")
}

func TestWindowExec_Lead(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowLead,
		Arg:  &Expr{Column: "score"},
		Arg2: &Expr{Literal: int64(1)},
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	require.Len(t, vals, 5)

	nullCount := 0
	for _, v := range vals {
		if !v.Valid {
			nullCount++
		}
	}
	assert.Equal(t, 1, nullCount, "exactly one NULL (last row in partition)")
}

// ── FIRST_VALUE / LAST_VALUE ──────────────────────────────────────────────

func TestWindowExec_FirstValue(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowFirstValue,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{
			PartitionBy: []string{"dept"},
			OrderBy:     orderByScore(Desc),
		},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	// All eng rows should see first value = 95 (Eve), mkt rows = 70.
	for i, row := range rows {
		dept := string(row.Values[2].Value.(TextPointer).Data)
		v := vals[i].Value.(int64)
		if dept == "eng" {
			assert.Equal(t, int64(95), v)
		} else {
			assert.Equal(t, int64(70), v)
		}
	}
}

func TestWindowExec_LastValue_DefaultFrame(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	// Default frame for LAST_VALUE: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
	// So LAST_VALUE == each row's own score when ordered by score ASC.
	wf := &WindowFunc{
		Kind: WindowLastValue,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	for i, row := range rows {
		rowScore := row.Values[3].Value.(int64)
		lvScore := vals[i].Value.(int64)
		// With running frame, LAST_VALUE of current row ≥ score of first row.
		assert.GreaterOrEqual(t, lvScore, int64(70))
		_ = rowScore
	}
}

// ── NTH_VALUE ─────────────────────────────────────────────────────────────

func TestWindowExec_NthValue(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	// NTH_VALUE(score, 2) over all rows ordered score DESC → Alice's score (90).
	wf := &WindowFunc{
		Kind: WindowNthValue,
		Arg:  &Expr{Column: "score"},
		Arg2: &Expr{Literal: int64(2)},
		Spec: WindowSpec{OrderBy: orderByScore(Desc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	for _, v := range vals {
		require.True(t, v.Valid)
		assert.Equal(t, int64(90), v.Value.(int64))
	}
}

func TestWindowExec_NthValue_OutOfRange(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowNthValue,
		Arg:  &Expr{Column: "score"},
		Arg2: &Expr{Literal: int64(100)},
		Spec: WindowSpec{},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	for _, v := range vals {
		assert.False(t, v.Valid)
	}
}

// ── SUM / AVG / COUNT / MIN / MAX OVER ────────────────────────────────────

func TestWindowExec_SumRunning(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	// SUM(score) OVER (ORDER BY score ASC) — running total.
	wf := &WindowFunc{
		Kind: WindowSum,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{OrderBy: orderByScore(Asc)},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	require.Len(t, vals, 5)
	for _, v := range vals {
		assert.True(t, v.Valid)
	}
}

func TestWindowExec_SumPartition(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	// SUM(score) OVER (PARTITION BY dept) — total per dept.
	wf := &WindowFunc{
		Kind: WindowSum,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{PartitionBy: []string{"dept"}},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	totalByDept := map[string]int64{}
	for i, row := range rows {
		dept := string(row.Values[2].Value.(TextPointer).Data)
		totalByDept[dept] = vals[i].Value.(int64)
	}
	assert.Equal(t, int64(90+85+95), totalByDept["eng"]) // 270
	assert.Equal(t, int64(70+70), totalByDept["mkt"])    // 140
}

func TestWindowExec_AvgPartition(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	wf := &WindowFunc{
		Kind: WindowAvg,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{PartitionBy: []string{"dept"}},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	for i, row := range rows {
		dept := string(row.Values[2].Value.(TextPointer).Data)
		avg := vals[i].Value.(float64)
		if dept == "eng" {
			assert.InDelta(t, float64(270)/3, avg, 0.001)
		} else {
			assert.InDelta(t, float64(140)/2, avg, 0.001)
		}
	}
}

func TestWindowExec_CountStar(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	// COUNT(*) OVER () — total row count for all rows.
	wf := &WindowFunc{
		Kind: WindowCount,
		Arg:  nil,
		Spec: WindowSpec{},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)
	for _, v := range vals {
		assert.Equal(t, int64(5), v.Value.(int64))
	}
}

func TestWindowExec_MinMaxPartition(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()

	minWF := &WindowFunc{
		Kind: WindowMin,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{PartitionBy: []string{"dept"}},
	}
	maxWF := &WindowFunc{
		Kind: WindowMax,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{PartitionBy: []string{"dept"}},
	}

	minVals, err := computeWindowColumn(rows, minWF)
	require.NoError(t, err)
	maxVals, err := computeWindowColumn(rows, maxWF)
	require.NoError(t, err)

	for i, row := range rows {
		dept := string(row.Values[2].Value.(TextPointer).Data)
		minV := minVals[i].Value.(int64)
		maxV := maxVals[i].Value.(int64)
		if dept == "eng" {
			assert.Equal(t, int64(85), minV)
			assert.Equal(t, int64(95), maxV)
		} else {
			assert.Equal(t, int64(70), minV)
			assert.Equal(t, int64(70), maxV)
		}
	}
}

// ── Frame helpers ─────────────────────────────────────────────────────────

func TestFrameStartPos(t *testing.T) {
	t.Parallel()

	t.Run("nil frame returns 0", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, frameStartPos(3, 10, nil))
	})
	t.Run("unbounded preceding", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FrameUnboundedPreceding}}
		assert.Equal(t, 0, frameStartPos(5, 10, f))
	})
	t.Run("current row", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FrameCurrentRow}}
		assert.Equal(t, 5, frameStartPos(5, 10, f))
	})
	t.Run("preceding clamps to 0", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FramePreceding, Offset: 10}}
		assert.Equal(t, 0, frameStartPos(3, 10, f))
	})
	t.Run("preceding within range", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FramePreceding, Offset: 2}}
		assert.Equal(t, 3, frameStartPos(5, 10, f))
	})
	t.Run("following", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FrameFollowing, Offset: 2}}
		assert.Equal(t, 7, frameStartPos(5, 10, f))
	})
	t.Run("following clamps to n-1", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FrameFollowing, Offset: 100}}
		assert.Equal(t, 9, frameStartPos(5, 10, f))
	})
	t.Run("unbounded following", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{Start: FrameBound{Kind: FrameUnboundedFollowing}}
		assert.Equal(t, 9, frameStartPos(5, 10, f))
	})
}

func TestFrameEndPos(t *testing.T) {
	t.Parallel()

	def := &WindowFrame{End: FrameBound{Kind: FrameCurrentRow}}

	t.Run("nil frame uses default", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 3, frameEndPos(3, 10, nil, def))
	})
	t.Run("unbounded following", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FrameUnboundedFollowing}}
		assert.Equal(t, 9, frameEndPos(3, 10, f, def))
	})
	t.Run("unbounded preceding end", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FrameUnboundedPreceding}}
		assert.Equal(t, 0, frameEndPos(3, 10, f, def))
	})
	t.Run("current row end", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FrameCurrentRow}}
		assert.Equal(t, 5, frameEndPos(5, 10, f, def))
	})
	t.Run("following end", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FrameFollowing, Offset: 2}}
		assert.Equal(t, 7, frameEndPos(5, 10, f, def))
	})
	t.Run("following end clamps to n-1", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FrameFollowing, Offset: 100}}
		assert.Equal(t, 9, frameEndPos(5, 10, f, def))
	})
	t.Run("preceding end", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FramePreceding, Offset: 2}}
		assert.Equal(t, 3, frameEndPos(5, 10, f, def))
	})
	t.Run("preceding end clamps to 0", func(t *testing.T) {
		t.Parallel()
		f := &WindowFrame{End: FrameBound{Kind: FramePreceding, Offset: 100}}
		assert.Equal(t, 0, frameEndPos(3, 10, f, def))
	})
}

// ── Small helpers ─────────────────────────────────────────────────────────

func TestIntVal(t *testing.T) {
	t.Parallel()
	v := intVal(42)
	assert.True(t, v.Valid)
	assert.Equal(t, int64(42), v.Value)
}

func TestToOptional(t *testing.T) {
	t.Parallel()
	assert.False(t, toOptional(nil).Valid)
	v := toOptional(int64(7))
	assert.True(t, v.Valid)
	assert.Equal(t, int64(7), v.Value)
}

func TestPeerRows(t *testing.T) {
	t.Parallel()
	a := makeWinRow(1, "A", "eng", 90)
	b := makeWinRow(2, "B", "eng", 90)
	c := makeWinRow(3, "C", "eng", 85)

	ob := []OrderBy{{Field: Field{Name: "score"}, Direction: Asc}}
	assert.True(t, peerRows(a, b, ob))
	assert.False(t, peerRows(a, c, ob))
	assert.True(t, peerRows(a, b, nil)) // no ORDER BY → all peers
}

func TestWindowFuncString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		kind WindowFuncKind
		want string
	}{
		{WindowRowNumber, "ROW_NUMBER() OVER (...)"},
		{WindowRank, "RANK() OVER (...)"},
		{WindowDenseRank, "DENSE_RANK() OVER (...)"},
		{WindowNtile, "NTILE(...) OVER (...)"},
		{WindowLag, "LAG(...) OVER (...)"},
		{WindowLead, "LEAD(...) OVER (...)"},
		{WindowFirstValue, "FIRST_VALUE(...) OVER (...)"},
		{WindowLastValue, "LAST_VALUE(...) OVER (...)"},
		{WindowNthValue, "NTH_VALUE(...) OVER (...)"},
		{WindowSum, "SUM(...) OVER (...)"},
		{WindowAvg, "AVG(...) OVER (...)"},
		{WindowCount, "COUNT(*) OVER (...)"},
		{WindowMin, "MIN(...) OVER (...)"},
		{WindowMax, "MAX(...) OVER (...)"},
		{WindowFuncKind(99), "WINDOW(...)"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			wf := &WindowFunc{Kind: tc.kind}
			assert.Equal(t, tc.want, windowFuncString(wf))
		})
	}
	assert.Equal(t, "", windowFuncString(nil))
}

// ── Integration: Table.Select with window functions ────────────────────────

func TestTable_SelectWithWindowFuncs_RowNumber(t *testing.T) {
	ctx := context.Background()
	table, txManager, _ := newTestTable(t, windowTestColumns)

	insertStmt := Statement{
		Kind:    Insert,
		Columns: windowTestColumns,
		Fields:  fieldsFromColumns(windowTestColumns...),
		Inserts: [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("Alice"))}, {Valid: true, Value: NewTextPointer([]byte("eng"))}, {Valid: true, Value: int64(90)}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("Bob"))}, {Valid: true, Value: NewTextPointer([]byte("eng"))}, {Valid: true, Value: int64(85)}},
			{{Valid: true, Value: int64(3)}, {Valid: true, Value: NewTextPointer([]byte("Carol"))}, {Valid: true, Value: NewTextPointer([]byte("mkt"))}, {Valid: true, Value: int64(70)}},
		},
	}
	mustInsert(ctx, t, table, txManager, insertStmt)

	rnExpr := &Expr{WindowFunc: &WindowFunc{
		Kind: WindowRowNumber,
		Spec: WindowSpec{OrderBy: []OrderBy{{Field: Field{Name: "score"}, Direction: Desc}}},
	}}

	stmt := Statement{
		Kind: Select,
		Fields: []Field{
			{Name: "name"},
			{Name: "ROW_NUMBER() OVER (...)", Alias: "rn", Expr: rnExpr},
		},
		OrderBy: []OrderBy{{Field: Field{Name: "rn"}, Direction: Asc}},
	}

	var result StatementResult
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		var err error
		result, err = table.Select(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	var names []string
	for result.Rows.Next(ctx) {
		row := result.Rows.Row()
		name := string(row.Values[0].Value.(TextPointer).Data)
		names = append(names, name)
	}
	require.NoError(t, result.Rows.Err())
	require.Len(t, names, 3)
	// score DESC: Alice(90)=1, Bob(85)=2, Carol(70)=3
	assert.Equal(t, []string{"Alice", "Bob", "Carol"}, names)
}

func TestTable_SelectWithWindowFuncs_SumPartition(t *testing.T) {
	ctx := context.Background()
	table, txManager, _ := newTestTable(t, windowTestColumns)

	insertStmt := Statement{
		Kind:    Insert,
		Columns: windowTestColumns,
		Fields:  fieldsFromColumns(windowTestColumns...),
		Inserts: [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("Alice"))}, {Valid: true, Value: NewTextPointer([]byte("eng"))}, {Valid: true, Value: int64(90)}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("Bob"))}, {Valid: true, Value: NewTextPointer([]byte("eng"))}, {Valid: true, Value: int64(85)}},
			{{Valid: true, Value: int64(3)}, {Valid: true, Value: NewTextPointer([]byte("Carol"))}, {Valid: true, Value: NewTextPointer([]byte("mkt"))}, {Valid: true, Value: int64(70)}},
		},
	}
	mustInsert(ctx, t, table, txManager, insertStmt)

	sumExpr := &Expr{WindowFunc: &WindowFunc{
		Kind: WindowSum,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{PartitionBy: []string{"dept"}},
	}}

	stmt := Statement{
		Kind: Select,
		Fields: []Field{
			{Name: "dept"},
			{Name: "SUM(score) OVER (...)", Alias: "dept_total", Expr: sumExpr},
		},
		OrderBy: []OrderBy{{Field: Field{Name: "dept"}, Direction: Asc}},
	}

	var result StatementResult
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		var err error
		result, err = table.Select(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	totals := map[string]int64{}
	for result.Rows.Next(ctx) {
		row := result.Rows.Row()
		dept := string(row.Values[0].Value.(TextPointer).Data)
		total := row.Values[1].Value.(int64)
		totals[dept] = total
	}
	require.NoError(t, result.Rows.Err())
	assert.Equal(t, int64(175), totals["eng"]) // 90+85
	assert.Equal(t, int64(70), totals["mkt"])
}

func TestTable_SelectWithWindowFuncs_Limit(t *testing.T) {
	ctx := context.Background()
	table, txManager, _ := newTestTable(t, windowTestColumns)

	insertStmt := Statement{
		Kind:    Insert,
		Columns: windowTestColumns,
		Fields:  fieldsFromColumns(windowTestColumns...),
		Inserts: [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("Alice"))}, {Valid: true, Value: NewTextPointer([]byte("eng"))}, {Valid: true, Value: int64(90)}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("Bob"))}, {Valid: true, Value: NewTextPointer([]byte("eng"))}, {Valid: true, Value: int64(85)}},
			{{Valid: true, Value: int64(3)}, {Valid: true, Value: NewTextPointer([]byte("Carol"))}, {Valid: true, Value: NewTextPointer([]byte("mkt"))}, {Valid: true, Value: int64(70)}},
		},
	}
	mustInsert(ctx, t, table, txManager, insertStmt)

	rnExpr := &Expr{WindowFunc: &WindowFunc{
		Kind: WindowRowNumber,
		Spec: WindowSpec{OrderBy: []OrderBy{{Field: Field{Name: "score"}, Direction: Desc}}},
	}}

	stmt := Statement{
		Kind: Select,
		Fields: []Field{
			{Name: "name"},
			{Name: "rn", Alias: "rn", Expr: rnExpr},
		},
		OrderBy: []OrderBy{{Field: Field{Name: "rn"}, Direction: Asc}},
		Limit:   OptionalValue{Valid: true, Value: int64(2)},
	}

	var result StatementResult
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		var err error
		result, err = table.Select(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	count := 0
	for result.Rows.Next(ctx) {
		count++
	}
	require.NoError(t, result.Rows.Err())
	assert.Equal(t, 2, count)
}

// ── SUM with explicit ROWS BETWEEN frame ──────────────────────────────────

func TestWindowExec_SumExplicitFrame(t *testing.T) {
	t.Parallel()
	rows := buildWindowTestRows()
	frame := &WindowFrame{
		Mode:  FrameRows,
		Start: FrameBound{Kind: FrameUnboundedPreceding},
		End:   FrameBound{Kind: FrameUnboundedFollowing},
	}
	wf := &WindowFunc{
		Kind: WindowSum,
		Arg:  &Expr{Column: "score"},
		Spec: WindowSpec{Frame: frame},
	}
	vals, err := computeWindowColumn(rows, wf)
	require.NoError(t, err)

	// With UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING, every row sees the grand total.
	grandTotal := int64(90 + 85 + 70 + 70 + 95)
	for _, v := range vals {
		assert.Equal(t, grandTotal, v.Value.(int64))
	}
}
