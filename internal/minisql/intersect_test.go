package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ── pure helper tests ────────────────────────────────────────────────────────

func TestSortRowIDs(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		sortRowIDs(nil)
		sortRowIDs([]RowID{})
	})

	t.Run("single element", func(t *testing.T) {
		t.Parallel()
		ids := []RowID{42}
		sortRowIDs(ids)
		assert.Equal(t, []RowID{42}, ids)
	})

	t.Run("already sorted small slice", func(t *testing.T) {
		t.Parallel()
		ids := []RowID{1, 2, 3}
		sortRowIDs(ids)
		assert.Equal(t, []RowID{1, 2, 3}, ids)
	})

	t.Run("reverse order small slice", func(t *testing.T) {
		t.Parallel()
		ids := []RowID{5, 3, 1}
		sortRowIDs(ids)
		assert.Equal(t, []RowID{1, 3, 5}, ids)
	})

	t.Run("large slice uses std sort path (>16 elements)", func(t *testing.T) {
		t.Parallel()
		ids := make([]RowID, 20)
		for i := range ids {
			ids[i] = RowID(20 - i)
		}
		sortRowIDs(ids)
		for i := 1; i < len(ids); i++ {
			assert.LessOrEqual(t, ids[i-1], ids[i])
		}
	})
}

func TestIntersectTwoSortedSets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		a, b     []RowID
		expected []RowID
	}{
		{"both empty", nil, nil, nil},
		{"a empty", nil, []RowID{1, 2}, nil},
		{"b empty", []RowID{1, 2}, nil, nil},
		{"no overlap", []RowID{1, 3}, []RowID{2, 4}, nil},
		{"full overlap", []RowID{1, 2, 3}, []RowID{1, 2, 3}, []RowID{1, 2, 3}},
		{"partial overlap", []RowID{1, 2, 3, 5}, []RowID{2, 4, 5}, []RowID{2, 5}},
		{"duplicates inside a deduped", []RowID{1, 1, 2}, []RowID{1, 2}, []RowID{1, 2}},
		{"single common element", []RowID{1, 3, 7}, []RowID{3, 5, 9}, []RowID{3}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := intersectTwoSortedSets(tc.a, tc.b)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIntersectSortedRowIDs(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, intersectSortedRowIDs(nil))
	})

	t.Run("single set is sorted and returned", func(t *testing.T) {
		t.Parallel()
		sets := [][]RowID{{3, 1, 2}}
		got := intersectSortedRowIDs(sets)
		assert.Equal(t, []RowID{1, 2, 3}, got)
	})

	t.Run("two overlapping sets", func(t *testing.T) {
		t.Parallel()
		sets := [][]RowID{{1, 3, 5, 7}, {3, 5, 9}}
		got := intersectSortedRowIDs(sets)
		assert.Equal(t, []RowID{3, 5}, got)
	})

	t.Run("three sets chained intersection", func(t *testing.T) {
		t.Parallel()
		sets := [][]RowID{{1, 2, 3, 4, 5}, {2, 3, 4}, {3, 4, 5}}
		got := intersectSortedRowIDs(sets)
		assert.Equal(t, []RowID{3, 4}, got)
	})

	t.Run("empty set in chain gives nil", func(t *testing.T) {
		t.Parallel()
		sets := [][]RowID{{1, 2}, {}, {1, 2}}
		got := intersectSortedRowIDs(sets)
		assert.Nil(t, got)
	})
}

// ── integration: collectRowIDsFromScan and indexIntersectScan ────────────────

// intersectTestTable creates a proper database with two secondary indexes on "events" table
// and inserts three rows. It returns the Table and the TransactionManager.
func intersectTestTable(t *testing.T) (*Table, *TransactionManager) {
	t.Helper()
	pager, dbFile := initTest(t)
	mockParser := new(MockParser)
	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, pager, pager, nil)
	require.NoError(t, err)

	const tblName = "events"
	cols := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "cat"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "status"},
	}

	createStmt := Statement{
		Kind:       CreateTable,
		TableName:  tblName,
		Columns:    cols,
		PrimaryKey: NewPrimaryKey(PrimaryKeyName(tblName), cols[0:1], true),
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, createStmt)
		return err
	})
	require.NoError(t, err)

	// CreateIndex calls tableFromSQL which parses the table DDL — set up the expectation.
	mockParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil)

	// Each CreateIndex must be in its own transaction: DDLChanges.CreateIndexes keys by
	// table name, so two indexes in one transaction would overwrite each other.
	for _, idxStmt := range []Statement{
		{Kind: CreateIndex, TableName: tblName, IndexName: "idx_cat", Columns: cols[1:2]},
		{Kind: CreateIndex, TableName: tblName, IndexName: "idx_status", Columns: cols[2:3]},
	} {
		stmt := idxStmt
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)
	}

	// Insert 3 rows.
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, row := range [][]OptionalValue{
			// id=1: sports/active
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("sports"))}, {Valid: true, Value: NewTextPointer([]byte("active"))}},
			// id=2: sports/inactive
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("sports"))}, {Valid: true, Value: NewTextPointer([]byte("inactive"))}},
			// id=3: music/active
			{{Valid: true, Value: int64(3)}, {Valid: true, Value: NewTextPointer([]byte("music"))}, {Valid: true, Value: NewTextPointer([]byte("active"))}},
		} {
			_, err := aDatabase.ExecuteStatement(ctx, Statement{
				Kind:      Insert,
				TableName: tblName,
				Columns:   cols,
				Fields:    fieldsFromColumns(cols...),
				Inserts:   [][]OptionalValue{row},
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	table, ok := aDatabase.GetTable(ctx, tblName)
	require.True(t, ok)
	return table, aDatabase.txManager
}

func TestTable_IndexIntersectScan(t *testing.T) {
	table, txManager := intersectTestTable(t)
	ctx := context.Background()
	cols := table.Columns
	catIdxName := "idx_cat"
	statusIdxName := "idx_status"

	t.Run("collectRowIDsFromScan — point scan", func(t *testing.T) {
		t.Parallel()
		scan := Scan{
			TableName:    table.Name,
			Type:         ScanTypeIndexPoint,
			IndexName:    catIdxName,
			IndexColumns: cols[1:2],
			IndexKeys:    []any{"sports"},
		}
		var rowIDs []RowID
		err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
			var err error
			rowIDs, err = table.collectRowIDsFromScan(ctx, scan)
			return err
		})
		require.NoError(t, err)
		sortRowIDs(rowIDs)
		// rows at positions 0 and 1 have cat='sports' (id=1 and id=2)
		assert.Equal(t, []RowID{0, 1}, rowIDs)
	})

	t.Run("collectRowIDsFromScan — range scan", func(t *testing.T) {
		t.Parallel()
		scan := Scan{
			TableName:    table.Name,
			Type:         ScanTypeIndexRange,
			IndexName:    catIdxName,
			IndexColumns: cols[1:2],
			// Range: cat >= 's'  →  'sports' >= 's'  →  both sports rows
			RangeCondition: RangeCondition{
				Lower: &RangeBound{Value: "s", Inclusive: true},
			},
		}
		var rowIDs []RowID
		err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
			var err error
			rowIDs, err = table.collectRowIDsFromScan(ctx, scan)
			return err
		})
		require.NoError(t, err)
		assert.Len(t, rowIDs, 2)
	})

	t.Run("indexIntersectScan — returns rows matching both indexes", func(t *testing.T) {
		// cat='sports' AND status='active'  →  only id=1
		t.Parallel()
		scan := Scan{
			TableName: table.Name,
			Type:      ScanTypeIndexIntersect,
			SubScans: []Scan{
				{
					TableName:    table.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    catIdxName,
					IndexColumns: cols[1:2],
					IndexKeys:    []any{"sports"},
				},
				{
					TableName:    table.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    statusIdxName,
					IndexColumns: cols[2:3],
					IndexKeys:    []any{"active"},
				},
			},
		}
		var rows []Row
		err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
			return table.indexIntersectScan(ctx, scan, fieldsFromColumns(cols...), func(row Row) error {
				rows = append(rows, row)
				return nil
			})
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(1), rows[0].Values[0].Value)
	})

	t.Run("indexIntersectScan — empty intersection", func(t *testing.T) {
		// cat='music' AND status='inactive'  →  no rows
		t.Parallel()
		scan := Scan{
			TableName: table.Name,
			Type:      ScanTypeIndexIntersect,
			SubScans: []Scan{
				{
					TableName:    table.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    catIdxName,
					IndexColumns: cols[1:2],
					IndexKeys:    []any{"music"},
				},
				{
					TableName:    table.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    statusIdxName,
					IndexColumns: cols[2:3],
					IndexKeys:    []any{"inactive"},
				},
			},
		}
		var rows []Row
		err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
			return table.indexIntersectScan(ctx, scan, fieldsFromColumns(cols...), func(row Row) error {
				rows = append(rows, row)
				return nil
			})
		})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("indexIntersectScan — post-filter rejects surviving rows", func(t *testing.T) {
		// Intersection gives id=1 (sports+active), but filter id>10 rejects it.
		t.Parallel()
		scan := Scan{
			TableName: table.Name,
			Type:      ScanTypeIndexIntersect,
			SubScans: []Scan{
				{
					TableName:    table.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    catIdxName,
					IndexColumns: cols[1:2],
					IndexKeys:    []any{"sports"},
				},
				{
					TableName:    table.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    statusIdxName,
					IndexColumns: cols[2:3],
					IndexKeys:    []any{"active"},
				},
			},
			Filters: OneOrMore{
				{
					{
						Operator:  Gt,
						Operand1:  Operand{Type: OperandField, Value: Field{Name: "id"}},
						Operand2:  Operand{Type: OperandInteger, Value: int64(10)},
					},
				},
			},
		}
		var rows []Row
		err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
			return table.indexIntersectScan(ctx, scan, fieldsFromColumns(cols...), func(row Row) error {
				rows = append(rows, row)
				return nil
			})
		})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})
}
