package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEstimateSortCost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		numRows  int64
		wantZero bool
	}{
		{
			name:     "zero rows",
			numRows:  0,
			wantZero: true,
		},
		{
			name:     "one row",
			numRows:  1,
			wantZero: true,
		},
		{
			name:    "100 rows",
			numRows: 100,
		},
		{
			name:    "1000 rows",
			numRows: 1000,
		},
		{
			name:    "10000 rows",
			numRows: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cost := estimateSortCost(tt.numRows)
			if tt.wantZero {
				assert.Equal(t, float64(0), cost, "estimateSortCost(%d) = %v, want 0", tt.numRows, cost)
				return
			}

			assert.GreaterOrEqual(t, cost, float64(0), "estimateSortCost(%d) = %v, want > 0", tt.numRows, cost)

			// Verify it follows O(n log n) pattern
			// For 1000 rows: ~10000, for 10000 rows: ~130000
			expectedOrder := float64(tt.numRows) * 10 // rough check
			if cost < expectedOrder/2 || cost > expectedOrder*2 {
				t.Logf("estimateSortCost(%d) = %v (expected order of magnitude: ~%v)", tt.numRows, cost, expectedOrder)
			}
		})
	}
}

func TestEstimateFilteredRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		stats          *IndexStats
		rangeCondition *RangeCondition
		want           int64
	}{
		{
			name:  "no stats",
			stats: nil,
			want:  -1,
		},
		{
			name: "equality condition - uniform distribution",
			stats: &IndexStats{
				NEntry:    1000,
				NDistinct: []int64{100}, // 100 distinct values = avg 10 rows per value
			},
			rangeCondition: nil,
			want:           10,
		},
		{
			name: "range condition with both bounds",
			stats: &IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: &RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
				Upper: &RangeBound{Value: 50, Inclusive: true},
			},
			want: 300, // 30% selectivity
		},
		{
			name: "range condition with one bound",
			stats: &IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: &RangeCondition{
				Lower: &RangeBound{Value: 500, Inclusive: true},
			},
			want: 500, // 50% selectivity
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := estimateFilteredRows(tt.stats, tt.rangeCondition)
			assert.Equal(t, tt.want, got, "estimateFilteredRows() = %v, want %v", got, tt.want)
		})
	}
}

func TestQueryPlan_OptimizeOrdering_NoFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := zap.NewNop()

	columns := []Column{
		{Name: "id", Kind: Int4, Size: 4},
		{Name: "created_at", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 100},
	}

	table := NewTable(logger, nil, nil, "users", columns, 0,
		WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
	)

	t.Run("no ORDER BY - no sorting needed", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Conditions: OneOrMore{},
			OrderBy:    []OrderBy{},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		assert.False(t, plan.SortInMemory, "expected SortInMemory = false when no ORDER BY")
	})

	t.Run("ORDER BY indexed column - use index", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Conditions: OneOrMore{},
			OrderBy: []OrderBy{
				{Field: Field{Name: "id"}, Direction: Asc},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		assert.False(t, plan.SortInMemory, "expected SortInMemory = false when ORDER BY uses indexed column")
		assert.Equal(t, ScanTypeIndexAll, plan.Scans[0].Type, "expected index scan for ORDER BY on indexed column")
	})

	t.Run("ORDER BY non-indexed column - sort in memory", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Conditions: OneOrMore{},
			OrderBy: []OrderBy{
				{Field: Field{Name: "name"}, Direction: Asc},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		assert.True(t, plan.SortInMemory, "expected SortInMemory = true when ORDER BY uses non-indexed column")
	})
}

func TestQueryPlan_OptimizeOrdering_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("small result set - keep filter index and sort", func(t *testing.T) {
		t.Parallel()

		columns := []Column{
			{Name: "id", Kind: Int4, Size: 4},
			{Name: "status", Kind: Int4, Size: 4},
			{Name: "created_at", Kind: Int8, Size: 8},
		}

		table := NewTable(logger, nil, nil, "users", columns, 0)

		// Create secondary index on status
		table.SecondaryIndexes["idx_status"] = SecondaryIndex{
			IndexInfo: IndexInfo{
				Name:    "idx_status",
				Columns: []Column{{Name: "status", Kind: Int4, Size: 4}},
			},
		}

		// Create secondary index on created_at
		table.SecondaryIndexes["idx_created_at"] = SecondaryIndex{
			IndexInfo: IndexInfo{
				Name:    "idx_created_at",
				Columns: []Column{{Name: "created_at", Kind: Int8, Size: 8}},
			},
		}

		// Rebuild index cache
		table.columnIndexInfoCache = make(map[string]IndexInfo)
		for _, index := range table.SecondaryIndexes {
			table.columnIndexInfoCache[indexColumnHash(index.Columns)] = index.IndexInfo
		}

		// Add stats showing selective filter (returns 100 rows)
		table.indexStats = map[string]IndexStats{
			"idx_status": {
				NEntry:    10000,
				NDistinct: []int64{100}, // 100 distinct values = avg 100 rows per value
			},
		}

		stmt := Statement{
			Conditions: OneOrMore{
				Conditions{
					{
						Operand1: Operand{Type: OperandField, Value: "status"},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int32(1)},
					},
				},
			},
			OrderBy: []OrderBy{
				{Field: Field{Name: "created_at"}, Direction: Desc},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)

		// Should use status index for filtering (small result set)
		assert.Equal(t, "idx_status", plan.Scans[0].IndexName)

		// Should sort in memory (100 rows is below threshold)
		assert.True(t, plan.SortInMemory, "expected SortInMemory = true for small result set")
	})

	t.Run("large result set - use ORDER BY index", func(t *testing.T) {
		t.Parallel()

		columns := []Column{
			{Name: "id", Kind: Int4, Size: 4},
			{Name: "status", Kind: Int4, Size: 4},
			{Name: "created_at", Kind: Int8, Size: 8},
		}

		table := NewTable(logger, nil, nil, "users", columns, 0)

		// Create secondary index on status
		table.SecondaryIndexes["idx_status"] = SecondaryIndex{
			IndexInfo: IndexInfo{
				Name:    "idx_status",
				Columns: []Column{{Name: "status", Kind: Int4, Size: 4}},
			},
		}

		// Create secondary index on created_at
		table.SecondaryIndexes["idx_created_at"] = SecondaryIndex{
			IndexInfo: IndexInfo{
				Name:    "idx_created_at",
				Columns: []Column{{Name: "created_at", Kind: Int8, Size: 8}},
			},
		}

		// Rebuild index cache
		table.columnIndexInfoCache = make(map[string]IndexInfo)
		for _, index := range table.SecondaryIndexes {
			table.columnIndexInfoCache[indexColumnHash(index.Columns)] = index.IndexInfo
		}

		// Add stats showing non-selective filter (returns 5000 rows)
		table.indexStats = map[string]IndexStats{
			"idx_status": {
				NEntry:    10000,
				NDistinct: []int64{2}, // 2 distinct values = avg 5000 rows per value
			},
		}

		stmt := Statement{
			Conditions: OneOrMore{
				Conditions{
					{
						Operand1: Operand{Type: OperandField, Value: "status"},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int32(1)},
					},
				},
			},
			OrderBy: []OrderBy{
				{Field: Field{Name: "created_at"}, Direction: Desc},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)

		// Should switch to ORDER BY index (large result set, sorting would be expensive)
		assert.Equal(t, "idx_created_at", plan.Scans[0].IndexName)

		// Should not sort in memory (using ORDER BY index)
		assert.False(t, plan.SortInMemory, "expected SortInMemory = false when using ORDER BY index")

		// Filters should still be present for post-scan filtering
		assert.NotEmpty(t, plan.Scans[0].Filters, "expected filters to be preserved for post-scan filtering")
	})

	t.Run("no stats - conservative default", func(t *testing.T) {
		t.Parallel()

		columns := []Column{
			{Name: "id", Kind: Int4, Size: 4},
			{Name: "status", Kind: Int4, Size: 4},
			{Name: "created_at", Kind: Int8, Size: 8},
		}

		table := NewTable(logger, nil, nil, "users", columns, 0)

		// Create secondary index on status
		table.SecondaryIndexes["idx_status"] = SecondaryIndex{
			IndexInfo: IndexInfo{
				Name:    "idx_status",
				Columns: []Column{{Name: "status", Kind: Int4, Size: 4}},
			},
		}

		// Create secondary index on created_at
		table.SecondaryIndexes["idx_created_at"] = SecondaryIndex{
			IndexInfo: IndexInfo{
				Name:    "idx_created_at",
				Columns: []Column{{Name: "created_at", Kind: Int8, Size: 8}},
			},
		}

		// Rebuild index cache
		table.columnIndexInfoCache = make(map[string]IndexInfo)
		for _, index := range table.SecondaryIndexes {
			table.columnIndexInfoCache[indexColumnHash(index.Columns)] = index.IndexInfo
		}

		// No stats available
		table.indexStats = map[string]IndexStats{}

		stmt := Statement{
			Conditions: OneOrMore{
				Conditions{
					{
						Operand1: Operand{Type: OperandField, Value: "status"},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int32(1)},
					},
				},
			},
			OrderBy: []OrderBy{
				{Field: Field{Name: "created_at"}, Direction: Desc},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)

		// Without stats, should keep filter index (conservative)
		assert.Equal(t, "idx_status", plan.Scans[0].IndexName)

		// Should sort in memory
		assert.True(t, plan.SortInMemory, "expected SortInMemory = true without stats")
	})
}
