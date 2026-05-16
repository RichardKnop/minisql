package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestPlanQuery_ORUnion verifies that when every OR branch has its own index
// the planner emits a single ScanTypeIndexUnion scan instead of two independent scans.
func TestPlanQuery_ORUnion(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "status"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "priority"},
		{Kind: Int8, Size: 8, Name: "score"},
	}
	statusIdx := "idx__items__status"
	priorityIdx := "idx__items__priority"

	table := NewTable(zap.NewNop(), nil, nil, "items", columns, 0, nil,
		WithPrimaryKey(NewPrimaryKey("pkey__items", columns[0:1], true)),
	)
	table.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: statusIdx, Columns: columns[1:2]}})
	table.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: priorityIdx, Columns: columns[2:3]}})

	ctx := context.Background()

	t.Run("or_two_indexes_produces_union_scan", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:      Select,
			TableName: "items",
			Columns:   columns,
			Fields:    []Field{{Name: "*"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "status"}, OperandQuotedString, NewTextPointer([]byte("open")))},
				{FieldIsEqual(Field{Name: "priority"}, OperandQuotedString, NewTextPointer([]byte("critical")))},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexUnion, plan.Scans[0].Type,
			"two OR groups each with an index should yield union scan")
		assert.Len(t, plan.Scans[0].SubScans, 2,
			"one sub-scan per OR group")
	})

	t.Run("or_one_group_no_index_falls_back_to_sequential", func(t *testing.T) {
		t.Parallel()
		// score has no index → one group is not index-eligible → falls back to sequential.
		stmt := Statement{
			Kind:      Select,
			TableName: "items",
			Columns:   columns,
			Fields:    []Field{{Name: "*"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "status"}, OperandQuotedString, NewTextPointer([]byte("open")))},
				{FieldIsEqual(Field{Name: "score"}, OperandInteger, int64(100))},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type,
			"when any OR group lacks an index the plan must fall back to sequential scan")
	})

	t.Run("single_or_group_no_union", func(t *testing.T) {
		t.Parallel()
		// Only one AND group — no OR at the top level — should not produce a union scan.
		stmt := Statement{
			Kind:      Select,
			TableName: "items",
			Columns:   columns,
			Fields:    []Field{{Name: "*"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "status"}, OperandQuotedString, NewTextPointer([]byte("open")))},
			},
		}

		plan, err := table.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.NotEqual(t, ScanTypeIndexUnion, plan.Scans[0].Type,
			"a single OR group must not be wrapped in a union scan")
	})

	t.Run("or_three_indexes_all_union", func(t *testing.T) {
		t.Parallel()

		scoreIdx := "idx__items__score"
		table3 := NewTable(zap.NewNop(), nil, nil, "items", columns, 0, nil,
			WithPrimaryKey(NewPrimaryKey("pkey__items", columns[0:1], true)),
		)
		table3.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: statusIdx, Columns: columns[1:2]}})
		table3.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: priorityIdx, Columns: columns[2:3]}})
		table3.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: scoreIdx, Columns: columns[3:4]}})

		stmt := Statement{
			Kind:      Select,
			TableName: "items",
			Columns:   columns,
			Fields:    []Field{{Name: "*"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "status"}, OperandQuotedString, NewTextPointer([]byte("open")))},
				{FieldIsEqual(Field{Name: "priority"}, OperandQuotedString, NewTextPointer([]byte("critical")))},
				{FieldIsEqual(Field{Name: "score"}, OperandInteger, int64(100))},
			},
		}

		plan, err := table3.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexUnion, plan.Scans[0].Type)
		assert.Len(t, plan.Scans[0].SubScans, 3, "three OR groups → three sub-scans")
	})
}

// TestUnionSortedRowIDs verifies the sorted-union helper.
func TestUnionSortedRowIDs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sets [][]RowID
		want []RowID
	}{
		{
			name: "empty",
			sets: nil,
			want: nil,
		},
		{
			name: "single set",
			sets: [][]RowID{{3, 1, 2}},
			want: []RowID{1, 2, 3},
		},
		{
			name: "disjoint sets",
			sets: [][]RowID{{1, 3}, {2, 4}},
			want: []RowID{1, 2, 3, 4},
		},
		{
			name: "overlapping sets",
			sets: [][]RowID{{1, 2, 3}, {2, 3, 4}},
			want: []RowID{1, 2, 3, 4},
		},
		{
			name: "identical sets",
			sets: [][]RowID{{1, 2, 3}, {1, 2, 3}},
			want: []RowID{1, 2, 3},
		},
		{
			name: "three sets with overlap",
			sets: [][]RowID{{1, 4}, {2, 4}, {3, 4}},
			want: []RowID{1, 2, 3, 4},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := unionSortedRowIDs(tc.sets)
			assert.Equal(t, tc.want, got)
		})
	}
}
