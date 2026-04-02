package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// tableForMinMax builds a Table with a primary key on "id" and a secondary
// non-unique index on "score", used across all MIN/MAX planner tests.
func tableForMinMax(t *testing.T) *Table {
	t.Helper()
	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "score", Kind: Int4, Size: 4},
		{Name: "label", Kind: Varchar, Size: 100},
	}
	tbl := NewTable(zap.NewNop(), nil, nil, "results", cols, 0, nil,
		WithPrimaryKey(NewPrimaryKey("pk_results", cols[0:1], false)),
	)
	tbl.SetSecondaryIndex("idx_score", []Column{cols[1]}, nil)
	return tbl
}

func TestTable_PlanQuery_MinMaxOptimisation(t *testing.T) {
	t.Parallel()

	var (
		ctx = context.Background()
		tbl = tableForMinMax(t)
	)

	t.Run("MIN on indexed column uses ScanTypeIndexFirst", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMin, Column: "score"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexFirst, plan.Scans[0].Type)
		assert.Equal(t, "idx_score", plan.Scans[0].IndexName)
	})

	t.Run("MAX on indexed column uses ScanTypeIndexLast", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMax, Column: "score"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexLast, plan.Scans[0].Type)
		assert.Equal(t, "idx_score", plan.Scans[0].IndexName)
	})

	t.Run("MIN on primary key column uses ScanTypeIndexFirst", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMin, Column: "id"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexFirst, plan.Scans[0].Type)
	})

	t.Run("MAX on primary key column uses ScanTypeIndexLast", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMax, Column: "id"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexLast, plan.Scans[0].Type)
	})

	t.Run("MIN on non-indexed column falls back to sequential scan", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMin, Column: "label"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
	})

	t.Run("MAX on non-indexed column falls back to sequential scan", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMax, Column: "label"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
	})

	t.Run("WHERE clause disables index endpoint optimisation", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMin, Column: "score"},
			},
			Conditions: OneOrMore{
				{FieldIsGreater(Field{Name: "id"}, OperandInteger, int64(10))},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.NotEqual(t, ScanTypeIndexFirst, plan.Scans[0].Type)
		assert.NotEqual(t, ScanTypeIndexLast, plan.Scans[0].Type)
	})

	t.Run("multiple aggregates fall back to sequential scan", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateMin, Column: "score"},
				{Kind: AggregateMax, Column: "score"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
	})

	t.Run("SUM aggregate does not use index endpoint", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateSum, Column: "score"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
	})

	t.Run("AVG aggregate does not use index endpoint", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind: Select,
			Aggregates: []AggregateExpr{
				{Kind: AggregateAvg, Column: "score"},
			},
		}

		plan, err := tbl.PlanQuery(ctx, stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
	})
}
