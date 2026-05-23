package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_WindowFunctions(t *testing.T) {
	t.Parallel()

	parse := func(t *testing.T, sql string) minisql.Statement {
		t.Helper()
		stmts, err := New().Parse(context.Background(), sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		return stmts[0]
	}

	t.Run("ROW_NUMBER no partition", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM t;`)
		require.Len(t, stmt.Fields, 1)
		f := stmt.Fields[0]
		assert.Equal(t, "rn", f.Alias)
		require.NotNil(t, f.Expr)
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowRowNumber, f.Expr.WindowFunc.Kind)
		spec := f.Expr.WindowFunc.Spec
		assert.Empty(t, spec.PartitionBy)
		require.Len(t, spec.OrderBy, 1)
		assert.Equal(t, "score", spec.OrderBy[0].Field.Name)
		assert.Equal(t, minisql.Desc, spec.OrderBy[0].Direction)
	})

	t.Run("ROW_NUMBER with partition", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score) AS rn FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		spec := f.Expr.WindowFunc.Spec
		assert.Equal(t, []string{"dept"}, spec.PartitionBy)
		require.Len(t, spec.OrderBy, 1)
		assert.Equal(t, "score", spec.OrderBy[0].Field.Name)
		assert.Equal(t, minisql.Asc, spec.OrderBy[0].Direction)
	})

	t.Run("RANK with partition", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS r FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowRank, f.Expr.WindowFunc.Kind)
	})

	t.Run("DENSE_RANK", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT DENSE_RANK() OVER (ORDER BY score) AS dr FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowDenseRank, f.Expr.WindowFunc.Kind)
	})

	t.Run("NTILE with argument", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT NTILE(4) OVER (ORDER BY score DESC) AS bucket FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowNtile, f.Expr.WindowFunc.Kind)
		require.NotNil(t, f.Expr.WindowFunc.Arg)
		assert.Equal(t, int64(4), f.Expr.WindowFunc.Arg.Literal)
	})

	t.Run("LAG with offset argument", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT LAG(score, 1) OVER (ORDER BY score) AS prev FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowLag, f.Expr.WindowFunc.Kind)
		require.NotNil(t, f.Expr.WindowFunc.Arg)
		assert.Equal(t, "score", f.Expr.WindowFunc.Arg.Column)
		require.NotNil(t, f.Expr.WindowFunc.Arg2)
		assert.Equal(t, int64(1), f.Expr.WindowFunc.Arg2.Literal)
	})

	t.Run("LEAD with offset", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT LEAD(score, 2) OVER (ORDER BY id) AS next2 FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowLead, f.Expr.WindowFunc.Kind)
	})

	t.Run("FIRST_VALUE", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT FIRST_VALUE(score) OVER (PARTITION BY dept ORDER BY score DESC) AS top FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowFirstValue, f.Expr.WindowFunc.Kind)
		require.NotNil(t, f.Expr.WindowFunc.Arg)
		assert.Equal(t, "score", f.Expr.WindowFunc.Arg.Column)
	})

	t.Run("LAST_VALUE", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT LAST_VALUE(score) OVER (ORDER BY score) AS lv FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowLastValue, f.Expr.WindowFunc.Kind)
	})

	t.Run("NTH_VALUE", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT NTH_VALUE(score, 2) OVER (ORDER BY score DESC) AS second FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowNthValue, f.Expr.WindowFunc.Kind)
		require.NotNil(t, f.Expr.WindowFunc.Arg)
		assert.Equal(t, "score", f.Expr.WindowFunc.Arg.Column)
		require.NotNil(t, f.Expr.WindowFunc.Arg2)
		assert.Equal(t, int64(2), f.Expr.WindowFunc.Arg2.Literal)
	})

	t.Run("SUM OVER partition", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT SUM(score) OVER (PARTITION BY dept) AS dept_total FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr)
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowSum, f.Expr.WindowFunc.Kind)
		require.NotNil(t, f.Expr.WindowFunc.Arg)
		assert.Equal(t, "score", f.Expr.WindowFunc.Arg.Column)
		assert.Equal(t, []string{"dept"}, f.Expr.WindowFunc.Spec.PartitionBy)
	})

	t.Run("AVG OVER", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT AVG(score) OVER (ORDER BY id) AS running_avg FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowAvg, f.Expr.WindowFunc.Kind)
	})

	t.Run("COUNT(*) OVER", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT COUNT(*) OVER () AS total FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr)
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowCount, f.Expr.WindowFunc.Kind)
	})

	t.Run("MIN OVER", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT MIN(score) OVER (PARTITION BY dept) AS min_score FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowMin, f.Expr.WindowFunc.Kind)
	})

	t.Run("MAX OVER", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT MAX(score) OVER (PARTITION BY dept) AS max_score FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		assert.Equal(t, minisql.WindowMax, f.Expr.WindowFunc.Kind)
	})

	t.Run("ROWS BETWEEN frame", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT SUM(score) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS roll FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		spec := f.Expr.WindowFunc.Spec
		require.NotNil(t, spec.Frame)
		assert.Equal(t, minisql.FrameRows, spec.Frame.Mode)
		assert.Equal(t, minisql.FramePreceding, spec.Frame.Start.Kind)
		assert.Equal(t, 1, spec.Frame.Start.Offset)
		assert.Equal(t, minisql.FrameFollowing, spec.Frame.End.Kind)
		assert.Equal(t, 1, spec.Frame.End.Offset)
	})

	t.Run("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT SUM(score) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cs FROM t;`)
		f := stmt.Fields[0]
		require.NotNil(t, f.Expr.WindowFunc)
		spec := f.Expr.WindowFunc.Spec
		require.NotNil(t, spec.Frame)
		assert.Equal(t, minisql.FrameUnboundedPreceding, spec.Frame.Start.Kind)
		assert.Equal(t, minisql.FrameCurrentRow, spec.Frame.End.Kind)
	})

	t.Run("multiple window functions in same query", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT ROW_NUMBER() OVER (ORDER BY score DESC) AS rn, SUM(score) OVER (PARTITION BY dept) AS total FROM t;`)
		require.Len(t, stmt.Fields, 2)
		assert.Equal(t, minisql.WindowRowNumber, stmt.Fields[0].Expr.WindowFunc.Kind)
		assert.Equal(t, minisql.WindowSum, stmt.Fields[1].Expr.WindowFunc.Kind)
	})

	t.Run("window mixed with regular fields", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT name, ROW_NUMBER() OVER (ORDER BY score) AS rn FROM t;`)
		require.Len(t, stmt.Fields, 2)
		assert.Equal(t, "name", stmt.Fields[0].Name)
		assert.Nil(t, stmt.Fields[0].Expr)
		require.NotNil(t, stmt.Fields[1].Expr)
		assert.Equal(t, minisql.WindowRowNumber, stmt.Fields[1].Expr.WindowFunc.Kind)
	})

	t.Run("HasWindowFuncs true", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t;`)
		assert.True(t, stmt.HasWindowFuncs())
	})

	t.Run("HasWindowFuncs false for plain select", func(t *testing.T) {
		t.Parallel()
		stmt := parse(t, `SELECT name, score FROM t;`)
		assert.False(t, stmt.HasWindowFuncs())
	})
}
