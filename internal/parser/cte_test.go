package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_CTE_Single(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH active AS (SELECT id, name FROM users WHERE score > 80) SELECT active.id, active.name FROM active")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Equal(t, minisql.Select, stmt.Kind)
	assert.Equal(t, "active", stmt.TableName)
	require.Len(t, stmt.CTEs, 1)
	assert.Equal(t, "active", stmt.CTEs[0].Name)
	require.NotNil(t, stmt.CTEs[0].Body)
	assert.Equal(t, minisql.Select, stmt.CTEs[0].Body.Kind)
	assert.Equal(t, "users", stmt.CTEs[0].Body.TableName)
	assert.NotEmpty(t, stmt.CTEs[0].Body.Conditions)
}

func TestParse_CTE_Multiple(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM orders) SELECT cte1.id FROM cte1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Equal(t, minisql.Select, stmt.Kind)
	require.Len(t, stmt.CTEs, 2)
	assert.Equal(t, "cte1", stmt.CTEs[0].Name)
	assert.Equal(t, "users", stmt.CTEs[0].Body.TableName)
	assert.Equal(t, "cte2", stmt.CTEs[1].Name)
	assert.Equal(t, "orders", stmt.CTEs[1].Body.TableName)
}

func TestParse_CTE_OuterWhere(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH t AS (SELECT id, score FROM users) SELECT t.id FROM t WHERE t.score > 90")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	require.Len(t, stmt.CTEs, 1)
	assert.NotEmpty(t, stmt.Conditions)
}

func TestParse_CTE_BodyWithGroupBy(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH totals AS (SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id) SELECT totals.user_id, totals.cnt FROM totals")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	require.Len(t, stmt.CTEs, 1)
	assert.Equal(t, "totals", stmt.CTEs[0].Name)
	assert.Equal(t, "orders", stmt.CTEs[0].Body.TableName)
	assert.NotEmpty(t, stmt.CTEs[0].Body.GroupBy)
}

func TestParse_CTE_MissingSelect_Error(t *testing.T) {
	t.Parallel()

	_, err := New().Parse(context.Background(),
		"WITH cte AS (INSERT INTO users VALUES (1))")
	require.Error(t, err)
}

func TestParse_CTE_MissingAlias_Error(t *testing.T) {
	t.Parallel()

	_, err := New().Parse(context.Background(),
		"WITH AS (SELECT id FROM users) SELECT * FROM t")
	require.Error(t, err)
}

func TestParse_CTE_NumPlaceholders(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH t AS (SELECT id FROM users WHERE score > ?) SELECT t.id FROM t WHERE t.id = ?")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	assert.Equal(t, 2, stmts[0].NumPlaceholders())
}

func TestParse_CTE_BindArguments(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH t AS (SELECT id FROM users WHERE score > ?) SELECT t.id FROM t WHERE t.id = ?")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	bound, err := stmts[0].BindArguments(int64(80), int64(1))
	require.NoError(t, err)

	// CTE placeholder bound
	require.Len(t, bound.CTEs, 1)
	require.NotEmpty(t, bound.CTEs[0].Body.Conditions)
	assert.Equal(t, minisql.OperandInteger, bound.CTEs[0].Body.Conditions[0][0].Operand2.Type)
	assert.Equal(t, int64(80), bound.CTEs[0].Body.Conditions[0][0].Operand2.Value)

	// Outer WHERE placeholder bound
	require.NotEmpty(t, bound.Conditions)
	assert.Equal(t, minisql.OperandInteger, bound.Conditions[0][0].Operand2.Type)
	assert.Equal(t, int64(1), bound.Conditions[0][0].Operand2.Value)
}

func TestParse_CTE_Clone(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"WITH t AS (SELECT id FROM users) SELECT t.id FROM t")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	clone := stmts[0].Clone()
	require.Len(t, clone.CTEs, 1)

	// Mutation of clone does not affect original.
	clone.CTEs[0].Body.TableName = "other"
	assert.Equal(t, "users", stmts[0].CTEs[0].Body.TableName)
}
