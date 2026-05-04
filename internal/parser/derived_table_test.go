package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_DerivedTableBasic(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"SELECT t.user_id, t.cnt FROM (SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id) t")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	assert.Equal(t, minisql.Select, stmt.Kind)
	assert.Equal(t, "", stmt.TableName)
	require.NotNil(t, stmt.FromSubquery)
	assert.Equal(t, "t", stmt.FromSubqueryAlias)
	assert.Equal(t, minisql.Select, stmt.FromSubquery.Kind)
	assert.Equal(t, "orders", stmt.FromSubquery.TableName)
}

func TestParse_DerivedTableWithWhere(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"SELECT t.name FROM (SELECT name FROM users WHERE score > 80) t WHERE t.name != 'Bob'")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	require.NotNil(t, stmt.FromSubquery)
	assert.Equal(t, "t", stmt.FromSubqueryAlias)
	assert.NotEmpty(t, stmt.FromSubquery.Conditions)
	assert.NotEmpty(t, stmt.Conditions)
}

func TestParse_DerivedTableWithAsKeyword(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"SELECT sub.id FROM (SELECT id FROM users) AS sub")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	require.NotNil(t, stmt.FromSubquery)
	assert.Equal(t, "sub", stmt.FromSubqueryAlias)
}

func TestParse_DerivedTableSelectStar(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"SELECT * FROM (SELECT id, name FROM users) t")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	require.NotNil(t, stmt.FromSubquery)
	assert.Equal(t, "t", stmt.FromSubqueryAlias)
}

func TestParse_DerivedTableMissingAlias_Error(t *testing.T) {
	t.Parallel()

	_, err := New().Parse(context.Background(),
		"SELECT * FROM (SELECT id FROM users)")
	require.Error(t, err)
}
