package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_FullTextSearchFunctions(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(), `
		SELECT id, ts_rank(body, 'mini database') AS score
		FROM articles
		WHERE MATCH(body, 'mini database')
		ORDER BY score DESC;
	`)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	require.Len(t, stmt.Fields, 2)
	assert.Equal(t, minisql.Field{Name: "id"}, stmt.Fields[0])
	require.NotNil(t, stmt.Fields[1].Expr)
	assert.Equal(t, "TS_RANK", stmt.Fields[1].Expr.FuncName)
	assert.Equal(t, "score", stmt.Fields[1].Alias)

	require.Len(t, stmt.Conditions, 1)
	require.Len(t, stmt.Conditions[0], 1)
	cond := stmt.Conditions[0][0]
	assert.Equal(t, minisql.Eq, cond.Operator)
	assert.Equal(t, minisql.OperandBoolean, cond.Operand2.Type)
	assert.Equal(t, true, cond.Operand2.Value)
	require.Equal(t, minisql.OperandExpr, cond.Operand1.Type)
	matchExpr := cond.Operand1.Value.(*minisql.Expr)
	assert.Equal(t, "MATCH", matchExpr.FuncName)
	require.Len(t, matchExpr.Args, 2)
	assert.Equal(t, "body", matchExpr.Args[0].Column)
	assert.Equal(t, minisql.NewTextPointer([]byte("mini database")), matchExpr.Args[1].Literal)

	require.Len(t, stmt.OrderBy, 1)
	assert.Equal(t, minisql.Field{Name: "score"}, stmt.OrderBy[0].Field)
	assert.Equal(t, minisql.Desc, stmt.OrderBy[0].Direction)
}
