package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_ScalarSubquery(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		sql  string
		// We only assert on the subquery operand to keep tests focused.
		wantOperandType minisql.OperandType
		wantSubKind     minisql.StatementKind
		wantSubTable    string
		wantErr         bool
	}{
		{
			name:            "WHERE id = (SELECT id FROM orders)",
			sql:             "SELECT * FROM users WHERE id = (SELECT id FROM orders)",
			wantOperandType: minisql.OperandSubquery,
			wantSubKind:     minisql.Select,
			wantSubTable:    "orders",
		},
		{
			name:            "WHERE id != (SELECT id FROM orders)",
			sql:             "SELECT * FROM users WHERE id != (SELECT id FROM orders)",
			wantOperandType: minisql.OperandSubquery,
			wantSubKind:     minisql.Select,
			wantSubTable:    "orders",
		},
		{
			name:            "WHERE score > (SELECT score FROM baseline)",
			sql:             "SELECT * FROM users WHERE score > (SELECT score FROM baseline)",
			wantOperandType: minisql.OperandSubquery,
			wantSubKind:     minisql.Select,
			wantSubTable:    "baseline",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := New().Parse(context.Background(), tc.sql)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			stmt := stmts[0]
			require.NotEmpty(t, stmt.Conditions)
			cond := stmt.Conditions[0][0]
			assert.Equal(t, tc.wantOperandType, cond.Operand2.Type)
			subStmt, ok := cond.Operand2.Value.(*minisql.Statement)
			require.True(t, ok, "expected *minisql.Statement, got %T", cond.Operand2.Value)
			assert.Equal(t, tc.wantSubKind, subStmt.Kind)
			assert.Equal(t, tc.wantSubTable, subStmt.TableName)
		})
	}
}

func TestParse_InSubquery(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	require.NotEmpty(t, stmt.Conditions)
	cond := stmt.Conditions[0][0]
	assert.Equal(t, minisql.In, cond.Operator)
	assert.Equal(t, minisql.OperandSubquery, cond.Operand2.Type)
	subStmt, ok := cond.Operand2.Value.(*minisql.Statement)
	require.True(t, ok)
	assert.Equal(t, minisql.Select, subStmt.Kind)
	assert.Equal(t, "orders", subStmt.TableName)
}

func TestParse_NotInSubquery(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	cond := stmts[0].Conditions[0][0]
	assert.Equal(t, minisql.NotIn, cond.Operator)
	assert.Equal(t, minisql.OperandSubquery, cond.Operand2.Type)
	sub := cond.Operand2.Value.(*minisql.Statement)
	assert.Equal(t, "banned", sub.TableName)
}

func TestParse_SubqueryWithNestedParens(t *testing.T) {
	t.Parallel()

	// Subquery contains COUNT(*) which has parens inside it.
	stmts, err := New().Parse(context.Background(),
		"SELECT * FROM users WHERE total = (SELECT COUNT(*) FROM orders)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	cond := stmts[0].Conditions[0][0]
	assert.Equal(t, minisql.OperandSubquery, cond.Operand2.Type)
	sub := cond.Operand2.Value.(*minisql.Statement)
	assert.Equal(t, "orders", sub.TableName)
}

func TestParse_SubqueryWithWhere(t *testing.T) {
	t.Parallel()

	// Subquery has its own WHERE clause.
	stmts, err := New().Parse(context.Background(),
		"SELECT * FROM users WHERE id = (SELECT id FROM orders WHERE status = 'active')")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	cond := stmts[0].Conditions[0][0]
	assert.Equal(t, minisql.OperandSubquery, cond.Operand2.Type)
	sub := cond.Operand2.Value.(*minisql.Statement)
	assert.Equal(t, "orders", sub.TableName)
	assert.NotEmpty(t, sub.Conditions)
}

func TestParse_SubqueryAndLiteralInSameWhere(t *testing.T) {
	t.Parallel()

	// One condition is a subquery, another is a plain literal.
	stmts, err := New().Parse(context.Background(),
		"SELECT * FROM users WHERE name = 'Alice' AND id = (SELECT id FROM orders)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	// DNF: one group with two conditions.
	require.Len(t, stmts[0].Conditions, 1)
	require.Len(t, stmts[0].Conditions[0], 2)

	literalCond := stmts[0].Conditions[0][0]
	assert.Equal(t, minisql.OperandQuotedString, literalCond.Operand2.Type)

	subqueryCond := stmts[0].Conditions[0][1]
	assert.Equal(t, minisql.OperandSubquery, subqueryCond.Operand2.Type)
}

func TestParse_UpdateWithSubquery(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"UPDATE users SET name = 'Bob' WHERE id = (SELECT id FROM orders)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	assert.Equal(t, minisql.Update, stmt.Kind)
	require.NotEmpty(t, stmt.Conditions)
	cond := stmt.Conditions[0][0]
	assert.Equal(t, minisql.OperandSubquery, cond.Operand2.Type)
}

func TestParse_DeleteWithSubquery(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(),
		"DELETE FROM users WHERE id = (SELECT id FROM banned)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	assert.Equal(t, minisql.Delete, stmt.Kind)
	cond := stmt.Conditions[0][0]
	assert.Equal(t, minisql.OperandSubquery, cond.Operand2.Type)
}
