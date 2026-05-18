package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_UpdateSetSubquery_NonCorrelated(t *testing.T) {
	t.Parallel()

	sql := `UPDATE products SET price = (SELECT AVG(price) FROM products)`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Equal(t, minisql.Update, stmt.Kind)
	assert.Equal(t, "products", stmt.TableName)

	val, ok := stmt.Updates["price"]
	require.True(t, ok, "price must be in Updates")
	require.True(t, val.IsStatement(), "SET value must be *Statement (subquery)")
	inner := val.AsStatement()
	assert.Equal(t, minisql.Select, inner.Kind)
	assert.Equal(t, "products", inner.TableName)
}

func TestParse_UpdateSetSubquery_Correlated(t *testing.T) {
	t.Parallel()

	sql := `UPDATE employees e SET salary = (SELECT avg_salary FROM depts WHERE id = e.dept_id) WHERE e.active = true`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Equal(t, minisql.Update, stmt.Kind)
	assert.Equal(t, "employees", stmt.TableName)
	assert.Equal(t, "e", stmt.TableAlias)

	val, ok := stmt.Updates["salary"]
	require.True(t, ok, "salary must be in Updates")
	require.True(t, val.IsStatement(), "SET value must be *Statement (subquery)")
	inner := val.AsStatement()
	assert.Equal(t, minisql.Select, inner.Kind)
	assert.Equal(t, "depts", inner.TableName)
	require.NotEmpty(t, inner.Conditions)
}

func TestParse_UpdateSetSubquery_MultipleColumns(t *testing.T) {
	t.Parallel()

	sql := `UPDATE employees e SET salary = (SELECT avg_salary FROM depts WHERE id = e.dept_id), bonus = (SELECT avg_bonus FROM depts WHERE id = e.dept_id)`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Equal(t, minisql.Update, stmt.Kind)

	salaryVal, ok := stmt.Updates["salary"]
	require.True(t, ok)
	require.True(t, salaryVal.IsStatement(), "salary SET value must be *Statement")

	bonusVal, ok := stmt.Updates["bonus"]
	require.True(t, ok)
	require.True(t, bonusVal.IsStatement(), "bonus SET value must be *Statement")
}

func TestParse_UpdateSetSubquery_MixedWithLiteral(t *testing.T) {
	t.Parallel()

	sql := `UPDATE employees e SET salary = (SELECT avg_salary FROM depts WHERE id = e.dept_id), active = true`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.True(t, stmt.Updates["salary"].IsStatement(), "salary must be subquery")
	assert.Equal(t, true, stmt.Updates["active"].AsBool(), "active must be bool literal")
}

func TestParse_UpdateSetSubquery_WithoutWhere(t *testing.T) {
	t.Parallel()

	sql := `UPDATE products SET price = (SELECT AVG(price) FROM products)`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Empty(t, stmt.Conditions, "no WHERE clause should produce empty conditions")
}
