package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_VectorColumn(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"CREATE TABLE with vector column",
			"CREATE TABLE docs (id int8 primary key, embedding vector(3));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "docs",
					Columns: []minisql.Column{
						{Name: "id", Kind: minisql.Int8, Size: 8, Nullable: false},
						{Name: "embedding", Kind: minisql.Vector, Size: 3, Nullable: true},
					},
					PrimaryKey: minisql.NewPrimaryKey(
						minisql.PrimaryKeyName("docs"),
						[]minisql.Column{{Name: "id", Kind: minisql.Int8, Size: 8, Nullable: false}},
						false,
					),
				},
			},
			nil,
		},
		{
			"CREATE TABLE vector not null",
			"CREATE TABLE embeddings (id int8 primary key, vec vector(1536) not null);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "embeddings",
					Columns: []minisql.Column{
						{Name: "id", Kind: minisql.Int8, Size: 8, Nullable: false},
						{Name: "vec", Kind: minisql.Vector, Size: 1536, Nullable: false},
					},
					PrimaryKey: minisql.NewPrimaryKey(
						minisql.PrimaryKeyName("embeddings"),
						[]minisql.Column{{Name: "id", Kind: minisql.Int8, Size: 8, Nullable: false}},
						false,
					),
				},
			},
			nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			t.Parallel()
			got, err := New().Parse(context.Background(), aTestCase.SQL)
			if aTestCase.Err != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, got)
		})
	}
}

func TestParse_VectorColumnDimensionBounds(t *testing.T) {
	t.Parallel()

	t.Run("zero dimension rejected", func(t *testing.T) {
		t.Parallel()
		_, err := New().Parse(context.Background(), "CREATE TABLE t (id int8 primary key, v vector(0));")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dimension")
	})

	t.Run("max dimension accepted", func(t *testing.T) {
		t.Parallel()
		_, err := New().Parse(context.Background(), "CREATE TABLE t (id int8 primary key, v vector(16384));")
		require.NoError(t, err)
	})

	t.Run("dimension exceeding max rejected", func(t *testing.T) {
		t.Parallel()
		_, err := New().Parse(context.Background(), "CREATE TABLE t (id int8 primary key, v vector(16385));")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds maximum")
	})
}

func TestParse_VecL2Function(t *testing.T) {
	t.Parallel()

	sql := `SELECT id, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist FROM docs ORDER BY dist LIMIT 5;`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	assert.Equal(t, minisql.Select, stmt.Kind)

	// Verify VEC_L2 is parsed as a function expression.
	var found bool
	for _, f := range stmt.Fields {
		if f.Expr != nil && f.Expr.FuncName == "VEC_L2" {
			found = true
			assert.Equal(t, "dist", f.Alias)
			assert.Len(t, f.Expr.Args, 2)
		}
	}
	assert.True(t, found, "VEC_L2 expression not found in SELECT fields")
}

func TestParse_VecCosineFunction(t *testing.T) {
	t.Parallel()

	sql := `SELECT id, VEC_COSINE(embedding, '[1.0, 0.0]') AS dist FROM docs ORDER BY dist LIMIT 10;`
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	var found bool
	for _, f := range stmt.Fields {
		if f.Expr != nil && f.Expr.FuncName == "VEC_COSINE" {
			found = true
			assert.Equal(t, "dist", f.Alias)
			assert.Len(t, f.Expr.Args, 2)
		}
	}
	assert.True(t, found, "VEC_COSINE expression not found in SELECT fields")
}

func TestParse_VectorColumnDDLRoundTrip(t *testing.T) {
	t.Parallel()

	sql := "CREATE TABLE docs (id int8 primary key, embedding vector(1536) not null);"
	stmts, err := New().Parse(context.Background(), sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	ddl := stmts[0].DDL()
	assert.Contains(t, ddl, "vector(1536)", "DDL must include vector dimension")

	// Round-trip: parse the generated DDL.
	stmts2, err := New().Parse(context.Background(), ddl)
	require.NoError(t, err)
	require.Len(t, stmts2, 1)
	assert.Equal(t, stmts[0].Columns, stmts2[0].Columns)
}
