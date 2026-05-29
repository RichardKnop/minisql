package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_CreateHNSWIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := New()

	t.Run("basic syntax with default params", func(t *testing.T) {
		stmts, err := p.Parse(ctx, `CREATE HNSW INDEX "idx_vec" ON "docs" (embedding);`)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		assert.Equal(t, minisql.CreateIndex, stmt.Kind)
		assert.Equal(t, minisql.IndexMethodHNSW, stmt.IndexMethod)
		assert.Equal(t, "idx_vec", stmt.IndexName)
		assert.Equal(t, "docs", stmt.TableName)
		require.Len(t, stmt.Columns, 1)
		assert.Equal(t, "embedding", stmt.Columns[0].Name)
		assert.Equal(t, 0, stmt.IndexHNSWM)
		assert.Equal(t, 0, stmt.IndexHNSWEfConstruct)
	})

	t.Run("with m and ef_construction options", func(t *testing.T) {
		stmts, err := p.Parse(ctx, `CREATE HNSW INDEX "idx_vec" ON "docs" (embedding) WITH (m = 8, ef_construction = 100);`)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		assert.Equal(t, minisql.IndexMethodHNSW, stmt.IndexMethod)
		assert.Equal(t, 8, stmt.IndexHNSWM)
		assert.Equal(t, 100, stmt.IndexHNSWEfConstruct)
	})

	t.Run("with m option only", func(t *testing.T) {
		stmts, err := p.Parse(ctx, `CREATE HNSW INDEX "idx_vec" ON "docs" (embedding) WITH (m = 32);`)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		assert.Equal(t, 32, stmt.IndexHNSWM)
		assert.Equal(t, 0, stmt.IndexHNSWEfConstruct)
	})

	t.Run("IF NOT EXISTS", func(t *testing.T) {
		stmts, err := p.Parse(ctx, `CREATE HNSW INDEX IF NOT EXISTS "idx_vec" ON "docs" (embedding);`)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		assert.True(t, stmts[0].IfNotExists)
		assert.Equal(t, minisql.IndexMethodHNSW, stmts[0].IndexMethod)
	})

	t.Run("DDL round-trip with default params", func(t *testing.T) {
		original := `CREATE HNSW INDEX "idx_vec" ON "docs" (embedding);`
		stmts, err := p.Parse(ctx, original)
		require.NoError(t, err)
		stmt := stmts[0]
		ddl := stmt.DDL()
		// Should include default m and ef_construction.
		assert.Contains(t, ddl, "create hnsw index")
		assert.Contains(t, ddl, "embedding")
		assert.Contains(t, ddl, "m = 16")
		assert.Contains(t, ddl, "ef_construction = 200")
	})

	t.Run("DDL round-trip with custom params", func(t *testing.T) {
		stmts, err := p.Parse(ctx, `CREATE HNSW INDEX "idx_vec" ON "docs" (embedding) WITH (m = 8, ef_construction = 50);`)
		require.NoError(t, err)
		ddl := stmts[0].DDL()
		assert.Contains(t, ddl, "m = 8")
		assert.Contains(t, ddl, "ef_construction = 50")
	})
}
