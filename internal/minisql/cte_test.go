package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCTEFromContext_EmptyContext(t *testing.T) {
	t.Parallel()

	_, ok := cteFromContext(context.Background(), "cte1")
	assert.False(t, ok)
}

func TestCTEFromContext_RegistryMiss(t *testing.T) {
	t.Parallel()

	registry := map[string]*Table{"other": {Name: "other"}}
	ctx := ctxWithCTERegistry(context.Background(), registry)
	_, ok := cteFromContext(ctx, "cte1")
	assert.False(t, ok)
}

func TestCTEFromContext_RegistryHit(t *testing.T) {
	t.Parallel()

	vt := &Table{Name: "cte1"}
	registry := map[string]*Table{"cte1": vt}
	ctx := ctxWithCTERegistry(context.Background(), registry)

	got, ok := cteFromContext(ctx, "cte1")
	require.True(t, ok)
	assert.Same(t, vt, got)
}

func TestCTEFromContext_RegistryUpdatedAfterContext(t *testing.T) {
	t.Parallel()

	// Maps are reference types: adding to the map after storing it in ctx
	// is visible through cteFromContext (important for sequential materialisation).
	registry := map[string]*Table{}
	ctx := ctxWithCTERegistry(context.Background(), registry)

	vt := &Table{Name: "cte1"}
	registry["cte1"] = vt

	got, ok := cteFromContext(ctx, "cte1")
	require.True(t, ok)
	assert.Same(t, vt, got)
}

func TestCtxWithCTERegistry_NestedContexts(t *testing.T) {
	t.Parallel()

	// Inner context's registry shadows the outer one.
	outer := map[string]*Table{"a": {Name: "a-outer"}}
	inner := map[string]*Table{"a": {Name: "a-inner"}, "b": {Name: "b"}}

	outerCtx := ctxWithCTERegistry(context.Background(), outer)
	innerCtx := ctxWithCTERegistry(outerCtx, inner)

	got, ok := cteFromContext(innerCtx, "a")
	require.True(t, ok)
	assert.Equal(t, "a-inner", got.Name)

	got, ok = cteFromContext(innerCtx, "b")
	require.True(t, ok)
	assert.Equal(t, "b", got.Name)

	// Outer context still sees its own registry.
	got, ok = cteFromContext(outerCtx, "a")
	require.True(t, ok)
	assert.Equal(t, "a-outer", got.Name)
}
