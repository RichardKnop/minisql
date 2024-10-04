package pager

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
	"github.com/RichardKnop/minisql/internal/pkg/minisql/minisqltest"
)

var (
	gen = minisqltest.NewDataGen(time.Now().Unix())
)

func TestNew_Empty(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aPager, err := New(dbFile, "minisql_main")
	require.NoError(t, err)

	assert.Equal(t, int64(0), aPager.fileSize)
	assert.Equal(t, 0, int(aPager.totalPages))
	assert.Len(t, aPager.pages, MaxPages)
}

func TestNew_GetPage(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aPager, err := New(dbFile, "minisql_main")
	require.NoError(t, err)

	aRootPage, internalPages, leafPages := gen.NewTestBtree()

	aPager.pages[0] = aRootPage
	aPager.pages[1] = internalPages[0]
	aPager.pages[2] = internalPages[1]
	aPager.pages[3] = leafPages[0]
	aPager.pages[4] = leafPages[1]
	aPager.pages[5] = leafPages[2]
	aPager.pages[6] = leafPages[3]
	aPager.totalPages = 7

	var (
		ctx    = context.Background()
		aTable = &minisql.Table{RowSize: 270}
	)

	for pageIdx := 0; pageIdx < int(aPager.totalPages); pageIdx++ {
		err := aPager.Flush(ctx, uint32(pageIdx), PageSize)
		require.NoError(t, err)
	}

	// Reset pager cache to empty the cache
	aPager.pages = make([]*minisql.Page, MaxPages)
	aPager.totalPages = 0

	// Root page
	aPage, err := aPager.GetPage(ctx, aTable, uint32(0))
	require.NoError(t, err)
	assert.Equal(t, aRootPage, aPage)

	// Internal pages

	aPage, err = aPager.GetPage(ctx, aTable, uint32(1))
	require.NoError(t, err)
	assert.Equal(t, internalPages[0], aPage)

	aPage, err = aPager.GetPage(ctx, aTable, uint32(2))
	require.NoError(t, err)
	assert.Equal(t, internalPages[1], aPage)

	// Leaf pages

	aPage, err = aPager.GetPage(ctx, aTable, uint32(3))
	require.NoError(t, err)
	assert.Equal(t, leafPages[0], aPage)

	aPage, err = aPager.GetPage(ctx, aTable, uint32(4))
	require.NoError(t, err)
	assert.Equal(t, leafPages[1], aPage)

	aPage, err = aPager.GetPage(ctx, aTable, uint32(5))
	require.NoError(t, err)
	assert.Equal(t, leafPages[2], aPage)

	aPage, err = aPager.GetPage(ctx, aTable, uint32(6))
	require.NoError(t, err)
	assert.Equal(t, leafPages[3], aPage)
}
