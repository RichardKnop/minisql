package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPager_Empty(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aPager, err := NewPager(dbFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	assert.Equal(t, int64(0), aPager.fileSize)
	assert.Equal(t, 0, int(aPager.totalPages))
	assert.Len(t, aPager.pages, 0)
}

func TestNewPager_WithDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	rowSize := 270
	aRootLeaf := NewLeafNode(uint64(rowSize))
	aRootLeaf.Header.Header.IsRoot = true

	aPager, err := NewPager(dbFile, PageSize, SchemaTableName)
	require.NoError(t, err)
	aPager.dbHeader.FirstFreePage = 125
	aPager.dbHeader.FreePageCount = 2
	aPager.pages = append(aPager.pages, &Page{LeafNode: aRootLeaf})

	// Flushing the root page should also write the DB header
	err = aPager.Flush(context.Background(), 0)
	require.NoError(t, err)

	// Reset pager to empty the cache
	dbFile.Seek(0, 0)
	aPager, err = NewPager(dbFile, PageSize, SchemaTableName)
	require.NoError(t, err)
	assert.Equal(t, 1, int(aPager.totalPages))
	assert.Equal(t, uint32(125), aPager.dbHeader.FirstFreePage)
	assert.Equal(t, uint32(2), aPager.dbHeader.FreePageCount)
}

func TestPager_GetPage(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aPager, err := NewPager(dbFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	aRootPage, internalPages, leafPages := newTestBtree()

	aPager.pages = append(aPager.pages, aRootPage)
	aPager.pages = append(aPager.pages, internalPages[0])
	aPager.pages = append(aPager.pages, internalPages[1])
	aPager.pages = append(aPager.pages, leafPages[0])
	aPager.pages = append(aPager.pages, leafPages[1])
	aPager.pages = append(aPager.pages, leafPages[2])
	aPager.pages = append(aPager.pages, leafPages[3])
	aPager.totalPages = 7
	assert.Len(t, aPager.pages, 7)

	var (
		ctx    = context.Background()
		aTable = &Table{RowSize: 270}
	)

	for pageIdx := 0; pageIdx < int(aPager.TotalPages()); pageIdx++ {
		err := aPager.Flush(ctx, uint32(pageIdx))
		require.NoError(t, err)
	}

	// Reset pager to empty the cache
	dbFile.Seek(0, 0)
	aPager, err = NewPager(dbFile, PageSize, SchemaTableName)
	require.NoError(t, err)
	assert.Equal(t, 7, int(aPager.totalPages))

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
