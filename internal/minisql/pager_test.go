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

	dbFile, err := os.CreateTemp(".", testDBName)
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)

	assert.Equal(t, int64(0), pager.fileSize)
	assert.Equal(t, 0, int(pager.totalPages))
	assert.Len(t, pager.pages, 0)
}

func TestNewPager_WithDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", testDBName)
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aRootLeaf := NewLeafNode()
	aRootLeaf.Header.IsRoot = true

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.dbHeader.FirstFreePage = 125
	pager.dbHeader.FreePageCount = 2
	pager.pages = append(pager.pages, &Page{LeafNode: aRootLeaf})

	// Flushing the root page should also write the DB header
	err = pager.Flush(context.Background(), 0)
	require.NoError(t, err)

	// Reset pager to empty the cache
	dbFile.Seek(0, 0)
	pager, err = NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	assert.Equal(t, 1, int(pager.totalPages))
	assert.Equal(t, PageIndex(125), pager.dbHeader.FirstFreePage)
	assert.Equal(t, uint32(2), pager.dbHeader.FreePageCount)
}

func TestPager_GetPage(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", testDBName)
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)

	rootPage, internalPages, leafPages := newTestBtree()

	pager.pages = append(pager.pages, rootPage)
	pager.pages = append(pager.pages, internalPages[0])
	pager.pages = append(pager.pages, internalPages[1])
	pager.pages = append(pager.pages, leafPages[0])
	pager.pages = append(pager.pages, leafPages[1])
	pager.pages = append(pager.pages, leafPages[2])
	pager.pages = append(pager.pages, leafPages[3])
	pager.totalPages = 7
	assert.Len(t, pager.pages, 7)

	var (
		ctx     = context.Background()
		columns = []Column{
			{Kind: Varchar, Size: 270},
		}
		tablePager = pager.ForTable(columns)
	)

	for pageIdx := PageIndex(0); pageIdx < PageIndex(pager.TotalPages()); pageIdx++ {
		err := pager.Flush(ctx, pageIdx)
		require.NoError(t, err)
	}

	// Reset pager to empty the cache
	dbFile.Seek(0, 0)
	pager, err = NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	assert.Equal(t, 7, int(pager.totalPages))
	tablePager = pager.ForTable(columns)

	// Root page
	page, err := tablePager.GetPage(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, rootPage, page)

	// Internal pages

	page, err = tablePager.GetPage(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, internalPages[0], page)

	page, err = tablePager.GetPage(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, internalPages[1], page)

	// Leaf pages

	page, err = tablePager.GetPage(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, leafPages[0], page)

	page, err = tablePager.GetPage(ctx, 4)
	require.NoError(t, err)
	assert.Equal(t, leafPages[1], page)

	page, err = tablePager.GetPage(ctx, 5)
	require.NoError(t, err)
	assert.Equal(t, leafPages[2], page)

	page, err = tablePager.GetPage(ctx, 6)
	require.NoError(t, err)
	assert.Equal(t, leafPages[3], page)
}

func TestNewPager_InvalidDatabaseHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", testDBName)
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	buf := make([]byte, PageSize)
	copy(buf, []byte("notmini!"))
	buf[databaseHeaderMetadataSize] = 1
	_, err = dbFile.Write(buf)
	require.NoError(t, err)

	_, err = NewPager(dbFile, PageSize, 1000)
	require.Error(t, err)
	assert.Equal(t, "invalid database header magic", err.Error())
}
