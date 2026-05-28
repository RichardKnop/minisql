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
	assert.Empty(t, pager.pages)
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

func TestPager_WAL_GetPage_CacheMissUsesWALIndex(t *testing.T) {
	t.Parallel()

	// Setup: create a pager backed by an empty DB file and a WAL index that
	// already has page 3 pre-loaded.
	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer func() {
		_ = dbFile.Close()
		_ = os.Remove(dbFile.Name())
	}()

	// Write a minimal DB file so the pager accepts 4 pages (pages 0-3).
	blank := make([]byte, PageSize*4)
	// Page 0 needs a valid header magic.
	hdr := DatabaseHeader{}
	hdrBytes, err := hdr.Marshal()
	require.NoError(t, err)
	copy(blank, hdrBytes)
	// Mark page 1 and page 3 as leaf nodes so unmarshal succeeds.
	blank[PageSize] = PageTypeLeaf
	blank[PageSize*3] = PageTypeLeaf
	_, err = dbFile.Write(blank)
	require.NoError(t, err)

	columns := []Column{{Kind: Varchar, Size: 270, Name: "val"}}
	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	tablePager := pager.ForTable(columns)

	// Put a modified version of page 3 in the WAL index.
	// NextLeaf is used as a distinguishing marker because it is safely
	// serialised/deserialised without touching the cell-data slice.
	walIndex := NewWALIndex()
	modifiedPage := &Page{Index: 3, LeafNode: NewLeafNode()}
	modifiedPage.LeafNode.Header.NextLeaf = PageIndex(99)

	walBuf := make([]byte, PageSize)
	require.NoError(t, marshalPage(modifiedPage, walBuf))
	walIndex.Update(PageIndex(3), walBuf)

	pager.SetWALIndex(walIndex)

	ctx := context.Background()

	// Page 3 should come from the WAL index (NextLeaf = 99), not from the
	// all-zero DB file bytes (which would produce NextLeaf = 0).
	got, err := tablePager.GetPage(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, got.LeafNode)
	assert.Equal(t, PageIndex(99), got.LeafNode.Header.NextLeaf)
}

func TestPager_WAL_GetPage_CacheHitSkipsWALIndex(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer func() {
		_ = dbFile.Close()
		_ = os.Remove(dbFile.Name())
	}()

	columns := []Column{{Kind: Varchar, Size: 270, Name: "val"}}
	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)

	// Pre-populate the cache with a page (NextLeaf = 7).
	cachedPage := &Page{Index: 1, LeafNode: NewLeafNode()}
	cachedPage.LeafNode.Header.NextLeaf = PageIndex(7)
	pager.pages = append(pager.pages, nil, cachedPage) // index 0 = nil, index 1 = cachedPage
	pager.totalPages = 2

	tablePager := pager.ForTable(columns)

	// WAL index has a different version of page 1 (NextLeaf = 99).
	walIndex := NewWALIndex()
	walPage := &Page{Index: 1, LeafNode: NewLeafNode()}
	walPage.LeafNode.Header.NextLeaf = PageIndex(99)
	walBuf := make([]byte, PageSize)
	require.NoError(t, marshalPage(walPage, walBuf))
	walIndex.Update(PageIndex(1), walBuf)

	pager.SetWALIndex(walIndex)

	// GetPage must return the cached version (NextLeaf = 7), not the WAL version.
	got, err := tablePager.GetPage(context.Background(), 1)
	require.NoError(t, err)
	require.NotNil(t, got.LeafNode)
	assert.Equal(t, PageIndex(7), got.LeafNode.Header.NextLeaf, "cache hit must not be overridden by WAL index")
}

func TestPager_WAL_GetPage_Page0UpdatesDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer func() {
		_ = dbFile.Close()
		_ = os.Remove(dbFile.Name())
	}()

	// Write a minimal DB file with one page.
	blank := make([]byte, PageSize)
	origHdr := DatabaseHeader{FirstFreePage: 10, FreePageCount: 5}
	origHdrBytes, err := origHdr.Marshal()
	require.NoError(t, err)
	copy(blank, origHdrBytes)
	blank[RootPageConfigSize] = PageTypeLeaf
	_, err = dbFile.Write(blank)
	require.NoError(t, err)

	columns := []Column{{Kind: Varchar, Size: 270, Name: "val"}}
	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	assert.Equal(t, PageIndex(10), pager.dbHeader.FirstFreePage)

	// WAL index has page 0 with an updated header (FirstFreePage = 20).
	updatedHdr := DatabaseHeader{FirstFreePage: 20, FreePageCount: 3}
	updatedHdrBytes, err := updatedHdr.Marshal()
	require.NoError(t, err)

	page0Buf := make([]byte, PageSize)
	copy(page0Buf, updatedHdrBytes[0:RootPageConfigSize])
	page0Buf[RootPageConfigSize] = PageTypeLeaf // B-tree portion

	walIndex := NewWALIndex()
	walIndex.Update(PageIndex(0), page0Buf)
	pager.SetWALIndex(walIndex)

	tablePager := pager.ForTable(columns)

	// GetPage for page 0 must load from WAL and refresh dbHeader.
	_, err = tablePager.GetPage(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, PageIndex(20), pager.dbHeader.FirstFreePage,
		"dbHeader must be updated to reflect the WAL-committed header")
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
