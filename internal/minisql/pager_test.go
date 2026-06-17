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

// TestPager_NoIntermediateSync_FlushIsNoop verifies that Flush is a no-op
// when noIntermediateSync is true: the page stays in the in-memory cache but
// nothing reaches the file until Close is called.
func TestPager_NoIntermediateSync_FlushIsNoop(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.SetNoIntermediateSync(true)

	leaf := NewLeafNode()
	leaf.Header.IsRoot = true
	leaf.Header.NextLeaf = PageIndex(42) // sentinel value to check after reload
	pager.pages = append(pager.pages, &Page{Index: 0, LeafNode: leaf})
	pager.totalPages = 1

	// Flush must be a no-op: the file should remain empty.
	require.NoError(t, pager.Flush(context.Background(), 0))

	info, err := dbFile.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size(), "file must still be empty after Flush with noIntermediateSync")
}

// TestPager_NoIntermediateSync_FlushBatchIsNoop verifies that FlushBatch is a
// no-op when noIntermediateSync is true.
func TestPager_NoIntermediateSync_FlushBatchIsNoop(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.SetNoIntermediateSync(true)

	rootPage, internalPages, leafPages := newTestBtree()
	pager.pages = append(pager.pages,
		rootPage,
		internalPages[0],
		internalPages[1],
		leafPages[0],
		leafPages[1],
	)
	pager.totalPages = 5

	indices := []PageIndex{0, 1, 2, 3, 4}
	require.NoError(t, pager.FlushBatch(context.Background(), indices))

	info, err := dbFile.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size(), "file must still be empty after FlushBatch with noIntermediateSync")
}

// TestPager_NoIntermediateSync_CloseWritesAllPages verifies the full deferred
// path: Flush/FlushBatch are no-ops, but Close flushes all cached pages to
// disk. A fresh pager opened on the same file must read back identical pages.
func TestPager_NoIntermediateSync_CloseWritesAllPages(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.SetNoIntermediateSync(true)

	rootPage, internalPages, leafPages := newTestBtree()
	allPages := []*Page{rootPage, internalPages[0], internalPages[1], leafPages[0], leafPages[1], leafPages[2], leafPages[3]}
	pager.pages = append(pager.pages, allPages...)
	pager.totalPages = uint32(len(allPages))

	// FlushBatch should be a no-op; file stays empty.
	indices := make([]PageIndex, len(allPages))
	for i := range indices {
		indices[i] = PageIndex(i)
	}
	require.NoError(t, pager.FlushBatch(context.Background(), indices))

	info, err := dbFile.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(0), info.Size(), "file must still be empty before Close")

	// Close must flush all cached pages, then sync and close the file.
	require.NoError(t, pager.Close())

	info, err = os.Stat(dbFile.Name())
	require.NoError(t, err)
	assert.Equal(t, int64(len(allPages))*int64(PageSize), info.Size(),
		"file must contain all pages after Close")

	// Reopen and verify every page is readable with the correct content.
	dbFile2, err := os.Open(dbFile.Name())
	require.NoError(t, err)
	defer dbFile2.Close()

	pager2, err := NewPager(dbFile2, PageSize, 1000)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(allPages)), pager2.TotalPages())

	columns := []Column{{Kind: Varchar, Size: 270}}
	tablePager := pager2.ForTable(columns)
	ctx := context.Background()

	page0, err := tablePager.GetPage(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, rootPage, page0)

	page1, err := tablePager.GetPage(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, internalPages[0], page1)

	page3, err := tablePager.GetPage(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, leafPages[0], page3)
}

// TestPager_NoIntermediateSync_NonConsecutivePages verifies that
// flushAllCached correctly handles a sparse set of page indices (non-consecutive
// runs are each written in separate WriteAt calls, not one combined buffer).
func TestPager_NoIntermediateSync_NonConsecutivePages(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.SetNoIntermediateSync(true)

	// Place pages at non-consecutive indices: 0, 2, 4 (gaps at 1 and 3).
	leaf0 := NewLeafNode()
	leaf0.Header.IsRoot = true
	leaf0.Header.NextLeaf = 11

	leaf2 := NewLeafNode()
	leaf2.Header.NextLeaf = 22

	leaf4 := NewLeafNode()
	leaf4.Header.NextLeaf = 44

	pager.pages = []*Page{
		{Index: 0, LeafNode: leaf0},
		nil,
		{Index: 2, LeafNode: leaf2},
		nil,
		{Index: 4, LeafNode: leaf4},
	}
	pager.totalPages = 5

	require.NoError(t, pager.Close())

	// File must be exactly 5 pages (indices 0-4), even though 1 and 3 are nil.
	info, err := os.Stat(dbFile.Name())
	require.NoError(t, err)
	assert.Equal(t, int64(5)*int64(PageSize), info.Size())

	// Reopen and read back the three written pages.
	dbFile2, err := os.Open(dbFile.Name())
	require.NoError(t, err)
	defer dbFile2.Close()

	pager2, err := NewPager(dbFile2, PageSize, 1000)
	require.NoError(t, err)

	columns := []Column{{Kind: Varchar, Size: 270}}
	tp := pager2.ForTable(columns)
	ctx := context.Background()

	p0, err := tp.GetPage(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, PageIndex(11), p0.LeafNode.Header.NextLeaf)

	p2, err := tp.GetPage(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, PageIndex(22), p2.LeafNode.Header.NextLeaf)

	p4, err := tp.GetPage(ctx, 4)
	require.NoError(t, err)
	assert.Equal(t, PageIndex(44), p4.LeafNode.Header.NextLeaf)
}
