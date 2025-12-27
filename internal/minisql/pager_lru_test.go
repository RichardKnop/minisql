package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPager_LRUEviction(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", testDbName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())
	defer dbFile.Close()

	// Create pager with small cache (max 5 pages)
	aPager, err := NewPager(dbFile, PageSize, 5)
	require.NoError(t, err)

	ctx := context.Background()
	columns := []Column{
		{Kind: Int8, Size: 8},
		{Kind: Varchar, Size: 100},
	}

	// Create 10 pages
	for i := range 10 {
		leafNode := NewLeafNode()
		if i == 0 {
			leafNode.Header.Header.IsRoot = true
		}
		aPager.pages = append(aPager.pages, &Page{
			Index:    PageIndex(i),
			LeafNode: leafNode,
		})
	}
	aPager.totalPages = 10

	// Flush all pages to disk
	for i := range 10 {
		err := aPager.Flush(ctx, PageIndex(i))
		require.NoError(t, err)
	}

	// Reset pager with small cache
	dbFile.Seek(0, 0)
	aPager, err = NewPager(dbFile, PageSize, 5)
	require.NoError(t, err)
	assert.Equal(t, 10, int(aPager.totalPages))

	tablePager := aPager.ForTable(columns)

	// Access pages 0, 1, 2, 3, 4 - should all be cached
	for i := range 5 {
		_, err := tablePager.GetPage(ctx, PageIndex(i))
		require.NoError(t, err)
	}

	// Verify pages are in cache
	cachedCount := 0
	for _, page := range aPager.pages {
		if page != nil {
			cachedCount += 1
		}
	}
	assert.Equal(t, 5, cachedCount, "Should have 5 pages cached")
	assert.Equal(t, 5, len(aPager.lruList), "LRU list should have 5 entries")

	// Access page 5 - should evict least recently used (page 1)
	// Page 0 should never be evicted (root page)
	_, err = tablePager.GetPage(ctx, PageIndex(5))
	require.NoError(t, err)

	// Count cached pages
	cachedCount = 0
	for _, page := range aPager.pages {
		if page != nil {
			cachedCount++
		}
	}
	assert.LessOrEqual(t, cachedCount, 5, "Should have at most 5 pages cached")

	// Page 0 (root) should still be cached
	assert.NotNil(t, aPager.pages[0], "Root page should never be evicted")

	// Access pages in order: 6, 7, 8, 9
	for i := 6; i < 10; i++ {
		_, err := tablePager.GetPage(ctx, PageIndex(i))
		require.NoError(t, err)
	}

	// Final cache count should be <= max
	cachedCount = 0
	for _, page := range aPager.pages {
		if page != nil {
			cachedCount++
		}
	}
	assert.LessOrEqual(t, cachedCount, 5, "Should have at most 5 pages cached after many accesses")

	// Page 0 should still be there
	assert.NotNil(t, aPager.pages[0], "Root page should still be cached")
}

func TestPager_LRUAccessOrder(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", testDbName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())
	defer dbFile.Close()

	// Create pager with cache for 3 pages
	aPager, err := NewPager(dbFile, PageSize, 3)
	require.NoError(t, err)

	ctx := context.Background()
	columns := []Column{
		{Kind: Int8, Size: 8},
	}

	// Create 5 pages
	for i := range 5 {
		leafNode := NewLeafNode()
		if i == 0 {
			leafNode.Header.Header.IsRoot = true
		}
		aPager.pages = append(aPager.pages, &Page{
			Index:    PageIndex(i),
			LeafNode: leafNode,
		})
	}
	aPager.totalPages = 5

	// Flush all pages
	for i := 0; i < 5; i++ {
		err := aPager.Flush(ctx, PageIndex(i))
		require.NoError(t, err)
	}

	// Reset pager
	dbFile.Seek(0, 0)
	aPager, err = NewPager(dbFile, PageSize, 3)
	require.NoError(t, err)
	tablePager := aPager.ForTable(columns)

	// Access pages 0, 1, 2 (fills cache)
	_, err = tablePager.GetPage(ctx, PageIndex(0))
	require.NoError(t, err)
	_, err = tablePager.GetPage(ctx, PageIndex(1))
	require.NoError(t, err)
	_, err = tablePager.GetPage(ctx, PageIndex(2))
	require.NoError(t, err)

	// LRU order should be: 0, 1, 2 (2 is most recent)
	assert.Len(t, aPager.lruList, 3)

	// Re-access page 0 (moves it to end of LRU)
	_, err = tablePager.GetPage(ctx, PageIndex(0))
	require.NoError(t, err)

	// LRU order should now be: 1, 2, 0 (0 is most recent)
	// When we access page 3, page 1 should be evicted (least recently used, and not page 0)

	// Access page 3
	_, err = tablePager.GetPage(ctx, PageIndex(3))
	require.NoError(t, err)

	// Verify: page 0 should still be cached (we just accessed it)
	assert.NotNil(t, aPager.pages[0], "Page 0 should be cached")
	// Page 2 should still be cached
	assert.NotNil(t, aPager.pages[2], "Page 2 should be cached")
	// Page 3 should be cached (just loaded)
	assert.NotNil(t, aPager.pages[3], "Page 3 should be cached")
}

func TestPager_UnlimitedCache(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", testDbName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())
	defer dbFile.Close()

	// Create pager with 0 max (should default to 1000)
	aPager, err := NewPager(dbFile, PageSize, 0)
	require.NoError(t, err)

	assert.Equal(t, 1000, aPager.maxCachedPages, "Should default to 1000 when 0 is passed")
}
