package minisql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDedicatedInvertedIndex_InitRootPage(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "articles", "idx_body_fts")

	index, err := NewDedicatedInvertedIndex("idx_body_fts", invertedIndexPostingModePositions, txPager, 0)
	require.NoError(t, err)
	assert.Equal(t, PageIndex(0), index.GetRootPageIdx())
	assert.Equal(t, invertedIndexPostingModePositions, index.Mode())

	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	page, err := basePager.GetPage(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, page.InvertedEntryPage)
	assert.True(t, page.InvertedEntryPage.Header.IsLeaf)
	assert.Equal(t, invertedPageFormatVersion, page.InvertedEntryPage.Header.FormatVersion)
	assert.Empty(t, page.InvertedEntryPage.Cells)
}

func TestDedicatedInvertedIndex_ReopenRootPage(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload_inv")

	index, err := NewDedicatedInvertedIndex("idx_payload_inv", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	require.NoError(t, pager.Flush(ctx, 0))
	reopenedFile, err := os.OpenFile(dbFile.Name(), os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedFile.Close() })

	reopenedPager, err := NewPager(reopenedFile, PageSize, 1000)
	require.NoError(t, err)
	reopenedPage, err := reopenedPager.ForInvertedIndex().GetPage(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, reopenedPage.InvertedEntryPage)
	assert.True(t, reopenedPage.InvertedEntryPage.Header.IsLeaf)
}

func TestDedicatedInvertedIndex_SkeletonMethods(t *testing.T) {
	t.Parallel()

	_, err := NewDedicatedInvertedIndex("idx", invertedIndexPostingModeRowIDs, nil, 0)
	require.NoError(t, err)

	_, err = NewDedicatedInvertedIndex("idx", invertedIndexPostingMode(99), nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown inverted index posting mode")
}

func TestDedicatedInvertedIndex_RowIDInlinePostings(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		require.NoError(t, index.Insert(ctx, "kv:type:s:\"click\"", invertedPosting{RowID: 3}))
		require.NoError(t, index.Insert(ctx, "kv:type:s:\"click\"", invertedPosting{RowID: 1}))
		require.NoError(t, index.Insert(ctx, "kv:type:s:\"click\"", invertedPosting{RowID: 3}))
		return index.Insert(ctx, "k:user.id", invertedPosting{RowID: 2})
	}))

	stats, err := index.Stats(ctx, "kv:type:s:\"click\"")
	require.NoError(t, err)
	assert.Equal(t, invertedPostingStats{DocFreq: 2, PostingCount: 2}, stats)

	postings := collectDedicatedInvertedPostings(t, ctx, index, "kv:type:s:\"click\"")
	assert.Equal(t, []invertedPosting{{RowID: 1}, {RowID: 3}}, postings)
	assert.Equal(t, []invertedPosting{{RowID: 2}}, collectDedicatedInvertedPostings(t, ctx, index, "k:user.id"))
	assert.Empty(t, collectDedicatedInvertedPostings(t, ctx, index, "missing"))
}

func TestDedicatedInvertedIndex_PositionalInlinePostings(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_body", invertedIndexPostingModePositions)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		require.NoError(t, index.Insert(ctx, "database", invertedPosting{RowID: 10, Positions: []uint32{4, 2, 4}}))
		require.NoError(t, index.Insert(ctx, "database", invertedPosting{RowID: 5, Positions: []uint32{1}}))
		return index.Insert(ctx, "database", invertedPosting{RowID: 10, Positions: []uint32{7}})
	}))

	stats, err := index.Stats(ctx, "database")
	require.NoError(t, err)
	assert.Equal(t, invertedPostingStats{DocFreq: 2, PostingCount: 4}, stats)

	postings := collectDedicatedInvertedPostings(t, ctx, index, "database")
	assert.Equal(t, []invertedPosting{
		{RowID: 5, Positions: []uint32{1}},
		{RowID: 10, Positions: []uint32{2, 4, 7}},
	}, postings)
}

func TestDedicatedInvertedIndex_DeleteInlinePostings(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_body", invertedIndexPostingModePositions)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		require.NoError(t, index.Insert(ctx, "query", invertedPosting{RowID: 1, Positions: []uint32{1, 3}}))
		require.NoError(t, index.Insert(ctx, "query", invertedPosting{RowID: 2, Positions: []uint32{4}}))
		return index.Delete(ctx, "query", invertedPosting{RowID: 1, Positions: []uint32{3}})
	}))
	assert.Equal(t, []invertedPosting{
		{RowID: 1, Positions: []uint32{1}},
		{RowID: 2, Positions: []uint32{4}},
	}, collectDedicatedInvertedPostings(t, ctx, index, "query"))

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		require.NoError(t, index.Delete(ctx, "query", invertedPosting{RowID: 1}))
		require.NoError(t, index.Delete(ctx, "query", invertedPosting{RowID: 2, Positions: []uint32{4}}))
		return index.Delete(ctx, "missing", invertedPosting{RowID: 99})
	}))
	assert.Empty(t, collectDedicatedInvertedPostings(t, ctx, index, "query"))

	stats, err := index.Stats(ctx, "query")
	require.NoError(t, err)
	assert.Equal(t, invertedPostingStats{}, stats)
}

func TestDedicatedInvertedIndex_PersistsInlinePostings(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "articles", "idx_body")
	index, err := NewDedicatedInvertedIndex("idx_body", invertedIndexPostingModePositions, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.Insert(ctx, "stored", invertedPosting{RowID: 42, Positions: []uint32{6, 9}})
	}))
	require.NoError(t, pager.Flush(ctx, 0))

	reopenedFile, err := os.OpenFile(dbFile.Name(), os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedFile.Close() })
	reopenedPager, err := NewPager(reopenedFile, PageSize, 1000)
	require.NoError(t, err)
	reopenedBasePager := reopenedPager.ForInvertedIndex()
	reopenedTxManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(reopenedBasePager), reopenedPager, nil)
	reopenedIndex, err := NewDedicatedInvertedIndex(
		"idx_body",
		invertedIndexPostingModePositions,
		NewTransactionalPager(reopenedBasePager, reopenedTxManager, "articles", "idx_body"),
		0,
	)
	require.NoError(t, err)

	assert.Equal(t, []invertedPosting{{RowID: 42, Positions: []uint32{6, 9}}}, collectDedicatedInvertedPostings(t, ctx, reopenedIndex, "stored"))
}

func TestDedicatedInvertedIndex_InlineEntryPageFull(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := range 128 {
			term := fmt.Sprintf("term-%03d-%0240d", i, i)
			if err := index.Insert(ctx, term, invertedPosting{RowID: RowID(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errInvertedIndexEntryPageFull))
}

func newTestDedicatedInvertedIndex(t *testing.T, name string, mode invertedIndexPostingMode) (*dedicatedInvertedIndex, *TransactionManager) {
	t.Helper()

	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "test_table", name)
	index, err := NewDedicatedInvertedIndex(name, mode, txPager, 0)
	require.NoError(t, err)
	require.NoError(t, txManager.ExecuteInTransaction(context.Background(), index.InitRootPage))
	return index, txManager
}

func collectDedicatedInvertedPostings(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex, term string) []invertedPosting {
	t.Helper()

	iter, err := index.Lookup(ctx, term)
	require.NoError(t, err)
	var postings []invertedPosting
	for {
		block, ok, err := iter.NextBlock(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		mode, decoded, err := decodeInvertedPostingList(block.Payload)
		require.NoError(t, err)
		assert.Equal(t, index.Mode(), mode)
		postings = append(postings, decoded...)
	}
	return postings
}
