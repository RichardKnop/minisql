package minisql

import (
	"context"
	"errors"
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

	index, err := NewDedicatedInvertedIndex("idx", invertedIndexPostingModeRowIDs, nil, 0)
	require.NoError(t, err)

	err = index.Insert(context.Background(), "term", invertedPosting{RowID: 1})
	assert.True(t, errors.Is(err, errInvertedIndexStorageNotImplemented))

	err = index.Delete(context.Background(), "term", invertedPosting{RowID: 1})
	assert.True(t, errors.Is(err, errInvertedIndexStorageNotImplemented))

	_, err = index.Lookup(context.Background(), "term")
	assert.True(t, errors.Is(err, errInvertedIndexStorageNotImplemented))

	_, err = index.Stats(context.Background(), "term")
	assert.True(t, errors.Is(err, errInvertedIndexStorageNotImplemented))

	_, err = NewDedicatedInvertedIndex("idx", invertedIndexPostingMode(99), nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown inverted index posting mode")
}
