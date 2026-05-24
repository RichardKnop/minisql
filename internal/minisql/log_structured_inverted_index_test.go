package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLogStructuredInvertedIndex_OpenMetadataRoot(t *testing.T) {
	ctx := context.Background()
	pager, tempFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, testTableName, "idx_payload")

	var metaRoot PageIndex
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		metaPage, err := txPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		basePage, err := txPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		metaRoot = metaPage.Index
		index, err := NewLogStructuredInvertedIndex(ctx, "idx_payload", invertedIndexPostingModeRowIDs, txPager, metaPage.Index, basePage.Index)
		if err != nil {
			return err
		}
		return index.Insert(ctx, "kv:type:s:\"click\"", invertedPosting{RowID: 7})
	}))

	opened, err := OpenInvertedIndex(ctx, "idx_payload", invertedIndexPostingModeRowIDs, txPager, metaRoot)
	require.NoError(t, err)
	logIndex, ok := opened.(*logStructuredInvertedIndex)
	require.True(t, ok)
	assert.Equal(t, metaRoot, logIndex.GetRootPageIdx())

	iter, err := opened.Lookup(ctx, "kv:type:s:\"click\"")
	require.NoError(t, err)
	block, ok, err := iter.NextBlock(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	_, postings, err := decodeInvertedPostingList(block.Payload)
	require.NoError(t, err)
	assert.Equal(t, []invertedPosting{{RowID: 7}}, postings)
}

func TestOpenInvertedIndex_DirectEntryRootCompatibility(t *testing.T) {
	ctx := context.Background()
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_payload", invertedIndexPostingModeRowIDs)
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.Insert(ctx, "kv:type:s:\"click\"", invertedPosting{RowID: 3})
	}))

	opened, err := OpenInvertedIndex(ctx, "idx_payload", invertedIndexPostingModeRowIDs, index.pager, index.GetRootPageIdx())
	require.NoError(t, err)
	_, ok := opened.(*dedicatedInvertedIndex)
	require.True(t, ok)

	iter, err := opened.Lookup(ctx, "kv:type:s:\"click\"")
	require.NoError(t, err)
	block, ok, err := iter.NextBlock(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	_, postings, err := decodeInvertedPostingList(block.Payload)
	require.NoError(t, err)
	assert.Equal(t, []invertedPosting{{RowID: 3}}, postings)
}
