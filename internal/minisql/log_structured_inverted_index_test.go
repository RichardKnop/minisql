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

func TestLogStructuredInvertedIndex_LookupMergesBaseAndSegment(t *testing.T) {
	ctx := context.Background()
	pager, tempFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, testTableName, "idx_payload")

	const term = "kv:type:s:\"click\""
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
		segmentPage, err := txPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		metaRoot = metaPage.Index
		index, err := NewLogStructuredInvertedIndex(ctx, "idx_payload", invertedIndexPostingModeRowIDs, txPager, metaPage.Index, basePage.Index)
		if err != nil {
			return err
		}
		if err := index.Insert(ctx, term, invertedPosting{RowID: 7}); err != nil {
			return err
		}
		if err := writeTestInvertedSegment(ctx, txPager, segmentPage.Index, term, []invertedPosting{{RowID: 11}, {RowID: 13}}); err != nil {
			return err
		}
		meta, err := txPager.ModifyPage(ctx, metaRoot)
		if err != nil {
			return err
		}
		meta.InvertedMetaPage.Segments = append(meta.InvertedMetaPage.Segments, invertedSegmentDescriptor{
			Generation:   1,
			RootPage:     segmentPage.Index,
			PostingCount: 2,
			Kind:         invertedSegmentKindInsert,
		})
		return nil
	}))

	opened, err := OpenInvertedIndex(ctx, "idx_payload", invertedIndexPostingModeRowIDs, txPager, metaRoot)
	require.NoError(t, err)

	iter, err := opened.Lookup(ctx, term)
	require.NoError(t, err)
	postings := collectInvertedIteratorPostings(t, ctx, iter)
	assert.Equal(t, []invertedPosting{{RowID: 7}, {RowID: 11}, {RowID: 13}}, postings)

	stats, err := opened.Stats(ctx, term)
	require.NoError(t, err)
	assert.Equal(t, invertedPostingStats{DocFreq: 3, PostingCount: 3}, stats)
}

func TestLogStructuredInvertedIndex_ApplyBatchWritesSegments(t *testing.T) {
	ctx := context.Background()
	index, txManager, metaRoot := newTestLogStructuredInvertedIndex(t, invertedIndexPostingModeRowIDs)

	const term = "kv:type:s:\"click\""
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		batch := newInvertedIndexMutationBatch(index.Mode())
		batch.Insert(term, invertedPosting{RowID: 21})
		batch.Insert(term, invertedPosting{RowID: 22})
		return index.ApplyBatch(ctx, batch)
	}))

	page, err := index.pager.ReadPage(ctx, metaRoot)
	require.NoError(t, err)
	require.NotNil(t, page.InvertedMetaPage)
	require.Len(t, page.InvertedMetaPage.Segments, 1)
	assert.Equal(t, invertedSegmentKindInsert, page.InvertedMetaPage.Segments[0].Kind)

	iter, err := index.Lookup(ctx, term)
	require.NoError(t, err)
	postings := collectInvertedIteratorPostings(t, ctx, iter)
	assert.Equal(t, []invertedPosting{{RowID: 21}, {RowID: 22}}, postings)
}

func TestLogStructuredInvertedIndex_DeleteSegmentFiltersEarlierPostings(t *testing.T) {
	ctx := context.Background()
	index, txManager, _ := newTestLogStructuredInvertedIndex(t, invertedIndexPostingModeRowIDs)

	const term = "kv:type:s:\"click\""
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		insertBatch := newInvertedIndexMutationBatch(index.Mode())
		insertBatch.Insert(term, invertedPosting{RowID: 21})
		insertBatch.Insert(term, invertedPosting{RowID: 22})
		if err := index.ApplyBatch(ctx, insertBatch); err != nil {
			return err
		}

		deleteBatch := newInvertedIndexMutationBatch(index.Mode())
		deleteBatch.Delete(term, invertedPosting{RowID: 21})
		return index.ApplyBatch(ctx, deleteBatch)
	}))

	iter, err := index.Lookup(ctx, term)
	require.NoError(t, err)
	postings := collectInvertedIteratorPostings(t, ctx, iter)
	assert.Equal(t, []invertedPosting{{RowID: 22}}, postings)

	stats, err := index.Stats(ctx, term)
	require.NoError(t, err)
	assert.Equal(t, invertedPostingStats{DocFreq: 1, PostingCount: 1}, stats)
}

func TestLogStructuredInvertedIndex_ReplaceSegmentReinsertsSameRow(t *testing.T) {
	ctx := context.Background()
	index, txManager, _ := newTestLogStructuredInvertedIndex(t, invertedIndexPostingModePositions)

	const term = "database"
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		batch := newInvertedIndexMutationBatch(index.Mode())
		batch.Insert(term, invertedPosting{RowID: 5, Positions: []uint32{1}})
		if err := index.ApplyBatch(ctx, batch); err != nil {
			return err
		}
		return index.Replace(
			ctx,
			term,
			invertedPosting{RowID: 5, Positions: []uint32{1}},
			invertedPosting{RowID: 5, Positions: []uint32{2, 4}},
		)
	}))

	iter, err := index.Lookup(ctx, term)
	require.NoError(t, err)
	postings := collectInvertedIteratorPostings(t, ctx, iter)
	assert.Equal(t, []invertedPosting{{RowID: 5, Positions: []uint32{2, 4}}}, postings)
}

func TestLogStructuredInvertedIndex_CompactsSegmentsIntoBase(t *testing.T) {
	ctx := context.Background()
	index, txManager, metaRoot := newTestLogStructuredInvertedIndex(t, invertedIndexPostingModeRowIDs)

	const term = "kv:type:s:\"click\""
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := 0; i < logStructuredInvertedIndexCompactSegmentThreshold; i++ {
			batch := newInvertedIndexMutationBatch(index.Mode())
			batch.Insert(term, invertedPosting{RowID: RowID(i + 1)})
			if i%3 == 0 {
				batch.Delete(term, invertedPosting{RowID: RowID(i)})
			}
			if err := index.ApplyBatch(ctx, batch); err != nil {
				return err
			}
		}
		return nil
	}))

	page, err := index.pager.ReadPage(ctx, metaRoot)
	require.NoError(t, err)
	require.NotNil(t, page.InvertedMetaPage)
	assert.Empty(t, page.InvertedMetaPage.Segments)

	iter, err := index.Lookup(ctx, term)
	require.NoError(t, err)
	postings := collectInvertedIteratorPostings(t, ctx, iter)
	require.NotEmpty(t, postings)
	assert.Equal(t, RowID(logStructuredInvertedIndexCompactSegmentThreshold), postings[len(postings)-1].RowID)
}

func TestLogStructuredInvertedIndex_CompactionPreservesPositionReplacement(t *testing.T) {
	ctx := context.Background()
	index, txManager, _ := newTestLogStructuredInvertedIndex(t, invertedIndexPostingModePositions)

	const term = "database"
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		batch := newInvertedIndexMutationBatch(index.Mode())
		batch.Insert(term, invertedPosting{RowID: 5, Positions: []uint32{1}})
		if err := index.ApplyBatch(ctx, batch); err != nil {
			return err
		}
		for i := 1; i < logStructuredInvertedIndexCompactSegmentThreshold; i++ {
			oldPosting := invertedPosting{RowID: 5, Positions: []uint32{uint32(i)}}
			newPosting := invertedPosting{RowID: 5, Positions: []uint32{uint32(i + 1)}}
			if err := index.Replace(ctx, term, oldPosting, newPosting); err != nil {
				return err
			}
		}
		return nil
	}))

	iter, err := index.Lookup(ctx, term)
	require.NoError(t, err)
	postings := collectInvertedIteratorPostings(t, ctx, iter)
	assert.Equal(
		t,
		[]invertedPosting{{RowID: 5, Positions: []uint32{logStructuredInvertedIndexCompactSegmentThreshold}}},
		postings,
	)
}

func newTestLogStructuredInvertedIndex(
	t *testing.T,
	mode invertedIndexPostingMode,
) (*logStructuredInvertedIndex, *TransactionManager, PageIndex) {
	t.Helper()

	ctx := context.Background()
	pager, tempFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, testTableName, "idx_payload")

	var metaRoot PageIndex
	var index *logStructuredInvertedIndex
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
		index, err = NewLogStructuredInvertedIndex(ctx, "idx_payload", mode, txPager, metaPage.Index, basePage.Index)
		return err
	}))
	return index, txManager, metaRoot
}

func writeTestInvertedSegment(ctx context.Context, pager TxPager, pageIdx PageIndex, term string, postings []invertedPosting) error {
	payload, err := encodeInvertedPostingList(invertedPostingModeRowIDs, postings)
	if err != nil {
		return err
	}
	page, err := pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return err
	}
	page.Clear()
	page.InvertedSegmentPage = NewInvertedSegmentPage()
	page.InvertedSegmentPage.Cells = []invertedSegmentCell{{
		Term: term,
		Block: invertedPostingBlock{
			FirstRowID:   postings[0].RowID,
			LastRowID:    postings[len(postings)-1].RowID,
			PostingCount: countInvertedPostings(invertedPostingModeRowIDs, postings),
			CodecVersion: invertedPostingCodecVersion,
			Payload:      payload,
		},
		DocFreq:      uint32(len(postings)),
		PostingCount: countInvertedPostings(invertedPostingModeRowIDs, postings),
		Kind:         invertedSegmentKindInsert,
	}}
	return nil
}

func collectInvertedIteratorPostings(
	t *testing.T,
	ctx context.Context,
	iter invertedPostingIterator,
) []invertedPosting {
	t.Helper()

	var postings []invertedPosting
	for {
		block, ok, err := iter.NextBlock(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		_, blockPostings, err := decodeInvertedPostingList(block.Payload)
		require.NoError(t, err)
		postings = append(postings, blockPostings...)
	}
	return postings
}
