package minisql

import (
	"context"
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

func TestDedicatedInvertedIndex_SplitsEntryTreeForManyTerms(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := range 420 {
			term := fmt.Sprintf("term-%03d-%0240d", i, i)
			if err := index.Insert(ctx, term, invertedPosting{RowID: RowID(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	}))

	root, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	require.NotNil(t, root.InvertedEntryPage)
	assert.False(t, root.InvertedEntryPage.Header.IsLeaf)
	assert.Greater(t, countDedicatedInvertedEntryLeaves(t, ctx, index), 1)

	assert.Greater(t, countDedicatedInvertedEntryLeaves(t, ctx, index), 15)
	assert.Greater(t, dedicatedInvertedEntryTreeHeight(t, ctx, index), 2)
	assert.Len(t, assertDedicatedInvertedEntryTreeValid(t, ctx, index), 420)

	for _, i := range []int{0, 17, 127, 419} {
		term := fmt.Sprintf("term-%03d-%0240d", i, i)
		assert.Equal(t, []invertedPosting{{RowID: RowID(i + 1)}}, collectDedicatedInvertedPostings(t, ctx, index, term))
	}
}

func TestDedicatedInvertedIndex_DeleteRebalancesEntryTreeAndFreesPages(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload")
	index, err := NewDedicatedInvertedIndex("idx_payload", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := range 128 {
			term := fmt.Sprintf("term-%03d-%0240d", i, i)
			if err := index.Insert(ctx, term, invertedPosting{RowID: RowID(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	}))
	root, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	require.False(t, root.InvertedEntryPage.Header.IsLeaf)
	require.Greater(t, countDedicatedInvertedEntryLeaves(t, ctx, index), 1)
	beforeFreePages := basePager.GetHeader(ctx).FreePageCount

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := 1; i < 128; i++ {
			term := fmt.Sprintf("term-%03d-%0240d", i, i)
			if err := index.Delete(ctx, term, invertedPosting{RowID: RowID(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	}))

	root, err = index.readRootEntryPage(ctx)
	require.NoError(t, err)
	require.True(t, root.InvertedEntryPage.Header.IsLeaf)
	require.Len(t, root.InvertedEntryPage.Cells, 1)
	assert.Equal(t, "term-000-"+fmt.Sprintf("%0240d", 0), root.InvertedEntryPage.Cells[0].Term)
	assert.Greater(t, basePager.GetHeader(ctx).FreePageCount, beforeFreePages)
	assert.Equal(t, []invertedPosting{{RowID: 1}}, collectDedicatedInvertedPostings(t, ctx, index, "term-000-"+fmt.Sprintf("%0240d", 0)))
	assert.Empty(t, collectDedicatedInvertedPostings(t, ctx, index, "term-127-"+fmt.Sprintf("%0240d", 127)))
	assert.Equal(t, []string{"term-000-" + fmt.Sprintf("%0240d", 0)}, assertDedicatedInvertedEntryTreeValid(t, ctx, index))
}

func TestDedicatedInvertedIndex_DeleteLastEntryLeavesEmptyRoot(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		require.NoError(t, index.Insert(ctx, "alpha", invertedPosting{RowID: 1}))
		return index.Delete(ctx, "alpha", invertedPosting{RowID: 1})
	}))

	root, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	require.True(t, root.InvertedEntryPage.Header.IsLeaf)
	assert.Empty(t, root.InvertedEntryPage.Cells)
	assert.Empty(t, collectDedicatedInvertedPostings(t, ctx, index, "alpha"))
	assert.Empty(t, assertDedicatedInvertedEntryTreeValid(t, ctx, index))
}

func TestDedicatedInvertedIndex_PersistsMultiPageEntryTree(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload")
	index, err := NewDedicatedInvertedIndex("idx_payload", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := range 420 {
			term := fmt.Sprintf("term-%03d-%0240d", i, i)
			if err := index.Insert(ctx, term, invertedPosting{RowID: RowID(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	}))
	assert.Len(t, assertDedicatedInvertedEntryTreeValid(t, ctx, index), 420)

	for pageIdx := PageIndex(0); pageIdx < PageIndex(pager.TotalPages()); pageIdx++ {
		require.NoError(t, pager.Flush(ctx, pageIdx))
	}
	reopenedFile, err := os.OpenFile(dbFile.Name(), os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedFile.Close() })
	reopenedPager, err := NewPager(reopenedFile, PageSize, 1000)
	require.NoError(t, err)
	reopenedBasePager := reopenedPager.ForInvertedIndex()
	reopenedTxManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(reopenedBasePager), reopenedPager, nil)
	reopenedIndex, err := NewDedicatedInvertedIndex(
		"idx_payload",
		invertedIndexPostingModeRowIDs,
		NewTransactionalPager(reopenedBasePager, reopenedTxManager, "events", "idx_payload"),
		0,
	)
	require.NoError(t, err)

	assert.Len(t, assertDedicatedInvertedEntryTreeValid(t, ctx, reopenedIndex), 420)
	for _, i := range []int{0, 211, 419} {
		term := fmt.Sprintf("term-%03d-%0240d", i, i)
		assert.Equal(t, []invertedPosting{{RowID: RowID(i + 1)}}, collectDedicatedInvertedPostings(t, ctx, reopenedIndex, term))
	}
}

func TestDedicatedInvertedIndex_LeafRebalanceBorrowsFromLeft(t *testing.T) {
	t.Parallel()

	index := &dedicatedInvertedIndex{}
	parent := testInvertedEntryPage(0, false, []string{"m"})
	parent.InvertedEntryPage.Cells[0].Child = 1
	parent.InvertedEntryPage.Header.RightChild = 2
	left := testInvertedEntryPage(1, true, []string{"a", "c"})
	page := testInvertedEntryPage(2, true, []string{"m"})

	require.NoError(t, index.borrowEntryFromLeft(context.Background(), parent, page, left, 1))

	assert.Equal(t, []string{"a"}, invertedEntryTerms(left.InvertedEntryPage.Cells))
	assert.Equal(t, []string{"c", "m"}, invertedEntryTerms(page.InvertedEntryPage.Cells))
	assert.Equal(t, "c", parent.InvertedEntryPage.Cells[0].Term)
}

func TestDedicatedInvertedIndex_LeafRebalanceBorrowsFromRight(t *testing.T) {
	t.Parallel()

	index := &dedicatedInvertedIndex{}
	parent := testInvertedEntryPage(0, false, []string{"m"})
	parent.InvertedEntryPage.Cells[0].Child = 1
	parent.InvertedEntryPage.Header.RightChild = 2
	page := testInvertedEntryPage(1, true, []string{"a"})
	right := testInvertedEntryPage(2, true, []string{"m", "z"})

	require.NoError(t, index.borrowEntryFromRight(context.Background(), parent, page, right, 0))

	assert.Equal(t, []string{"a", "m"}, invertedEntryTerms(page.InvertedEntryPage.Cells))
	assert.Equal(t, []string{"z"}, invertedEntryTerms(right.InvertedEntryPage.Cells))
	assert.Equal(t, "z", parent.InvertedEntryPage.Cells[0].Term)
}

func TestDedicatedInvertedIndex_LeafRebalanceMergesAndFreesRightPage(t *testing.T) {
	t.Parallel()

	index := &dedicatedInvertedIndex{}
	parent := testInvertedEntryPage(0, false, []string{"m"})
	parent.InvertedEntryPage.Cells[0].Child = 1
	parent.InvertedEntryPage.Header.RightChild = 2
	left := testInvertedEntryPage(1, true, []string{"a"})
	right := testInvertedEntryPage(2, true, []string{"m"})

	freed, err := index.mergeEntryPages(context.Background(), parent, left, right, 0)
	require.NoError(t, err)

	assert.Equal(t, PageIndex(2), freed)
	assert.Equal(t, []string{"a", "m"}, invertedEntryTerms(left.InvertedEntryPage.Cells))
	assert.Empty(t, parent.InvertedEntryPage.Cells)
	assert.Equal(t, PageIndex(1), parent.InvertedEntryPage.Header.RightChild)
}

func TestDedicatedInvertedIndex_PromotesLargePostingListToPostingTree(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := 1; i <= 140; i++ {
			if err := index.Insert(ctx, "k:type", invertedPosting{RowID: largeTestRowID(i)}); err != nil {
				return err
			}
		}
		return nil
	}))

	cell := requireDedicatedInvertedEntryCell(t, ctx, index, "k:type")
	assert.Equal(t, invertedPostingKindTree, cell.PostingKind)
	assert.NotZero(t, cell.Child)
	assert.Empty(t, cell.Payload)

	stats, err := index.Stats(ctx, "k:type")
	require.NoError(t, err)
	assert.Equal(t, invertedPostingStats{DocFreq: 140, PostingCount: 140}, stats)

	postings := collectDedicatedInvertedPostings(t, ctx, index, "k:type")
	require.Len(t, postings, 140)
	assert.Equal(t, invertedPosting{RowID: largeTestRowID(1)}, postings[0])
	assert.Equal(t, invertedPosting{RowID: largeTestRowID(140)}, postings[len(postings)-1])

	page, err := index.pager.ReadPage(ctx, cell.Child)
	require.NoError(t, err)
	require.NotNil(t, page.InvertedPostPage)
	assert.NotEmpty(t, page.InvertedPostPage.Blocks)
}

func TestDedicatedInvertedIndex_MutatesPostingTreeInPlace(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := 1; i <= 140; i++ {
			if err := index.Insert(ctx, "k:type", invertedPosting{RowID: largeTestRowID(i)}); err != nil {
				return err
			}
		}
		return nil
	}))

	before := requireDedicatedInvertedEntryCell(t, ctx, index, "k:type")
	require.Equal(t, invertedPostingKindTree, before.PostingKind)
	require.NotZero(t, before.Child)

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.Insert(ctx, "k:type", invertedPosting{RowID: largeTestRowID(141)})
	}))
	afterInsert := requireDedicatedInvertedEntryCell(t, ctx, index, "k:type")
	assert.Equal(t, before.Child, afterInsert.Child)
	assert.Equal(t, uint32(141), afterInsert.DocFreq)
	assert.Equal(t, uint32(141), afterInsert.PostingCount)

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.Delete(ctx, "k:type", invertedPosting{RowID: largeTestRowID(141)})
	}))
	afterDelete := requireDedicatedInvertedEntryCell(t, ctx, index, "k:type")
	assert.Equal(t, before.Child, afterDelete.Child)
	assert.Equal(t, uint32(140), afterDelete.DocFreq)
	assert.Equal(t, uint32(140), afterDelete.PostingCount)

	postings := collectDedicatedInvertedPostings(t, ctx, index, "k:type")
	require.Len(t, postings, 140)
	assert.Equal(t, invertedPosting{RowID: largeTestRowID(1)}, postings[0])
	assert.Equal(t, invertedPosting{RowID: largeTestRowID(140)}, postings[len(postings)-1])
}

func TestDedicatedInvertedIndex_PostingTreeInternalRouting(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload_inv")
	index, err := NewDedicatedInvertedIndex("idx_payload_inv", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	postings := make([]invertedPosting, 1800)
	for i := range postings {
		postings[i] = invertedPosting{RowID: wideTestRowID(i + 1)}
	}

	var rootIdx PageIndex
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		rootIdx, err = index.writePostingTree(ctx, postings)
		return err
	}))

	rootPage, err := index.pager.ReadPage(ctx, rootIdx)
	require.NoError(t, err)
	require.NotNil(t, rootPage.InvertedPostPage)
	assert.Greater(t, rootPage.InvertedPostPage.Header.Level, byte(0))
	pageCount := assertDedicatedInvertedPostingTreeValid(t, ctx, index, rootIdx, 0)

	iter := &postingTreeInvertedPostingIterator{
		pager:    index.pager,
		nextPage: rootIdx,
	}
	var decoded []invertedPosting
	for {
		block, ok, err := iter.NextBlock(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
		require.NoError(t, err)
		require.Equal(t, invertedPostingModeRowIDs, mode)
		decoded = append(decoded, blockPostings...)
	}
	require.Len(t, decoded, len(postings))
	assert.Equal(t, postings[0], decoded[0])
	assert.Equal(t, postings[len(postings)-1], decoded[len(decoded)-1])

	beforeFreePages := basePager.GetHeader(ctx).FreePageCount
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.freePostingTree(ctx, invertedEntryCell{
			PostingKind: invertedPostingKindTree,
			Child:       rootIdx,
		})
	}))
	assert.Equal(t, beforeFreePages+uint32(pageCount), basePager.GetHeader(ctx).FreePageCount)
}

func TestDedicatedInvertedIndex_MutatesPostingTreeAndRefreshesInternalRouting(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload_inv")
	index, err := NewDedicatedInvertedIndex("idx_payload_inv", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	postings := make([]invertedPosting, 1800)
	for i := range postings {
		postings[i] = invertedPosting{RowID: wideTestRowID(i + 1)}
	}

	var rootIdx PageIndex
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		rootIdx, err = index.writePostingTree(ctx, postings)
		return err
	}))

	rootPage, err := index.pager.ReadPage(ctx, rootIdx)
	require.NoError(t, err)
	require.NotNil(t, rootPage.InvertedPostPage)
	require.Greater(t, rootPage.InvertedPostPage.Header.Level, byte(0))
	_, oldLast, _, err := invertedPostingPageRange(rootPage.InvertedPostPage)
	require.NoError(t, err)

	cell := invertedEntryCell{
		Term:         "k:type",
		DocFreq:      uint32(len(postings)),
		PostingCount: uint32(len(postings)),
		Child:        rootIdx,
		PostingKind:  invertedPostingKindTree,
		CodecVersion: invertedPostingCodecVersion,
	}
	newPosting := invertedPosting{RowID: wideTestRowID(len(postings) + 1)}
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var mutated bool
		cell, mutated, err = index.insertPostingIntoTreeCell(ctx, cell, newPosting)
		require.True(t, mutated)
		return err
	}))

	assert.Equal(t, rootIdx, cell.Child)
	assert.Equal(t, uint32(len(postings)+1), cell.DocFreq)
	rootPage, err = index.pager.ReadPage(ctx, rootIdx)
	require.NoError(t, err)
	_, newLast, count, err := invertedPostingPageRange(rootPage.InvertedPostPage)
	require.NoError(t, err)
	assert.Greater(t, newLast, oldLast)
	assert.Equal(t, uint32(len(postings)+1), count)
}

func TestDedicatedInvertedIndex_InsertSplitsPostingLeafWithoutRebuild(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	var rootIdx PageIndex
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		leaf, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		leaf.Clear()
		postingPage := NewInvertedPostingPage(0)
		for i := 1; ; i++ {
			block, err := testInvertedPostingBlock(invertedPostingModeRowIDs, RowID(i*10))
			if err != nil {
				return err
			}
			candidate := postingPage.Clone()
			candidate.Blocks = append(candidate.Blocks, block)
			if err := ensureInvertedPostingPageFits(candidate, invertedPageBodySize(leaf.Index)); err != nil {
				break
			}
			postingPage = candidate
		}
		require.Greater(t, len(postingPage.Blocks), 2)
		leaf.InvertedPostPage = postingPage

		replacementA, err := testInvertedPostingBlock(invertedPostingModeRowIDs, 5)
		if err != nil {
			return err
		}
		replacementB, err := testInvertedPostingBlock(invertedPostingModeRowIDs, 10)
		if err != nil {
			return err
		}
		rootIdx, err = index.splitPostingLeafForBlocks(ctx, leaf.Index, leaf, 0, []invertedPostingBlock{replacementA, replacementB})
		return err
	}))

	require.NotZero(t, rootIdx)
	rootPage, err := index.pager.ReadPage(ctx, rootIdx)
	require.NoError(t, err)
	require.NotNil(t, rootPage.InvertedPostPage)
	assert.Equal(t, byte(1), rootPage.InvertedPostPage.Header.Level)
	require.Len(t, rootPage.InvertedPostPage.Blocks, 2)
	assertDedicatedInvertedPostingTreeValid(t, ctx, index, rootIdx, 0)
}

func TestDedicatedInvertedIndex_DeleteRemovesPostingLeafAndCollapsesRoot(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload_inv")
	index, err := NewDedicatedInvertedIndex("idx_payload_inv", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	var cell invertedEntryCell
	var leftLeafIdx, oldRootIdx PageIndex
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		leftLeaf, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		rightLeaf, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}

		leftLeaf.Clear()
		leftPage := NewInvertedPostingPage(0)
		for i := 1; i <= 65; i++ {
			block, err := testInvertedPostingBlock(invertedPostingModeRowIDs, RowID(i))
			if err != nil {
				return err
			}
			leftPage.Blocks = append(leftPage.Blocks, block)
		}
		leftPage.Header.NextLeaf = rightLeaf.Index
		leftLeaf.InvertedPostPage = leftPage

		rightLeaf.Clear()
		rightBlock, err := testInvertedPostingBlock(invertedPostingModeRowIDs, RowID(1000))
		if err != nil {
			return err
		}
		rightPage := NewInvertedPostingPage(0)
		rightPage.Blocks = []invertedPostingBlock{rightBlock}
		rightLeaf.InvertedPostPage = rightPage

		oldRootIdx, err = index.createPostingRoot(ctx, []*Page{leftLeaf, rightLeaf})
		if err != nil {
			return err
		}
		leftLeafIdx = leftLeaf.Index
		cell = invertedEntryCell{
			Term:         "k:type",
			DocFreq:      66,
			PostingCount: 66,
			Child:        oldRootIdx,
			PostingKind:  invertedPostingKindTree,
			CodecVersion: invertedPostingCodecVersion,
		}
		return nil
	}))

	beforeFreePages := basePager.GetHeader(ctx).FreePageCount
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var result deletePostingTreeCellResult
		result, err = index.deletePostingFromTreeCell(ctx, cell, invertedPosting{RowID: 1000})
		require.True(t, result.Mutated)
		require.False(t, result.RemoveEntry)
		cell = result.Cell
		return err
	}))

	assert.Equal(t, leftLeafIdx, cell.Child)
	assert.Equal(t, uint32(65), cell.DocFreq)
	assert.Equal(t, uint32(65), cell.PostingCount)
	assert.Equal(t, beforeFreePages+2, basePager.GetHeader(ctx).FreePageCount)

	leftLeaf, err := index.pager.ReadPage(ctx, leftLeafIdx)
	require.NoError(t, err)
	require.NotNil(t, leftLeaf.InvertedPostPage)
	assert.Zero(t, leftLeaf.InvertedPostPage.Header.Parent)
	assert.Zero(t, leftLeaf.InvertedPostPage.Header.NextLeaf)
}

func TestDedicatedInvertedIndex_RebalancesPostingLeafByBorrowingFromLeft(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload_inv")
	index, err := NewDedicatedInvertedIndex("idx_payload_inv", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	var rootIdx, leftIdx, rightIdx PageIndex
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		left, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		right, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		leftIdx = left.Index
		rightIdx = right.Index

		left.Clear()
		leftPage := NewInvertedPostingPage(0)
		for i := 1; i <= 85; i++ {
			block, err := testInvertedPostingBlock(invertedPostingModeRowIDs, RowID(i*10))
			if err != nil {
				return err
			}
			leftPage.Blocks = append(leftPage.Blocks, block)
		}
		require.NoError(t, ensureInvertedPostingPageFits(leftPage, invertedPageBodySize(left.Index)))
		leftPage.Header.NextLeaf = right.Index
		left.InvertedPostPage = leftPage

		right.Clear()
		rightBlock, err := testInvertedPostingBlock(invertedPostingModeRowIDs, 10000)
		if err != nil {
			return err
		}
		rightPage := NewInvertedPostingPage(0)
		rightPage.Blocks = []invertedPostingBlock{rightBlock}
		right.InvertedPostPage = rightPage

		rootIdx, err = index.createPostingRoot(ctx, []*Page{left, right})
		if err != nil {
			return err
		}
		rootIdx, err = index.rebalancePostingPageAfterDelete(ctx, rootIdx, right.Index)
		return err
	}))

	assertDedicatedInvertedPostingTreeValid(t, ctx, index, rootIdx, 0)
	left, err := index.pager.ReadPage(ctx, leftIdx)
	require.NoError(t, err)
	right, err := index.pager.ReadPage(ctx, rightIdx)
	require.NoError(t, err)
	require.NotNil(t, left.InvertedPostPage)
	require.NotNil(t, right.InvertedPostPage)
	assert.Len(t, left.InvertedPostPage.Blocks, 84)
	require.Len(t, right.InvertedPostPage.Blocks, 2)
	assert.Equal(t, RowID(850), right.InvertedPostPage.Blocks[0].FirstRowID)
	assert.Equal(t, RowID(10000), right.InvertedPostPage.Blocks[1].FirstRowID)
}

func TestDedicatedInvertedIndex_MergesPostingInternalPagesAndCollapsesRoot(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "events", "idx_payload_inv")
	index, err := NewDedicatedInvertedIndex("idx_payload_inv", invertedIndexPostingModeRowIDs, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, txManager.ExecuteInTransaction(ctx, index.InitRootPage))

	var rootIdx, collapsedRootIdx, leftInternalIdx, rightInternalIdx PageIndex
	var beforeFreePages uint32
	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		leftLeaf, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		rightLeaf, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		leftInternal, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		rightInternal, err := index.pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		leftInternalIdx = leftInternal.Index
		rightInternalIdx = rightInternal.Index

		leftLeaf.Clear()
		leftBlock, err := testInvertedPostingBlock(invertedPostingModeRowIDs, 10)
		if err != nil {
			return err
		}
		leftLeafPage := NewInvertedPostingPage(0)
		leftLeafPage.Header.Parent = leftInternal.Index
		leftLeafPage.Header.NextLeaf = rightLeaf.Index
		leftLeafPage.Blocks = []invertedPostingBlock{leftBlock}
		leftLeaf.InvertedPostPage = leftLeafPage

		rightLeaf.Clear()
		rightBlock, err := testInvertedPostingBlock(invertedPostingModeRowIDs, 20)
		if err != nil {
			return err
		}
		rightLeafPage := NewInvertedPostingPage(0)
		rightLeafPage.Header.Parent = rightInternal.Index
		rightLeafPage.Blocks = []invertedPostingBlock{rightBlock}
		rightLeaf.InvertedPostPage = rightLeafPage

		leftInternal.Clear()
		leftRoute, err := index.routingBlockForPostingPage(leftLeaf)
		if err != nil {
			return err
		}
		leftInternalPage := NewInvertedPostingPage(1)
		leftInternalPage.Blocks = []invertedPostingBlock{leftRoute}
		leftInternal.InvertedPostPage = leftInternalPage

		rightInternal.Clear()
		rightRoute, err := index.routingBlockForPostingPage(rightLeaf)
		if err != nil {
			return err
		}
		rightInternalPage := NewInvertedPostingPage(1)
		rightInternalPage.Blocks = []invertedPostingBlock{rightRoute}
		rightInternal.InvertedPostPage = rightInternalPage

		rootIdx, err = index.createPostingRoot(ctx, []*Page{leftInternal, rightInternal})
		if err != nil {
			return err
		}
		beforeFreePages = basePager.GetHeader(ctx).FreePageCount
		collapsedRootIdx, err = index.rebalancePostingPageAfterDelete(ctx, rootIdx, leftInternal.Index)
		if err != nil {
			return err
		}
		return nil
	}))

	assert.Equal(t, beforeFreePages+2, basePager.GetHeader(ctx).FreePageCount)
	assert.NotEqual(t, rootIdx, collapsedRootIdx)
	assert.Equal(t, leftInternalIdx, collapsedRootIdx)
	assertDedicatedInvertedPostingTreeValid(t, ctx, index, collapsedRootIdx, 0)
	leftInternal, err := index.pager.ReadPage(ctx, leftInternalIdx)
	require.NoError(t, err)
	require.NotNil(t, leftInternal.InvertedPostPage)
	require.Len(t, leftInternal.InvertedPostPage.Blocks, 2)
	assert.Zero(t, leftInternal.InvertedPostPage.Header.Parent)
	freedRight, err := index.pager.ReadPage(ctx, rightInternalIdx)
	require.NoError(t, err)
	assert.NotNil(t, freedRight.FreePage)
}

func TestDedicatedInvertedIndex_DemotesPostingTreeAfterDeletes(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_json", invertedIndexPostingModeRowIDs)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := 1; i <= 140; i++ {
			if err := index.Insert(ctx, "status", invertedPosting{RowID: largeTestRowID(i)}); err != nil {
				return err
			}
		}
		return nil
	}))
	require.Equal(t, invertedPostingKindTree, requireDedicatedInvertedEntryCell(t, ctx, index, "status").PostingKind)

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i := 21; i <= 140; i++ {
			if err := index.Delete(ctx, "status", invertedPosting{RowID: largeTestRowID(i)}); err != nil {
				return err
			}
		}
		return nil
	}))

	cell := requireDedicatedInvertedEntryCell(t, ctx, index, "status")
	assert.Equal(t, invertedPostingKindInline, cell.PostingKind)
	assert.Zero(t, cell.Child)

	postings := collectDedicatedInvertedPostings(t, ctx, index, "status")
	require.Len(t, postings, 20)
	assert.Equal(t, invertedPosting{RowID: largeTestRowID(1)}, postings[0])
	assert.Equal(t, invertedPosting{RowID: largeTestRowID(20)}, postings[len(postings)-1])
}

func TestDedicatedInvertedIndex_DeleteTreePostingCanRemoveEntry(t *testing.T) {
	index, txManager := newTestDedicatedInvertedIndex(t, "idx_body", invertedIndexPostingModePositions)
	ctx := context.Background()
	positions := make([]uint32, 2000)
	for i := range positions {
		positions[i] = uint32(i + 1)
	}

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.Insert(ctx, "database", invertedPosting{RowID: 1, Positions: positions})
	}))
	require.Equal(t, invertedPostingKindTree, requireDedicatedInvertedEntryCell(t, ctx, index, "database").PostingKind)

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return index.Delete(ctx, "database", invertedPosting{RowID: 1, Positions: positions})
	}))

	iter, err := index.Lookup(ctx, "database")
	require.NoError(t, err)
	_, ok, err := iter.NextBlock(ctx)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestDedicatedInvertedIndex_PersistsPostingTree(t *testing.T) {
	pager, dbFile := initTest(t)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "articles", "idx_body")
	index, err := NewDedicatedInvertedIndex("idx_body", invertedIndexPostingModePositions, txPager, 0)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for rowID := 1; rowID <= 120; rowID++ {
			if err := index.Insert(ctx, "database", invertedPosting{RowID: RowID(rowID), Positions: []uint32{1, 3, 5, 8, 13, 21, 34, 55}}); err != nil {
				return err
			}
		}
		return nil
	}))
	require.Equal(t, invertedPostingKindTree, requireDedicatedInvertedEntryCell(t, ctx, index, "database").PostingKind)
	require.NoError(t, pager.Flush(ctx, 0))
	for pageIdx := PageIndex(1); pageIdx < PageIndex(pager.TotalPages()); pageIdx++ {
		require.NoError(t, pager.Flush(ctx, pageIdx))
	}

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

	postings := collectDedicatedInvertedPostings(t, ctx, reopenedIndex, "database")
	require.Len(t, postings, 120)
	assert.Equal(t, invertedPosting{RowID: 1, Positions: []uint32{1, 3, 5, 8, 13, 21, 34, 55}}, postings[0])
	assert.Equal(t, invertedPosting{RowID: 120, Positions: []uint32{1, 3, 5, 8, 13, 21, 34, 55}}, postings[len(postings)-1])
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

func requireDedicatedInvertedEntryCell(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex, term string) invertedEntryCell {
	t.Helper()

	page, err := index.findEntryLeafPage(ctx, term)
	require.NoError(t, err)
	cellIdx, found := findInvertedEntryCell(page.InvertedEntryPage.Cells, term)
	require.True(t, found)
	return page.InvertedEntryPage.Cells[cellIdx]
}

func countDedicatedInvertedEntryLeaves(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex) int {
	t.Helper()

	root, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	page := root
	for !page.InvertedEntryPage.Header.IsLeaf {
		childIdx := page.InvertedEntryPage.Cells[0].Child
		page, err = index.pager.ReadPage(ctx, childIdx)
		require.NoError(t, err)
		require.NotNil(t, page.InvertedEntryPage)
	}

	count := 0
	for {
		count++
		next := page.InvertedEntryPage.Header.NextLeaf
		if next == 0 {
			return count
		}
		page, err = index.pager.ReadPage(ctx, next)
		require.NoError(t, err)
		require.NotNil(t, page.InvertedEntryPage)
	}
}

func dedicatedInvertedEntryTreeHeight(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex) int {
	t.Helper()

	page, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	height := 1
	for !page.InvertedEntryPage.Header.IsLeaf {
		childIdx := page.InvertedEntryPage.Cells[0].Child
		page, err = index.pager.ReadPage(ctx, childIdx)
		require.NoError(t, err)
		require.NotNil(t, page.InvertedEntryPage)
		height++
	}
	return height
}

func assertDedicatedInvertedEntryTreeValid(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex) []string {
	t.Helper()

	root, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	terms := assertDedicatedInvertedEntryPageValid(t, ctx, index, root.Index, 0, "", "", true)
	assert.Equal(t, terms, collectDedicatedInvertedLeafChainTerms(t, ctx, index))
	return terms
}

func assertDedicatedInvertedEntryPageValid(
	t *testing.T,
	ctx context.Context,
	index *dedicatedInvertedIndex,
	pageIdx PageIndex,
	parentIdx PageIndex,
	minTerm string,
	maxTerm string,
	isRoot bool,
) []string {
	t.Helper()

	page, err := index.pager.ReadPage(ctx, pageIdx)
	require.NoError(t, err)
	require.NotNil(t, page.InvertedEntryPage)
	entryPage := page.InvertedEntryPage
	assert.Equal(t, parentIdx, entryPage.Header.Parent)
	if !isRoot {
		assert.NotEqual(t, index.rootPageIdx, pageIdx)
	}

	for i := 1; i < len(entryPage.Cells); i++ {
		assert.Less(t, entryPage.Cells[i-1].Term, entryPage.Cells[i].Term)
	}

	if entryPage.Header.IsLeaf {
		terms := invertedEntryTerms(entryPage.Cells)
		for _, term := range terms {
			if minTerm != "" {
				assert.GreaterOrEqual(t, term, minTerm)
			}
			if maxTerm != "" {
				assert.Less(t, term, maxTerm)
			}
		}
		return terms
	}

	children := invertedEntryChildren(entryPage)
	require.Len(t, children, len(entryPage.Cells)+1)
	terms := make([]string, 0)
	for i, childIdx := range children {
		childMin := minTerm
		if i > 0 {
			childMin = entryPage.Cells[i-1].Term
		}
		childMax := maxTerm
		if i < len(entryPage.Cells) {
			childMax = entryPage.Cells[i].Term
		}
		childTerms := assertDedicatedInvertedEntryPageValid(t, ctx, index, childIdx, pageIdx, childMin, childMax, false)
		if i > 0 && len(childTerms) > 0 {
			assert.Equal(t, childTerms[0], entryPage.Cells[i-1].Term)
		}
		terms = append(terms, childTerms...)
	}
	return terms
}

func collectDedicatedInvertedLeafChainTerms(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex) []string {
	t.Helper()

	page, err := index.readRootEntryPage(ctx)
	require.NoError(t, err)
	for !page.InvertedEntryPage.Header.IsLeaf {
		childIdx := page.InvertedEntryPage.Cells[0].Child
		page, err = index.pager.ReadPage(ctx, childIdx)
		require.NoError(t, err)
		require.NotNil(t, page.InvertedEntryPage)
	}

	terms := make([]string, 0)
	for {
		terms = append(terms, invertedEntryTerms(page.InvertedEntryPage.Cells)...)
		next := page.InvertedEntryPage.Header.NextLeaf
		if next == 0 {
			return terms
		}
		page, err = index.pager.ReadPage(ctx, next)
		require.NoError(t, err)
		require.NotNil(t, page.InvertedEntryPage)
		require.True(t, page.InvertedEntryPage.Header.IsLeaf)
	}
}

func assertDedicatedInvertedPostingTreeValid(t *testing.T, ctx context.Context, index *dedicatedInvertedIndex, pageIdx, parentIdx PageIndex) int {
	t.Helper()

	page, err := index.pager.ReadPage(ctx, pageIdx)
	require.NoError(t, err)
	require.NotNil(t, page.InvertedPostPage)
	postingPage := page.InvertedPostPage
	assert.Equal(t, parentIdx, postingPage.Header.Parent)
	require.NotEmpty(t, postingPage.Blocks)
	_, _, _, err = invertedPostingPageRange(postingPage)
	require.NoError(t, err)

	if postingPage.Header.Level == 0 {
		for _, block := range postingPage.Blocks {
			assert.Zero(t, block.Child)
			assert.NotEmpty(t, block.Payload)
			assert.Equal(t, invertedPostingCodecVersion, block.CodecVersion)
		}
		return 1
	}

	pageCount := 1
	for _, block := range postingPage.Blocks {
		assert.Empty(t, block.Payload)
		assert.NotZero(t, block.Child)
		childPage, err := index.pager.ReadPage(ctx, block.Child)
		require.NoError(t, err)
		require.NotNil(t, childPage.InvertedPostPage)
		assert.Equal(t, postingPage.Header.Level-1, childPage.InvertedPostPage.Header.Level)
		first, last, count, err := invertedPostingPageRange(childPage.InvertedPostPage)
		require.NoError(t, err)
		assert.Equal(t, block.FirstRowID, first)
		assert.Equal(t, block.LastRowID, last)
		assert.Equal(t, block.PostingCount, count)
		pageCount += assertDedicatedInvertedPostingTreeValid(t, ctx, index, block.Child, pageIdx)
	}
	return pageCount
}

func testInvertedEntryPage(pageIdx PageIndex, isLeaf bool, terms []string) *Page {
	page := &Page{Index: pageIdx, InvertedEntryPage: NewInvertedEntryPage(isLeaf)}
	for _, term := range terms {
		page.InvertedEntryPage.Cells = append(page.InvertedEntryPage.Cells, invertedEntryCell{
			Term:         term,
			PostingKind:  invertedPostingKindInline,
			CodecVersion: invertedPostingCodecVersion,
			DocFreq:      1,
			PostingCount: 1,
			Payload:      []byte{invertedPostingCodecVersion, byte(invertedPostingModeRowIDs), 1},
		})
	}
	return page
}

func invertedEntryTerms(cells []invertedEntryCell) []string {
	terms := make([]string, len(cells))
	for i, cell := range cells {
		terms[i] = cell.Term
	}
	return terms
}

func largeTestRowID(i int) RowID {
	return RowID(i) << 56
}

func wideTestRowID(i int) RowID {
	return RowID(i) << 40
}

func testInvertedPostingBlock(mode invertedPostingMode, rowID RowID) (invertedPostingBlock, error) {
	payload, err := encodeInvertedPostingList(mode, []invertedPosting{{RowID: rowID}})
	if err != nil {
		return invertedPostingBlock{}, err
	}
	return postingBlockFromPostings(mode, []invertedPosting{{RowID: rowID}}, payload), nil
}
