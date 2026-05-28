package minisql

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_BTreeInvariants_InsertSequence(t *testing.T) {
	var (
		ctx           = context.Background()
		pager, dbFile = initTest(t)
		rows          = gen.MediumRows(60)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)
	table.maximumICells = 5

	for i, row := range rows {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(testMediumColumns...),
			Inserts: [][]OptionalValue{row.Values},
		}
		mustInsert(ctx, t, table, txManager, stmt)

		assertTableBTreeInvariants(t, pager, table)
		checkRows(ctx, t, table, rows[:i+1])
	}
}

func TestTable_BTreeInvariants_RandomDeleteSequence(t *testing.T) {
	var (
		ctx           = context.Background()
		pager, dbFile = initTest(t)
		rows          = gen.MediumRows(60)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)
	table.maximumICells = 5

	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: make([][]OptionalValue, 0, len(rows)),
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}
	mustInsert(ctx, t, table, txManager, stmt)
	assertTableBTreeInvariants(t, pager, table)

	remaining := append([]Row(nil), rows...)
	order := rand.New(rand.NewSource(42)).Perm(len(rows))
	for step, idx := range order {
		row := rows[idx]
		ids := rowIDs(row)
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})
		require.Equal(t, 1, result.RowsAffected)

		for i := range remaining {
			if remaining[i].Values[0] == row.Values[0] {
				remaining = append(remaining[:i], remaining[i+1:]...)
				break
			}
		}

		assertTableBTreeInvariants(t, pager, table)
		if t.Failed() {
			t.Fatalf("btree invariants failed at delete step=%d row_idx=%d", step, idx)
		}
		checkRows(ctx, t, table, remaining)
	}
}

func TestTable_BTreeInvariants_DeleteRegression_52Then5(t *testing.T) {
	var (
		ctx           = context.Background()
		pager, dbFile = initTest(t)
		rows          = gen.MediumRows(60)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)
	table.maximumICells = 5

	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: make([][]OptionalValue, 0, len(rows)),
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}
	mustInsert(ctx, t, table, txManager, stmt)
	assertTableBTreeInvariants(t, pager, table)

	for _, idx := range []int{52, 5} {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, rowIDs(rows[idx])...),
				},
			},
		})
		require.Equal(t, 1, result.RowsAffected)
		assertTableBTreeInvariants(t, pager, table)
	}
}

func assertTableBTreeInvariants(t *testing.T, pager *pagerImpl, table *Table) {
	t.Helper()

	require.Positive(t, int(pager.TotalPages()))
	rootPage := pager.pages[table.GetRootPageIdx()]
	require.NotNil(t, rootPage)
	require.NotEqual(t, rootPage.LeafNode != nil, rootPage.InternalNode != nil, "root must be either leaf or internal")

	state := &tableTreeCheckState{
		t:       t,
		pager:   pager,
		table:   table,
		visited: make(map[PageIndex]struct{}),
	}

	summary := state.walk(table.GetRootPageIdx(), 0, 0)
	if rootPage.LeafNode != nil {
		assert.Equal(t, 0, int(rootPage.LeafNode.Header.Parent))
		assert.True(t, rootPage.LeafNode.Header.IsRoot)
		if summary.hasRows {
			assert.Equal(t, rootPage.LeafNode.Keys()[0], summary.minKey)
			assert.Equal(t, rootPage.LeafNode.Keys()[len(rootPage.LeafNode.Keys())-1], summary.maxKey)
		}
	} else {
		assert.Equal(t, 0, int(rootPage.InternalNode.Header.Parent))
		assert.True(t, rootPage.InternalNode.Header.IsRoot)
	}

	for i, leafIdx := range state.leaves {
		page := pager.pages[leafIdx]
		require.NotNil(t, page)
		require.NotNil(t, page.LeafNode)
		if i == len(state.leaves)-1 {
			assert.Equal(t, 0, int(page.LeafNode.Header.NextLeaf), "last leaf should terminate the chain")
			continue
		}
		assert.Equal(t, state.leaves[i+1], page.LeafNode.Header.NextLeaf, "leaf chain must follow in-order traversal")
	}
}

type tableTreeCheckState struct {
	t       *testing.T
	pager   *pagerImpl
	table   *Table
	visited map[PageIndex]struct{}
	leaves  []PageIndex
}

type tableSubtreeSummary struct {
	hasRows   bool
	minKey    RowID
	maxKey    RowID
	leafDepth int
}

func (s *tableTreeCheckState) walk(pageIdx, parentIdx PageIndex, depth int) tableSubtreeSummary {
	s.t.Helper()

	if _, ok := s.visited[pageIdx]; ok {
		require.FailNowf(s.t, "table tree cycle", "page %d visited more than once", pageIdx)
	}
	s.visited[pageIdx] = struct{}{}

	require.Less(s.t, int(pageIdx), len(s.pager.pages))
	page := s.pager.pages[pageIdx]
	require.NotNil(s.t, page)
	require.Nil(s.t, page.FreePage, "reachable table page %d cannot also be free", pageIdx)
	require.NotEqual(s.t, page.LeafNode != nil, page.InternalNode != nil, "page %d must be either leaf or internal", pageIdx)

	if page.LeafNode != nil {
		node := page.LeafNode
		assert.Equal(s.t, parentIdx, node.Header.Parent, "leaf %d has wrong parent", pageIdx)
		if pageIdx == s.table.GetRootPageIdx() {
			assert.True(s.t, node.Header.IsRoot)
		} else {
			assert.False(s.t, node.Header.IsRoot)
		}

		keys := node.Keys()
		for i := 1; i < len(keys); i++ {
			assert.Less(s.t, keys[i-1], keys[i], "leaf %d keys must be strictly increasing", pageIdx)
		}

		s.leaves = append(s.leaves, pageIdx)
		if len(keys) == 0 {
			assert.Equal(s.t, s.table.GetRootPageIdx(), pageIdx, "only the root leaf may be empty")
			assert.Equal(s.t, 0, int(node.Header.NextLeaf))
			return tableSubtreeSummary{leafDepth: depth}
		}

		return tableSubtreeSummary{
			hasRows:   true,
			minKey:    keys[0],
			maxKey:    keys[len(keys)-1],
			leafDepth: depth,
		}
	}

	node := page.InternalNode
	assert.Equal(s.t, parentIdx, node.Header.Parent, "internal page %d has wrong parent", pageIdx)
	if pageIdx == s.table.GetRootPageIdx() {
		assert.True(s.t, node.Header.IsRoot)
	} else {
		assert.False(s.t, node.Header.IsRoot)
	}
	assert.NotEqual(s.t, RightChildNotSet, node.Header.RightChild, "internal page %d must have a right child", pageIdx)
	assert.Positive(s.t, int(node.Header.KeysNum), "reachable internal page %d must have at least one key", pageIdx)
	assert.LessOrEqual(s.t, int(node.Header.KeysNum), s.table.maxICells(pageIdx), "internal page %d exceeds max keys", pageIdx)
	if pageIdx != s.table.GetRootPageIdx() {
		assert.GreaterOrEqual(s.t, int(node.Header.KeysNum), s.table.maxICells(pageIdx)/2, "internal page %d underflowed", pageIdx)
	}

	keys := node.Keys()
	for i := 1; i < len(keys); i++ {
		assert.Less(s.t, keys[i-1], keys[i], "internal page %d separator keys must be strictly increasing", pageIdx)
	}

	firstChildIdx, err := node.Child(0)
	require.NoError(s.t, err)
	firstSummary := s.walk(firstChildIdx, pageIdx, depth+1)
	require.True(s.t, firstSummary.hasRows, "child %d of internal page %d must not be empty", firstChildIdx, pageIdx)
	assert.Equal(s.t, node.ICells[0].Key, firstSummary.maxKey, "internal page %d first separator should equal left child max", pageIdx)

	prevMax := firstSummary.maxKey
	minKey := firstSummary.minKey
	leafDepth := firstSummary.leafDepth

	for i := uint32(1); i < node.Header.KeysNum; i++ {
		childIdx, err := node.Child(i)
		require.NoError(s.t, err)
		childSummary := s.walk(childIdx, pageIdx, depth+1)
		require.True(s.t, childSummary.hasRows, "child %d of internal page %d must not be empty", childIdx, pageIdx)
		assert.Equal(s.t, leafDepth, childSummary.leafDepth, "all leaves must be at the same depth")
		assert.Less(s.t, prevMax, childSummary.minKey, "child %d of internal page %d overlaps previous range", childIdx, pageIdx)
		assert.Equal(s.t, node.ICells[i].Key, childSummary.maxKey, "internal page %d separator should equal child max", pageIdx)
		prevMax = childSummary.maxKey
	}

	rightSummary := s.walk(node.Header.RightChild, pageIdx, depth+1)
	require.True(s.t, rightSummary.hasRows, "right child %d of internal page %d must not be empty", node.Header.RightChild, pageIdx)
	assert.Equal(s.t, leafDepth, rightSummary.leafDepth, "all leaves must be at the same depth")
	assert.Less(s.t, prevMax, rightSummary.minKey, "right child of internal page %d overlaps previous range", pageIdx)
	maxKey := rightSummary.maxKey

	return tableSubtreeSummary{
		hasRows:   true,
		minKey:    minKey,
		maxKey:    maxKey,
		leafDepth: leafDepth,
	}
}
