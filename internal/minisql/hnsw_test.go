package minisql

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- in-memory graph tests ----

func TestHNSWGraph_EmptySearch(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	dist := func(RowID) (float64, error) { return 0, nil }
	ids, err := g.search(5, 10, dist)
	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestHNSWGraph_InsertAndSearch_SingleNode(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	vecs := map[RowID][]float32{
		1: {1, 0, 0},
	}
	testBuildGraph(t, g, vecs)

	query := VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}
	distFn := testL2DistFn(vecs, query)
	ids, err := g.search(1, 10, distFn)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, RowID(1), ids[0])
}

func TestHNSWGraph_InsertAndSearch_ReturnsNearestFirst(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(8, 50)
	vecs := map[RowID][]float32{
		1: {0, 0, 0},
		2: {1, 0, 0},
		3: {2, 0, 0},
		4: {10, 0, 0},
		5: {11, 0, 0},
	}
	testBuildGraph(t, g, vecs)

	// Query closest to {1, 0, 0} → expect row 2 first (distance 0).
	query := VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}
	distFn := testL2DistFn(vecs, query)
	ids, err := g.search(3, 20, distFn)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	assert.Equal(t, RowID(2), ids[0], "nearest to [1,0,0] should be row 2 (dist=0)")
}

func TestHNSWGraph_SearchKCapped(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	vecs := map[RowID][]float32{
		1: {0, 0}, 2: {1, 0}, 3: {2, 0}, 4: {3, 0}, 5: {4, 0},
	}
	testBuildGraph(t, g, vecs)

	query := VectorPointer{Dims: 2, Data: []float32{0, 0}}
	distFn := testL2DistFn(vecs, query)
	ids, err := g.search(3, 10, distFn)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(ids), 3)
}

// ---- page marshal round-trip tests ----

func TestHNSWMetaPage_MarshalUnmarshal(t *testing.T) {
	t.Parallel()
	original := &hnswMetaPage{
		M:              16,
		EfConstruction: 200,
		EntryPoint:     42,
		EntryLevel:     3,
		NodeCount:      100,
		FirstDataPage:  7,
	}
	buf := make([]byte, hnswMetaHeaderSize)
	require.NoError(t, original.Marshal(buf))

	decoded := new(hnswMetaPage)
	require.NoError(t, decoded.Unmarshal(buf))

	assert.Equal(t, original, decoded)
}

func TestHNSWMetaPage_EmptyGraph(t *testing.T) {
	t.Parallel()
	original := &hnswMetaPage{
		M:              8,
		EfConstruction: 100,
		EntryPoint:     hnswNoEntryPoint,
		EntryLevel:     0,
		NodeCount:      0,
		FirstDataPage:  0,
	}
	buf := make([]byte, hnswMetaHeaderSize)
	require.NoError(t, original.Marshal(buf))

	decoded := new(hnswMetaPage)
	require.NoError(t, decoded.Unmarshal(buf))
	assert.Equal(t, uint64(math.MaxUint64), decoded.EntryPoint)
}

func TestHNSWDataPage_MarshalUnmarshal(t *testing.T) {
	t.Parallel()
	original := &hnswDataPage{
		NextPage: 5,
		Nodes: []hnswNodeRecord{
			{RowID: 1, Neighbors: [][]uint64{{2, 3}, {4}}},
			{RowID: 7, Neighbors: [][]uint64{{8, 9, 10}}},
		},
	}
	buf := make([]byte, 4096-4)
	require.NoError(t, original.Marshal(buf))

	decoded := new(hnswDataPage)
	require.NoError(t, decoded.Unmarshal(buf))

	assert.Equal(t, original.NextPage, decoded.NextPage)
	require.Len(t, decoded.Nodes, 2)
	assert.Equal(t, uint64(1), decoded.Nodes[0].RowID)
	assert.Equal(t, [][]uint64{{2, 3}, {4}}, decoded.Nodes[0].Neighbors)
	assert.Equal(t, uint64(7), decoded.Nodes[1].RowID)
	assert.Equal(t, [][]uint64{{8, 9, 10}}, decoded.Nodes[1].Neighbors)
}

// ---- page persistence round-trip test ----

func TestBuildAndReadHNSWGraph(t *testing.T) {
	ctx := context.Background()

	pagerImpl, dbFile := initTest(t)
	hnswPagerInst := pagerImpl.ForHNSWIndex()
	txManager := NewTransactionManager(testLogger, dbFile.Name(), mockPagerFactory(hnswPagerInst), pagerImpl, nil)

	// Build a small graph.
	vecs := map[RowID][]float32{
		1: {1, 0, 0},
		2: {0, 1, 0},
		3: {0, 0, 1},
		4: {1, 1, 0},
	}
	var rows []hnswBuildRow
	for id := RowID(1); int(id) <= len(vecs); id++ {
		rows = append(rows, hnswBuildRow{RowID: id, Vec: VectorPointer{Dims: 3, Data: vecs[id]}})
	}

	var rootPageIdx PageIndex
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		txPager := NewTransactionalPager(hnswPagerInst, txManager, "t", "idx_vec")
		var err error
		rootPageIdx, err = BuildHNSWIndex(ctx, txPager, 4, 20, rows)
		return err
	})
	require.NoError(t, err)

	// Read back and verify.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		txPager := NewTransactionalPager(hnswPagerInst, txManager, "t", "idx_vec")
		g, err := readHNSWGraph(ctx, txPager, rootPageIdx)
		if err != nil {
			return err
		}
		assert.True(t, g.hasEntry)
		assert.Len(t, g.Nodes, len(vecs))

		query := VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}
		distFn := testL2DistFn(vecs, query)
		ids, err := g.search(2, 10, distFn)
		if err != nil {
			return err
		}
		require.NotEmpty(t, ids)
		assert.Equal(t, RowID(1), ids[0], "nearest to [1,0,0] should be row 1")
		return nil
	})
	require.NoError(t, err)
}

// ---- hnswIndex online DML tests ----

// newTestHNSWIndex builds an HNSW index from seedVecs and returns the index
// handle plus the transaction manager so callers can run further transactions.
func newTestHNSWIndex(t *testing.T, seedVecs map[RowID][]float32, dims uint32) (*hnswIndex, *TransactionManager) {
	t.Helper()
	ctx := context.Background()
	pagerImpl, dbFile := initTest(t)
	hnswPagerInst := pagerImpl.ForHNSWIndex()
	txManager := NewTransactionManager(testLogger, dbFile.Name(), mockPagerFactory(hnswPagerInst), pagerImpl, nil)

	var rows []hnswBuildRow
	for id, data := range seedVecs {
		rows = append(rows, hnswBuildRow{RowID: id, Vec: VectorPointer{Dims: dims, Data: data}})
	}

	txPager := NewTransactionalPager(hnswPagerInst, txManager, "t", "idx_v")

	var rootPageIdx PageIndex
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		rootPageIdx, err = BuildHNSWIndex(ctx, txPager, 4, 20, rows)
		return err
	})
	require.NoError(t, err)

	idx := OpenHNSWIndex(txPager, rootPageIdx, defaultHNSWVecCacheSize)
	return idx, txManager
}

func TestHNSWIndex_GetRootPageIdx(t *testing.T) {
	ctx := context.Background()
	pagerImpl, dbFile := initTest(t)
	hnswPagerInst := pagerImpl.ForHNSWIndex()
	txManager := NewTransactionManager(testLogger, dbFile.Name(), mockPagerFactory(hnswPagerInst), pagerImpl, nil)
	txPager := NewTransactionalPager(hnswPagerInst, txManager, "t", "idx_v")

	var rootPageIdx PageIndex
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		rootPageIdx, err = BuildHNSWIndex(ctx, txPager, 4, 20, nil)
		return err
	})
	require.NoError(t, err)

	idx := OpenHNSWIndex(txPager, rootPageIdx, defaultHNSWVecCacheSize)
	assert.Equal(t, rootPageIdx, idx.GetRootPageIdx())
}

func TestHNSWIndex_SearchAfterBuild(t *testing.T) {
	ctx := context.Background()
	seedVecs := map[RowID][]float32{
		0: {1, 0, 0},
		1: {2, 0, 0},
		2: {3, 0, 0},
	}
	idx, txManager := newTestHNSWIndex(t, seedVecs, 3)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		query := VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}
		distFn := testL2DistFn(seedVecs, query)
		ids, err := idx.Search(ctx, 1, 10, distFn)
		if err != nil {
			return err
		}
		assert.NotEmpty(t, ids)
		assert.Equal(t, RowID(0), ids[0], "nearest to [1,0,0] should be row 0")
		return nil
	})
	require.NoError(t, err)
}

func TestHNSWIndex_Insert_MakesNodeSearchable(t *testing.T) {
	ctx := context.Background()
	seedVecs := map[RowID][]float32{
		0: {1, 0, 0},
		1: {2, 0, 0},
	}
	idx, txManager := newTestHNSWIndex(t, seedVecs, 3)

	// Insert a new node far from the existing ones.
	newRowID := RowID(2)
	allVecs := map[RowID][]float32{0: {1, 0, 0}, 1: {2, 0, 0}, 2: {100, 0, 0}}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := testL2DistFn(allVecs, VectorPointer{Dims: 3, Data: []float32{100, 0, 0}})
		return idx.Insert(ctx, newRowID, distFn)
	})
	require.NoError(t, err)

	// The inserted node should now be nearest to its own position.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := testL2DistFn(allVecs, VectorPointer{Dims: 3, Data: []float32{100, 0, 0}})
		ids, err := idx.Search(ctx, 1, 10, distFn)
		if err != nil {
			return err
		}
		assert.NotEmpty(t, ids)
		assert.Equal(t, newRowID, ids[0])
		return nil
	})
	require.NoError(t, err)
}

func TestHNSWIndex_Delete_NodeNotReturned(t *testing.T) {
	ctx := context.Background()
	seedVecs := map[RowID][]float32{
		0: {1, 0, 0},
		1: {50, 0, 0},
		2: {100, 0, 0},
	}
	idx, txManager := newTestHNSWIndex(t, seedVecs, 3)

	// Delete row 2.
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return idx.Delete(ctx, RowID(2))
	})
	require.NoError(t, err)

	// Row 2 must not appear in search results near [100,0,0].
	remaining := map[RowID][]float32{0: {1, 0, 0}, 1: {50, 0, 0}}
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := testL2DistFn(remaining, VectorPointer{Dims: 3, Data: []float32{100, 0, 0}})
		ids, err := idx.Search(ctx, 3, 10, distFn)
		if err != nil {
			return err
		}
		for _, id := range ids {
			assert.NotEqual(t, RowID(2), id, "deleted row must not be returned")
		}
		return nil
	})
	require.NoError(t, err)
}

func TestHNSWIndex_Delete_EntryPointReassigned(t *testing.T) {
	ctx := context.Background()
	// Single-node graph so the entry point is the only node.
	seedVecs := map[RowID][]float32{
		0: {1, 0, 0},
		1: {2, 0, 0},
	}
	idx, txManager := newTestHNSWIndex(t, seedVecs, 3)

	// Force a known entry point by inspecting the graph.
	g, err := idx.loadGraph(ctx)
	require.NoError(t, err)
	entryPoint := g.EntryPoint

	// Delete the current entry point.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return idx.Delete(ctx, entryPoint)
	})
	require.NoError(t, err)

	// Graph should still be usable (entry point reassigned to the remaining node).
	remaining := map[RowID][]float32{}
	for id, v := range seedVecs {
		if id != entryPoint {
			remaining[id] = v
		}
	}
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for id, v := range remaining {
			distFn := testL2DistFn(remaining, VectorPointer{Dims: 3, Data: v})
			ids, err := idx.Search(ctx, 1, 10, distFn)
			if err != nil {
				return err
			}
			assert.NotEmpty(t, ids)
			assert.Equal(t, id, ids[0])
		}
		return nil
	})
	require.NoError(t, err)
}

func TestHNSWIndex_Insert_IntoEmptyGraph(t *testing.T) {
	ctx := context.Background()
	// Start with an empty graph.
	idx, txManager := newTestHNSWIndex(t, nil, 2)

	vecs := map[RowID][]float32{0: {1, 0}}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := testL2DistFn(vecs, VectorPointer{Dims: 2, Data: []float32{1, 0}})
		return idx.Insert(ctx, RowID(0), distFn)
	})
	require.NoError(t, err)

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := testL2DistFn(vecs, VectorPointer{Dims: 2, Data: []float32{1, 0}})
		ids, err := idx.Search(ctx, 1, 10, distFn)
		if err != nil {
			return err
		}
		assert.NotEmpty(t, ids)
		assert.Equal(t, RowID(0), ids[0])
		return nil
	})
	require.NoError(t, err)
}

// TestHNSWGraph_PruneNeighbors verifies that pruneNeighbors keeps the mMax
// closest neighbors, drops the self-node, and returns them nearest-first.
func TestHNSWGraph_PruneNeighbors(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	// Nodes 1..5 at x = 1..5; self is 0.
	distFn := func(id RowID) (float64, error) { return float64(id), nil }

	pruned := g.pruneNeighbors(0, []RowID{1, 2, 3, 4, 5}, 3, distFn)
	require.Len(t, pruned, 3)
	assert.Equal(t, RowID(1), pruned[0])
	assert.Equal(t, RowID(2), pruned[1])
	assert.Equal(t, RowID(3), pruned[2])
}

func TestHNSWGraph_PruneNeighbors_DropsSelf(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	distFn := func(id RowID) (float64, error) { return float64(id), nil }

	// Self (RowID 2) must be excluded from the result.
	pruned := g.pruneNeighbors(2, []RowID{1, 2, 3}, 3, distFn)
	for _, id := range pruned {
		assert.NotEqual(t, RowID(2), id)
	}
}

func TestHNSWGraph_PruneNeighbors_FewerThanMax(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	distFn := func(id RowID) (float64, error) { return float64(id), nil }

	pruned := g.pruneNeighbors(0, []RowID{1, 2}, 10, distFn)
	assert.Len(t, pruned, 2)
}

// ---- Table HNSW index key helpers ----

// newTestHNSWTable creates a Table with a single VECTOR(dims) column and an
// empty HNSW secondary index.  Both share one pager and TransactionManager.
func newTestHNSWTable(t *testing.T, dims uint32) (*Table, SecondaryIndex, *TransactionManager) {
	t.Helper()
	ctx := context.Background()
	pagerImpl, dbFile := initTest(t)
	hnswPager := pagerImpl.ForHNSWIndex()
	txManager := NewTransactionManager(testLogger, dbFile.Name(), mockPagerFactory(hnswPager), pagerImpl, nil)

	vecCol := Column{Name: "v", Kind: Vector, Size: dims}
	txPager := NewTransactionalPager(hnswPager, txManager, testTableName, "idx_v")
	table := NewTable(testLogger, txPager, txManager, testTableName, []Column{vecCol}, 0, nil)

	var rootPageIdx PageIndex
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		rootPageIdx, err = BuildHNSWIndex(ctx, txPager, 4, 20, nil)
		return err
	})
	require.NoError(t, err)

	idx := OpenHNSWIndex(txPager, rootPageIdx, defaultHNSWVecCacheSize)
	si := SecondaryIndex{
		HNSWIndex: idx,
		IndexInfo: IndexInfo{
			Name:    "idx_v",
			Method:  IndexMethodHNSW,
			Columns: []Column{vecCol},
		},
	}
	return table, si, txManager
}

func TestTable_InsertHNSWIndexKey_NilIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	si := SecondaryIndex{HNSWIndex: nil}
	// nil HNSWIndex must be a no-op.
	err := new(Table).insertHNSWIndexKey(ctx, si, 0, Row{})
	require.NoError(t, err)
}

func TestTable_DeleteHNSWIndexKey_NilIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	si := SecondaryIndex{HNSWIndex: nil}
	err := new(Table).deleteHNSWIndexKey(ctx, si, 0)
	require.NoError(t, err)
}

func TestTable_UpdateHNSWIndexKey_NilIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	si := SecondaryIndex{HNSWIndex: nil}
	err := new(Table).updateHNSWIndexKey(ctx, si, Row{})
	require.NoError(t, err)
}

func TestTable_InsertHNSWIndexKey_FirstInsert(t *testing.T) {
	ctx := context.Background()
	table, si, txManager := newTestHNSWTable(t, 3)
	vecCol := si.Columns[0]

	row := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: true, Value: VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}}},
		Key:     RowID(0),
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.insertHNSWIndexKey(ctx, si, RowID(0), row)
	})
	require.NoError(t, err)

	// The node should now be the entry point and searchable.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		vecs := map[RowID][]float32{0: {1, 0, 0}}
		distFn := testL2DistFn(vecs, VectorPointer{Dims: 3, Data: []float32{1, 0, 0}})
		ids, err := si.HNSWIndex.Search(ctx, 1, 10, distFn)
		if err != nil {
			return err
		}
		assert.NotEmpty(t, ids)
		assert.Equal(t, RowID(0), ids[0])
		return nil
	})
	require.NoError(t, err)
}

func TestTable_InsertHNSWIndexKey_NullVector(t *testing.T) {
	ctx := context.Background()
	table, si, txManager := newTestHNSWTable(t, 3)
	vecCol := si.Columns[0]

	// NULL vector — must be skipped silently.
	row := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: false}},
		Key:     RowID(0),
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.insertHNSWIndexKey(ctx, si, RowID(0), row)
	})
	require.NoError(t, err)
}

func TestTable_DeleteHNSWIndexKey_RemovesNode(t *testing.T) {
	ctx := context.Background()
	table, si, txManager := newTestHNSWTable(t, 2)
	vecCol := si.Columns[0]

	// Insert one node.
	row := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: true, Value: VectorPointer{Dims: 2, Data: []float32{1, 0}}}},
		Key:     RowID(0),
	}
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.insertHNSWIndexKey(ctx, si, RowID(0), row)
	})
	require.NoError(t, err)

	// Now delete it.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.deleteHNSWIndexKey(ctx, si, RowID(0))
	})
	require.NoError(t, err)

	// Graph should be empty — no results.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		vecs := map[RowID][]float32{}
		distFn := testL2DistFn(vecs, VectorPointer{Dims: 2, Data: []float32{1, 0}})
		ids, err := si.HNSWIndex.Search(ctx, 1, 10, distFn)
		if err != nil {
			return err
		}
		assert.Empty(t, ids)
		return nil
	})
	require.NoError(t, err)
}

func TestTable_UpdateHNSWIndexKey_NullVectorAfterDelete(t *testing.T) {
	ctx := context.Background()
	table, si, txManager := newTestHNSWTable(t, 2)
	vecCol := si.Columns[0]

	// Insert one node.
	insertRow := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: true, Value: VectorPointer{Dims: 2, Data: []float32{5, 0}}}},
		Key:     RowID(0),
	}
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.insertHNSWIndexKey(ctx, si, RowID(0), insertRow)
	})
	require.NoError(t, err)

	// Update with a NULL vector — should delete from index and not re-insert.
	updateRow := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: false}},
		Key:     RowID(0),
	}
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.updateHNSWIndexKey(ctx, si, updateRow)
	})
	require.NoError(t, err)
}

func TestTable_UpdateHNSWIndexKey_ReinsertNewVector(t *testing.T) {
	ctx := context.Background()
	table, si, txManager := newTestHNSWTable(t, 2)
	vecCol := si.Columns[0]

	// Insert one node at [1, 0].
	insertRow := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: true, Value: VectorPointer{Dims: 2, Data: []float32{1, 0}}}},
		Key:     RowID(0),
	}
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.insertHNSWIndexKey(ctx, si, RowID(0), insertRow)
	})
	require.NoError(t, err)

	// Update the vector to [99, 0].
	updateRow := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: true, Value: VectorPointer{Dims: 2, Data: []float32{99, 0}}}},
		Key:     RowID(0),
	}
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.updateHNSWIndexKey(ctx, si, updateRow)
	})
	require.NoError(t, err)

	// The node should now be searchable at its new position.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		vecs := map[RowID][]float32{0: {99, 0}}
		distFn := testL2DistFn(vecs, VectorPointer{Dims: 2, Data: []float32{99, 0}})
		ids, err := si.HNSWIndex.Search(ctx, 1, 10, distFn)
		if err != nil {
			return err
		}
		assert.NotEmpty(t, ids)
		assert.Equal(t, RowID(0), ids[0])
		return nil
	})
	require.NoError(t, err)
}

// ---- additional graph parameter / search edge-case tests ----

func TestHNSWGraph_NewWithDefaultParams(t *testing.T) {
	t.Parallel()
	// m = 0 is clamped to HNSWDefaultM.
	g1 := newHNSWGraph(0, 20)
	assert.Equal(t, HNSWDefaultM, g1.M)

	// efConstruction < m is clamped up to m.
	g2 := newHNSWGraph(8, 3)
	assert.Equal(t, 8, g2.EfConstruction)
}

func TestHNSWGraph_Search_EfSearchLessThanK(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	vecs := map[RowID][]float32{1: {1, 0}, 2: {2, 0}, 3: {3, 0}}
	testBuildGraph(t, g, vecs)

	query := VectorPointer{Dims: 2, Data: []float32{1, 0}}
	distFn := testL2DistFn(vecs, query)
	// k=5, efSearch=2 → efSearch is bumped up to k inside search.
	ids, err := g.search(5, 2, distFn)
	require.NoError(t, err)
	assert.NotEmpty(t, ids)
}

func TestHNSWGraph_GreedyStep_LayerOOB(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	// Node 1 has only layer 0 (len(Neighbors)==1).
	g.Nodes[1] = &hnswNodeData{Neighbors: [][]RowID{{}}}
	distFn := func(RowID) (float64, error) { return 0, nil }
	// Requesting layer 1 is out of bounds → greedyStep breaks immediately.
	ep, _, err := g.greedyStep(1, 0.0, 99, 1, distFn)
	require.NoError(t, err)
	assert.Equal(t, RowID(1), ep, "ep unchanged when layer is OOB")
}

// ---- hnswIndexScan / executeExplainScan ----

func TestTable_HNSWIndexScan_DefaultScanLimit(t *testing.T) {
	ctx := context.Background()
	dims := uint32(3)
	vecs := [][]float32{{1, 0, 0}, {5, 0, 0}}
	table, si, txManager := newTestTableWithVectorRows(t, vecs, dims)
	vecCol := si.Columns[0]

	// ScanLimit == 0 → k defaults to HNSWDefaultEfSearch inside hnswIndexScan.
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		scan := Scan{
			IndexName:    "idx_v",
			IndexColumns: []Column{vecCol},
			HNSWQueryVec: VectorPointer{Dims: dims, Data: []float32{1, 0, 0}},
			HNSWFuncName: "VEC_L2",
			ScanLimit:    0,
		}
		return table.hnswIndexScan(ctx, scan, []Field{{Name: "v"}}, func(Row) error { return nil })
	})
	require.NoError(t, err)
}

func TestTable_ExecuteExplainScan_HNSW(t *testing.T) {
	ctx := context.Background()
	dims := uint32(3)
	vecs := [][]float32{{1, 0, 0}, {2, 0, 0}}
	table, si, txManager := newTestTableWithVectorRows(t, vecs, dims)
	vecCol := si.Columns[0]

	// executeExplainScan dispatches ScanTypeHNSW → hnswIndexScan.
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		scan := Scan{
			Type:         ScanTypeHNSW,
			IndexName:    "idx_v",
			IndexColumns: []Column{vecCol},
			HNSWQueryVec: VectorPointer{Dims: dims, Data: []float32{1, 0, 0}},
			HNSWFuncName: "VEC_L2",
			ScanLimit:    1,
		}
		return table.executeExplainScan(ctx, QueryPlan{}, scan, []Field{{Name: "v"}}, func(Row) error { return nil })
	})
	require.NoError(t, err)
}

// ---- secondary-index dispatch with live HNSW graph ----

// TestTable_Insert_WithHNSWIndex verifies that inserting into a table whose
// HNSW index already has nodes exercises insertSecondaryIndexKey's HNSW
// dispatch branch and the distFn closure inside insertHNSWIndexKey.
func TestTable_Insert_WithHNSWIndex(t *testing.T) {
	ctx := context.Background()
	dims := uint32(3)
	vecs := [][]float32{{1, 0, 0}, {2, 0, 0}}
	table, _, txManager := newTestTableWithVectorRows(t, vecs, dims)

	// Insert a third row; the HNSW index is already wired with nodes 0 and 1,
	// so insertHNSWIndexKey's distFn is called (triggering loadVectorByRowID
	// for the existing nodes).
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, Statement{
			Kind:   Insert,
			Fields: []Field{{Name: "v"}},
			Inserts: [][]OptionalValue{
				{{Valid: true, Value: VectorPointer{Dims: dims, Data: []float32{3, 0, 0}}}},
			},
		})
		return err
	})
	require.NoError(t, err)
}

// TestTable_UpdateSecondaryIndex_HNSW exercises updateSecondaryIndexKey's HNSW
// dispatch branch and updateHNSWIndexKey's entry-point-reassignment loop by
// moving the HNSW graph's current entry point to a new position.
func TestTable_UpdateSecondaryIndex_HNSW(t *testing.T) {
	ctx := context.Background()
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {10, 0}}
	table, si, txManager := newTestTableWithVectorRows(t, vecs, dims)
	vecCol := si.Columns[0]

	// Determine which RowID is the current entry point.
	g, err := si.HNSWIndex.loadGraph(ctx)
	require.NoError(t, err)
	entryPoint := g.EntryPoint

	// Update the entry-point node to a new vector; this exercises:
	//  - updateSecondaryIndexKey → HNSW dispatch (covers that branch)
	//  - updateHNSWIndexKey entry-point loop (remaining node reassigned)
	//  - updateHNSWIndexKey distFn (reads other row via loadVectorByRowID)
	updateRow := Row{
		Columns: []Column{vecCol},
		Values:  []OptionalValue{{Valid: true, Value: VectorPointer{Dims: dims, Data: []float32{50, 0}}}},
		Key:     entryPoint,
	}
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return table.updateSecondaryIndexKey(ctx, si, nil, updateRow, updateRow)
	})
	require.NoError(t, err)
}

// ---- tryHNSWScan planner tests ----

// TestTable_TryHNSWScan_KZero verifies that a LIMIT of 0 is rejected.
func TestTable_TryHNSWScan_KZero(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, _, _ := newTestTableWithVectorRows(t, vecs, dims)

	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(0)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "dist", Expr: &Expr{FuncName: "VEC_L2", Args: []*Expr{{Column: "v"}, {Literal: VectorPointer{Dims: dims, Data: []float32{1, 0}}}}}}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok, "k=0 should not produce an HNSW scan")
}

// TestTable_TryHNSWScan_FullMatch verifies the happy path: a VEC_L2 ORDER BY
// query against a table with a matching HNSW index produces a correct Scan.
func TestTable_TryHNSWScan_FullMatch(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, si, _ := newTestTableWithVectorRows(t, vecs, dims)

	distExpr := &Expr{
		FuncName: "VEC_L2",
		Args: []*Expr{
			{Column: si.Columns[0].Name},
			{Literal: VectorPointer{Dims: dims, Data: []float32{1, 0}}},
		},
	}
	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(3)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Name: "dist", Alias: "dist", Expr: distExpr}},
	}
	scan, ok := table.tryHNSWScan(stmt)
	assert.True(t, ok, "should produce an HNSW scan")
	assert.Equal(t, ScanTypeHNSW, scan.Type)
	assert.Equal(t, int64(3), scan.ScanLimit)
	assert.Equal(t, "idx_v", scan.IndexName)
	assert.Equal(t, "VEC_L2", scan.HNSWFuncName)
}

// TestTable_TryHNSWScan_NoMatchingIndex verifies that when no HNSW index exists
// for the referenced column the planner returns false.
func TestTable_TryHNSWScan_NoMatchingIndex(t *testing.T) {
	dims := uint32(2)
	// Build a table with no secondary indexes at all.
	vecCol := Column{Name: "v", Kind: Vector, Size: dims}
	table, _, _ := newTestTable(t, []Column{vecCol})

	distExpr := &Expr{
		FuncName: "VEC_L2",
		Args: []*Expr{
			{Column: "v"},
			{Literal: VectorPointer{Dims: dims, Data: []float32{1, 0}}},
		},
	}
	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(5)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "dist", Expr: distExpr}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok, "no HNSW index means no scan")
}

// TestTable_TryHNSWScan_NonVecFunc verifies that a non-VEC function in Fields is skipped.
func TestTable_TryHNSWScan_NonVecFunc(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, _, _ := newTestTableWithVectorRows(t, vecs, dims)

	stmt := Statement{
		Kind:  Select,
		Limit: OptionalValue{Valid: true, Value: int64(3)},
		// UPPER is not a VEC function, so the loop continues past it.
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "dist", Expr: &Expr{FuncName: "UPPER", Args: []*Expr{{Column: "v"}}}}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok)
}

// TestTable_TryHNSWScan_EmptyAlias verifies that an unaliased VEC_L2 field whose
// Expr.String() doesn't match the ORDER BY name doesn't produce a scan.
func TestTable_TryHNSWScan_EmptyAlias(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, _, _ := newTestTableWithVectorRows(t, vecs, dims)

	distExpr := &Expr{
		FuncName: "VEC_L2",
		Args:     []*Expr{{Column: "v"}, {Literal: VectorPointer{Dims: dims, Data: []float32{1, 0}}}},
	}
	// No alias: Expr.String() is "VEC_L2(v, ...)" which won't match "dist".
	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(3)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "", Expr: distExpr}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok)
}

// TestTable_TryHNSWScan_NonColumnFirstArg verifies that a VEC_L2 whose first arg
// is a literal (not a column reference) does not produce a scan.
func TestTable_TryHNSWScan_NonColumnFirstArg(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, _, _ := newTestTableWithVectorRows(t, vecs, dims)

	distExpr := &Expr{
		FuncName: "VEC_L2",
		Args: []*Expr{
			{Literal: VectorPointer{Dims: dims, Data: []float32{0, 0}}}, // literal, not a column
			{Literal: VectorPointer{Dims: dims, Data: []float32{1, 0}}},
		},
	}
	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(3)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "dist", Expr: distExpr}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok)
}

// TestTable_TryHNSWScan_SecondArgEvalFails verifies that when the second arg is a
// column reference (not a literal), Eval returns an error and the planner returns false.
func TestTable_TryHNSWScan_SecondArgEvalFails(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, _, _ := newTestTableWithVectorRows(t, vecs, dims)

	distExpr := &Expr{
		FuncName: "VEC_L2",
		Args: []*Expr{
			{Column: "v"},
			{Column: "v"}, // column ref evaluated against empty row → fails
		},
	}
	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(3)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "dist", Expr: distExpr}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok)
}

// TestTable_TryHNSWScan_SecondArgNotVector verifies that when the second arg evaluates
// to a non-vector type, toVectorPointer fails and the planner returns false.
func TestTable_TryHNSWScan_SecondArgNotVector(t *testing.T) {
	dims := uint32(2)
	vecs := [][]float32{{1, 0}, {2, 0}}
	table, _, _ := newTestTableWithVectorRows(t, vecs, dims)

	distExpr := &Expr{
		FuncName: "VEC_L2",
		Args: []*Expr{
			{Column: "v"},
			{Literal: int64(42)}, // int64 literal → toVectorPointer fails
		},
	}
	stmt := Statement{
		Kind:    Select,
		Limit:   OptionalValue{Valid: true, Value: int64(3)},
		OrderBy: []OrderBy{{Field: Field{Name: "dist"}}},
		Fields:  []Field{{Alias: "dist", Expr: distExpr}},
	}
	_, ok := table.tryHNSWScan(stmt)
	assert.False(t, ok)
}

// ---- hnsw_page.go error-path tests ----

func TestHNSWMetaPage_Marshal_TooSmall(t *testing.T) {
	t.Parallel()
	p := &hnswMetaPage{M: 16, EfConstruction: 200}
	err := p.Marshal(make([]byte, 0))
	require.ErrorContains(t, err, "buffer too small")
}

func TestHNSWMetaPage_Unmarshal_TooSmall(t *testing.T) {
	t.Parallel()
	err := new(hnswMetaPage).Unmarshal(make([]byte, 0))
	require.ErrorContains(t, err, "buffer too small")
}

func TestHNSWMetaPage_Unmarshal_WrongType(t *testing.T) {
	t.Parallel()
	buf := make([]byte, hnswMetaHeaderSize)
	buf[0] = 0xFF // wrong page type
	err := new(hnswMetaPage).Unmarshal(buf)
	require.ErrorContains(t, err, "unexpected page type")
}

func TestHNSWDataPage_Marshal_TooSmall(t *testing.T) {
	t.Parallel()
	err := new(hnswDataPage).Marshal(make([]byte, 0))
	require.ErrorContains(t, err, "buffer too small")
}

func TestHNSWDataPage_Marshal_NodeOverflow(t *testing.T) {
	t.Parallel()
	// Node record (9 bytes) does not fit in a header-only buffer (7 bytes).
	p := &hnswDataPage{Nodes: []hnswNodeRecord{{RowID: 1, Neighbors: [][]uint64{{}}}}}
	err := p.Marshal(make([]byte, hnswDataPageHeaderSize))
	require.ErrorContains(t, err, "overflows page buffer")
}

func TestHNSWDataPage_Marshal_NodeNilNeighbors(t *testing.T) {
	t.Parallel()
	// Node with nil Neighbors (len==0 → level = -1 → clamped to 0).
	p := &hnswDataPage{Nodes: []hnswNodeRecord{{RowID: 7, Neighbors: nil}}}
	buf := make([]byte, 4096)
	require.NoError(t, p.Marshal(buf))
	q := new(hnswDataPage)
	require.NoError(t, q.Unmarshal(buf))
	require.Len(t, q.Nodes, 1)
	assert.Equal(t, uint64(7), q.Nodes[0].RowID)
}

func TestHNSWDataPage_Unmarshal_TooSmall(t *testing.T) {
	t.Parallel()
	err := new(hnswDataPage).Unmarshal(make([]byte, 0))
	require.ErrorContains(t, err, "buffer too small")
}

func TestHNSWDataPage_Unmarshal_WrongType(t *testing.T) {
	t.Parallel()
	buf := make([]byte, hnswDataPageHeaderSize)
	buf[0] = 0xFF // wrong page type
	err := new(hnswDataPage).Unmarshal(buf)
	require.ErrorContains(t, err, "unexpected page type")
}

func TestHNSWDataPage_Unmarshal_TruncatedNodeHeader(t *testing.T) {
	t.Parallel()
	// nodeCount=1 but only 1 byte of space after header (need 9 for node).
	buf := make([]byte, hnswDataPageHeaderSize+1)
	buf[0] = PageTypeHNSWData
	binary.BigEndian.PutUint16(buf[5:7], 1) // nodeCount = 1
	err := new(hnswDataPage).Unmarshal(buf)
	require.ErrorContains(t, err, "unexpected end of buffer")
}

func TestHNSWDataPage_Unmarshal_TruncatedLayerCount(t *testing.T) {
	t.Parallel()
	// nodeCount=1, full node header (9 bytes), level=0 → needs 2 bytes for layer count but none provided.
	buf := make([]byte, hnswDataPageHeaderSize+9) // exactly header + node header
	buf[0] = PageTypeHNSWData
	binary.BigEndian.PutUint16(buf[5:7], 1) // nodeCount = 1
	// buf[hnswDataPageHeaderSize+8] = 0 (level = 0, already zero-init)
	err := new(hnswDataPage).Unmarshal(buf)
	require.ErrorContains(t, err, "truncated layer count")
}

func TestHNSWDataPage_Unmarshal_TruncatedNeighborList(t *testing.T) {
	t.Parallel()
	// nodeCount=1, level=0, layer count=1 neighbor, but no 8-byte neighbor data.
	buf := make([]byte, hnswDataPageHeaderSize+9+2) // header + node header + layer count field
	buf[0] = PageTypeHNSWData
	binary.BigEndian.PutUint16(buf[5:7], 1) // nodeCount = 1
	// level = 0 (already zero-init)
	binary.BigEndian.PutUint16(buf[hnswDataPageHeaderSize+9:], 1) // count = 1 neighbor
	err := new(hnswDataPage).Unmarshal(buf)
	require.ErrorContains(t, err, "truncated neighbor list")
}

// ---- hnsw.go search error path ----

func TestHNSWGraph_Search_DistFnError(t *testing.T) {
	t.Parallel()
	g := newHNSWGraph(4, 20)
	vecs := map[RowID][]float32{1: {1, 0}, 2: {2, 0}}
	testBuildGraph(t, g, vecs)

	// distFn always errors → search should propagate the error.
	distFn := func(RowID) (float64, error) { return 0, fmt.Errorf("distFn error") }
	_, err := g.search(1, 10, distFn)
	require.ErrorContains(t, err, "distFn error")
}

// ---- clone test ----

func TestHNSWDataPage_Clone(t *testing.T) {
	t.Parallel()
	original := &hnswDataPage{
		NextPage: 5,
		Nodes: []hnswNodeRecord{
			{RowID: 1, Neighbors: [][]uint64{{2, 3}, {4}}},
		},
	}
	c := original.clone()
	assert.Equal(t, original.NextPage, c.NextPage)
	require.Len(t, c.Nodes, 1)
	assert.Equal(t, original.Nodes[0].RowID, c.Nodes[0].RowID)
	assert.Equal(t, [][]uint64{{2, 3}, {4}}, c.Nodes[0].Neighbors)

	// Verify deep copy: mutating original must not affect the clone.
	original.Nodes[0].Neighbors[0][0] = 99
	assert.Equal(t, uint64(2), c.Nodes[0].Neighbors[0][0], "clone must be independent of original")
}

// ---- integration tests: loadVectorByRowID / makeDistFunc / hnswIndexScan ----

// newTestTableWithVectorRows creates a Table backed by a proper B-tree table pager,
// inserts vecs as rows (RowID 0, 1, 2, …), builds an HNSW index over those same
// RowIDs using the shared pagerImpl's HNSW pager, and wires the index into the
// table's SecondaryIndexes.
func newTestTableWithVectorRows(t *testing.T, vecs [][]float32, dims uint32) (*Table, SecondaryIndex, *TransactionManager) {
	t.Helper()
	ctx := context.Background()
	vecCol := Column{Name: "v", Kind: Vector, Size: dims}

	table, txManager, pagerInst := newTestTable(t, []Column{vecCol})

	// Insert rows one-by-one; RowIDs are assigned 0, 1, 2, … by the B-tree.
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, data := range vecs {
			_, err := table.Insert(ctx, Statement{
				Kind: Insert,
				Inserts: [][]OptionalValue{
					{{Valid: true, Value: VectorPointer{Dims: dims, Data: data}}},
				},
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Build HNSW index with matching RowIDs over the HNSW-dedicated pager.
	hnswPagerInst := pagerInst.ForHNSWIndex()
	hnswTxPager := NewTransactionalPager(hnswPagerInst, txManager, testTableName, "idx_v")

	var buildRows []hnswBuildRow
	for i, data := range vecs {
		buildRows = append(buildRows, hnswBuildRow{
			RowID: RowID(i),
			Vec:   VectorPointer{Dims: dims, Data: data},
		})
	}

	var rootPageIdx PageIndex
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		rootPageIdx, err = BuildHNSWIndex(ctx, hnswTxPager, 4, 20, buildRows)
		return err
	})
	require.NoError(t, err)

	idx := OpenHNSWIndex(hnswTxPager, rootPageIdx, defaultHNSWVecCacheSize)
	si := SecondaryIndex{
		HNSWIndex: idx,
		IndexInfo: IndexInfo{
			Name:    "idx_v",
			Method:  IndexMethodHNSW,
			Columns: []Column{vecCol},
		},
	}
	table.SetSecondaryIndex(si)
	return table, si, txManager
}

func TestTable_LoadVectorByRowID(t *testing.T) {
	ctx := context.Background()
	dims := uint32(3)
	vecs := [][]float32{{1, 0, 0}, {10, 0, 0}}
	table, _, txManager := newTestTableWithVectorRows(t, vecs, dims)

	// Happy path: both rows are retrievable with the correct data.
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for i, expected := range vecs {
			vp, err := table.loadVectorByRowID(ctx, RowID(i), "v")
			if err != nil {
				return err
			}
			require.Len(t, vp.Data, len(expected))
			for j, f := range expected {
				assert.InDelta(t, f, vp.Data[j], 1e-5, "row %d component %d", i, j)
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Error: column name does not exist on the table.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.loadVectorByRowID(ctx, RowID(0), "no_such_col")
		return err
	})
	require.ErrorContains(t, err, "not found on table")

	// Error: row ID does not exist in the B-tree.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.loadVectorByRowID(ctx, RowID(999), "v")
		return err
	})
	require.ErrorContains(t, err, "load row 999")
}

func TestTable_MakeDistFunc(t *testing.T) {
	ctx := context.Background()
	dims := uint32(3)
	vecs := [][]float32{{1, 0, 0}, {0, 1, 0}}
	table, _, txManager := newTestTableWithVectorRows(t, vecs, dims)

	query := VectorPointer{Dims: dims, Data: []float32{1, 0, 0}}

	// VEC_L2: distance from [1,0,0] to itself should be 0; to [0,1,0] sqrt(2).
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := makeDistFunc(ctx, nil, table, "v", query, "VEC_L2")

		d0, err := distFn(RowID(0))
		if err != nil {
			return err
		}
		assert.InDelta(t, 0.0, d0, 1e-6, "L2 distance to self")

		// Second call on the same RowID exercises the cache hit path.
		d0b, err := distFn(RowID(0))
		if err != nil {
			return err
		}
		assert.InDelta(t, d0, d0b, 0, "cached result must match")

		d1, err := distFn(RowID(1))
		if err != nil {
			return err
		}
		assert.InDelta(t, math.Sqrt(2), d1, 1e-5, "L2 distance from [1,0,0] to [0,1,0]")
		return nil
	})
	require.NoError(t, err)

	// VEC_COSINE path.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := makeDistFunc(ctx, nil, table, "v", query, "VEC_COSINE")
		d, err := distFn(RowID(0))
		if err != nil {
			return err
		}
		assert.InDelta(t, 0.0, d, 1e-6, "cosine distance to self")
		return nil
	})
	require.NoError(t, err)

	// Unknown distance function must return an error.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := makeDistFunc(ctx, nil, table, "v", query, "VEC_UNKNOWN")
		_, err := distFn(RowID(0))
		return err
	})
	require.ErrorContains(t, err, "unknown HNSW distance function")

	// loadVectorByRowID failure: bad colName propagates through distFn.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		distFn := makeDistFunc(ctx, nil, table, "no_such_col", query, "VEC_L2")
		_, err := distFn(RowID(0))
		return err
	})
	require.ErrorContains(t, err, "not found on table")

	// Dimension mismatch: query has 2 dims but rows have 3 dims → L2Distance error.
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		mismatchQuery := VectorPointer{Dims: 2, Data: []float32{1, 0}}
		distFn := makeDistFunc(ctx, nil, table, "v", mismatchQuery, "VEC_L2")
		_, err := distFn(RowID(0))
		return err
	})
	require.ErrorContains(t, err, "dimension mismatch")
}

func TestTable_HNSWIndexScan(t *testing.T) {
	ctx := context.Background()
	dims := uint32(3)
	vecs := [][]float32{{1, 0, 0}, {2, 0, 0}, {10, 0, 0}}
	table, si, txManager := newTestTableWithVectorRows(t, vecs, dims)
	vecCol := si.Columns[0]

	// Error path: index name not in SecondaryIndexes.
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		badScan := Scan{
			IndexName:    "no_such_idx",
			IndexColumns: []Column{vecCol},
			HNSWQueryVec: VectorPointer{Dims: dims, Data: []float32{1, 0, 0}},
			HNSWFuncName: "VEC_L2",
			ScanLimit:    1,
		}
		return table.hnswIndexScan(ctx, badScan, []Field{{Name: "v"}}, func(Row) error { return nil })
	})
	require.ErrorContains(t, err, "no HNSW index found")

	// Happy path: nearest to [1,0,0] should be row 0 (distance 0).
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		scan := Scan{
			IndexName:    "idx_v",
			IndexColumns: []Column{vecCol},
			HNSWQueryVec: VectorPointer{Dims: dims, Data: []float32{1, 0, 0}},
			HNSWFuncName: "VEC_L2",
			ScanLimit:    1,
		}
		var rows []Row
		err := table.hnswIndexScan(ctx, scan, []Field{{Name: "v"}}, func(row Row) error {
			rows = append(rows, row)
			return nil
		})
		if err != nil {
			return err
		}
		require.NotEmpty(t, rows)
		assert.Equal(t, RowID(0), rows[0].Key, "nearest to [1,0,0] should be row 0")
		return nil
	})
	require.NoError(t, err)
}

// ---- helpers ----

func testBuildGraph(t *testing.T, g *hnswGraph, vecs map[RowID][]float32) {
	t.Helper()
	for id := RowID(1); int(id) <= len(vecs); id++ {
		v := vecs[id]
		distFn := func(other RowID) (float64, error) {
			ov := vecs[other]
			var sum float64
			for i := range v {
				d := float64(v[i]) - float64(ov[i])
				sum += d * d
			}
			return math.Sqrt(sum), nil
		}
		require.NoError(t, g.insert(id, distFn))
	}
}

// TestHNSW_VecCacheLRU_Cap verifies that the vector cache evicts entries when
// it reaches its configured size limit, bounding memory usage on large tables.
func TestHNSW_VecCacheLRU_Cap(t *testing.T) {
	const cacheSize = 3

	idx := OpenHNSWIndex(nil, 0, cacheSize)

	// Fill the cache exactly to capacity.
	for i := range cacheSize {
		idx.vecCache.Put(RowID(i), VectorPointer{Dims: 2, Data: []float32{float32(i), 0}}, true)
	}

	// All entries should be present.
	for i := range cacheSize {
		_, ok := idx.vecCache.Get(RowID(i))
		assert.True(t, ok, "entry %d should be cached", i)
	}

	// Adding one more entry must evict the LRU entry (the cache is bounded).
	idx.vecCache.Put(RowID(cacheSize), VectorPointer{Dims: 2, Data: []float32{float32(cacheSize), 0}}, true)

	present := 0
	for i := range cacheSize + 1 {
		if _, ok := idx.vecCache.Get(RowID(i)); ok {
			present += 1
		}
	}
	// Exactly cacheSize entries should remain after eviction.
	assert.Equal(t, cacheSize, present, "cache should hold at most %d entries", cacheSize)

	// evictVector removes a specific entry.
	// Repopulate so we have a known entry to evict.
	idx.vecCache.Put(RowID(99), VectorPointer{Dims: 2, Data: []float32{99, 0}}, true)
	_, before := idx.vecCache.Get(RowID(99))
	assert.True(t, before, "entry 99 should be present before eviction")

	idx.evictVector(RowID(99))
	_, after := idx.vecCache.Get(RowID(99))
	assert.False(t, after, "entry 99 should be absent after eviction")
}

func testL2DistFn(vecs map[RowID][]float32, query VectorPointer) func(RowID) (float64, error) {
	return func(id RowID) (float64, error) {
		v := vecs[id]
		var sum float64
		for i := range v {
			d := float64(v[i]) - float64(query.Data[i])
			sum += d * d
		}
		return math.Sqrt(sum), nil
	}
}
