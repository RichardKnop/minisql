package minisql

import (
	"context"
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
		assert.Equal(t, len(vecs), len(g.Nodes))

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
