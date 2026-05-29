package minisql

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
)

const (
	// HNSWDefaultM is the default maximum number of bidirectional links per node
	// per layer.  Layer 0 allows up to 2*M links.
	HNSWDefaultM = 16
	// HNSWDefaultEfConstruction is the default beam width during graph construction.
	// Must be >= M.
	HNSWDefaultEfConstruction = 200
	// HNSWDefaultEfSearch is the default beam width during a search query.
	HNSWDefaultEfSearch = 50
)

// hnswGraph is the in-memory representation of a fully-built HNSW graph.
// It is populated during CREATE INDEX (batch build) and then serialised to pages.
// On subsequent database opens it is lazily reconstructed from those pages.
type hnswGraph struct {
	Nodes          map[RowID]*hnswNodeData
	M              int
	EfConstruction int
	EntryPoint     RowID
	EntryLevel     int
	hasEntry       bool    // false when graph is empty
	ml             float64 // = 1 / ln(M), controls level assignment probability
}

type hnswNodeData struct {
	Neighbors [][]RowID // Neighbors[l] = neighbor RowIDs at layer l
}

// hnswIndex is the runtime handle for an HNSW vector index.  The graph is
// loaded lazily on the first Search call and cached for subsequent calls.
type hnswIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	graph       *hnswGraph
	mu          sync.RWMutex
}

// GetRootPageIdx returns the index of the HNSW metadata page in the database file.
func (idx *hnswIndex) GetRootPageIdx() PageIndex { return idx.rootPageIdx }

// newHNSWGraph constructs an empty HNSW graph with the given parameters.
func newHNSWGraph(m, efConstruction int) *hnswGraph {
	if m <= 0 {
		m = HNSWDefaultM
	}
	if efConstruction < m {
		efConstruction = m
	}
	return &hnswGraph{
		Nodes:          make(map[RowID]*hnswNodeData),
		M:              m,
		EfConstruction: efConstruction,
		ml:             1.0 / math.Log(float64(m)),
	}
}

// ---- HNSW graph build ----

// insert adds a new node with the given rowID to the HNSW graph.
// distFn returns the distance from the new node's vector to any other node's vector.
func (g *hnswGraph) insert(rowID RowID, distFn func(RowID) (float64, error)) error {
	level := g.randomLevel()

	node := &hnswNodeData{Neighbors: make([][]RowID, level+1)}
	g.Nodes[rowID] = node

	if !g.hasEntry {
		g.EntryPoint = rowID
		g.EntryLevel = level
		g.hasEntry = true
		return nil
	}

	ep := g.EntryPoint
	epDist, err := distFn(ep)
	if err != nil {
		return err
	}

	// Greedy descent from the current top layer down to level+1.
	for l := g.EntryLevel; l > level; l-- {
		ep, epDist, err = g.greedyStep(ep, epDist, rowID, l, distFn)
		if err != nil {
			return err
		}
	}

	// Beam search and wiring from min(level, entryLevel) down to 0.
	for l := min(level, g.EntryLevel); l >= 0; l-- {
		mMax := g.M
		if l == 0 {
			mMax = 2 * g.M
		}
		candidates, err := g.beamSearch(ep, epDist, rowID, l, g.EfConstruction, distFn)
		if err != nil {
			return err
		}
		neighbors := g.selectNeighbors(candidates, mMax)
		node.Neighbors[l] = neighbors

		// Bidirectional connections — update each chosen neighbour's list.
		for _, nb := range neighbors {
			nbNode := g.Nodes[nb]
			if nbNode == nil || l >= len(nbNode.Neighbors) {
				continue
			}
			nbNode.Neighbors[l] = append(nbNode.Neighbors[l], rowID)
			if len(nbNode.Neighbors[l]) > mMax {
				// Build a dist-fn relative to nb for pruning.
				nbDistFn := func(other RowID) (float64, error) { return distFn(other) }
				nbNode.Neighbors[l] = g.pruneNeighbors(nb, nbNode.Neighbors[l], mMax, nbDistFn)
			}
		}

		// Advance entry point to the closest candidate for the next layer.
		if len(candidates) > 0 {
			ep = candidates[0].rowID
			epDist = candidates[0].dist
		}
	}

	if level > g.EntryLevel {
		g.EntryPoint = rowID
		g.EntryLevel = level
	}
	return nil
}

// ---- HNSW search ----

// search returns up to k row IDs whose stored vectors are nearest to the query,
// ordered nearest-first.  distFn returns the distance from any node's vector to
// the query vector.
func (g *hnswGraph) search(k, efSearch int, distFn func(RowID) (float64, error)) ([]RowID, error) {
	if !g.hasEntry || len(g.Nodes) == 0 {
		return nil, nil
	}
	if efSearch < k {
		efSearch = k
	}

	ep := g.EntryPoint
	epDist, err := distFn(ep)
	if err != nil {
		return nil, err
	}

	// Greedy descent from the top layer to layer 1.
	for l := g.EntryLevel; l >= 1; l-- {
		ep, epDist, err = g.greedyStep(ep, epDist, ^RowID(0), l, distFn)
		if err != nil {
			return nil, err
		}
	}

	// Beam search at layer 0.
	candidates, err := g.beamSearch(ep, epDist, ^RowID(0), 0, efSearch, distFn)
	if err != nil {
		return nil, err
	}

	n := min(k, len(candidates))
	result := make([]RowID, n)
	for i := range n {
		result[i] = candidates[i].rowID
	}
	return result, nil
}

// ---- internal helpers ----

type hnswCandidate struct {
	rowID RowID
	dist  float64
}

// maxHeap is a max-heap of candidates by distance (furthest element at top).
type maxHeap []hnswCandidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)         { *h = append(*h, x.(hnswCandidate)) }
func (h *maxHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// minHeap is a min-heap of candidates by distance (nearest element at top).
type minHeap []hnswCandidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)         { *h = append(*h, x.(hnswCandidate)) }
func (h *minHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// greedyStep performs a single-step greedy 1-NN descent at a given layer.
// skipID is a RowID to ignore (the node being inserted, or ^RowID(0) for pure search).
func (g *hnswGraph) greedyStep(ep RowID, epDist float64, skipID RowID, layer int, distFn func(RowID) (float64, error)) (RowID, float64, error) {
	for {
		node := g.Nodes[ep]
		if node == nil || layer >= len(node.Neighbors) {
			break
		}
		improved := false
		for _, nb := range node.Neighbors[layer] {
			if nb == skipID {
				continue
			}
			d, err := distFn(nb)
			if err != nil {
				return ep, epDist, err
			}
			if d < epDist {
				ep = nb
				epDist = d
				improved = true
			}
		}
		if !improved {
			break
		}
	}
	return ep, epDist, nil
}

// beamSearch performs an ef-wide beam search at a given layer.
// skipID is excluded from expansion (the inserting node or ^RowID(0) for search).
// Returns candidates sorted nearest-first.
func (g *hnswGraph) beamSearch(ep RowID, epDist float64, skipID RowID, layer, ef int, distFn func(RowID) (float64, error)) ([]hnswCandidate, error) {
	visited := make(map[RowID]bool, ef*2)
	visited[ep] = true
	if skipID != ^RowID(0) {
		visited[skipID] = true
	}

	cands := &minHeap{{ep, epDist}}
	heap.Init(cands)
	results := &maxHeap{{ep, epDist}}
	heap.Init(results)

	for cands.Len() > 0 {
		c := heap.Pop(cands).(hnswCandidate)
		if results.Len() >= ef && c.dist > (*results)[0].dist {
			break
		}
		node := g.Nodes[c.rowID]
		if node == nil || layer >= len(node.Neighbors) {
			continue
		}
		for _, nb := range node.Neighbors[layer] {
			if visited[nb] {
				continue
			}
			visited[nb] = true
			d, err := distFn(nb)
			if err != nil {
				return nil, err
			}
			if results.Len() < ef || d < (*results)[0].dist {
				heap.Push(cands, hnswCandidate{nb, d})
				heap.Push(results, hnswCandidate{nb, d})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	// Drain the max-heap into a nearest-first slice.
	out := make([]hnswCandidate, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(hnswCandidate)
	}
	return out, nil
}

// selectNeighbors chooses up to mMax neighbors from candidates (already sorted nearest-first).
func (g *hnswGraph) selectNeighbors(candidates []hnswCandidate, mMax int) []RowID {
	n := min(mMax, len(candidates))
	result := make([]RowID, n)
	for i := range n {
		result[i] = candidates[i].rowID
	}
	return result
}

// pruneNeighbors trims a neighbor list to mMax, keeping the mMax closest neighbors.
func (g *hnswGraph) pruneNeighbors(self RowID, neighbors []RowID, mMax int, distFn func(RowID) (float64, error)) []RowID {
	type pair struct {
		rowID RowID
		dist  float64
	}
	pairs := make([]pair, 0, len(neighbors))
	for _, nb := range neighbors {
		if nb == self {
			continue
		}
		d, err := distFn(nb)
		if err != nil {
			continue
		}
		pairs = append(pairs, pair{nb, d})
	}
	// Insertion sort (neighbor lists are small).
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0 && pairs[j].dist < pairs[j-1].dist; j-- {
			pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
		}
	}
	n := min(mMax, len(pairs))
	result := make([]RowID, n)
	for i := range n {
		result[i] = pairs[i].rowID
	}
	return result
}

// randomLevel assigns a random layer to a new node using the HNSW level
// generation distribution: P(l > L) = (1 - 1/M)^(L+1).
func (g *hnswGraph) randomLevel() int {
	level := 0
	for rand.Float64() < (1.0-1.0/float64(g.M)) && level < 16 {
		level++
	}
	return level
}

// ---- serialisation ----

// writeHNSWGraph serialises the graph to pages from pager and returns the
// PageIndex of the meta page (stored as the index RootPage in the schema table).
func writeHNSWGraph(ctx context.Context, pager TxPager, graph *hnswGraph) (PageIndex, error) {
	// Allocate the meta page first so its index is known before writing data pages.
	metaPage, err := pager.GetFreePage(ctx)
	if err != nil {
		return 0, fmt.Errorf("HNSW write: get meta page: %w", err)
	}
	metaPageIdx := metaPage.Index

	if !graph.hasEntry {
		metaPage.HNSWMetaPage = &hnswMetaPage{
			M:              uint16(graph.M),
			EfConstruction: uint32(graph.EfConstruction),
			EntryPoint:     hnswNoEntryPoint,
		}
		return metaPageIdx, nil
	}

	// Build node records from the graph.
	records := make([]hnswNodeRecord, 0, len(graph.Nodes))
	for rowID, node := range graph.Nodes {
		rec := hnswNodeRecord{RowID: uint64(rowID)}
		for _, layerNeighbors := range node.Neighbors {
			uNeighbors := make([]uint64, len(layerNeighbors))
			for i, nb := range layerNeighbors {
				uNeighbors[i] = uint64(nb)
			}
			rec.Neighbors = append(rec.Neighbors, uNeighbors)
		}
		records = append(records, rec)
	}

	// Pack records into page groups that fit within one page's usable area.
	usableSize := PageSize - pageChecksumSize - hnswDataPageHeaderSize
	type pageGroup struct{ nodes []hnswNodeRecord }
	var groups []pageGroup
	var grpNodes []hnswNodeRecord
	grpSize := 0
	for _, rec := range records {
		sz := nodeRecordSize(rec)
		if grpSize+sz > usableSize && len(grpNodes) > 0 {
			groups = append(groups, pageGroup{grpNodes})
			grpNodes = nil
			grpSize = 0
		}
		grpNodes = append(grpNodes, rec)
		grpSize += sz
	}
	if len(grpNodes) > 0 {
		groups = append(groups, pageGroup{grpNodes})
	}

	// Allocate a page for each group.
	allocatedPages := make([]*Page, len(groups))
	for i := range groups {
		p, err := pager.GetFreePage(ctx)
		if err != nil {
			return 0, fmt.Errorf("HNSW write: get data page: %w", err)
		}
		allocatedPages[i] = p
	}

	// Wire next-page pointers and set page content.
	for i, grp := range groups {
		var nextPage uint32
		if i+1 < len(allocatedPages) {
			nextPage = uint32(allocatedPages[i+1].Index)
		}
		allocatedPages[i].HNSWDataPage = &hnswDataPage{NextPage: nextPage, Nodes: grp.nodes}
	}

	var firstDataPage uint32
	if len(allocatedPages) > 0 {
		firstDataPage = uint32(allocatedPages[0].Index)
	}

	entryLevel := graph.EntryLevel
	if entryLevel < 0 {
		entryLevel = 0
	}
	metaPage.HNSWMetaPage = &hnswMetaPage{
		M:              uint16(graph.M),
		EfConstruction: uint32(graph.EfConstruction),
		EntryPoint:     uint64(graph.EntryPoint),
		EntryLevel:     uint8(entryLevel),
		NodeCount:      uint32(len(graph.Nodes)),
		FirstDataPage:  firstDataPage,
	}
	return metaPageIdx, nil
}

// readHNSWGraph reconstructs an hnswGraph from the page chain starting at rootPageIdx.
func readHNSWGraph(ctx context.Context, pager TxPager, rootPageIdx PageIndex) (*hnswGraph, error) {
	metaP, err := pager.ReadPage(ctx, rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("HNSW read: meta page %d: %w", rootPageIdx, err)
	}
	if metaP.HNSWMetaPage == nil {
		return nil, fmt.Errorf("HNSW read: page %d is not an HNSW meta page", rootPageIdx)
	}
	meta := metaP.HNSWMetaPage

	g := newHNSWGraph(int(meta.M), int(meta.EfConstruction))
	if meta.EntryPoint != hnswNoEntryPoint {
		g.EntryPoint = RowID(meta.EntryPoint)
		g.EntryLevel = int(meta.EntryLevel)
		g.hasEntry = true
	}

	if meta.NodeCount == 0 || meta.FirstDataPage == 0 {
		return g, nil
	}

	// Walk the data page chain and populate Nodes.
	pageIdx := PageIndex(meta.FirstDataPage)
	for pageIdx != 0 {
		p, err := pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return nil, fmt.Errorf("HNSW read: data page %d: %w", pageIdx, err)
		}
		if p.HNSWDataPage == nil {
			return nil, fmt.Errorf("HNSW read: page %d is not an HNSW data page", pageIdx)
		}
		dp := p.HNSWDataPage
		for _, rec := range dp.Nodes {
			node := &hnswNodeData{Neighbors: make([][]RowID, len(rec.Neighbors))}
			for l, layer := range rec.Neighbors {
				neighbors := make([]RowID, len(layer))
				for i, nb := range layer {
					neighbors[i] = RowID(nb)
				}
				node.Neighbors[l] = neighbors
			}
			g.Nodes[RowID(rec.RowID)] = node
		}
		pageIdx = PageIndex(dp.NextPage)
	}
	return g, nil
}

// loadGraph ensures the in-memory graph is populated, reading from pages on first access.
func (idx *hnswIndex) loadGraph(ctx context.Context) (*hnswGraph, error) {
	idx.mu.RLock()
	g := idx.graph
	idx.mu.RUnlock()
	if g != nil {
		return g, nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.graph != nil {
		return idx.graph, nil
	}
	g, err := readHNSWGraph(ctx, idx.pager, idx.rootPageIdx)
	if err != nil {
		return nil, err
	}
	idx.graph = g
	return g, nil
}

// Search returns up to k row IDs whose stored vectors are nearest to the query.
// distFn returns the distance from any node (identified by RowID) to the query.
func (idx *hnswIndex) Search(ctx context.Context, k, efSearch int, distFn func(RowID) (float64, error)) ([]RowID, error) {
	g, err := idx.loadGraph(ctx)
	if err != nil {
		return nil, err
	}
	return g.search(k, efSearch, distFn)
}

// ---- factory functions ----

// OpenHNSWIndex returns a handle for an existing HNSW index whose meta page is
// at rootPageIdx.  The graph is loaded lazily on first Search.
func OpenHNSWIndex(pager TxPager, rootPageIdx PageIndex) *hnswIndex {
	return &hnswIndex{pager: pager, rootPageIdx: rootPageIdx}
}

// BuildHNSWIndex builds a new HNSW graph from the supplied row set, writes it
// to pages, and returns the meta page index (used as the schema RootPage).
//
// rows contains (RowID, VectorPointer) pairs.  The graph always uses L2
// distance during construction; queries may use any supported distance function.
func BuildHNSWIndex(ctx context.Context, pager TxPager, m, efConstruction int, rows []hnswBuildRow) (PageIndex, error) {
	graph := newHNSWGraph(m, efConstruction)

	// Cache all vectors to avoid repeated page reads during graph construction.
	vecCache := make(map[RowID]VectorPointer, len(rows))
	for _, r := range rows {
		vecCache[r.RowID] = r.Vec
	}

	for _, r := range rows {
		currentVec := r.Vec
		distFn := func(otherID RowID) (float64, error) {
			otherVec, ok := vecCache[otherID]
			if !ok {
				return 0, fmt.Errorf("HNSW build: vector not cached for rowID %d", otherID)
			}
			return L2Distance(currentVec, otherVec)
		}
		if err := graph.insert(r.RowID, distFn); err != nil {
			return 0, fmt.Errorf("HNSW build: insert rowID %d: %w", r.RowID, err)
		}
	}

	return writeHNSWGraph(ctx, pager, graph)
}

// hnswBuildRow is a (RowID, vector) pair used when building an HNSW index.
type hnswBuildRow struct {
	RowID RowID
	Vec   VectorPointer
}

// ---- distance helper ----

// makeDistFunc builds a closure that loads and caches the vector for each RowID
// from the main table and returns its distance to the query vector under the
// named function ("VEC_L2" or "VEC_COSINE").
func makeDistFunc(ctx context.Context, table *Table, colName string, query VectorPointer, funcName string) func(RowID) (float64, error) {
	cache := make(map[RowID]float64)
	return func(rowID RowID) (float64, error) {
		if d, ok := cache[rowID]; ok {
			return d, nil
		}
		vp, err := table.loadVectorByRowID(ctx, rowID, colName)
		if err != nil {
			return 0, err
		}
		var d float64
		switch funcName {
		case "VEC_L2":
			d, err = L2Distance(query, vp)
		case "VEC_COSINE":
			d, err = CosineDistance(query, vp)
		default:
			return 0, fmt.Errorf("unknown HNSW distance function: %s", funcName)
		}
		if err != nil {
			return 0, err
		}
		cache[rowID] = d
		return d, nil
	}
}

// loadVectorByRowID fetches the VectorPointer stored in colName for the row at rowID.
func (t *Table) loadVectorByRowID(ctx context.Context, rowID RowID, colName string) (VectorPointer, error) {
	colIdx := -1
	for i, c := range t.Columns {
		if c.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return VectorPointer{}, fmt.Errorf("HNSW: column %q not found on table %s", colName, t.Name)
	}

	selectedMask := make([]bool, len(t.Columns))
	selectedMask[colIdx] = true

	row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, 1, nil)
	if err != nil {
		return VectorPointer{}, fmt.Errorf("HNSW: load row %d: %w", rowID, err)
	}
	if !ok {
		return VectorPointer{}, fmt.Errorf("HNSW: row %d not found", rowID)
	}

	val, ok2 := row.GetValue(colName)
	if !ok2 || !val.Valid {
		return VectorPointer{}, fmt.Errorf("HNSW: column %q value missing for row %d", colName, rowID)
	}
	vp, ok3 := val.Value.(VectorPointer)
	if !ok3 {
		return VectorPointer{}, fmt.Errorf("HNSW: expected VectorPointer for column %q, got %T", colName, val.Value)
	}
	return vp, nil
}
