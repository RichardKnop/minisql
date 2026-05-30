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
//
// nodeToPage / lastDataPage / dirtyNodes support incremental page updates:
// readHNSWGraph populates nodeToPage and lastDataPage; insert marks dirty nodes;
// incrementalInsert rewrites only the dirty pages and appends the new node.
// replaceDataPages (used by Delete) always rebuilds nodeToPage from scratch.
//
// nodeStore is a pre-allocated flat backing array for node data loaded by
// readHNSWGraph.  All Nodes map values for pre-loaded nodes point into it,
// giving contiguous memory layout and better cache locality during traversal.
// Online-inserted nodes are individually heap-allocated and not in nodeStore.
type hnswGraph struct {
	Nodes          map[RowID]*hnswNodeData
	nodeStore      []hnswNodeData      // flat backing store for readHNSWGraph-loaded nodes
	M              int
	EfConstruction int
	EntryPoint     RowID
	EntryLevel     int
	hasEntry       bool    // false when graph is empty
	ml             float64 // = 1 / ln(M), controls level assignment probability

	nodeToPage   map[RowID]PageIndex  // nil = page tracking not active (batch build path)
	lastDataPage PageIndex            // index of the last page in the data-page chain
	dirtyNodes   map[RowID]struct{}   // nodes modified since last serialisation; reset after each DML
}

type hnswNodeData struct {
	Neighbors [][]RowID // Neighbors[l] = neighbor RowIDs at layer l
}

// hnswIndex is the runtime handle for an HNSW vector index.  The graph is
// loaded lazily on the first Search call and cached for subsequent calls.
//
// vecCache stores the raw float32 vectors for each indexed row, keyed by RowID.
// It is populated lazily on first distance-function miss and updated by online
// DML (Insert/Delete operations keep it consistent with the in-memory graph).
// This eliminates overflow-page I/O on the hot search path after the first query.
type hnswIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	graph       *hnswGraph
	mu          sync.RWMutex
	vecCache    map[RowID]VectorPointer
	vecMu       sync.RWMutex
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
// When g.nodeToPage != nil (incremental mode), modified nodes are recorded in g.dirtyNodes.
func (g *hnswGraph) insert(rowID RowID, distFn func(RowID) (float64, error)) error {
	level := g.randomLevel()

	node := &hnswNodeData{Neighbors: make([][]RowID, level+1)}
	g.Nodes[rowID] = node

	if g.nodeToPage != nil {
		if g.dirtyNodes == nil {
			g.dirtyNodes = make(map[RowID]struct{}, 64)
		}
		g.dirtyNodes[rowID] = struct{}{} // new node
	}

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
			// Mark the modified neighbour dirty (its page record needs updating).
			if g.nodeToPage != nil {
				g.dirtyNodes[nb] = struct{}{}
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

// hnswSearchBuf holds pre-allocated scratch structures reused across beamSearch
// calls via hnswSearchBufPool.  Each get/put cycle clears the maps and resets
// the slice lengths so the underlying backing arrays are kept alive.
type hnswSearchBuf struct {
	visited map[RowID]bool
	cands   minHeap
	results maxHeap
}

var hnswSearchBufPool = sync.Pool{
	New: func() any {
		return &hnswSearchBuf{
			visited: make(map[RowID]bool, HNSWDefaultEfConstruction*2),
			cands:   make(minHeap, 0, HNSWDefaultEfConstruction),
			results: make(maxHeap, 0, HNSWDefaultEfConstruction),
		}
	},
}

// hnswPair is used by pruneNeighbors for its stack-allocated sort buffer.
type hnswPair struct {
	rowID RowID
	dist  float64
}

// maxHeap is a max-heap of candidates by distance (furthest element at top).
type maxHeap []hnswCandidate

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push implements heap.Interface.
func (h *maxHeap) Push(x any) { *h = append(*h, x.(hnswCandidate)) }

// Pop implements heap.Interface.
func (h *maxHeap) Pop() any { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// minHeap is a min-heap of candidates by distance (nearest element at top).
type minHeap []hnswCandidate

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push implements heap.Interface.
func (h *minHeap) Push(x any) { *h = append(*h, x.(hnswCandidate)) }

// Pop implements heap.Interface.
func (h *minHeap) Pop() any { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

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
			if g.Nodes[nb] == nil {
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
	buf := hnswSearchBufPool.Get().(*hnswSearchBuf)
	defer hnswSearchBufPool.Put(buf)

	// Reset pooled structures without freeing backing arrays.
	clear(buf.visited)
	buf.cands = buf.cands[:0]
	buf.results = buf.results[:0]

	visited := buf.visited
	visited[ep] = true
	if skipID != ^RowID(0) {
		visited[skipID] = true
	}

	buf.cands = append(buf.cands, hnswCandidate{ep, epDist})
	cands := &buf.cands
	heap.Init(cands)

	buf.results = append(buf.results, hnswCandidate{ep, epDist})
	results := &buf.results
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
			if g.Nodes[nb] == nil {
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

	// Drain the max-heap into a nearest-first slice (caller owns this allocation).
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
// pairsBuf is a stack-allocated scratch buffer; M≤32 in practice so 2*M+4=68
// fits comfortably within the 72-slot array and avoids a heap allocation.
func (g *hnswGraph) pruneNeighbors(self RowID, neighbors []RowID, mMax int, distFn func(RowID) (float64, error)) []RowID {
	var pairsBuf [72]hnswPair
	pairs := pairsBuf[:0]
	for _, nb := range neighbors {
		if nb == self {
			continue
		}
		d, err := distFn(nb)
		if err != nil {
			continue
		}
		if len(pairs) < len(pairsBuf) {
			pairs = append(pairs, hnswPair{nb, d})
		}
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

	entryLevel := max(graph.EntryLevel, 0)
	if graph.M < 0 || graph.M > math.MaxUint16 {
		return 0, fmt.Errorf("HNSW write: M out of uint16 range: %d", graph.M)
	}
	if graph.EfConstruction < 0 || graph.EfConstruction > math.MaxUint32 {
		return 0, fmt.Errorf("HNSW write: ef_construction out of uint32 range: %d", graph.EfConstruction)
	}
	if entryLevel > math.MaxUint8 {
		return 0, fmt.Errorf("HNSW write: entry level out of uint8 range: %d", entryLevel)
	}
	if len(graph.Nodes) > math.MaxUint32 {
		return 0, fmt.Errorf("HNSW write: node count out of uint32 range: %d", len(graph.Nodes))
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
		// Empty graph: initialise tracking structures so incremental insert works.
		g.nodeToPage = make(map[RowID]PageIndex)
		return g, nil
	}

	// Walk the data page chain, populate Nodes, and build the nodeToPage index
	// for incremental page updates during online DML.
	g.nodeToPage = make(map[RowID]PageIndex, int(meta.NodeCount))
	// Pre-allocate flat backing store so all Nodes map values point into contiguous
	// memory — improves cache locality during beamSearch neighbour traversal.
	g.nodeStore = make([]hnswNodeData, 0, int(meta.NodeCount))
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
			node := hnswNodeData{Neighbors: make([][]RowID, len(rec.Neighbors))}
			for l, layer := range rec.Neighbors {
				neighbors := make([]RowID, len(layer))
				for i, nb := range layer {
					neighbors[i] = RowID(nb)
				}
				node.Neighbors[l] = neighbors
			}
			g.nodeStore = append(g.nodeStore, node)
			g.Nodes[RowID(rec.RowID)] = &g.nodeStore[len(g.nodeStore)-1]
			g.nodeToPage[RowID(rec.RowID)] = pageIdx
		}
		if dp.NextPage == 0 {
			g.lastDataPage = pageIdx
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

// ---- online DML maintenance ----

// Insert adds rowID to the HNSW index.  distFn returns the L2 distance from
// rowID's vector to any existing node's vector.  Both the in-memory graph and
// the on-disk pages are updated within the current transaction.
// When nodeToPage tracking is active (normal operation after loadGraph), only
// the dirty pages are rewritten instead of the full O(N) page chain.
func (idx *hnswIndex) Insert(ctx context.Context, rowID RowID, distFn func(RowID) (float64, error)) error {
	g, err := idx.loadGraph(ctx)
	if err != nil {
		return fmt.Errorf("HNSW Insert: load graph: %w", err)
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := g.insert(rowID, distFn); err != nil {
		return fmt.Errorf("HNSW Insert: graph insert rowID %d: %w", rowID, err)
	}
	if g.nodeToPage != nil {
		return idx.incrementalInsert(ctx, g, rowID)
	}
	return idx.replaceDataPages(ctx, g)
}

// Delete removes rowID from the HNSW index.  Dangling neighbor references to
// the removed node are silently skipped at search time because greedyStep and
// beamSearch guard on g.Nodes[rowID] == nil.
func (idx *hnswIndex) Delete(ctx context.Context, rowID RowID) error {
	g, err := idx.loadGraph(ctx)
	if err != nil {
		return fmt.Errorf("HNSW Delete: load graph: %w", err)
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(g.Nodes, rowID)
	if g.hasEntry && g.EntryPoint == rowID {
		g.hasEntry = false
		var bestLevel int
		for id, node := range g.Nodes {
			if level := len(node.Neighbors) - 1; !g.hasEntry || level > bestLevel {
				g.EntryPoint = id
				g.EntryLevel = level
				bestLevel = level
				g.hasEntry = true
			}
		}
	}
	return idx.replaceDataPages(ctx, g)
}

// replaceDataPages frees the existing data page chain for the index and writes
// a fresh one for g, then updates the meta page at idx.rootPageIdx in-place.
// M and EfConstruction are preserved from the existing meta page.
func (idx *hnswIndex) replaceDataPages(ctx context.Context, g *hnswGraph) error {
	// Fetch the meta page in write mode.
	metaPage, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("HNSW replaceDataPages: get meta page: %w", err)
	}
	if metaPage.HNSWMetaPage == nil {
		return fmt.Errorf("HNSW replaceDataPages: page %d is not an HNSW meta page", idx.rootPageIdx)
	}

	// Free the old data page chain.
	pageIdx := PageIndex(metaPage.HNSWMetaPage.FirstDataPage)
	for pageIdx != 0 {
		p, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("HNSW replaceDataPages: read data page %d: %w", pageIdx, err)
		}
		if p.HNSWDataPage == nil {
			return fmt.Errorf("HNSW replaceDataPages: page %d is not an HNSW data page", pageIdx)
		}
		nextIdx := PageIndex(p.HNSWDataPage.NextPage)
		if err := idx.pager.AddFreePage(ctx, pageIdx); err != nil {
			return fmt.Errorf("HNSW replaceDataPages: free data page %d: %w", pageIdx, err)
		}
		pageIdx = nextIdx
	}

	// Build node records from the current graph.
	records := make([]hnswNodeRecord, 0, len(g.Nodes))
	for rowID, node := range g.Nodes {
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

	// Pack records into page-sized groups.
	usableSize := PageSize - pageChecksumSize - hnswDataPageHeaderSize
	var groups [][]hnswNodeRecord
	var grpNodes []hnswNodeRecord
	grpSize := 0
	for _, rec := range records {
		sz := nodeRecordSize(rec)
		if grpSize+sz > usableSize && len(grpNodes) > 0 {
			groups = append(groups, grpNodes)
			grpNodes = nil
			grpSize = 0
		}
		grpNodes = append(grpNodes, rec)
		grpSize += sz
	}
	if len(grpNodes) > 0 {
		groups = append(groups, grpNodes)
	}

	// Allocate new data pages.
	newDataPages := make([]*Page, len(groups))
	for i := range groups {
		p, err := idx.pager.GetFreePage(ctx)
		if err != nil {
			return fmt.Errorf("HNSW replaceDataPages: alloc data page: %w", err)
		}
		newDataPages[i] = p
	}

	// Wire next-page pointers and assign content.
	for i, grp := range groups {
		var nextPage uint32
		if i+1 < len(newDataPages) {
			nextPage = uint32(newDataPages[i+1].Index)
		}
		newDataPages[i].HNSWDataPage = &hnswDataPage{NextPage: nextPage, Nodes: grp}
	}

	// Update the meta page in-place, preserving M and EfConstruction.
	var firstDataPage uint32
	if len(newDataPages) > 0 {
		firstDataPage = uint32(newDataPages[0].Index)
	}
	entryLevel := max(g.EntryLevel, 0)
	entryPoint := hnswNoEntryPoint
	if g.hasEntry {
		entryPoint = uint64(g.EntryPoint)
	}
	metaPage.HNSWMetaPage = &hnswMetaPage{
		M:              metaPage.HNSWMetaPage.M,
		EfConstruction: metaPage.HNSWMetaPage.EfConstruction,
		EntryPoint:     entryPoint,
		EntryLevel:     uint8(entryLevel),
		NodeCount:      uint32(len(g.Nodes)),
		FirstDataPage:  firstDataPage,
	}

	// Rebuild nodeToPage so incremental inserts after this full rewrite are accurate.
	g.nodeToPage = make(map[RowID]PageIndex, len(g.Nodes))
	g.lastDataPage = 0
	for i, p := range newDataPages {
		for _, rec := range p.HNSWDataPage.Nodes {
			g.nodeToPage[RowID(rec.RowID)] = p.Index
		}
		if i == len(newDataPages)-1 {
			g.lastDataPage = p.Index
		}
	}
	g.dirtyNodes = nil
	return nil
}

// buildNodeRecord serialises the current in-memory state of a graph node into
// an hnswNodeRecord ready for page writes.
func buildNodeRecord(rowID RowID, node *hnswNodeData) hnswNodeRecord {
	rec := hnswNodeRecord{RowID: uint64(rowID)}
	for _, layerNeighbors := range node.Neighbors {
		uNeighbors := make([]uint64, len(layerNeighbors))
		for i, nb := range layerNeighbors {
			uNeighbors[i] = uint64(nb)
		}
		rec.Neighbors = append(rec.Neighbors, uNeighbors)
	}
	return rec
}

// incrementalInsert updates only the pages that changed as a result of inserting
// newRowID into the graph.  It rewrites each page that contains a dirty (modified)
// existing node and appends the new node's record to the last data page (or a new
// one if the last page is full).  Falls back to replaceDataPages on overflow.
func (idx *hnswIndex) incrementalInsert(ctx context.Context, g *hnswGraph, newRowID RowID) error {
	usableSize := PageSize - pageChecksumSize - hnswDataPageHeaderSize

	// 1. Collect pages that contain dirty existing nodes (excluding the brand-new node).
	dirtyPageSet := make(map[PageIndex]bool, len(g.dirtyNodes))
	for rowID := range g.dirtyNodes {
		if rowID == newRowID {
			continue // new node — handled in step 2
		}
		if pageIdx, ok := g.nodeToPage[rowID]; ok {
			dirtyPageSet[pageIdx] = true
		}
	}

	// 2. Rewrite each dirty page in-place.
	for pageIdx := range dirtyPageSet {
		p, err := idx.pager.ModifyPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("HNSW incrementalInsert: modify page %d: %w", pageIdx, err)
		}
		newRecs := make([]hnswNodeRecord, 0, len(p.HNSWDataPage.Nodes))
		totalSz := 0
		for _, oldRec := range p.HNSWDataPage.Nodes {
			rowID := RowID(oldRec.RowID)
			node := g.Nodes[rowID]
			if node == nil {
				continue // node was deleted between writes — omit
			}
			rec := buildNodeRecord(rowID, node)
			totalSz += nodeRecordSize(rec)
			if totalSz > usableSize {
				// Records no longer fit after update — fall back to full rewrite.
				g.nodeToPage = nil
				return idx.replaceDataPages(ctx, g)
			}
			newRecs = append(newRecs, rec)
		}
		p.HNSWDataPage.Nodes = newRecs
	}

	// 3. Append the new node to the last data page or a new page.
	if newNode := g.Nodes[newRowID]; newNode != nil {
		newRec := buildNodeRecord(newRowID, newNode)
		newRecSz := nodeRecordSize(newRec)

		appended := false
		if g.lastDataPage != 0 {
			lastP, err := idx.pager.ModifyPage(ctx, g.lastDataPage)
			if err != nil {
				return fmt.Errorf("HNSW incrementalInsert: modify last page %d: %w", g.lastDataPage, err)
			}
			existingSz := 0
			for _, rec := range lastP.HNSWDataPage.Nodes {
				existingSz += nodeRecordSize(rec)
			}
			if existingSz+newRecSz <= usableSize {
				lastP.HNSWDataPage.Nodes = append(lastP.HNSWDataPage.Nodes, newRec)
				g.nodeToPage[newRowID] = g.lastDataPage
				appended = true
			}
		}

		if !appended {
			// Allocate a new data page for the new node.
			newDataPage, err := idx.pager.GetFreePage(ctx)
			if err != nil {
				return fmt.Errorf("HNSW incrementalInsert: alloc data page: %w", err)
			}
			newDataPage.HNSWDataPage = &hnswDataPage{Nodes: []hnswNodeRecord{newRec}}
			g.nodeToPage[newRowID] = newDataPage.Index

			if g.lastDataPage != 0 {
				// Chain new page after the current last page.
				prevLast, err := idx.pager.ModifyPage(ctx, g.lastDataPage)
				if err != nil {
					return fmt.Errorf("HNSW incrementalInsert: modify prev-last page %d: %w", g.lastDataPage, err)
				}
				prevLast.HNSWDataPage.NextPage = uint32(newDataPage.Index)
			} else {
				// No data pages existed yet — update meta's FirstDataPage.
				metaP, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
				if err != nil {
					return fmt.Errorf("HNSW incrementalInsert: modify meta page: %w", err)
				}
				metaP.HNSWMetaPage.FirstDataPage = uint32(newDataPage.Index)
			}
			g.lastDataPage = newDataPage.Index
		}
	}

	// 4. Update the meta page (entry point + node count may have changed).
	metaPage, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("HNSW incrementalInsert: modify meta page: %w", err)
	}
	entryPoint := uint64(hnswNoEntryPoint)
	if g.hasEntry {
		entryPoint = uint64(g.EntryPoint)
	}
	metaPage.HNSWMetaPage = &hnswMetaPage{
		M:              metaPage.HNSWMetaPage.M,
		EfConstruction: metaPage.HNSWMetaPage.EfConstruction,
		EntryPoint:     entryPoint,
		EntryLevel:     uint8(max(g.EntryLevel, 0)),
		NodeCount:      uint32(len(g.Nodes)),
		FirstDataPage:  metaPage.HNSWMetaPage.FirstDataPage,
	}

	g.dirtyNodes = nil
	return nil
}

// ---- factory functions ----

// OpenHNSWIndex returns a handle for an existing HNSW index whose meta page is
// at rootPageIdx.  The graph is loaded lazily on first Search.
func OpenHNSWIndex(pager TxPager, rootPageIdx PageIndex) *hnswIndex {
	return &hnswIndex{pager: pager, rootPageIdx: rootPageIdx}
}

// freeHNSWIndexPages releases every page belonging to an HNSW index — the meta
// page and the entire data-page chain — back to the free list.  Called by
// DROP INDEX to reclaim space without leaving orphan pages.
func freeHNSWIndexPages(ctx context.Context, pager TxPager, rootPageIdx PageIndex) error {
	metaPage, err := pager.ReadPage(ctx, rootPageIdx)
	if err != nil {
		return fmt.Errorf("HNSW drop: read meta page %d: %w", rootPageIdx, err)
	}
	if metaPage.HNSWMetaPage == nil {
		return fmt.Errorf("HNSW drop: page %d is not an HNSW meta page", rootPageIdx)
	}

	// Walk and free the data page chain first so the meta page can be freed last.
	dataPageIdx := PageIndex(metaPage.HNSWMetaPage.FirstDataPage)
	for dataPageIdx != 0 {
		dp, err := pager.ReadPage(ctx, dataPageIdx)
		if err != nil {
			return fmt.Errorf("HNSW drop: read data page %d: %w", dataPageIdx, err)
		}
		if dp.HNSWDataPage == nil {
			return fmt.Errorf("HNSW drop: page %d is not an HNSW data page", dataPageIdx)
		}
		nextIdx := PageIndex(dp.HNSWDataPage.NextPage)
		if err := pager.AddFreePage(ctx, dataPageIdx); err != nil {
			return fmt.Errorf("HNSW drop: free data page %d: %w", dataPageIdx, err)
		}
		dataPageIdx = nextIdx
	}

	if err := pager.AddFreePage(ctx, rootPageIdx); err != nil {
		return fmt.Errorf("HNSW drop: free meta page %d: %w", rootPageIdx, err)
	}
	return nil
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

// cachedVector returns the VectorPointer (with Data populated) for rowID,
// loading it from the table exactly once and caching the result for reuse
// across queries.  Online DML (insertHNSWIndexKey, deleteHNSWIndexKey) keeps
// the cache consistent with the in-memory graph.
func (idx *hnswIndex) cachedVector(ctx context.Context, table *Table, rowID RowID, colName string) (VectorPointer, error) {
	idx.vecMu.RLock()
	if vp, ok := idx.vecCache[rowID]; ok {
		idx.vecMu.RUnlock()
		return vp, nil
	}
	idx.vecMu.RUnlock()

	vp, err := table.loadVectorByRowID(ctx, rowID, colName)
	if err != nil {
		return VectorPointer{}, err
	}

	idx.vecMu.Lock()
	if idx.vecCache == nil {
		idx.vecCache = make(map[RowID]VectorPointer, 64)
	}
	idx.vecCache[rowID] = vp
	idx.vecMu.Unlock()
	return vp, nil
}

// evictVector removes a RowID from the vector cache.  Called by Delete to keep
// the cache consistent with the in-memory graph.
func (idx *hnswIndex) evictVector(rowID RowID) {
	idx.vecMu.Lock()
	delete(idx.vecCache, rowID)
	idx.vecMu.Unlock()
}

// makeDistFunc builds a closure that returns the distance from each node's
// vector to the query vector.  When idx is non-nil, vectors are fetched via
// idx.cachedVector so each node is read from overflow pages at most once per
// index lifetime.  When idx is nil (unit-test contexts), the function falls
// back to table.loadVectorByRowID on every call.
// A per-call map[RowID]float64 deduplicates re-visits within a single search.
func makeDistFunc(ctx context.Context, idx *hnswIndex, table *Table, colName string, query VectorPointer, funcName string) func(RowID) (float64, error) {
	cache := make(map[RowID]float64)
	return func(rowID RowID) (float64, error) {
		if d, ok := cache[rowID]; ok {
			return d, nil
		}
		var (
			vp  VectorPointer
			err error
		)
		if idx != nil {
			vp, err = idx.cachedVector(ctx, table, rowID, colName)
		} else {
			vp, err = table.loadVectorByRowID(ctx, rowID, colName)
		}
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
