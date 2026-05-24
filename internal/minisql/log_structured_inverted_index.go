package minisql

import (
	"context"
	"fmt"
	"sort"
)

const logStructuredInvertedIndexCompactSegmentThreshold = 96
const logStructuredInvertedIndexMetaCompactBytes = PageSize * 3 / 4
const logStructuredInvertedIndexMergeRunSize = 16

type logStructuredInvertedIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	base        *dedicatedInvertedIndex
	name        string
}

var _ invertedIndex = (*logStructuredInvertedIndex)(nil)
var _ invertedBatchApplier = (*logStructuredInvertedIndex)(nil)

func OpenInvertedIndex(ctx context.Context, name string, mode invertedIndexPostingMode, pager TxPager, rootPageIdx PageIndex) (invertedIndex, error) {
	page, err := pager.ReadPage(ctx, rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted index root: %w", err)
	}
	if page.InvertedMetaPage != nil {
		return newLogStructuredInvertedIndexFromMeta(name, mode, pager, rootPageIdx, page.InvertedMetaPage)
	}
	if page.InvertedEntryPage != nil {
		return NewDedicatedInvertedIndex(name, mode, pager, rootPageIdx)
	}
	return nil, fmt.Errorf("inverted index %s root page %d is neither metadata nor entry page", name, rootPageIdx)
}

func NewLogStructuredInvertedIndex(
	ctx context.Context,
	name string,
	mode invertedIndexPostingMode,
	pager TxPager,
	rootPageIdx, baseRootPageIdx PageIndex,
) (*logStructuredInvertedIndex, error) {
	if mode != invertedIndexPostingModeRowIDs && mode != invertedIndexPostingModePositions {
		return nil, fmt.Errorf("unknown inverted index posting mode %d", mode)
	}

	metaPage, err := pager.ModifyPage(ctx, rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("modify inverted metadata root: %w", err)
	}
	metaPage.Clear()
	metaPage.InvertedMetaPage = NewInvertedMetaPage(mode, baseRootPageIdx)

	base, err := NewDedicatedInvertedIndex(name, mode, pager, baseRootPageIdx)
	if err != nil {
		return nil, err
	}
	if err := base.InitRootPage(ctx); err != nil {
		return nil, err
	}
	return &logStructuredInvertedIndex{
		name:        name,
		pager:       pager,
		rootPageIdx: rootPageIdx,
		base:        base,
	}, nil
}

func newLogStructuredInvertedIndexFromMeta(
	name string,
	mode invertedIndexPostingMode,
	pager TxPager,
	rootPageIdx PageIndex,
	meta *invertedMetaPage,
) (*logStructuredInvertedIndex, error) {
	if meta.Mode != mode {
		return nil, fmt.Errorf("inverted index %s metadata uses posting mode %d, expected %d", name, meta.Mode, mode)
	}
	base, err := NewDedicatedInvertedIndex(name, mode, pager, meta.BaseRoot)
	if err != nil {
		return nil, err
	}
	return &logStructuredInvertedIndex{
		name:        name,
		pager:       pager,
		rootPageIdx: rootPageIdx,
		base:        base,
	}, nil
}

func (idx *logStructuredInvertedIndex) GetRootPageIdx() PageIndex {
	return idx.rootPageIdx
}

func (idx *logStructuredInvertedIndex) Mode() invertedIndexPostingMode {
	return idx.base.Mode()
}

func (idx *logStructuredInvertedIndex) Insert(ctx context.Context, term string, posting invertedPosting) error {
	batch := newInvertedIndexMutationBatch(idx.Mode())
	batch.Insert(term, posting)
	return idx.ApplyBatch(ctx, batch)
}

func (idx *logStructuredInvertedIndex) InsertMany(ctx context.Context, term string, postings []invertedPosting) error {
	if len(postings) == 0 {
		return nil
	}
	batch := newInvertedIndexMutationBatch(idx.Mode())
	for _, posting := range postings {
		batch.Insert(term, posting)
	}
	return idx.ApplyBatch(ctx, batch)
}

func (idx *logStructuredInvertedIndex) Replace(ctx context.Context, term string, oldPosting, newPosting invertedPosting) error {
	batch := newInvertedIndexMutationBatch(idx.Mode())
	batch.Delete(term, oldPosting)
	batch.Insert(term, newPosting)
	return idx.ApplyBatch(ctx, batch)
}

func (idx *logStructuredInvertedIndex) Delete(ctx context.Context, term string, posting invertedPosting) error {
	batch := newInvertedIndexMutationBatch(idx.Mode())
	batch.Delete(term, posting)
	return idx.ApplyBatch(ctx, batch)
}

func (idx *logStructuredInvertedIndex) ApplyBatch(ctx context.Context, batch invertedIndexMutationBatch) error {
	if batch.Empty() {
		return nil
	}
	if batch.mode != idx.Mode() {
		return fmt.Errorf("inverted mutation batch uses posting mode %d, expected %d", batch.mode, idx.Mode())
	}
	return idx.appendMutationBatchSegment(ctx, batch)
}

func (idx *logStructuredInvertedIndex) Lookup(ctx context.Context, term string) (invertedPostingIterator, error) {
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return nil, err
	}
	if len(meta.Segments) == 0 {
		return idx.base.Lookup(ctx, term)
	}
	if !segmentsMayContainTerm(meta.Segments, term) {
		return idx.base.Lookup(ctx, term)
	}
	if insertOnlySegmentsMayContainTerm(meta.Segments, term) {
		return idx.lookupInsertOnlySegments(ctx, meta, term)
	}
	postings, err := idx.materializeTermPostings(ctx, meta, term)
	if err != nil {
		return nil, err
	}
	blocks, err := makeInvertedPostingBlocks(idx.Mode(), postings)
	if err != nil {
		return nil, err
	}
	return &sliceInvertedPostingIterator{blocks: blocks}, nil
}

func (idx *logStructuredInvertedIndex) Stats(ctx context.Context, term string) (invertedPostingStats, error) {
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return invertedPostingStats{}, err
	}
	if len(meta.Segments) == 0 {
		return idx.base.Stats(ctx, term)
	}
	if !segmentsMayContainTerm(meta.Segments, term) {
		return idx.base.Stats(ctx, term)
	}
	if insertOnlySegmentsMayContainTerm(meta.Segments, term) {
		return idx.statsInsertOnlySegments(ctx, meta, term)
	}
	postings, err := idx.materializeTermPostings(ctx, meta, term)
	if err != nil {
		return invertedPostingStats{}, err
	}
	return invertedPostingStats{
		DocFreq:      uint32(len(postings)),
		PostingCount: countInvertedPostings(idx.Mode(), postings),
	}, nil
}

func (idx *logStructuredInvertedIndex) FreeAll(ctx context.Context) error {
	if err := idx.base.FreeAll(ctx); err != nil {
		return err
	}
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("read inverted metadata root for free: %w", err)
	}
	if page.InvertedMetaPage != nil {
		for _, segment := range page.InvertedMetaPage.Segments {
			if segment.RootPage != 0 {
				if err := idx.freeSegmentPages(ctx, segment.RootPage); err != nil {
					return fmt.Errorf("free inverted segment root %d: %w", segment.RootPage, err)
				}
			}
		}
	}
	return idx.pager.AddFreePage(ctx, idx.rootPageIdx)
}

func (idx *logStructuredInvertedIndex) readMetaPage(ctx context.Context) (*invertedMetaPage, error) {
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted metadata root: %w", err)
	}
	if page.InvertedMetaPage == nil {
		return nil, fmt.Errorf("inverted index %s root page %d is not a metadata page", idx.name, idx.rootPageIdx)
	}
	return page.InvertedMetaPage, nil
}

func (idx *logStructuredInvertedIndex) visitSegmentTermCells(ctx context.Context, root PageIndex, term string, visit func(invertedSegmentCell) error) error {
	for pageIdx := root; pageIdx != 0; {
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted segment page %d: %w", pageIdx, err)
		}
		if page.InvertedSegmentPage == nil {
			return fmt.Errorf("inverted segment page %d has unexpected page type", pageIdx)
		}
		cells := page.InvertedSegmentPage.Cells
		if len(cells) == 0 {
			pageIdx = page.InvertedSegmentPage.Header.NextPage
			continue
		}
		if term < cells[0].Term {
			return nil
		}
		if term > cells[len(cells)-1].Term {
			pageIdx = page.InvertedSegmentPage.Header.NextPage
			continue
		}
		i := sort.Search(len(cells), func(i int) bool {
			return cells[i].Term >= term
		})
		for i < len(cells) && cells[i].Term == term {
			if err := visit(cells[i]); err != nil {
				return err
			}
			i++
		}
		pageIdx = page.InvertedSegmentPage.Header.NextPage
	}
	return nil
}

func (idx *logStructuredInvertedIndex) freeSegmentPages(ctx context.Context, root PageIndex) error {
	for pageIdx := root; pageIdx != 0; {
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted segment page %d for free: %w", pageIdx, err)
		}
		nextPage := PageIndex(0)
		if page.InvertedSegmentPage != nil {
			nextPage = page.InvertedSegmentPage.Header.NextPage
		}
		if err := idx.pager.AddFreePage(ctx, pageIdx); err != nil {
			return err
		}
		pageIdx = nextPage
	}
	return nil
}

type concatenatingInvertedPostingIterator struct {
	iterators []invertedPostingIterator
	index     int
}

func (it *concatenatingInvertedPostingIterator) NextBlock(ctx context.Context) (invertedPostingBlock, bool, error) {
	for it.index < len(it.iterators) {
		block, ok, err := it.iterators[it.index].NextBlock(ctx)
		if err != nil {
			return invertedPostingBlock{}, false, err
		}
		if ok {
			return block, true, nil
		}
		it.index++
	}
	return invertedPostingBlock{}, false, nil
}

type sliceInvertedPostingIterator struct {
	blocks []invertedPostingBlock
	index  int
}

func (it *sliceInvertedPostingIterator) NextBlock(context.Context) (invertedPostingBlock, bool, error) {
	if it.index >= len(it.blocks) {
		return invertedPostingBlock{}, false, nil
	}
	block := it.blocks[it.index]
	it.index++
	return block, true, nil
}

func (idx *logStructuredInvertedIndex) lookupInsertOnlySegments(
	ctx context.Context,
	meta *invertedMetaPage,
	term string,
) (invertedPostingIterator, error) {
	baseIter, err := idx.base.Lookup(ctx, term)
	if err != nil {
		return nil, err
	}
	iterators := []invertedPostingIterator{baseIter}
	for _, segment := range meta.Segments {
		if !segmentMayContainTerm(segment, term) {
			continue
		}
		if err := idx.visitSegmentTermCells(ctx, segment.RootPage, term, func(cell invertedSegmentCell) error {
			iterators = append(iterators, &singleBlockInvertedPostingIterator{
				block:    cell.Block,
				hasBlock: true,
			})
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return &concatenatingInvertedPostingIterator{iterators: iterators}, nil
}

func (idx *logStructuredInvertedIndex) statsInsertOnlySegments(
	ctx context.Context,
	meta *invertedMetaPage,
	term string,
) (invertedPostingStats, error) {
	stats, err := idx.base.Stats(ctx, term)
	if err != nil {
		return invertedPostingStats{}, err
	}
	for _, segment := range meta.Segments {
		if !segmentMayContainTerm(segment, term) {
			continue
		}
		if err := idx.visitSegmentTermCells(ctx, segment.RootPage, term, func(cell invertedSegmentCell) error {
			stats.DocFreq += cell.DocFreq
			stats.PostingCount += cell.PostingCount
			return nil
		}); err != nil {
			return invertedPostingStats{}, err
		}
	}
	return stats, nil
}

func (idx *logStructuredInvertedIndex) materializeTermPostings(
	ctx context.Context,
	meta *invertedMetaPage,
	term string,
) ([]invertedPosting, error) {
	byRowID := make(map[RowID]invertedPosting)
	baseIter, err := idx.base.Lookup(ctx, term)
	if err != nil {
		return nil, err
	}
	if err := idx.applyIteratorPostings(ctx, byRowID, baseIter, invertedSegmentKindInsert); err != nil {
		return nil, err
	}

	for _, segment := range meta.Segments {
		if !segmentMayContainTerm(segment, term) {
			continue
		}
		if err := idx.visitSegmentTermCells(ctx, segment.RootPage, term, func(cell invertedSegmentCell) error {
			kind := segment.Kind
			if kind == invertedSegmentKindMixed {
				kind = cell.Kind
			}
			return idx.applyBlockPostings(byRowID, cell.Block, kind)
		}); err != nil {
			return nil, err
		}
	}

	postings := make([]invertedPosting, 0, len(byRowID))
	for _, posting := range byRowID {
		postings = append(postings, posting)
	}
	sortInvertedPostings(postings)
	return postings, nil
}

func (idx *logStructuredInvertedIndex) applyIteratorPostings(
	ctx context.Context,
	byRowID map[RowID]invertedPosting,
	iter invertedPostingIterator,
	kind byte,
) error {
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := idx.applyBlockPostings(byRowID, block, kind); err != nil {
			return err
		}
	}
}

func (idx *logStructuredInvertedIndex) applyBlockPostings(byRowID map[RowID]invertedPosting, block invertedPostingBlock, kind byte) error {
	mode, postings, err := decodeInvertedPostingList(block.Payload)
	if err != nil {
		return err
	}
	if mode != idx.Mode() {
		return fmt.Errorf("inverted segment block uses posting mode %d, expected %d", mode, idx.Mode())
	}
	for _, posting := range postings {
		switch kind {
		case invertedSegmentKindInsert:
			byRowID[posting.RowID] = posting
		case invertedSegmentKindDelete:
			delete(byRowID, posting.RowID)
		default:
			return fmt.Errorf("unknown inverted segment kind %d", kind)
		}
	}
	return nil
}

func (idx *logStructuredInvertedIndex) appendMutationBatchSegment(ctx context.Context, batch invertedIndexMutationBatch) error {
	if len(batch.deletes) == 0 && len(batch.inserts) == 0 {
		return nil
	}
	deleteCells, deletePostingCount, err := idx.mutationSegmentCells(invertedSegmentKindDelete, batch.deletes)
	if err != nil {
		return err
	}
	insertCells, insertPostingCount, err := idx.mutationSegmentCells(invertedSegmentKindInsert, batch.inserts)
	if err != nil {
		return err
	}
	cells := append(deleteCells, insertCells...)
	sortSegmentCells(cells)
	postingCount := deletePostingCount + insertPostingCount
	firstTerm, lastTerm := segmentTermBounds(cells)
	rootPage, err := idx.writeSegmentCells(ctx, cells)
	if err != nil {
		return err
	}
	kind := invertedSegmentKindMixed
	if len(deleteCells) == 0 {
		kind = invertedSegmentKindInsert
	} else if len(insertCells) == 0 {
		kind = invertedSegmentKindDelete
	}
	metaPage, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("modify inverted metadata root: %w", err)
	}
	if metaPage.InvertedMetaPage == nil {
		return fmt.Errorf("inverted index %s root page %d is not a metadata page", idx.name, idx.rootPageIdx)
	}
	generation := metaPage.InvertedMetaPage.NextGeneration
	metaPage.InvertedMetaPage.NextGeneration++
	metaPage.InvertedMetaPage.Segments = append(metaPage.InvertedMetaPage.Segments, invertedSegmentDescriptor{
		Generation:   generation,
		RootPage:     rootPage,
		PostingCount: postingCount,
		Kind:         kind,
		Level:        0,
		FirstTerm:    firstTerm,
		LastTerm:     lastTerm,
	})
	return idx.maybeCompactAfterNewSegment(ctx)
}

func (idx *logStructuredInvertedIndex) compactOldestSegmentRun(ctx context.Context) (bool, error) {
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return false, err
	}
	if len(meta.Segments) < logStructuredInvertedIndexMergeRunSize {
		return false, nil
	}
	start := -1
	end := -1
	for i := 0; i < len(meta.Segments); {
		level := meta.Segments[i].Level
		j := i + 1
		for j < len(meta.Segments) && meta.Segments[j].Level == level {
			j++
		}
		if j-i >= logStructuredInvertedIndexMergeRunSize {
			start = i
			end = i + logStructuredInvertedIndexMergeRunSize
			break
		}
		i = j
	}
	if start < 0 {
		return false, nil
	}

	run := append([]invertedSegmentDescriptor(nil), meta.Segments[start:end]...)
	cells, postingCount, err := idx.mergeSegmentRunCells(ctx, run)
	if err != nil {
		return false, err
	}
	sortSegmentCells(cells)
	rootPage, err := idx.writeSegmentCells(ctx, cells)
	if err != nil {
		return false, err
	}
	firstTerm, lastTerm := segmentTermBounds(cells)
	nextLevel := run[len(run)-1].Level + 1
	replacement := invertedSegmentDescriptor{
		Generation:   run[len(run)-1].Generation,
		RootPage:     rootPage,
		PostingCount: postingCount,
		Kind:         segmentCellsKind(cells),
		Level:        nextLevel,
		FirstTerm:    firstTerm,
		LastTerm:     lastTerm,
	}

	metaPage, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return false, fmt.Errorf("modify inverted metadata root after segment merge: %w", err)
	}
	if metaPage.InvertedMetaPage == nil {
		return false, fmt.Errorf("inverted index %s root page %d is not a metadata page", idx.name, idx.rootPageIdx)
	}
	updated := make([]invertedSegmentDescriptor, 0, len(metaPage.InvertedMetaPage.Segments)-len(run)+1)
	updated = append(updated, metaPage.InvertedMetaPage.Segments[:start]...)
	updated = append(updated, replacement)
	updated = append(updated, metaPage.InvertedMetaPage.Segments[end:]...)
	metaPage.InvertedMetaPage.Segments = updated

	for _, segment := range run {
		if err := idx.freeSegmentPages(ctx, segment.RootPage); err != nil {
			return false, fmt.Errorf("free merged inverted segment root %d: %w", segment.RootPage, err)
		}
	}
	return true, nil
}

func (idx *logStructuredInvertedIndex) mergeSegmentRunCells(
	ctx context.Context,
	segments []invertedSegmentDescriptor,
) ([]invertedSegmentCell, uint32, error) {
	states, err := idx.reduceSegmentStates(ctx, segments)
	if err != nil {
		return nil, 0, err
	}

	terms := sortedSegmentStateTerms(states)
	var cells []invertedSegmentCell
	var totalPostingCount uint32
	for _, term := range terms {
		state := states[term]
		deletePostings := postingsFromMap(state.deletes)
		deleteCells, deletePostingCount, err := idx.segmentCellsForPostings(invertedSegmentKindDelete, term, deletePostings)
		if err != nil {
			return nil, 0, err
		}
		insertPostings := postingsFromMap(state.inserts)
		insertCells, insertPostingCount, err := idx.segmentCellsForPostings(invertedSegmentKindInsert, term, insertPostings)
		if err != nil {
			return nil, 0, err
		}
		cells = append(cells, deleteCells...)
		cells = append(cells, insertCells...)
		totalPostingCount += deletePostingCount + insertPostingCount
	}
	if len(cells) == 0 {
		return nil, 0, fmt.Errorf("cannot merge empty inverted segment run")
	}
	return cells, totalPostingCount, nil
}

type segmentTermState struct {
	inserts map[RowID]invertedPosting
	deletes map[RowID]invertedPosting
}

func (idx *logStructuredInvertedIndex) reduceSegmentStates(
	ctx context.Context,
	segments []invertedSegmentDescriptor,
) (map[string]segmentTermState, error) {
	states := make(map[string]segmentTermState)
	for _, segment := range segments {
		if err := idx.visitSegmentCells(ctx, segment.RootPage, func(cell invertedSegmentCell) error {
			kind := segment.Kind
			if kind == invertedSegmentKindMixed {
				kind = cell.Kind
			}
			mode, postings, err := decodeInvertedPostingList(cell.Block.Payload)
			if err != nil {
				return err
			}
			if mode != idx.Mode() {
				return fmt.Errorf("inverted segment block uses posting mode %d, expected %d", mode, idx.Mode())
			}
			state := states[cell.Term]
			if state.inserts == nil {
				state.inserts = make(map[RowID]invertedPosting)
			}
			if state.deletes == nil {
				state.deletes = make(map[RowID]invertedPosting)
			}
			for _, posting := range postings {
				switch kind {
				case invertedSegmentKindInsert:
					state.inserts[posting.RowID] = posting
				case invertedSegmentKindDelete:
					delete(state.inserts, posting.RowID)
					state.deletes[posting.RowID] = posting
				default:
					return fmt.Errorf("unknown inverted segment kind %d", kind)
				}
			}
			states[cell.Term] = state
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return states, nil
}

func sortedSegmentStateTerms(states map[string]segmentTermState) []string {
	terms := make([]string, 0, len(states))
	for term := range states {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}

func postingsFromMap(postingsByRowID map[RowID]invertedPosting) []invertedPosting {
	postings := make([]invertedPosting, 0, len(postingsByRowID))
	for _, posting := range postingsByRowID {
		postings = append(postings, posting)
	}
	sortInvertedPostings(postings)
	return postings
}

func (idx *logStructuredInvertedIndex) segmentCellsForPostings(
	kind byte,
	term string,
	postings []invertedPosting,
) ([]invertedSegmentCell, uint32, error) {
	if len(postings) == 0 {
		return nil, 0, nil
	}
	blocks, err := makeInvertedPostingBlocks(idx.Mode(), postings)
	if err != nil {
		return nil, 0, err
	}
	cells := make([]invertedSegmentCell, 0, len(blocks))
	var totalPostingCount uint32
	for _, block := range blocks {
		mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
		if err != nil {
			return nil, 0, err
		}
		if mode != idx.Mode() {
			return nil, 0, fmt.Errorf("encoded inverted segment block uses posting mode %d, expected %d", mode, idx.Mode())
		}
		postingCount := countInvertedPostings(idx.Mode(), blockPostings)
		cells = append(cells, invertedSegmentCell{
			Term:         term,
			Block:        block,
			DocFreq:      uint32(len(blockPostings)),
			PostingCount: postingCount,
			Kind:         kind,
		})
		totalPostingCount += postingCount
	}
	return cells, totalPostingCount, nil
}

func shouldFoldSegmentsIntoBase(meta *invertedMetaPage) bool {
	if len(meta.Segments) >= logStructuredInvertedIndexCompactSegmentThreshold {
		return true
	}
	if meta.usedBytes() >= logStructuredInvertedIndexMetaCompactBytes {
		return true
	}
	for _, segment := range meta.Segments {
		if segment.Level >= 2 {
			return true
		}
	}
	return false
}

func (idx *logStructuredInvertedIndex) maybeFoldSegmentsIntoBase(ctx context.Context) error {
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return err
	}
	if shouldFoldSegmentsIntoBase(meta) {
		return idx.compactSegments(ctx)
	}
	return nil
}

func (idx *logStructuredInvertedIndex) maybeCompactAfterAppend(ctx context.Context) error {
	for {
		compacted, err := idx.compactOldestSegmentRun(ctx)
		if err != nil {
			return err
		}
		if !compacted {
			break
		}
	}
	return idx.maybeFoldSegmentsIntoBase(ctx)
}

func (idx *logStructuredInvertedIndex) compactSegmentsIfNeeded(ctx context.Context) error {
	if err := idx.maybeCompactAfterAppend(ctx); err != nil {
		return err
	}
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return err
	}
	if shouldFoldSegmentsIntoBase(meta) {
		return idx.compactSegments(ctx)
	}
	return nil
}

func (idx *logStructuredInvertedIndex) maybeCompactAfterNewSegment(ctx context.Context) error {
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return err
	}
	if len(meta.Segments) < logStructuredInvertedIndexMergeRunSize &&
		!shouldFoldSegmentsIntoBase(meta) {
		return nil
	}
	return idx.compactSegmentsIfNeeded(ctx)
}

func segmentMayContainTerm(segment invertedSegmentDescriptor, term string) bool {
	if segment.FirstTerm == "" && segment.LastTerm == "" {
		return true
	}
	return term >= segment.FirstTerm && term <= segment.LastTerm
}

func segmentsMayContainTerm(segments []invertedSegmentDescriptor, term string) bool {
	for _, segment := range segments {
		if segmentMayContainTerm(segment, term) {
			return true
		}
	}
	return false
}

func insertOnlySegmentsMayContainTerm(segments []invertedSegmentDescriptor, term string) bool {
	for _, segment := range segments {
		if !segmentMayContainTerm(segment, term) {
			continue
		}
		if segment.Kind != invertedSegmentKindInsert {
			return false
		}
	}
	return true
}

func segmentTermBounds(cells []invertedSegmentCell) (string, string) {
	if len(cells) == 0 {
		return "", ""
	}
	firstTerm := cells[0].Term
	lastTerm := cells[0].Term
	for _, cell := range cells[1:] {
		if cell.Term < firstTerm {
			firstTerm = cell.Term
		}
		if cell.Term > lastTerm {
			lastTerm = cell.Term
		}
	}
	return firstTerm, lastTerm
}

func sortSegmentCells(cells []invertedSegmentCell) {
	sort.SliceStable(cells, func(i, j int) bool {
		if cells[i].Term != cells[j].Term {
			return cells[i].Term < cells[j].Term
		}
		return invertedSegmentCellKindOrder(cells[i].Kind) < invertedSegmentCellKindOrder(cells[j].Kind)
	})
}

func segmentCellsKind(cells []invertedSegmentCell) byte {
	if len(cells) == 0 {
		return invertedSegmentKindMixed
	}
	kind := cells[0].Kind
	for _, cell := range cells[1:] {
		if cell.Kind != kind {
			return invertedSegmentKindMixed
		}
	}
	return kind
}

func invertedSegmentCellKindOrder(kind byte) int {
	if kind == invertedSegmentKindDelete {
		return 0
	}
	return 1
}

func (idx *logStructuredInvertedIndex) mutationSegmentCells(kind byte, postingsByTerm map[string][]invertedPosting) ([]invertedSegmentCell, uint32, error) {
	terms := sortedInvertedMutationTerms(postingsByTerm)
	cells := make([]invertedSegmentCell, 0, len(terms))
	var totalPostingCount uint32
	for _, term := range terms {
		postings := append([]invertedPosting(nil), postingsByTerm[term]...)
		postings = groupInvertedPostingsInPlace(idx.Mode(), postings)
		blocks, err := makeInvertedPostingBlocks(idx.Mode(), postings)
		if err != nil {
			return nil, 0, err
		}
		for _, block := range blocks {
			mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
			if err != nil {
				return nil, 0, err
			}
			if mode != idx.Mode() {
				return nil, 0, fmt.Errorf("encoded inverted segment block uses posting mode %d, expected %d", mode, idx.Mode())
			}
			postingCount := countInvertedPostings(idx.Mode(), blockPostings)
			cells = append(cells, invertedSegmentCell{
				Term:         term,
				Block:        block,
				DocFreq:      uint32(len(blockPostings)),
				PostingCount: postingCount,
				Kind:         kind,
			})
			totalPostingCount += postingCount
		}
	}
	return cells, totalPostingCount, nil
}

func (idx *logStructuredInvertedIndex) writeSegmentCells(ctx context.Context, cells []invertedSegmentCell) (PageIndex, error) {
	var rootPageIdx PageIndex
	var currentPage *Page
	var currentSize uint64
	for _, cell := range cells {
		cellSize := cell.size()
		if (invertedSegmentPageHeader{}).size()+2+cellSize > uint64(PageSize) {
			return 0, fmt.Errorf("inverted segment cell for term %q exceeds page size", cell.Term)
		}
		if currentPage == nil {
			page, err := idx.newSegmentPage(ctx)
			if err != nil {
				return 0, err
			}
			currentPage = page
			rootPageIdx = page.Index
			currentSize = currentSegmentPageSize(currentPage.InvertedSegmentPage)
		}
		if currentSize+2+cellSize > uint64(PageSize) {
			nextPage, err := idx.newSegmentPage(ctx)
			if err != nil {
				return 0, err
			}
			currentPage.InvertedSegmentPage.Header.NextPage = nextPage.Index
			currentPage = nextPage
			currentSize = currentSegmentPageSize(currentPage.InvertedSegmentPage)
		}
		currentPage.InvertedSegmentPage.Cells = append(currentPage.InvertedSegmentPage.Cells, cell)
		currentSize += 2 + cellSize
	}
	if rootPageIdx == 0 {
		return 0, fmt.Errorf("cannot write empty inverted segment")
	}
	return rootPageIdx, nil
}

func (idx *logStructuredInvertedIndex) newSegmentPage(ctx context.Context) (*Page, error) {
	page, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return nil, fmt.Errorf("allocate inverted segment page: %w", err)
	}
	page.Clear()
	page.InvertedSegmentPage = NewInvertedSegmentPage()
	return page, nil
}

func (idx *logStructuredInvertedIndex) compactSegments(ctx context.Context) error {
	meta, err := idx.readMetaPage(ctx)
	if err != nil {
		return err
	}
	if len(meta.Segments) == 0 {
		return nil
	}
	segments := append([]invertedSegmentDescriptor(nil), meta.Segments...)
	sort.SliceStable(segments, func(i, j int) bool {
		return segments[i].Generation < segments[j].Generation
	})
	states, err := idx.reduceSegmentStates(ctx, segments)
	if err != nil {
		return err
	}
	if err := idx.applySegmentStatesToBase(ctx, states); err != nil {
		return err
	}
	for _, segment := range segments {
		if err := idx.freeSegmentPages(ctx, segment.RootPage); err != nil {
			return fmt.Errorf("free compacted inverted segment root %d: %w", segment.RootPage, err)
		}
	}

	metaPage, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("modify inverted metadata root after compaction: %w", err)
	}
	if metaPage.InvertedMetaPage == nil {
		return fmt.Errorf("inverted index %s root page %d is not a metadata page", idx.name, idx.rootPageIdx)
	}
	metaPage.InvertedMetaPage.Segments = nil
	return nil
}

func (idx *logStructuredInvertedIndex) applySegmentStatesToBase(
	ctx context.Context,
	states map[string]segmentTermState,
) error {
	terms := sortedSegmentStateTerms(states)
	for _, term := range terms {
		state := states[term]
		for _, posting := range postingsFromMap(state.deletes) {
			if err := idx.base.Delete(ctx, term, posting); err != nil {
				return fmt.Errorf("compact inverted segment delete term %q: %w", term, err)
			}
		}
		insertPostings := postingsFromMap(state.inserts)
		if len(insertPostings) == 0 {
			continue
		}
		if err := idx.base.InsertMany(ctx, term, insertPostings); err != nil {
			return fmt.Errorf("compact inverted segment insert term %q: %w", term, err)
		}
	}
	return nil
}

func (idx *logStructuredInvertedIndex) visitSegmentCells(ctx context.Context, root PageIndex, visit func(invertedSegmentCell) error) error {
	for pageIdx := root; pageIdx != 0; {
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted segment page %d: %w", pageIdx, err)
		}
		if page.InvertedSegmentPage == nil {
			return fmt.Errorf("inverted segment page %d has unexpected page type", pageIdx)
		}
		for _, cell := range page.InvertedSegmentPage.Cells {
			if err := visit(cell); err != nil {
				return err
			}
		}
		pageIdx = page.InvertedSegmentPage.Header.NextPage
	}
	return nil
}

func currentSegmentPageSize(page *invertedSegmentPage) uint64 {
	size := page.Header.size() + uint64(len(page.Cells))*2
	for _, cell := range page.Cells {
		size += cell.size()
	}
	return size
}

func sortInvertedPostings(postings []invertedPosting) {
	sort.Slice(postings, func(i, j int) bool {
		return postings[i].RowID < postings[j].RowID
	})
}
