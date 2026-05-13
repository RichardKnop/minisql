package minisql

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

var errInvertedIndexEntryPageFull = errors.New("dedicated inverted index entry page is full")

type invertedIndexPostingMode = invertedPostingMode

const (
	invertedIndexPostingModeRowIDs    = invertedPostingModeRowIDs
	invertedIndexPostingModePositions = invertedPostingModePositions
)

type invertedPostingStats struct {
	DocFreq      uint32
	PostingCount uint32
}

type invertedPostingIterator interface {
	NextBlock(context.Context) (invertedPostingBlock, bool, error)
}

type invertedIndex interface {
	GetRootPageIdx() PageIndex
	Mode() invertedIndexPostingMode
	Insert(ctx context.Context, term string, posting invertedPosting) error
	Delete(ctx context.Context, term string, posting invertedPosting) error
	Lookup(ctx context.Context, term string) (invertedPostingIterator, error)
	Stats(ctx context.Context, term string) (invertedPostingStats, error)
}

type dedicatedInvertedIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	mode        invertedIndexPostingMode
	name        string
}

var _ invertedIndex = (*dedicatedInvertedIndex)(nil)

// NewDedicatedInvertedIndex opens a dedicated inverted index backed by the new
// inverted entry/posting page format. This storage increment keeps all posting
// lists inline in a single root leaf entry page.
func NewDedicatedInvertedIndex(name string, mode invertedIndexPostingMode, pager TxPager, rootPageIdx PageIndex) (*dedicatedInvertedIndex, error) {
	if mode != invertedIndexPostingModeRowIDs && mode != invertedIndexPostingModePositions {
		return nil, fmt.Errorf("unknown inverted index posting mode %d", mode)
	}
	return &dedicatedInvertedIndex{
		name:        name,
		mode:        mode,
		pager:       pager,
		rootPageIdx: rootPageIdx,
	}, nil
}

// GetRootPageIdx returns the root page of the inverted entry tree.
func (idx *dedicatedInvertedIndex) GetRootPageIdx() PageIndex {
	return idx.rootPageIdx
}

// Mode returns whether this index stores row-only or positional postings.
func (idx *dedicatedInvertedIndex) Mode() invertedIndexPostingMode {
	return idx.mode
}

// InitRootPage initializes the root page as an empty inverted entry leaf. It is
// idempotent for already-initialized inverted entry roots.
func (idx *dedicatedInvertedIndex) InitRootPage(ctx context.Context) error {
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("read inverted index root: %w", err)
	}
	if page.InvertedEntryPage != nil && len(page.InvertedEntryPage.Cells) > 0 {
		return nil
	}

	page, err = idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return fmt.Errorf("modify inverted index root: %w", err)
	}
	page.Clear()
	page.InvertedEntryPage = NewInvertedEntryPage(true)
	return nil
}

// Insert adds one posting to an inline compressed posting list for term.
func (idx *dedicatedInvertedIndex) Insert(ctx context.Context, term string, posting invertedPosting) error {
	if len([]byte(term)) > MaxIndexKeySize {
		return fmt.Errorf("inverted index term exceeds max index key size %d", MaxIndexKeySize)
	}

	page, err := idx.modifyRootEntryPage(ctx)
	if err != nil {
		return err
	}
	oldCells := cloneInvertedEntryCells(page.Cells)

	cellIdx, found := findInvertedEntryCell(page.Cells, term)
	postings := []invertedPosting{posting}
	if found {
		postings, err = decodeInlineInvertedEntryCell(idx.mode, page.Cells[cellIdx])
		if err != nil {
			return err
		}
		postings = append(postings, posting)
	}

	cell, err := makeInlineInvertedEntryCell(idx.mode, term, postings)
	if err != nil {
		return err
	}
	if found {
		page.Cells[cellIdx] = cell
	} else {
		page.Cells = slices.Insert(page.Cells, cellIdx, cell)
	}
	if err := ensureInvertedEntryPageFits(page, invertedPageBodySize(idx.rootPageIdx)); err != nil {
		page.Cells = oldCells
		return err
	}
	return nil
}

// Delete removes a posting from an inline compressed posting list for term.
func (idx *dedicatedInvertedIndex) Delete(ctx context.Context, term string, posting invertedPosting) error {
	page, err := idx.modifyRootEntryPage(ctx)
	if err != nil {
		return err
	}
	cellIdx, found := findInvertedEntryCell(page.Cells, term)
	if !found {
		return nil
	}

	postings, err := decodeInlineInvertedEntryCell(idx.mode, page.Cells[cellIdx])
	if err != nil {
		return err
	}
	postings = removeInvertedPosting(idx.mode, postings, posting)
	if len(postings) == 0 {
		page.Cells = slices.Delete(page.Cells, cellIdx, cellIdx+1)
		return nil
	}

	cell, err := makeInlineInvertedEntryCell(idx.mode, term, postings)
	if err != nil {
		return err
	}
	page.Cells[cellIdx] = cell
	if err := ensureInvertedEntryPageFits(page, invertedPageBodySize(idx.rootPageIdx)); err != nil {
		return err
	}
	return nil
}

// Lookup returns an iterator over the inline compressed posting block for term.
func (idx *dedicatedInvertedIndex) Lookup(ctx context.Context, term string) (invertedPostingIterator, error) {
	page, err := idx.readRootEntryPage(ctx)
	if err != nil {
		return nil, err
	}
	cellIdx, found := findInvertedEntryCell(page.Cells, term)
	if !found {
		return &singleBlockInvertedPostingIterator{}, nil
	}

	cell := page.Cells[cellIdx]
	if cell.PostingKind != invertedPostingKindInline {
		return nil, fmt.Errorf("inverted index %s term %q uses unsupported posting kind %d", idx.name, term, cell.PostingKind)
	}
	block, err := inlineInvertedPostingBlock(cell)
	if err != nil {
		return nil, err
	}
	return &singleBlockInvertedPostingIterator{block: block, hasBlock: true}, nil
}

// Stats returns document-frequency and posting-count metadata for term.
func (idx *dedicatedInvertedIndex) Stats(ctx context.Context, term string) (invertedPostingStats, error) {
	page, err := idx.readRootEntryPage(ctx)
	if err != nil {
		return invertedPostingStats{}, err
	}
	cellIdx, found := findInvertedEntryCell(page.Cells, term)
	if !found {
		return invertedPostingStats{}, nil
	}
	cell := page.Cells[cellIdx]
	return invertedPostingStats{
		DocFreq:      cell.DocFreq,
		PostingCount: cell.PostingCount,
	}, nil
}

func (idx *dedicatedInvertedIndex) readRootEntryPage(ctx context.Context) (*invertedEntryPage, error) {
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted index root: %w", err)
	}
	if page.InvertedEntryPage == nil {
		return nil, fmt.Errorf("inverted index %s root page is not an entry page", idx.name)
	}
	if !page.InvertedEntryPage.Header.IsLeaf {
		return nil, fmt.Errorf("inverted index %s root entry tree is not leaf-only", idx.name)
	}
	return page.InvertedEntryPage, nil
}

func (idx *dedicatedInvertedIndex) modifyRootEntryPage(ctx context.Context) (*invertedEntryPage, error) {
	page, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("modify inverted index root: %w", err)
	}
	if page.InvertedEntryPage == nil {
		page.Clear()
		page.InvertedEntryPage = NewInvertedEntryPage(true)
	}
	if !page.InvertedEntryPage.Header.IsLeaf {
		return nil, fmt.Errorf("inverted index %s root entry tree is not leaf-only", idx.name)
	}
	return page.InvertedEntryPage, nil
}

func findInvertedEntryCell(cells []invertedEntryCell, term string) (int, bool) {
	i, found := slices.BinarySearchFunc(cells, term, func(cell invertedEntryCell, term string) int {
		if cell.Term < term {
			return -1
		}
		if cell.Term > term {
			return 1
		}
		return 0
	})
	return i, found
}

func makeInlineInvertedEntryCell(mode invertedPostingMode, term string, postings []invertedPosting) (invertedEntryCell, error) {
	grouped := groupInvertedPostings(mode, postings)
	payload, err := encodeInvertedPostingList(mode, grouped)
	if err != nil {
		return invertedEntryCell{}, err
	}
	return invertedEntryCell{
		Term:         term,
		Payload:      payload,
		DocFreq:      uint32(len(grouped)),
		PostingCount: countInvertedPostings(mode, grouped),
		PostingKind:  invertedPostingKindInline,
		CodecVersion: invertedPostingCodecVersion,
	}, nil
}

func decodeInlineInvertedEntryCell(expectedMode invertedPostingMode, cell invertedEntryCell) ([]invertedPosting, error) {
	if cell.PostingKind != invertedPostingKindInline {
		return nil, fmt.Errorf("inverted entry term %q uses unsupported posting kind %d", cell.Term, cell.PostingKind)
	}
	if cell.CodecVersion != invertedPostingCodecVersion {
		return nil, fmt.Errorf("inverted entry term %q uses unsupported posting codec version %d", cell.Term, cell.CodecVersion)
	}
	mode, postings, err := decodeInvertedPostingList(cell.Payload)
	if err != nil {
		return nil, err
	}
	if mode != expectedMode {
		return nil, fmt.Errorf("inverted entry term %q uses posting mode %d, expected %d", cell.Term, mode, expectedMode)
	}
	return postings, nil
}

func countInvertedPostings(mode invertedPostingMode, postings []invertedPosting) uint32 {
	if mode == invertedPostingModeRowIDs {
		return uint32(len(postings))
	}
	var n uint32
	for _, posting := range postings {
		n += uint32(len(posting.Positions))
	}
	return n
}

func removeInvertedPosting(mode invertedPostingMode, postings []invertedPosting, remove invertedPosting) []invertedPosting {
	grouped := groupInvertedPostings(mode, postings)
	i, found := slices.BinarySearchFunc(grouped, remove.RowID, func(posting invertedPosting, rowID RowID) int {
		if posting.RowID < rowID {
			return -1
		}
		if posting.RowID > rowID {
			return 1
		}
		return 0
	})
	if !found {
		return grouped
	}
	if mode == invertedPostingModeRowIDs || len(remove.Positions) == 0 {
		return slices.Delete(grouped, i, i+1)
	}

	toRemove := append([]uint32(nil), remove.Positions...)
	slices.Sort(toRemove)
	toRemove = slices.Compact(toRemove)
	positions := grouped[i].Positions
	for _, position := range toRemove {
		j, found := slices.BinarySearch(positions, position)
		if found {
			positions = slices.Delete(positions, j, j+1)
		}
	}
	if len(positions) == 0 {
		return slices.Delete(grouped, i, i+1)
	}
	grouped[i].Positions = positions
	return grouped
}

func ensureInvertedEntryPageFits(page *invertedEntryPage, pageSize int) error {
	buf := make([]byte, pageSize)
	if err := page.Marshal(buf); err != nil {
		if errors.Is(err, errInvertedIndexEntryPageFull) {
			return err
		}
		return err
	}
	return nil
}

func invertedPageBodySize(pageIdx PageIndex) int {
	if pageIdx == 0 {
		return PageSize - RootPageConfigSize
	}
	return PageSize
}

func cloneInvertedEntryCells(cells []invertedEntryCell) []invertedEntryCell {
	clone := make([]invertedEntryCell, len(cells))
	for i, cell := range cells {
		clone[i] = cell
		clone[i].Payload = append([]byte(nil), cell.Payload...)
	}
	return clone
}

func inlineInvertedPostingBlock(cell invertedEntryCell) (invertedPostingBlock, error) {
	_, postings, err := decodeInvertedPostingList(cell.Payload)
	if err != nil {
		return invertedPostingBlock{}, err
	}
	block := invertedPostingBlock{
		Payload:      append([]byte(nil), cell.Payload...),
		PostingCount: cell.PostingCount,
		CodecVersion: cell.CodecVersion,
	}
	if len(postings) > 0 {
		block.FirstRowID = postings[0].RowID
		block.LastRowID = postings[len(postings)-1].RowID
	}
	return block, nil
}

type singleBlockInvertedPostingIterator struct {
	block    invertedPostingBlock
	hasBlock bool
}

// NextBlock returns the single inline posting block once.
func (it *singleBlockInvertedPostingIterator) NextBlock(context.Context) (invertedPostingBlock, bool, error) {
	if !it.hasBlock {
		return invertedPostingBlock{}, false, nil
	}
	it.hasBlock = false
	return it.block, true, nil
}
