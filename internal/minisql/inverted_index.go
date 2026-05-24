package minisql

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

var errInvertedIndexEntryPageFull = errors.New("dedicated inverted index entry page is full")

const (
	invertedInlinePostingPayloadMax = 1024
	invertedPostingBlockPayloadMax  = 1024
)

type invertedIndexPostingMode = invertedPostingMode

const (
	invertedIndexPostingModeRowIDs    = invertedPostingModeRowIDs
	invertedIndexPostingModePositions = invertedPostingModePositions
)

type invertedPostingStats struct {
	DocFreq      uint32
	PostingCount uint32
}

type deletePostingTreeCellResult struct {
	Cell        invertedEntryCell
	Mutated     bool
	RemoveEntry bool
}

// invertedPostingIterator streams compressed posting blocks for a single term.
type invertedPostingIterator interface {
	NextBlock(context.Context) (invertedPostingBlock, bool, error)
}

// invertedIndex is the storage interface for dedicated full-text/JSON inverted indexes.
type invertedIndex interface {
	GetRootPageIdx() PageIndex
	Mode() invertedIndexPostingMode
	Insert(ctx context.Context, term string, posting invertedPosting) error
	InsertMany(ctx context.Context, term string, postings []invertedPosting) error
	Replace(ctx context.Context, term string, oldPosting, newPosting invertedPosting) error
	Delete(ctx context.Context, term string, posting invertedPosting) error
	Lookup(ctx context.Context, term string) (invertedPostingIterator, error)
	Stats(ctx context.Context, term string) (invertedPostingStats, error)
}

type invertedRowIDLoader interface {
	LoadRowIDs(ctx context.Context, term string, hint uint32) ([]RowID, error)
}

type invertedDocFreqCounter interface {
	CountDocFreq(ctx context.Context, term string) (uint32, error)
}

type dedicatedInvertedIndex struct {
	pager       TxPager
	rootPageIdx PageIndex
	mode        invertedIndexPostingMode
	name        string
}

var _ invertedIndex = (*dedicatedInvertedIndex)(nil)

// NewDedicatedInvertedIndex opens a dedicated inverted index backed by the new
// inverted entry/posting page format.
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

// Insert adds one posting to a compressed posting list for term.
func (idx *dedicatedInvertedIndex) Insert(ctx context.Context, term string, posting invertedPosting) error {
	if len([]byte(term)) > MaxIndexKeySize {
		return fmt.Errorf("inverted index term exceeds max index key size %d", MaxIndexKeySize)
	}

	one := [1]invertedPosting{posting}
	separator, rightPageIdx, split, err := idx.insertEntry(ctx, idx.rootPageIdx, term, one[:])
	if err != nil {
		return err
	}
	if !split {
		return nil
	}

	return idx.splitRootEntryPage(ctx, separator, rightPageIdx)
}

// InsertMany adds a batch of postings for one term. It is primarily used by
// CREATE INDEX population so a term's posting list can be encoded once instead
// of being repeatedly decoded and re-encoded for every token occurrence.
func (idx *dedicatedInvertedIndex) InsertMany(ctx context.Context, term string, postings []invertedPosting) error {
	if len(postings) == 0 {
		return nil
	}
	if len([]byte(term)) > MaxIndexKeySize {
		return fmt.Errorf("inverted index term exceeds max index key size %d", MaxIndexKeySize)
	}

	separator, rightPageIdx, split, err := idx.insertEntry(ctx, idx.rootPageIdx, term, postings)
	if err != nil {
		return err
	}
	if !split {
		return nil
	}

	return idx.splitRootEntryPage(ctx, separator, rightPageIdx)
}

// Replace swaps one posting for another under the same term. It is used by
// UPDATE maintenance when a term remains present but its positions changed.
func (idx *dedicatedInvertedIndex) Replace(ctx context.Context, term string, oldPosting, newPosting invertedPosting) error {
	if len([]byte(term)) > MaxIndexKeySize {
		return fmt.Errorf("inverted index term exceeds max index key size %d", MaxIndexKeySize)
	}

	leafPage, err := idx.findEntryLeafPage(ctx, term)
	if err != nil {
		return err
	}
	page, err := idx.pager.ModifyPage(ctx, leafPage.Index)
	if err != nil {
		return fmt.Errorf("modify inverted entry leaf %d: %w", leafPage.Index, err)
	}
	if page.InvertedEntryPage == nil || !page.InvertedEntryPage.Header.IsLeaf {
		return fmt.Errorf("inverted entry page %d is not a leaf", page.Index)
	}
	entryPage := page.InvertedEntryPage
	cellIdx, found := findInvertedEntryCell(entryPage.Cells, term)
	if !found {
		return idx.Insert(ctx, term, newPosting)
	}

	oldCell := entryPage.Cells[cellIdx]
	if oldCell.PostingKind == invertedPostingKindTree && oldCell.PostingCount > 64 {
		updatedCell, mutated, err := idx.replacePostingInTreeCell(ctx, oldCell, oldPosting, newPosting)
		if err != nil {
			return err
		}
		if mutated {
			entryPage.Cells[cellIdx] = updatedCell
			return nil
		}
	}

	postings, err := idx.decodeInvertedEntryCell(ctx, oldCell)
	if err != nil {
		return err
	}
	postings = removeInvertedPosting(idx.mode, postings, oldPosting)
	postings = append(postings, newPosting)
	cell, err := idx.makeInvertedEntryCell(ctx, term, postings, []invertedEntryCell{oldCell})
	if err != nil {
		return err
	}
	entryPage.Cells[cellIdx] = cell
	if err := ensureInvertedEntryPageFits(entryPage, invertedPageBodySize(page.Index)); err != nil {
		return err
	}
	return nil
}

// Delete removes a posting from a compressed posting list for term.
func (idx *dedicatedInvertedIndex) Delete(ctx context.Context, term string, posting invertedPosting) error {
	leafPage, err := idx.findEntryLeafPage(ctx, term)
	if err != nil {
		return err
	}
	page, err := idx.pager.ModifyPage(ctx, leafPage.Index)
	if err != nil {
		return fmt.Errorf("modify inverted entry leaf %d: %w", leafPage.Index, err)
	}
	if page.InvertedEntryPage == nil || !page.InvertedEntryPage.Header.IsLeaf {
		return fmt.Errorf("inverted entry page %d is not a leaf", page.Index)
	}
	entryPage := page.InvertedEntryPage
	cellIdx, found := findInvertedEntryCell(entryPage.Cells, term)
	if !found {
		return nil
	}

	oldCell := entryPage.Cells[cellIdx]
	if oldCell.PostingKind == invertedPostingKindTree && oldCell.PostingCount > 64 {
		result, err := idx.deletePostingFromTreeCell(ctx, oldCell, posting)
		if err != nil {
			return err
		}
		if result.Mutated {
			if result.RemoveEntry {
				entryPage.Cells = slices.Delete(entryPage.Cells, cellIdx, cellIdx+1)
				return idx.rebalanceEntryPageAfterDelete(ctx, page.Index)
			}
			entryPage.Cells[cellIdx] = result.Cell
			return nil
		}
	}
	postings, err := idx.decodeInvertedEntryCell(ctx, oldCell)
	if err != nil {
		return err
	}
	postings = removeInvertedPosting(idx.mode, postings, posting)
	if len(postings) == 0 {
		if err := idx.freePostingTree(ctx, oldCell); err != nil {
			return err
		}
		entryPage.Cells = slices.Delete(entryPage.Cells, cellIdx, cellIdx+1)
		return idx.rebalanceEntryPageAfterDelete(ctx, page.Index)
	}

	cell, err := idx.makeInvertedEntryCell(ctx, term, postings, []invertedEntryCell{oldCell})
	if err != nil {
		return err
	}
	entryPage.Cells[cellIdx] = cell
	if err := ensureInvertedEntryPageFits(entryPage, invertedPageBodySize(page.Index)); err != nil {
		return err
	}
	return nil
}

// rebalanceEntryPageAfterDelete repairs entry-tree underflow after a term cell disappears.
func (idx *dedicatedInvertedIndex) rebalanceEntryPageAfterDelete(ctx context.Context, pageIdx PageIndex) error {
	page, err := idx.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("modify inverted entry page %d after delete: %w", pageIdx, err)
	}
	entryPage := page.InvertedEntryPage
	if entryPage == nil {
		return fmt.Errorf("inverted entry page %d is not an entry page", pageIdx)
	}
	if pageIdx == idx.rootPageIdx {
		return idx.collapseEntryRoot(ctx, page)
	}
	if len(entryPage.Cells) > 0 && !invertedEntryPageUnderfull(entryPage, invertedPageBodySize(pageIdx)) {
		return idx.refreshEntryParentSeparator(ctx, page)
	}

	parentPage, childPos, err := idx.modifyEntryParent(ctx, page)
	if err != nil {
		return err
	}
	left, right, err := idx.entrySiblings(ctx, parentPage.InvertedEntryPage, childPos)
	if err != nil {
		return err
	}

	if left != nil && invertedEntryPageCanSpare(left.InvertedEntryPage, invertedPageBodySize(left.Index)) {
		if err := idx.borrowEntryFromLeft(ctx, parentPage, page, left, childPos); err != nil {
			return err
		}
		return idx.refreshEntryParentSeparator(ctx, page)
	}
	if right != nil && invertedEntryPageCanSpare(right.InvertedEntryPage, invertedPageBodySize(right.Index)) {
		if err := idx.borrowEntryFromRight(ctx, parentPage, page, right, childPos); err != nil {
			return err
		}
		return idx.refreshEntryParentSeparator(ctx, right)
	}

	var freed PageIndex
	switch {
	case right != nil:
		freed, err = idx.mergeEntryPages(ctx, parentPage, page, right, childPos)
	case left != nil:
		freed, err = idx.mergeEntryPages(ctx, parentPage, left, page, childPos-1)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if err := idx.pager.AddFreePage(ctx, freed); err != nil {
		return fmt.Errorf("free merged inverted entry page %d: %w", freed, err)
	}
	if parentPage.Index == idx.rootPageIdx {
		return idx.collapseEntryRoot(ctx, parentPage)
	}
	return idx.rebalanceEntryPageAfterDelete(ctx, parentPage.Index)
}

// collapseEntryRoot replaces an empty internal root with its only child.
func (idx *dedicatedInvertedIndex) collapseEntryRoot(ctx context.Context, rootPage *Page) error {
	root := rootPage.InvertedEntryPage
	if root == nil || root.Header.IsLeaf || len(root.Cells) > 0 {
		return nil
	}
	childIdx := root.Header.RightChild
	if childIdx == 0 {
		return nil
	}
	childPage, err := idx.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("modify inverted entry child %d for root collapse: %w", childIdx, err)
	}
	if childPage.InvertedEntryPage == nil {
		return fmt.Errorf("inverted entry child %d is not an entry page", childIdx)
	}
	rootPage.Clear()
	rootPage.InvertedEntryPage = childPage.InvertedEntryPage.Clone()
	rootPage.InvertedEntryPage.Header.Parent = 0
	if err := idx.updateEntryChildrenParent(ctx, rootPage); err != nil {
		return err
	}
	return idx.pager.AddFreePage(ctx, childIdx)
}

// modifyEntryParent returns a writable parent and this page's child slot.
func (idx *dedicatedInvertedIndex) modifyEntryParent(ctx context.Context, page *Page) (*Page, int, error) {
	parentIdx := page.InvertedEntryPage.Header.Parent
	parentPage, err := idx.pager.ModifyPage(ctx, parentIdx)
	if err != nil {
		return nil, 0, fmt.Errorf("modify inverted entry parent %d: %w", parentIdx, err)
	}
	if parentPage.InvertedEntryPage == nil || parentPage.InvertedEntryPage.Header.IsLeaf {
		return nil, 0, fmt.Errorf("inverted entry parent %d is not internal", parentIdx)
	}
	childPos, ok := findInvertedEntryChildPosition(parentPage.InvertedEntryPage, page.Index)
	if !ok {
		return nil, 0, fmt.Errorf("inverted entry page %d is not a child of parent %d", page.Index, parentIdx)
	}
	return parentPage, childPos, nil
}

// entrySiblings returns writable left/right siblings around a child position.
func (idx *dedicatedInvertedIndex) entrySiblings(ctx context.Context, parent *invertedEntryPage, childPos int) (*Page, *Page, error) {
	children := invertedEntryChildren(parent)
	var left, right *Page
	if childPos > 0 {
		page, err := idx.pager.ModifyPage(ctx, children[childPos-1])
		if err != nil {
			return nil, nil, fmt.Errorf("modify left inverted entry sibling: %w", err)
		}
		left = page
	}
	if childPos+1 < len(children) {
		page, err := idx.pager.ModifyPage(ctx, children[childPos+1])
		if err != nil {
			return nil, nil, fmt.Errorf("modify right inverted entry sibling: %w", err)
		}
		right = page
	}
	return left, right, nil
}

// borrowEntryFromLeft moves one entry from the left sibling into page.
func (idx *dedicatedInvertedIndex) borrowEntryFromLeft(ctx context.Context, parentPage, page, leftPage *Page, childPos int) error {
	parent := parentPage.InvertedEntryPage
	node := page.InvertedEntryPage
	left := leftPage.InvertedEntryPage
	if node.Header.IsLeaf {
		moved := cloneInvertedEntryCell(left.Cells[len(left.Cells)-1])
		left.Cells = left.Cells[:len(left.Cells)-1]
		node.Cells = slices.Insert(node.Cells, 0, moved)
		parent.Cells[childPos-1].Term = node.Cells[0].Term
		return nil
	}

	leftLast := left.Cells[len(left.Cells)-1]
	movedChild := left.Header.RightChild
	node.Cells = slices.Insert(node.Cells, 0, invertedEntryCell{
		Term:  parent.Cells[childPos-1].Term,
		Child: movedChild,
	})
	parent.Cells[childPos-1].Term = leftLast.Term
	left.Header.RightChild = leftLast.Child
	left.Cells = left.Cells[:len(left.Cells)-1]
	return idx.updateSingleEntryChildParent(ctx, movedChild, page.Index)
}

// borrowEntryFromRight moves one entry from the right sibling into page.
func (idx *dedicatedInvertedIndex) borrowEntryFromRight(ctx context.Context, parentPage, page, rightPage *Page, childPos int) error {
	parent := parentPage.InvertedEntryPage
	node := page.InvertedEntryPage
	right := rightPage.InvertedEntryPage
	if node.Header.IsLeaf {
		moved := cloneInvertedEntryCell(right.Cells[0])
		right.Cells = slices.Delete(right.Cells, 0, 1)
		node.Cells = append(node.Cells, moved)
		if childPos < len(parent.Cells) && len(right.Cells) > 0 {
			parent.Cells[childPos].Term = right.Cells[0].Term
		}
		return nil
	}

	rightFirst := right.Cells[0]
	node.Cells = append(node.Cells, invertedEntryCell{
		Term:  parent.Cells[childPos].Term,
		Child: node.Header.RightChild,
	})
	node.Header.RightChild = rightFirst.Child
	parent.Cells[childPos].Term = rightFirst.Term
	right.Cells = slices.Delete(right.Cells, 0, 1)
	return idx.updateSingleEntryChildParent(ctx, node.Header.RightChild, page.Index)
}

// mergeEntryPages appends rightPage into leftPage and removes the parent separator.
func (idx *dedicatedInvertedIndex) mergeEntryPages(ctx context.Context, parentPage, leftPage, rightPage *Page, separatorIdx int) (PageIndex, error) {
	parent := parentPage.InvertedEntryPage
	left := leftPage.InvertedEntryPage
	right := rightPage.InvertedEntryPage
	if left.Header.IsLeaf != right.Header.IsLeaf {
		return 0, fmt.Errorf("cannot merge mismatched inverted entry page types")
	}
	if left.Header.IsLeaf {
		left.Cells = append(left.Cells, cloneInvertedEntryCells(right.Cells)...)
		left.Header.NextLeaf = right.Header.NextLeaf
	} else {
		left.Cells = append(left.Cells, invertedEntryCell{
			Term:  parent.Cells[separatorIdx].Term,
			Child: left.Header.RightChild,
		})
		left.Cells = append(left.Cells, cloneInvertedEntryCells(right.Cells)...)
		left.Header.RightChild = right.Header.RightChild
		if err := idx.updateEntryChildrenParent(ctx, leftPage); err != nil {
			return 0, err
		}
	}
	if separatorIdx+1 < len(parent.Cells) {
		parent.Cells[separatorIdx+1].Child = leftPage.Index
		parent.Cells = slices.Delete(parent.Cells, separatorIdx, separatorIdx+1)
	} else {
		parent.Cells = slices.Delete(parent.Cells, separatorIdx, separatorIdx+1)
		parent.Header.RightChild = leftPage.Index
	}
	if err := ensureInvertedEntryPageFits(left, invertedPageBodySize(leftPage.Index)); err != nil {
		return 0, err
	}
	return rightPage.Index, nil
}

// refreshEntryParentSeparator updates the separator that routes to page.
func (idx *dedicatedInvertedIndex) refreshEntryParentSeparator(ctx context.Context, page *Page) error {
	entryPage := page.InvertedEntryPage
	if entryPage == nil || page.Index == idx.rootPageIdx || len(entryPage.Cells) == 0 {
		return nil
	}
	parentPage, childPos, err := idx.modifyEntryParent(ctx, page)
	if err != nil {
		return err
	}
	if childPos == 0 {
		return nil
	}
	parentPage.InvertedEntryPage.Cells[childPos-1].Term = firstInvertedEntryTerm(entryPage)
	return nil
}

// updateSingleEntryChildParent rewrites one child page's parent pointer.
func (idx *dedicatedInvertedIndex) updateSingleEntryChildParent(ctx context.Context, childIdx, parentIdx PageIndex) error {
	childPage, err := idx.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("modify inverted entry child %d parent: %w", childIdx, err)
	}
	if childPage.InvertedEntryPage == nil {
		return fmt.Errorf("inverted entry child %d is not an entry page", childIdx)
	}
	childPage.InvertedEntryPage.Header.Parent = parentIdx
	return nil
}

// Lookup returns an iterator over the compressed posting blocks for term.
func (idx *dedicatedInvertedIndex) Lookup(ctx context.Context, term string) (invertedPostingIterator, error) {
	page, err := idx.findEntryLeafPage(ctx, term)
	if err != nil {
		return nil, err
	}
	cellIdx, found := findInvertedEntryCell(page.InvertedEntryPage.Cells, term)
	if !found {
		return &singleBlockInvertedPostingIterator{}, nil
	}

	cell := page.InvertedEntryPage.Cells[cellIdx]
	return idx.newPostingIterator(ctx, cell)
}

// Stats returns document-frequency and posting-count metadata for term.
func (idx *dedicatedInvertedIndex) Stats(ctx context.Context, term string) (invertedPostingStats, error) {
	page, err := idx.findEntryLeafPage(ctx, term)
	if err != nil {
		return invertedPostingStats{}, err
	}
	cellIdx, found := findInvertedEntryCell(page.InvertedEntryPage.Cells, term)
	if !found {
		return invertedPostingStats{}, nil
	}
	cell := page.InvertedEntryPage.Cells[cellIdx]
	return invertedPostingStats{
		DocFreq:      cell.DocFreq,
		PostingCount: cell.PostingCount,
	}, nil
}

func (idx *dedicatedInvertedIndex) CountDocFreq(ctx context.Context, term string) (uint32, error) {
	stats, err := idx.Stats(ctx, term)
	if err != nil {
		return 0, err
	}
	return stats.DocFreq, nil
}

func (idx *dedicatedInvertedIndex) LoadRowIDs(ctx context.Context, term string, hint uint32) ([]RowID, error) {
	if idx.mode != invertedPostingModeRowIDs {
		return nil, fmt.Errorf("inverted index %s uses posting mode %d", idx.name, idx.mode)
	}
	iter, err := idx.Lookup(ctx, term)
	if err != nil {
		return nil, err
	}
	return collectRowIDsFromIterator(ctx, iter, idx.mode, hint)
}

func collectRowIDsFromIterator(
	ctx context.Context,
	iter invertedPostingIterator,
	expectedMode invertedPostingMode,
	hint uint32,
) ([]RowID, error) {
	rowIDs := make([]RowID, 0, hint)
	var (
		lastRowID RowID
		haveLast  bool
	)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return rowIDs, nil
		}
		mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
			if haveLast && rowID == lastRowID {
				return nil
			}
			haveLast = true
			lastRowID = rowID
			rowIDs = append(rowIDs, rowID)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if mode != expectedMode {
			return nil, fmt.Errorf("inverted posting block uses posting mode %d, expected %d", mode, expectedMode)
		}
	}
}

// FreeAll releases every entry and posting page owned by the index.
func (idx *dedicatedInvertedIndex) FreeAll(ctx context.Context) error {
	stack := []PageIndex{idx.rootPageIdx}
	for len(stack) > 0 {
		pageIdx := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted entry page %d for free: %w", pageIdx, err)
		}
		if page.InvertedEntryPage == nil {
			return fmt.Errorf("inverted entry page %d is not an entry page", pageIdx)
		}
		entryPage := page.InvertedEntryPage
		if entryPage.Header.IsLeaf {
			for _, cell := range entryPage.Cells {
				if err := idx.freePostingTree(ctx, cell); err != nil {
					return err
				}
			}
		} else {
			stack = append(stack, invertedEntryChildren(entryPage)...)
		}
		if err := idx.pager.AddFreePage(ctx, pageIdx); err != nil {
			return fmt.Errorf("free inverted entry page %d: %w", pageIdx, err)
		}
	}
	return nil
}

// readRootEntryPage reads and validates the entry-tree root page.
func (idx *dedicatedInvertedIndex) readRootEntryPage(ctx context.Context) (*Page, error) {
	page, err := idx.pager.ReadPage(ctx, idx.rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted index root: %w", err)
	}
	if page.InvertedEntryPage == nil {
		return nil, fmt.Errorf("inverted index %s root page is not an entry page", idx.name)
	}
	return page, nil
}

// modifyRootEntryPage returns the root entry page in the transaction write set.
func (idx *dedicatedInvertedIndex) modifyRootEntryPage(ctx context.Context) (*Page, error) {
	page, err := idx.pager.ModifyPage(ctx, idx.rootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("modify inverted index root: %w", err)
	}
	if page.InvertedEntryPage == nil {
		page.Clear()
		page.InvertedEntryPage = NewInvertedEntryPage(true)
	}
	return page, nil
}

// findEntryLeafPage descends the entry B+ tree to the leaf that should contain term.
func (idx *dedicatedInvertedIndex) findEntryLeafPage(ctx context.Context, term string) (*Page, error) {
	page, err := idx.readRootEntryPage(ctx)
	if err != nil {
		return nil, err
	}
	for {
		entryPage := page.InvertedEntryPage
		if entryPage == nil {
			return nil, fmt.Errorf("inverted entry page %d is not an entry page", page.Index)
		}
		if entryPage.Header.IsLeaf {
			return page, nil
		}
		childIdx, err := invertedEntryChild(entryPage, term)
		if err != nil {
			return nil, err
		}
		page, err = idx.pager.ReadPage(ctx, childIdx)
		if err != nil {
			return nil, fmt.Errorf("read inverted entry child %d: %w", childIdx, err)
		}
	}
}

// insertEntry recursively inserts term postings and returns a split separator if needed.
func (idx *dedicatedInvertedIndex) insertEntry(ctx context.Context, pageIdx PageIndex, term string, postings []invertedPosting) (string, PageIndex, bool, error) {
	page, err := idx.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return "", 0, false, fmt.Errorf("read inverted entry page %d: %w", pageIdx, err)
	}
	if page.InvertedEntryPage == nil {
		return "", 0, false, fmt.Errorf("inverted entry page %d is not an entry page", pageIdx)
	}
	if page.InvertedEntryPage.Header.IsLeaf {
		return idx.insertEntryLeaf(ctx, pageIdx, term, postings)
	}
	return idx.insertEntryInternal(ctx, pageIdx, term, postings)
}

// insertEntryLeaf inserts or updates one leaf cell, splitting the leaf on overflow.
func (idx *dedicatedInvertedIndex) insertEntryLeaf(ctx context.Context, pageIdx PageIndex, term string, postings []invertedPosting) (string, PageIndex, bool, error) {
	page, err := idx.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return "", 0, false, fmt.Errorf("modify inverted entry leaf %d: %w", pageIdx, err)
	}
	entryPage := page.InvertedEntryPage
	if entryPage == nil || !entryPage.Header.IsLeaf {
		return "", 0, false, fmt.Errorf("inverted entry page %d is not a leaf", pageIdx)
	}

	cellIdx, found := findInvertedEntryCell(entryPage.Cells, term)
	var allPostings []invertedPosting
	if found {
		oldCell := entryPage.Cells[cellIdx]
		if oldCell.PostingKind == invertedPostingKindTree && len(postings) == 1 {
			updatedCell, mutated, err := idx.insertPostingIntoTreeCell(ctx, oldCell, postings[0])
			if err != nil {
				return "", 0, false, err
			}
			if mutated {
				entryPage.Cells[cellIdx] = updatedCell
				return "", 0, false, nil
			}
		}
		existingPostings, err := idx.decodeInvertedEntryCell(ctx, oldCell)
		if err != nil {
			return "", 0, false, err
		}
		allPostings = append(existingPostings, postings...)
	} else {
		allPostings = postings // groupInvertedPostings copies internally
	}

	cell, err := idx.makeInvertedEntryCell(ctx, term, allPostings, entryPage.Cells[cellIdx:cellIdx+boolToInt(found)])
	if err != nil {
		return "", 0, false, err
	}
	if found {
		entryPage.Cells[cellIdx] = cell
	} else {
		entryPage.Cells = slices.Insert(entryPage.Cells, cellIdx, cell)
	}

	if err := ensureInvertedEntryPageFits(entryPage, invertedPageBodySize(pageIdx)); err == nil {
		return "", 0, false, nil
	}
	separator, rightPageIdx, err := idx.splitEntryLeaf(ctx, page)
	if err != nil {
		return "", 0, false, err
	}
	return separator, rightPageIdx, true, nil
}

// insertEntryInternal descends into a child and absorbs child splits.
func (idx *dedicatedInvertedIndex) insertEntryInternal(ctx context.Context, pageIdx PageIndex, term string, postings []invertedPosting) (string, PageIndex, bool, error) {
	page, err := idx.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return "", 0, false, fmt.Errorf("read inverted entry internal %d: %w", pageIdx, err)
	}
	entryPage := page.InvertedEntryPage
	if entryPage == nil || entryPage.Header.IsLeaf {
		return "", 0, false, fmt.Errorf("inverted entry page %d is not an internal page", pageIdx)
	}
	childPos, childIdx, err := invertedEntryChildAt(entryPage, term)
	if err != nil {
		return "", 0, false, err
	}

	separator, rightChildIdx, split, err := idx.insertEntry(ctx, childIdx, term, postings)
	if err != nil || !split {
		return "", 0, false, err
	}

	page, err = idx.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return "", 0, false, fmt.Errorf("modify inverted entry internal %d: %w", pageIdx, err)
	}
	entryPage = page.InvertedEntryPage
	if childPos < len(entryPage.Cells) {
		entryPage.Cells[childPos].Child = rightChildIdx
	} else {
		entryPage.Header.RightChild = rightChildIdx
	}
	entryPage.Cells = slices.Insert(entryPage.Cells, childPos, invertedEntryCell{
		Term:  separator,
		Child: childIdx,
	})

	if err := ensureInvertedEntryPageFits(entryPage, invertedPageBodySize(pageIdx)); err == nil {
		return "", 0, false, nil
	}
	separator, rightPageIdx, err := idx.splitEntryInternal(ctx, page)
	if err != nil {
		return "", 0, false, err
	}
	return separator, rightPageIdx, true, nil
}

// splitRootEntryPage promotes a root split by making the root an internal page.
func (idx *dedicatedInvertedIndex) splitRootEntryPage(ctx context.Context, separator string, rightPageIdx PageIndex) error {
	rootPage, err := idx.modifyRootEntryPage(ctx)
	if err != nil {
		return err
	}
	leftEntryPage := rootPage.InvertedEntryPage.Clone()

	leftPage, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("allocate inverted entry left root child: %w", err)
	}
	leftPage.Clear()
	leftEntryPage.Header.Parent = idx.rootPageIdx
	leftPage.InvertedEntryPage = leftEntryPage
	if err := idx.updateEntryChildrenParent(ctx, leftPage); err != nil {
		return err
	}

	rightPage, err := idx.pager.ModifyPage(ctx, rightPageIdx)
	if err != nil {
		return fmt.Errorf("modify inverted entry right root child: %w", err)
	}
	rightPage.InvertedEntryPage.Header.Parent = idx.rootPageIdx

	rootEntryPage := NewInvertedEntryPage(false)
	rootEntryPage.Cells = []invertedEntryCell{{
		Term:  separator,
		Child: leftPage.Index,
	}}
	rootEntryPage.Header.RightChild = rightPageIdx
	rootPage.Clear()
	rootPage.InvertedEntryPage = rootEntryPage
	return ensureInvertedEntryPageFits(rootEntryPage, invertedPageBodySize(rootPage.Index))
}

// splitEntryLeaf splits a leaf page and links the new right leaf into the leaf chain.
func (idx *dedicatedInvertedIndex) splitEntryLeaf(ctx context.Context, page *Page) (string, PageIndex, error) {
	entryPage := page.InvertedEntryPage
	splitIdx, err := chooseInvertedEntrySplitIndex(entryPage.Cells)
	if err != nil {
		return "", 0, err
	}

	rightPage, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("allocate inverted entry right leaf: %w", err)
	}
	rightEntryPage := NewInvertedEntryPage(true)
	rightEntryPage.Header.Parent = entryPage.Header.Parent
	rightEntryPage.Header.NextLeaf = entryPage.Header.NextLeaf
	rightEntryPage.Cells = cloneInvertedEntryCells(entryPage.Cells[splitIdx:])

	entryPage.Cells = cloneInvertedEntryCells(entryPage.Cells[:splitIdx])
	entryPage.Header.NextLeaf = rightPage.Index
	rightPage.Clear()
	rightPage.InvertedEntryPage = rightEntryPage
	if err := ensureInvertedEntryPageFits(entryPage, invertedPageBodySize(page.Index)); err != nil {
		return "", 0, err
	}
	if err := ensureInvertedEntryPageFits(rightEntryPage, invertedPageBodySize(rightPage.Index)); err != nil {
		return "", 0, err
	}
	return rightEntryPage.Cells[0].Term, rightPage.Index, nil
}

// splitEntryInternal splits an internal routing page and promotes the middle separator.
func (idx *dedicatedInvertedIndex) splitEntryInternal(ctx context.Context, page *Page) (string, PageIndex, error) {
	entryPage := page.InvertedEntryPage
	if len(entryPage.Cells) < 2 {
		return "", 0, fmt.Errorf("cannot split inverted internal page with %d cells", len(entryPage.Cells))
	}
	splitIdx := len(entryPage.Cells) / 2
	promoted := entryPage.Cells[splitIdx]

	rightPage, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("allocate inverted entry right internal: %w", err)
	}
	rightEntryPage := NewInvertedEntryPage(false)
	rightEntryPage.Header.Parent = entryPage.Header.Parent
	rightEntryPage.Header.RightChild = entryPage.Header.RightChild
	rightEntryPage.Cells = cloneInvertedEntryCells(entryPage.Cells[splitIdx+1:])

	entryPage.Header.RightChild = promoted.Child
	entryPage.Cells = cloneInvertedEntryCells(entryPage.Cells[:splitIdx])
	rightPage.Clear()
	rightPage.InvertedEntryPage = rightEntryPage
	if err := idx.updateEntryChildrenParent(ctx, rightPage); err != nil {
		return "", 0, err
	}
	if err := ensureInvertedEntryPageFits(entryPage, invertedPageBodySize(page.Index)); err != nil {
		return "", 0, err
	}
	if err := ensureInvertedEntryPageFits(rightEntryPage, invertedPageBodySize(rightPage.Index)); err != nil {
		return "", 0, err
	}
	return promoted.Term, rightPage.Index, nil
}

// updateEntryChildrenParent rewrites all child parent pointers for an internal page.
func (idx *dedicatedInvertedIndex) updateEntryChildrenParent(ctx context.Context, page *Page) error {
	entryPage := page.InvertedEntryPage
	if entryPage == nil || entryPage.Header.IsLeaf {
		return nil
	}
	for _, childIdx := range invertedEntryChildren(entryPage) {
		childPage, err := idx.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("modify inverted entry child %d parent: %w", childIdx, err)
		}
		if childPage.InvertedEntryPage == nil {
			return fmt.Errorf("inverted entry child %d is not an entry page", childIdx)
		}
		childPage.InvertedEntryPage.Header.Parent = page.Index
	}
	return nil
}

// findInvertedEntryCell binary-searches sorted entry cells by term.
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

// invertedEntryChild returns the child page that should contain term.
func invertedEntryChild(page *invertedEntryPage, term string) (PageIndex, error) {
	_, childIdx, err := invertedEntryChildAt(page, term)
	return childIdx, err
}

// invertedEntryChildAt returns both child slot and page for a search term.
func invertedEntryChildAt(page *invertedEntryPage, term string) (int, PageIndex, error) {
	if page.Header.IsLeaf {
		return 0, 0, fmt.Errorf("inverted entry page is a leaf")
	}
	for i, cell := range page.Cells {
		if term < cell.Term {
			return i, cell.Child, nil
		}
	}
	if page.Header.RightChild == 0 {
		return 0, 0, fmt.Errorf("inverted entry internal page has no right child")
	}
	return len(page.Cells), page.Header.RightChild, nil
}

// invertedEntryChildren returns internal-page children in left-to-right order.
func invertedEntryChildren(page *invertedEntryPage) []PageIndex {
	children := make([]PageIndex, 0, len(page.Cells)+1)
	for _, cell := range page.Cells {
		children = append(children, cell.Child)
	}
	if page.Header.RightChild != 0 {
		children = append(children, page.Header.RightChild)
	}
	return children
}

// findInvertedEntryChildPosition finds a child pointer inside an internal page.
func findInvertedEntryChildPosition(page *invertedEntryPage, childIdx PageIndex) (int, bool) {
	for i, cell := range page.Cells {
		if cell.Child == childIdx {
			return i, true
		}
	}
	if page.Header.RightChild == childIdx {
		return len(page.Cells), true
	}
	return 0, false
}

// firstInvertedEntryTerm returns the first separator/data term in a page.
func firstInvertedEntryTerm(page *invertedEntryPage) string {
	if len(page.Cells) == 0 {
		return ""
	}
	return page.Cells[0].Term
}

// chooseInvertedEntrySplitIndex chooses a roughly byte-balanced split point.
func chooseInvertedEntrySplitIndex(cells []invertedEntryCell) (int, error) {
	if len(cells) < 2 {
		return 0, fmt.Errorf("cannot split inverted entry page with %d cells", len(cells))
	}
	total := uint64(0)
	for _, cell := range cells {
		total += cell.size() + 2
	}
	half := total / 2
	var left uint64
	for i, cell := range cells {
		if i > 0 && left >= half {
			return i, nil
		}
		left += cell.size() + 2
	}
	return len(cells) / 2, nil
}

// makeInvertedEntryCell builds either an inline entry or a posting-tree entry.
func (idx *dedicatedInvertedIndex) makeInvertedEntryCell(
	ctx context.Context,
	term string,
	postings []invertedPosting,
	oldCells []invertedEntryCell,
) (invertedEntryCell, error) {
	grouped := groupInvertedPostingsInPlace(idx.mode, postings)
	payload, err := encodeGroupedInvertedPostingList(idx.mode, grouped)
	if err != nil {
		return invertedEntryCell{}, err
	}
	if len(payload) > invertedInlinePostingPayloadMax {
		child, err := idx.writePostingTree(ctx, grouped)
		if err != nil {
			return invertedEntryCell{}, err
		}
		for _, oldCell := range oldCells {
			if err := idx.freePostingTree(ctx, oldCell); err != nil {
				return invertedEntryCell{}, err
			}
		}
		return invertedEntryCell{
			Term:         term,
			DocFreq:      uint32(len(grouped)),
			PostingCount: countInvertedPostings(idx.mode, grouped),
			Child:        child,
			PostingKind:  invertedPostingKindTree,
			CodecVersion: invertedPostingCodecVersion,
		}, nil
	}
	for _, oldCell := range oldCells {
		if err := idx.freePostingTree(ctx, oldCell); err != nil {
			return invertedEntryCell{}, err
		}
	}
	return invertedEntryCell{
		Term:         term,
		Payload:      payload,
		DocFreq:      uint32(len(grouped)),
		PostingCount: countInvertedPostings(idx.mode, grouped),
		PostingKind:  invertedPostingKindInline,
		CodecVersion: invertedPostingCodecVersion,
	}, nil
}

// decodeInvertedEntryCell returns all postings referenced by an entry cell.
func (idx *dedicatedInvertedIndex) decodeInvertedEntryCell(ctx context.Context, cell invertedEntryCell) ([]invertedPosting, error) {
	switch cell.PostingKind {
	case invertedPostingKindInline:
		return decodeInlineInvertedEntryCell(idx.mode, cell)
	case invertedPostingKindTree:
		return idx.readPostingTree(ctx, cell)
	default:
		return nil, fmt.Errorf("inverted entry term %q uses unsupported posting kind %d", cell.Term, cell.PostingKind)
	}
}

// decodeInlineInvertedEntryCell decodes and validates an inline posting payload.
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

// readPostingTree materializes postings from a posting tree.
func (idx *dedicatedInvertedIndex) readPostingTree(ctx context.Context, cell invertedEntryCell) ([]invertedPosting, error) {
	if cell.CodecVersion != invertedPostingCodecVersion {
		return nil, fmt.Errorf("inverted entry term %q uses unsupported posting codec version %d", cell.Term, cell.CodecVersion)
	}
	iter := &postingTreeInvertedPostingIterator{
		pager:    idx.pager,
		nextPage: cell.Child,
	}
	var postings []invertedPosting
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if block.CodecVersion != invertedPostingCodecVersion {
			return nil, fmt.Errorf("inverted posting block uses unsupported codec version %d", block.CodecVersion)
		}
		mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
		if err != nil {
			return nil, err
		}
		if mode != idx.mode {
			return nil, fmt.Errorf("inverted posting tree uses posting mode %d, expected %d", mode, idx.mode)
		}
		postings = append(postings, blockPostings...)
	}
	return postings, nil
}

// insertPostingIntoTreeCell updates one existing posting-tree leaf block in place.
func (idx *dedicatedInvertedIndex) insertPostingIntoTreeCell(ctx context.Context, cell invertedEntryCell, posting invertedPosting) (invertedEntryCell, bool, error) {
	leaf, blockIdx, err := idx.findPostingLeafBlock(ctx, cell.Child, posting.RowID)
	if err != nil {
		return invertedEntryCell{}, false, err
	}
	if blockIdx < 0 {
		return invertedEntryCell{}, false, nil
	}

	block := leaf.InvertedPostPage.Blocks[blockIdx]
	if updatedCell, ok, err := idx.appendPostingToTreeBlock(ctx, cell, leaf, blockIdx, block, posting); ok || err != nil {
		return updatedCell, ok, err
	}

	mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
	if err != nil {
		return invertedEntryCell{}, false, err
	}
	if mode != idx.mode {
		return invertedEntryCell{}, false, fmt.Errorf("inverted posting block uses posting mode %d, expected %d", mode, idx.mode)
	}

	oldDocFreq := len(blockPostings)
	oldPostingCount := countInvertedPostings(idx.mode, blockPostings)
	updatedPostings := groupInvertedPostingsInPlace(idx.mode, append(blockPostings, posting))
	newDocFreq := len(updatedPostings)
	newPostingCount := countInvertedPostings(idx.mode, updatedPostings)
	if oldDocFreq == newDocFreq && oldPostingCount == newPostingCount {
		return cell, true, nil
	}

	replacementBlocks, err := makeInvertedPostingBlocks(idx.mode, updatedPostings)
	if err != nil {
		return invertedEntryCell{}, false, err
	}
	candidate := leaf.InvertedPostPage.Clone()
	candidate.Blocks = replaceInvertedPostingBlocks(candidate.Blocks, blockIdx, replacementBlocks)
	fits := ensureInvertedPostingPageFits(candidate, invertedPageBodySize(leaf.Index)) == nil
	if fits {
		page, err := idx.pager.ModifyPage(ctx, leaf.Index)
		if err != nil {
			return invertedEntryCell{}, false, fmt.Errorf("modify inverted posting leaf %d: %w", leaf.Index, err)
		}
		page.InvertedPostPage = candidate
		if err := idx.refreshPostingAncestors(ctx, page.Index); err != nil {
			return invertedEntryCell{}, false, err
		}
	} else {
		newRoot, err := idx.splitPostingLeafForBlocks(ctx, cell.Child, leaf, blockIdx, replacementBlocks)
		if err != nil {
			return invertedEntryCell{}, false, err
		}
		cell.Child = newRoot
	}

	updatedCell := cell
	updatedCell.DocFreq = uint32(int(updatedCell.DocFreq) + newDocFreq - oldDocFreq)
	updatedCell.PostingCount = uint32(int(updatedCell.PostingCount) + int(newPostingCount) - int(oldPostingCount))
	return updatedCell, true, nil
}

// appendPostingToTreeBlock extends the rightmost matching posting block without
// decoding/re-encoding it. It handles the common INSERT/CREATE INDEX case where
// row IDs arrive in increasing order.
func (idx *dedicatedInvertedIndex) appendPostingToTreeBlock(
	ctx context.Context,
	cell invertedEntryCell,
	leaf *Page,
	blockIdx int,
	block invertedPostingBlock,
	posting invertedPosting,
) (invertedEntryCell, bool, error) {
	if posting.RowID <= block.LastRowID {
		return invertedEntryCell{}, false, nil
	}
	if idx.mode == invertedPostingModePositions && len(posting.Positions) == 0 {
		return invertedEntryCell{}, false, nil
	}

	appendPayload, postingCount := encodeTrailingInvertedPosting(idx.mode, block.LastRowID, posting)
	if len(block.Payload)+len(appendPayload) > invertedPostingBlockPayloadMax {
		return invertedEntryCell{}, false, nil
	}
	if invertedPostingPageUsedBytes(leaf.InvertedPostPage)+uint64(len(appendPayload)) > uint64(invertedPageBodySize(leaf.Index)) {
		return invertedEntryCell{}, false, nil
	}

	page, err := idx.pager.ModifyPage(ctx, leaf.Index)
	if err != nil {
		return invertedEntryCell{}, false, fmt.Errorf("modify inverted posting leaf %d: %w", leaf.Index, err)
	}
	postingPage := page.InvertedPostPage
	if postingPage == nil || postingPage.Header.Level != 0 || blockIdx >= len(postingPage.Blocks) {
		return invertedEntryCell{}, false, fmt.Errorf("inverted posting page %d changed during append", leaf.Index)
	}

	block = postingPage.Blocks[blockIdx]
	block.Payload = append(block.Payload, appendPayload...)
	block.LastRowID = posting.RowID
	block.PostingCount += postingCount
	postingPage.Blocks[blockIdx] = block

	if err := idx.refreshPostingAncestors(ctx, page.Index); err != nil {
		return invertedEntryCell{}, false, err
	}
	updatedCell := cell
	updatedCell.DocFreq += 1
	updatedCell.PostingCount += postingCount
	return updatedCell, true, nil
}

// replacePostingInTreeCell updates one posting-tree leaf block by removing an
// old posting and adding its replacement in a single page mutation.
func (idx *dedicatedInvertedIndex) replacePostingInTreeCell(
	ctx context.Context,
	cell invertedEntryCell,
	oldPosting, newPosting invertedPosting,
) (invertedEntryCell, bool, error) {
	if oldPosting.RowID != newPosting.RowID {
		return invertedEntryCell{}, false, nil
	}
	leaf, blockIdx, err := idx.findPostingLeafBlock(ctx, cell.Child, oldPosting.RowID)
	if err != nil {
		return invertedEntryCell{}, false, err
	}
	if blockIdx < 0 {
		return invertedEntryCell{}, false, nil
	}

	block := leaf.InvertedPostPage.Blocks[blockIdx]
	mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
	if err != nil {
		return invertedEntryCell{}, false, err
	}
	if mode != idx.mode {
		return invertedEntryCell{}, false, fmt.Errorf("inverted posting block uses posting mode %d, expected %d", mode, idx.mode)
	}

	oldDocFreq := len(blockPostings)
	oldPostingCount := countInvertedPostings(idx.mode, blockPostings)
	updatedPostings := removeInvertedPosting(idx.mode, blockPostings, oldPosting)
	updatedPostings = groupInvertedPostingsInPlace(idx.mode, append(updatedPostings, newPosting))
	newDocFreq := len(updatedPostings)
	newPostingCount := countInvertedPostings(idx.mode, updatedPostings)

	replacementBlocks, err := makeInvertedPostingBlocks(idx.mode, updatedPostings)
	if err != nil {
		return invertedEntryCell{}, false, err
	}
	candidate := leaf.InvertedPostPage.Clone()
	candidate.Blocks = replaceInvertedPostingBlocks(candidate.Blocks, blockIdx, replacementBlocks)
	fits := ensureInvertedPostingPageFits(candidate, invertedPageBodySize(leaf.Index)) == nil
	if fits {
		page, err := idx.pager.ModifyPage(ctx, leaf.Index)
		if err != nil {
			return invertedEntryCell{}, false, fmt.Errorf("modify inverted posting leaf %d: %w", leaf.Index, err)
		}
		page.InvertedPostPage = candidate
		if err := idx.refreshPostingAncestors(ctx, page.Index); err != nil {
			return invertedEntryCell{}, false, err
		}
	} else {
		newRoot, err := idx.splitPostingLeafForBlocks(ctx, cell.Child, leaf, blockIdx, replacementBlocks)
		if err != nil {
			return invertedEntryCell{}, false, err
		}
		cell.Child = newRoot
	}

	updatedCell := cell
	updatedCell.DocFreq = uint32(int(updatedCell.DocFreq) + newDocFreq - oldDocFreq)
	updatedCell.PostingCount = uint32(int(updatedCell.PostingCount) + int(newPostingCount) - int(oldPostingCount))
	return updatedCell, true, nil
}

// splitPostingLeafForBlocks replaces one leaf block, splits leaf pages as needed, and routes new pages upward.
func (idx *dedicatedInvertedIndex) splitPostingLeafForBlocks(ctx context.Context, rootIdx PageIndex, leaf *Page, blockIdx int, replacementBlocks []invertedPostingBlock) (PageIndex, error) {
	allBlocks := replaceInvertedPostingBlocks(leaf.InvertedPostPage.Blocks, blockIdx, replacementBlocks)
	blockGroups, err := groupInvertedPostingBlocksIntoPages(allBlocks, 0)
	if err != nil {
		return 0, err
	}
	if len(blockGroups) == 1 {
		page, err := idx.pager.ModifyPage(ctx, leaf.Index)
		if err != nil {
			return 0, fmt.Errorf("modify inverted posting leaf %d: %w", leaf.Index, err)
		}
		page.InvertedPostPage.Blocks = blockGroups[0]
		return rootIdx, idx.refreshPostingAncestors(ctx, page.Index)
	}

	newPages, err := idx.allocatePostingPages(ctx, len(blockGroups)-1)
	if err != nil {
		return 0, err
	}
	parentIdx := leaf.InvertedPostPage.Header.Parent
	oldNextLeaf := leaf.InvertedPostPage.Header.NextLeaf
	pages := make([]*Page, 0, len(blockGroups))

	current, err := idx.pager.ModifyPage(ctx, leaf.Index)
	if err != nil {
		return 0, fmt.Errorf("modify inverted posting leaf %d: %w", leaf.Index, err)
	}
	currentPage := NewInvertedPostingPage(0)
	currentPage.Header.Parent = parentIdx
	currentPage.Blocks = blockGroups[0]
	current.InvertedPostPage = currentPage
	pages = append(pages, current)

	for i, page := range newPages {
		page.Clear()
		postingPage := NewInvertedPostingPage(0)
		postingPage.Header.Parent = parentIdx
		postingPage.Blocks = blockGroups[i+1]
		page.InvertedPostPage = postingPage
		pages = append(pages, page)
	}
	for i, page := range pages {
		if i+1 < len(pages) {
			page.InvertedPostPage.Header.NextLeaf = pages[i+1].Index
		} else {
			page.InvertedPostPage.Header.NextLeaf = oldNextLeaf
		}
		if err := ensureInvertedPostingPageFits(page.InvertedPostPage, invertedPageBodySize(page.Index)); err != nil {
			return 0, err
		}
	}

	if parentIdx == 0 {
		return idx.createPostingRoot(ctx, pages)
	}
	if err := idx.refreshPostingAncestors(ctx, current.Index); err != nil {
		return 0, err
	}
	return idx.insertPostingRoutingBlocks(ctx, rootIdx, parentIdx, current.Index, pages[1:])
}

// insertPostingRoutingBlocks adds routing blocks for split child pages into an internal page.
func (idx *dedicatedInvertedIndex) insertPostingRoutingBlocks(ctx context.Context, rootIdx, parentIdx, afterChildIdx PageIndex, newChildren []*Page) (PageIndex, error) {
	parent, err := idx.pager.ModifyPage(ctx, parentIdx)
	if err != nil {
		return 0, fmt.Errorf("modify inverted posting parent %d: %w", parentIdx, err)
	}
	if parent.InvertedPostPage == nil || parent.InvertedPostPage.Header.Level == 0 {
		return 0, fmt.Errorf("inverted posting parent %d is not an internal posting page", parentIdx)
	}

	insertAt := -1
	first, last, count, err := idx.postingPageRangeByIndex(ctx, afterChildIdx)
	if err != nil {
		return 0, err
	}
	for i, block := range parent.InvertedPostPage.Blocks {
		if block.Child != afterChildIdx {
			continue
		}
		parent.InvertedPostPage.Blocks[i].FirstRowID = first
		parent.InvertedPostPage.Blocks[i].LastRowID = last
		parent.InvertedPostPage.Blocks[i].PostingCount = count
		insertAt = i + 1
		break
	}
	if insertAt < 0 {
		return 0, fmt.Errorf("inverted posting child %d not found in parent %d", afterChildIdx, parentIdx)
	}

	routingBlocks := make([]invertedPostingBlock, 0, len(newChildren))
	for _, child := range newChildren {
		child.InvertedPostPage.Header.Parent = parentIdx
		block, err := idx.routingBlockForPostingPage(child)
		if err != nil {
			return 0, err
		}
		routingBlocks = append(routingBlocks, block)
	}
	parent.InvertedPostPage.Blocks = slices.Insert(parent.InvertedPostPage.Blocks, insertAt, routingBlocks...)
	if err := ensureInvertedPostingPageFits(parent.InvertedPostPage, invertedPageBodySize(parent.Index)); err == nil {
		if err := idx.refreshPostingAncestors(ctx, parent.Index); err != nil {
			return 0, err
		}
		return rootIdx, nil
	}
	return idx.splitPostingInternalPage(ctx, rootIdx, parent)
}

// splitPostingInternalPage splits an overfull posting internal page and propagates routing upward.
func (idx *dedicatedInvertedIndex) splitPostingInternalPage(ctx context.Context, rootIdx PageIndex, page *Page) (PageIndex, error) {
	postingPage := page.InvertedPostPage
	blockGroups, err := groupInvertedPostingBlocksIntoPages(postingPage.Blocks, postingPage.Header.Level)
	if err != nil {
		return 0, err
	}
	if len(blockGroups) == 1 {
		return rootIdx, nil
	}

	newPages, err := idx.allocatePostingPages(ctx, len(blockGroups)-1)
	if err != nil {
		return 0, err
	}
	parentIdx := postingPage.Header.Parent
	pages := make([]*Page, 0, len(blockGroups))

	current := page
	currentPage := NewInvertedPostingPage(postingPage.Header.Level)
	currentPage.Header.Parent = parentIdx
	currentPage.Blocks = blockGroups[0]
	current.InvertedPostPage = currentPage
	pages = append(pages, current)

	for i, newPage := range newPages {
		newPage.Clear()
		childPage := NewInvertedPostingPage(postingPage.Header.Level)
		childPage.Header.Parent = parentIdx
		childPage.Blocks = blockGroups[i+1]
		newPage.InvertedPostPage = childPage
		pages = append(pages, newPage)
	}
	for _, childPage := range pages {
		if err := idx.updatePostingChildrenParent(ctx, childPage); err != nil {
			return 0, err
		}
		if err := ensureInvertedPostingPageFits(childPage.InvertedPostPage, invertedPageBodySize(childPage.Index)); err != nil {
			return 0, err
		}
	}

	if parentIdx == 0 {
		return idx.createPostingRoot(ctx, pages)
	}
	if err := idx.refreshPostingAncestors(ctx, current.Index); err != nil {
		return 0, err
	}
	return idx.insertPostingRoutingBlocks(ctx, rootIdx, parentIdx, current.Index, pages[1:])
}

// createPostingRoot builds a new posting-tree root above split child pages.
func (idx *dedicatedInvertedIndex) createPostingRoot(ctx context.Context, children []*Page) (PageIndex, error) {
	pages := children
	var err error
	for len(pages) > 1 {
		pages, err = idx.writePostingInternalLevel(ctx, pages)
		if err != nil {
			return 0, err
		}
	}
	pages[0].InvertedPostPage.Header.Parent = 0
	return pages[0].Index, nil
}

// updatePostingChildrenParent points every child of an internal posting page back to that page.
func (idx *dedicatedInvertedIndex) updatePostingChildrenParent(ctx context.Context, page *Page) error {
	if page.InvertedPostPage == nil || page.InvertedPostPage.Header.Level == 0 {
		return nil
	}
	for _, block := range page.InvertedPostPage.Blocks {
		child, err := idx.pager.ModifyPage(ctx, block.Child)
		if err != nil {
			return fmt.Errorf("modify inverted posting child %d parent: %w", block.Child, err)
		}
		if child.InvertedPostPage == nil {
			return fmt.Errorf("inverted posting child %d is not a posting page", block.Child)
		}
		child.InvertedPostPage.Header.Parent = page.Index
	}
	return nil
}

// deletePostingFromTreeCell removes one posting from an existing posting-tree leaf block.
func (idx *dedicatedInvertedIndex) deletePostingFromTreeCell(
	ctx context.Context,
	cell invertedEntryCell,
	posting invertedPosting,
) (deletePostingTreeCellResult, error) {
	leaf, blockIdx, err := idx.findPostingLeafBlock(ctx, cell.Child, posting.RowID)
	if err != nil {
		return deletePostingTreeCellResult{}, err
	}
	if blockIdx < 0 {
		return deletePostingTreeCellResult{Cell: cell, Mutated: true}, nil
	}

	block := leaf.InvertedPostPage.Blocks[blockIdx]
	mode, blockPostings, err := decodeInvertedPostingList(block.Payload)
	if err != nil {
		return deletePostingTreeCellResult{}, err
	}
	if mode != idx.mode {
		return deletePostingTreeCellResult{}, fmt.Errorf("inverted posting block uses posting mode %d, expected %d", mode, idx.mode)
	}

	oldDocFreq := len(blockPostings)
	oldPostingCount := countInvertedPostings(idx.mode, blockPostings)
	updatedPostings := removeInvertedPosting(idx.mode, blockPostings, posting)
	newDocFreq := len(updatedPostings)
	newPostingCount := countInvertedPostings(idx.mode, updatedPostings)
	if oldDocFreq == newDocFreq && oldPostingCount == newPostingCount {
		return deletePostingTreeCellResult{Cell: cell, Mutated: true}, nil
	}
	nextPostingCount := int(cell.PostingCount) + int(newPostingCount) - int(oldPostingCount)
	if nextPostingCount <= 64 {
		postings, err := idx.readPostingTree(ctx, cell)
		if err != nil {
			return deletePostingTreeCellResult{}, err
		}
		postings = removeInvertedPosting(idx.mode, postings, posting)
		if len(postings) == 0 {
			if err := idx.freePostingTree(ctx, cell); err != nil {
				return deletePostingTreeCellResult{}, err
			}
			return deletePostingTreeCellResult{Mutated: true, RemoveEntry: true}, nil
		}
		updatedCell, err := idx.makeInvertedEntryCell(ctx, cell.Term, postings, []invertedEntryCell{cell})
		if err != nil {
			return deletePostingTreeCellResult{}, err
		}
		return deletePostingTreeCellResult{Cell: updatedCell, Mutated: true}, nil
	}

	candidate := leaf.InvertedPostPage.Clone()
	if len(updatedPostings) == 0 {
		if len(candidate.Blocks) == 1 {
			newRoot, removed, err := idx.removePostingLeafPage(ctx, cell.Child, leaf)
			if err != nil {
				return deletePostingTreeCellResult{}, err
			}
			if !removed {
				return deletePostingTreeCellResult{}, nil
			}
			updatedCell := cell
			updatedCell.Child = newRoot
			updatedCell.DocFreq = uint32(int(updatedCell.DocFreq) - oldDocFreq)
			updatedCell.PostingCount = uint32(nextPostingCount)
			return deletePostingTreeCellResult{Cell: updatedCell, Mutated: true}, nil
		}
		candidate.Blocks = slices.Delete(candidate.Blocks, blockIdx, blockIdx+1)
	} else {
		payload, err := encodeInvertedPostingList(idx.mode, updatedPostings)
		if err != nil {
			return deletePostingTreeCellResult{}, err
		}
		candidate.Blocks[blockIdx] = postingBlockFromPostings(idx.mode, updatedPostings, payload)
	}
	if err := ensureInvertedPostingPageFits(candidate, invertedPageBodySize(leaf.Index)); err != nil {
		return deletePostingTreeCellResult{}, err
	}

	page, err := idx.pager.ModifyPage(ctx, leaf.Index)
	if err != nil {
		return deletePostingTreeCellResult{}, fmt.Errorf("modify inverted posting leaf %d: %w", leaf.Index, err)
	}
	page.InvertedPostPage = candidate
	newRoot, err := idx.rebalancePostingPageAfterDelete(ctx, cell.Child, page.Index)
	if err != nil {
		return deletePostingTreeCellResult{}, err
	}

	updatedCell := cell
	updatedCell.Child = newRoot
	updatedCell.DocFreq = uint32(int(updatedCell.DocFreq) + newDocFreq - oldDocFreq)
	updatedCell.PostingCount = uint32(nextPostingCount)
	return deletePostingTreeCellResult{Cell: updatedCell, Mutated: true}, nil
}

// removePostingLeafPage unlinks an empty posting leaf and removes its parent route.
func (idx *dedicatedInvertedIndex) removePostingLeafPage(ctx context.Context, rootIdx PageIndex, leaf *Page) (PageIndex, bool, error) {
	if leaf.InvertedPostPage == nil || leaf.InvertedPostPage.Header.Level != 0 {
		return 0, false, fmt.Errorf("inverted posting page %d is not a leaf", leaf.Index)
	}
	parentIdx := leaf.InvertedPostPage.Header.Parent
	if parentIdx == 0 {
		return 0, false, nil
	}

	canRemove, err := idx.canRemovePostingChild(ctx, parentIdx, leaf.Index)
	if err != nil || !canRemove {
		return 0, false, err
	}

	previousLeaf, err := idx.findPreviousPostingLeaf(ctx, rootIdx, leaf.Index)
	if err != nil {
		return 0, false, err
	}
	if previousLeaf != nil {
		page, err := idx.pager.ModifyPage(ctx, previousLeaf.Index)
		if err != nil {
			return 0, false, fmt.Errorf("modify previous inverted posting leaf %d: %w", previousLeaf.Index, err)
		}
		page.InvertedPostPage.Header.NextLeaf = leaf.InvertedPostPage.Header.NextLeaf
	}

	newRoot, err := idx.removePostingChildRoute(ctx, rootIdx, parentIdx, leaf.Index)
	if err != nil {
		return 0, false, err
	}
	if err := idx.pager.AddFreePage(ctx, leaf.Index); err != nil {
		return 0, false, fmt.Errorf("free empty inverted posting leaf %d: %w", leaf.Index, err)
	}
	return newRoot, true, nil
}

// rebalancePostingPageAfterDelete repairs underfull posting pages after shrink.
func (idx *dedicatedInvertedIndex) rebalancePostingPageAfterDelete(ctx context.Context, rootIdx, pageIdx PageIndex) (PageIndex, error) {
	page, err := idx.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return 0, fmt.Errorf("modify inverted posting page %d after delete: %w", pageIdx, err)
	}
	if page.InvertedPostPage == nil {
		return 0, fmt.Errorf("inverted posting page %d is not a posting page", pageIdx)
	}
	if pageIdx == rootIdx {
		return idx.collapsePostingRoot(ctx, rootIdx)
	}
	if len(page.InvertedPostPage.Blocks) > 0 && !invertedPostingPageUnderfull(page.InvertedPostPage, invertedPageBodySize(pageIdx)) {
		return rootIdx, idx.refreshPostingAncestors(ctx, pageIdx)
	}

	parent, childPos, err := idx.modifyPostingParent(ctx, page)
	if err != nil {
		return 0, err
	}
	left, right, err := idx.postingSiblings(ctx, parent.InvertedPostPage, childPos)
	if err != nil {
		return 0, err
	}

	if left != nil && invertedPostingPageCanSpare(left.InvertedPostPage, invertedPageBodySize(left.Index)) {
		if err := idx.borrowPostingBlockFromLeft(ctx, page, left); err != nil {
			return 0, err
		}
		if err := idx.refreshPostingAncestors(ctx, left.Index); err != nil {
			return 0, err
		}
		return rootIdx, idx.refreshPostingAncestors(ctx, pageIdx)
	}
	if right != nil && invertedPostingPageCanSpare(right.InvertedPostPage, invertedPageBodySize(right.Index)) {
		if err := idx.borrowPostingBlockFromRight(ctx, page, right); err != nil {
			return 0, err
		}
		if err := idx.refreshPostingAncestors(ctx, right.Index); err != nil {
			return 0, err
		}
		return rootIdx, idx.refreshPostingAncestors(ctx, pageIdx)
	}

	switch {
	case right != nil:
		return idx.mergePostingPagesAndRemoveRoute(ctx, rootIdx, page, right)
	case left != nil:
		return idx.mergePostingPagesAndRemoveRoute(ctx, rootIdx, left, page)
	default:
		return rootIdx, nil
	}
}

// collapsePostingRoot replaces a single-child internal posting root with its child.
func (idx *dedicatedInvertedIndex) collapsePostingRoot(ctx context.Context, rootIdx PageIndex) (PageIndex, error) {
	root, err := idx.pager.ModifyPage(ctx, rootIdx)
	if err != nil {
		return 0, fmt.Errorf("modify inverted posting root %d: %w", rootIdx, err)
	}
	if root.InvertedPostPage == nil || root.InvertedPostPage.Header.Level == 0 || len(root.InvertedPostPage.Blocks) != 1 {
		return rootIdx, nil
	}
	childIdx := root.InvertedPostPage.Blocks[0].Child
	child, err := idx.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return 0, fmt.Errorf("modify inverted posting child %d for root collapse: %w", childIdx, err)
	}
	if child.InvertedPostPage == nil {
		return 0, fmt.Errorf("inverted posting child %d is not a posting page", childIdx)
	}
	child.InvertedPostPage.Header.Parent = 0
	if err := idx.pager.AddFreePage(ctx, rootIdx); err != nil {
		return 0, fmt.Errorf("free collapsed inverted posting root %d: %w", rootIdx, err)
	}
	return childIdx, nil
}

// modifyPostingParent returns a writable parent and this page's child slot.
func (idx *dedicatedInvertedIndex) modifyPostingParent(ctx context.Context, page *Page) (*Page, int, error) {
	parentIdx := page.InvertedPostPage.Header.Parent
	parent, err := idx.pager.ModifyPage(ctx, parentIdx)
	if err != nil {
		return nil, 0, fmt.Errorf("modify inverted posting parent %d: %w", parentIdx, err)
	}
	if parent.InvertedPostPage == nil || parent.InvertedPostPage.Header.Level == 0 {
		return nil, 0, fmt.Errorf("inverted posting parent %d is not an internal page", parentIdx)
	}
	for i, block := range parent.InvertedPostPage.Blocks {
		if block.Child == page.Index {
			return parent, i, nil
		}
	}
	return nil, 0, fmt.Errorf("inverted posting child %d not found in parent %d", page.Index, parentIdx)
}

// postingSiblings returns writable siblings around a child position.
func (idx *dedicatedInvertedIndex) postingSiblings(ctx context.Context, parent *invertedPostingPage, childPos int) (*Page, *Page, error) {
	var left, right *Page
	if childPos > 0 {
		page, err := idx.pager.ModifyPage(ctx, parent.Blocks[childPos-1].Child)
		if err != nil {
			return nil, nil, fmt.Errorf("modify left inverted posting sibling: %w", err)
		}
		left = page
	}
	if childPos+1 < len(parent.Blocks) {
		page, err := idx.pager.ModifyPage(ctx, parent.Blocks[childPos+1].Child)
		if err != nil {
			return nil, nil, fmt.Errorf("modify right inverted posting sibling: %w", err)
		}
		right = page
	}
	return left, right, nil
}

// borrowPostingBlockFromLeft moves the left sibling's last block into page.
func (idx *dedicatedInvertedIndex) borrowPostingBlockFromLeft(ctx context.Context, page, left *Page) error {
	moved := left.InvertedPostPage.Blocks[len(left.InvertedPostPage.Blocks)-1]
	left.InvertedPostPage.Blocks = left.InvertedPostPage.Blocks[:len(left.InvertedPostPage.Blocks)-1]
	page.InvertedPostPage.Blocks = slices.Insert(page.InvertedPostPage.Blocks, 0, moved)
	if page.InvertedPostPage.Header.Level > 0 {
		if err := idx.updateSinglePostingChildParent(ctx, moved.Child, page.Index); err != nil {
			return err
		}
	}
	if err := ensureInvertedPostingPageFits(left.InvertedPostPage, invertedPageBodySize(left.Index)); err != nil {
		return err
	}
	return ensureInvertedPostingPageFits(page.InvertedPostPage, invertedPageBodySize(page.Index))
}

// borrowPostingBlockFromRight moves the right sibling's first block into page.
func (idx *dedicatedInvertedIndex) borrowPostingBlockFromRight(ctx context.Context, page, right *Page) error {
	moved := right.InvertedPostPage.Blocks[0]
	right.InvertedPostPage.Blocks = slices.Delete(right.InvertedPostPage.Blocks, 0, 1)
	page.InvertedPostPage.Blocks = append(page.InvertedPostPage.Blocks, moved)
	if page.InvertedPostPage.Header.Level > 0 {
		if err := idx.updateSinglePostingChildParent(ctx, moved.Child, page.Index); err != nil {
			return err
		}
	}
	if err := ensureInvertedPostingPageFits(right.InvertedPostPage, invertedPageBodySize(right.Index)); err != nil {
		return err
	}
	return ensureInvertedPostingPageFits(page.InvertedPostPage, invertedPageBodySize(page.Index))
}

// mergePostingPagesAndRemoveRoute appends right into left, frees right, and repairs the parent.
func (idx *dedicatedInvertedIndex) mergePostingPagesAndRemoveRoute(ctx context.Context, rootIdx PageIndex, left, right *Page) (PageIndex, error) {
	if left.InvertedPostPage.Header.Level != right.InvertedPostPage.Header.Level {
		return 0, fmt.Errorf("cannot merge posting pages with different levels")
	}
	left.InvertedPostPage.Blocks = append(left.InvertedPostPage.Blocks, right.InvertedPostPage.Blocks...)
	if left.InvertedPostPage.Header.Level == 0 {
		left.InvertedPostPage.Header.NextLeaf = right.InvertedPostPage.Header.NextLeaf
	} else if err := idx.updatePostingChildrenParent(ctx, left); err != nil {
		return 0, err
	}
	if err := ensureInvertedPostingPageFits(left.InvertedPostPage, invertedPageBodySize(left.Index)); err != nil {
		return 0, err
	}
	newRoot, err := idx.removePostingChildRoute(ctx, rootIdx, right.InvertedPostPage.Header.Parent, right.Index)
	if err != nil {
		return 0, err
	}
	if err := idx.pager.AddFreePage(ctx, right.Index); err != nil {
		return 0, fmt.Errorf("free merged inverted posting page %d: %w", right.Index, err)
	}
	return newRoot, nil
}

// updateSinglePostingChildParent rewrites one posting child page's parent pointer.
func (idx *dedicatedInvertedIndex) updateSinglePostingChildParent(ctx context.Context, childIdx, parentIdx PageIndex) error {
	child, err := idx.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("modify inverted posting child %d parent: %w", childIdx, err)
	}
	if child.InvertedPostPage == nil {
		return fmt.Errorf("inverted posting child %d is not a posting page", childIdx)
	}
	child.InvertedPostPage.Header.Parent = parentIdx
	return nil
}

// canRemovePostingChild reports whether deleting a child route leaves a valid parent.
func (idx *dedicatedInvertedIndex) canRemovePostingChild(ctx context.Context, parentIdx, childIdx PageIndex) (bool, error) {
	parent, err := idx.pager.ReadPage(ctx, parentIdx)
	if err != nil {
		return false, fmt.Errorf("read inverted posting parent %d: %w", parentIdx, err)
	}
	if parent.InvertedPostPage == nil || parent.InvertedPostPage.Header.Level == 0 {
		return false, fmt.Errorf("inverted posting parent %d is not an internal page", parentIdx)
	}
	found := false
	for _, block := range parent.InvertedPostPage.Blocks {
		if block.Child == childIdx {
			found = true
			break
		}
	}
	if !found {
		return false, fmt.Errorf("inverted posting child %d not found in parent %d", childIdx, parentIdx)
	}
	return len(parent.InvertedPostPage.Blocks) > 1, nil
}

// findPreviousPostingLeaf walks the leaf chain and returns the leaf before leafIdx.
func (idx *dedicatedInvertedIndex) findPreviousPostingLeaf(ctx context.Context, rootIdx, leafIdx PageIndex) (*Page, error) {
	page, err := idx.leftmostPostingLeaf(ctx, rootIdx)
	if err != nil {
		return nil, err
	}
	var previous *Page
	for page.Index != leafIdx {
		next := page.InvertedPostPage.Header.NextLeaf
		if next == 0 {
			return nil, fmt.Errorf("inverted posting leaf %d not found in leaf chain", leafIdx)
		}
		previous = page
		page, err = idx.pager.ReadPage(ctx, next)
		if err != nil {
			return nil, fmt.Errorf("read inverted posting leaf %d: %w", next, err)
		}
		if page.InvertedPostPage == nil || page.InvertedPostPage.Header.Level != 0 {
			return nil, fmt.Errorf("inverted posting page %d is not a leaf", next)
		}
	}
	return previous, nil
}

// removePostingChildRoute deletes one child routing block and collapses root when possible.
func (idx *dedicatedInvertedIndex) removePostingChildRoute(ctx context.Context, rootIdx, parentIdx, childIdx PageIndex) (PageIndex, error) {
	parent, err := idx.pager.ModifyPage(ctx, parentIdx)
	if err != nil {
		return 0, fmt.Errorf("modify inverted posting parent %d: %w", parentIdx, err)
	}
	if parent.InvertedPostPage == nil || parent.InvertedPostPage.Header.Level == 0 {
		return 0, fmt.Errorf("inverted posting parent %d is not an internal page", parentIdx)
	}

	blockIdx := -1
	for i, block := range parent.InvertedPostPage.Blocks {
		if block.Child == childIdx {
			blockIdx = i
			break
		}
	}
	if blockIdx < 0 {
		return 0, fmt.Errorf("inverted posting child %d not found in parent %d", childIdx, parentIdx)
	}

	parent.InvertedPostPage.Blocks = slices.Delete(parent.InvertedPostPage.Blocks, blockIdx, blockIdx+1)
	if len(parent.InvertedPostPage.Blocks) == 0 {
		return 0, fmt.Errorf("inverted posting parent %d has no remaining children", parentIdx)
	}
	if err := ensureInvertedPostingPageFits(parent.InvertedPostPage, invertedPageBodySize(parent.Index)); err != nil {
		return 0, err
	}
	return idx.rebalancePostingPageAfterDelete(ctx, rootIdx, parentIdx)
}

// findPostingLeafBlock descends to the leaf block whose row range should contain rowID.
func (idx *dedicatedInvertedIndex) findPostingLeafBlock(ctx context.Context, rootIdx PageIndex, rowID RowID) (*Page, int, error) {
	page, err := idx.pager.ReadPage(ctx, rootIdx)
	if err != nil {
		return nil, 0, fmt.Errorf("read inverted posting root %d: %w", rootIdx, err)
	}
	for {
		if page.InvertedPostPage == nil {
			return nil, 0, fmt.Errorf("inverted posting page %d is not a posting page", page.Index)
		}
		postingPage := page.InvertedPostPage
		if len(postingPage.Blocks) == 0 {
			return page, -1, nil
		}
		blockIdx := postingBlockIndexForRowID(postingPage.Blocks, rowID)
		if postingPage.Header.Level == 0 {
			return page, blockIdx, nil
		}
		childIdx := postingPage.Blocks[blockIdx].Child
		if childIdx == 0 {
			return nil, 0, fmt.Errorf("inverted posting internal page %d has zero child", page.Index)
		}
		page, err = idx.pager.ReadPage(ctx, childIdx)
		if err != nil {
			return nil, 0, fmt.Errorf("read inverted posting child %d: %w", childIdx, err)
		}
	}
}

// leftmostPostingLeaf follows internal posting pages down to the first leaf.
func (idx *dedicatedInvertedIndex) leftmostPostingLeaf(ctx context.Context, rootIdx PageIndex) (*Page, error) {
	page, err := idx.pager.ReadPage(ctx, rootIdx)
	if err != nil {
		return nil, fmt.Errorf("read inverted posting root %d: %w", rootIdx, err)
	}
	for {
		if page.InvertedPostPage == nil {
			return nil, fmt.Errorf("inverted posting page %d is not a posting page", page.Index)
		}
		if page.InvertedPostPage.Header.Level == 0 {
			return page, nil
		}
		if len(page.InvertedPostPage.Blocks) == 0 {
			return nil, fmt.Errorf("inverted posting internal page %d has no routing blocks", page.Index)
		}
		childIdx := page.InvertedPostPage.Blocks[0].Child
		if childIdx == 0 {
			return nil, fmt.Errorf("inverted posting internal page %d has zero child", page.Index)
		}
		var err error
		page, err = idx.pager.ReadPage(ctx, childIdx)
		if err != nil {
			return nil, fmt.Errorf("read inverted posting child %d: %w", childIdx, err)
		}
	}
}

// postingBlockIndexForRowID returns the first block whose range can hold rowID.
func postingBlockIndexForRowID(blocks []invertedPostingBlock, rowID RowID) int {
	for i, block := range blocks {
		if rowID <= block.LastRowID {
			return i
		}
	}
	return len(blocks) - 1
}

// refreshPostingAncestors recomputes routing metadata after a child page changes.
func (idx *dedicatedInvertedIndex) refreshPostingAncestors(ctx context.Context, childIdx PageIndex) error {
	child, err := idx.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("read inverted posting child %d: %w", childIdx, err)
	}
	if child.InvertedPostPage == nil {
		return fmt.Errorf("inverted posting child %d is not a posting page", childIdx)
	}
	parentIdx := child.InvertedPostPage.Header.Parent
	if parentIdx == 0 {
		return nil
	}

	parent, err := idx.pager.ModifyPage(ctx, parentIdx)
	if err != nil {
		return fmt.Errorf("modify inverted posting parent %d: %w", parentIdx, err)
	}
	if parent.InvertedPostPage == nil {
		return fmt.Errorf("inverted posting parent %d is not a posting page", parentIdx)
	}
	first, last, count, err := invertedPostingPageRange(child.InvertedPostPage)
	if err != nil {
		return err
	}
	updated := false
	for i, block := range parent.InvertedPostPage.Blocks {
		if block.Child != childIdx {
			continue
		}
		parent.InvertedPostPage.Blocks[i].FirstRowID = first
		parent.InvertedPostPage.Blocks[i].LastRowID = last
		parent.InvertedPostPage.Blocks[i].PostingCount = count
		updated = true
		break
	}
	if !updated {
		return fmt.Errorf("inverted posting child %d not found in parent %d", childIdx, parentIdx)
	}
	if err := ensureInvertedPostingPageFits(parent.InvertedPostPage, invertedPageBodySize(parent.Index)); err != nil {
		return err
	}
	return idx.refreshPostingAncestors(ctx, parentIdx)
}

// routingBlockForPostingPage builds the internal routing block for a child page.
func (idx *dedicatedInvertedIndex) routingBlockForPostingPage(page *Page) (invertedPostingBlock, error) {
	first, last, count, err := invertedPostingPageRange(page.InvertedPostPage)
	if err != nil {
		return invertedPostingBlock{}, err
	}
	return invertedPostingBlock{
		FirstRowID:   first,
		LastRowID:    last,
		PostingCount: count,
		Child:        page.Index,
		CodecVersion: invertedPostingCodecVersion,
	}, nil
}

// postingPageRangeByIndex reads a posting page and summarizes its row-id range.
func (idx *dedicatedInvertedIndex) postingPageRangeByIndex(ctx context.Context, pageIdx PageIndex) (RowID, RowID, uint32, error) {
	page, err := idx.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read inverted posting page %d range: %w", pageIdx, err)
	}
	if page.InvertedPostPage == nil {
		return 0, 0, 0, fmt.Errorf("inverted posting page %d is not a posting page", pageIdx)
	}
	return invertedPostingPageRange(page.InvertedPostPage)
}

// writePostingTree writes compressed posting blocks into a posting-page tree.
func (idx *dedicatedInvertedIndex) writePostingTree(ctx context.Context, postings []invertedPosting) (PageIndex, error) {
	blocks, err := makeInvertedPostingBlocks(idx.mode, postings)
	if err != nil {
		return 0, err
	}
	pages, err := idx.writePostingLeafPages(ctx, blocks)
	if err != nil {
		return 0, err
	}
	for len(pages) > 1 {
		pages, err = idx.writePostingInternalLevel(ctx, pages)
		if err != nil {
			return 0, err
		}
	}
	if len(pages) == 0 {
		return 0, nil
	}
	pages[0].InvertedPostPage.Header.Parent = 0
	return pages[0].Index, nil
}

// writePostingLeafPages writes level-0 posting pages and links their leaf chain.
func (idx *dedicatedInvertedIndex) writePostingLeafPages(ctx context.Context, blocks []invertedPostingBlock) ([]*Page, error) {
	blockGroups, err := groupInvertedPostingBlocksIntoPages(blocks, 0)
	if err != nil {
		return nil, err
	}

	pages, err := idx.allocatePostingPages(ctx, len(blockGroups))
	if err != nil {
		return nil, err
	}
	for i, blocks := range blockGroups {
		nextLeaf := PageIndex(0)
		if i+1 < len(pages) {
			nextLeaf = pages[i+1].Index
		}
		pages[i].Clear()
		postingPage := NewInvertedPostingPage(0)
		postingPage.Header.NextLeaf = nextLeaf
		postingPage.Blocks = blocks
		pages[i].InvertedPostPage = postingPage
		if err := ensureInvertedPostingPageFits(postingPage, invertedPageBodySize(pages[i].Index)); err != nil {
			return nil, err
		}
	}
	return pages, nil
}

// writePostingInternalLevel writes one internal routing level above child pages.
func (idx *dedicatedInvertedIndex) writePostingInternalLevel(ctx context.Context, children []*Page) ([]*Page, error) {
	if len(children) == 0 {
		return nil, nil
	}
	level := children[0].InvertedPostPage.Header.Level + 1
	routingBlocks := make([]invertedPostingBlock, 0, len(children))
	for _, child := range children {
		first, last, count, err := invertedPostingPageRange(child.InvertedPostPage)
		if err != nil {
			return nil, err
		}
		routingBlocks = append(routingBlocks, invertedPostingBlock{
			FirstRowID:   first,
			LastRowID:    last,
			PostingCount: count,
			Child:        child.Index,
			CodecVersion: invertedPostingCodecVersion,
		})
	}
	blockGroups, err := groupInvertedPostingBlocksIntoPages(routingBlocks, level)
	if err != nil {
		return nil, err
	}
	pages, err := idx.allocatePostingPages(ctx, len(blockGroups))
	if err != nil {
		return nil, err
	}
	childByIdx := make(map[PageIndex]*Page, len(children))
	for _, child := range children {
		childByIdx[child.Index] = child
	}
	for i, blocks := range blockGroups {
		pages[i].Clear()
		postingPage := NewInvertedPostingPage(level)
		postingPage.Blocks = blocks
		pages[i].InvertedPostPage = postingPage
		for _, block := range blocks {
			child := childByIdx[block.Child]
			child.InvertedPostPage.Header.Parent = pages[i].Index
		}
		if err := ensureInvertedPostingPageFits(postingPage, invertedPageBodySize(pages[i].Index)); err != nil {
			return nil, err
		}
	}
	return pages, nil
}

// invertedPostingPageRange summarizes the row-id range covered by one posting page.
func invertedPostingPageRange(page *invertedPostingPage) (RowID, RowID, uint32, error) {
	if page == nil {
		return 0, 0, 0, fmt.Errorf("inverted posting page is nil")
	}
	if len(page.Blocks) == 0 {
		return 0, 0, 0, fmt.Errorf("inverted posting page has no blocks")
	}
	first := page.Blocks[0].FirstRowID
	last := page.Blocks[len(page.Blocks)-1].LastRowID
	var count uint32
	var previousLast RowID
	for i, block := range page.Blocks {
		if i > 0 && block.FirstRowID <= previousLast {
			return 0, 0, 0, fmt.Errorf("inverted posting page block ranges are not ordered")
		}
		if block.FirstRowID > block.LastRowID {
			return 0, 0, 0, fmt.Errorf("inverted posting page block range is inverted")
		}
		if page.Header.Level > 0 && block.Child == 0 {
			return 0, 0, 0, fmt.Errorf("inverted posting internal block has no child")
		}
		count += block.PostingCount
		previousLast = block.LastRowID
	}
	return first, last, count, nil
}

// allocatePostingPages reserves count pages for posting tree storage.
func (idx *dedicatedInvertedIndex) allocatePostingPages(ctx context.Context, count int) ([]*Page, error) {
	pages := make([]*Page, count)
	for i := range count {
		page, err := idx.pager.GetFreePage(ctx)
		if err != nil {
			return nil, fmt.Errorf("allocate inverted posting page: %w", err)
		}
		pages[i] = page
	}
	return pages, nil
}

// freePostingTree releases all posting pages referenced by a tree-backed entry.
func (idx *dedicatedInvertedIndex) freePostingTree(ctx context.Context, cell invertedEntryCell) error {
	if cell.PostingKind != invertedPostingKindTree {
		return nil
	}
	stack := []PageIndex{cell.Child}
	for len(stack) > 0 {
		pageIdx := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if pageIdx == 0 {
			continue
		}
		page, err := idx.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted posting page %d for free: %w", pageIdx, err)
		}
		if page.InvertedPostPage == nil {
			return fmt.Errorf("inverted posting page %d is not a posting page", pageIdx)
		}
		if page.InvertedPostPage.Header.Level > 0 {
			for _, block := range page.InvertedPostPage.Blocks {
				stack = append(stack, block.Child)
			}
		}
		if err := idx.pager.AddFreePage(ctx, pageIdx); err != nil {
			return err
		}
	}
	return nil
}

// makeInvertedPostingBlocks packs already-grouped postings into bounded
// compressed blocks. Callers are responsible for grouping before this call.
func makeInvertedPostingBlocks(mode invertedPostingMode, postings []invertedPosting) ([]invertedPostingBlock, error) {
	blocks := make([]invertedPostingBlock, 0)
	for len(postings) > 0 {
		n, payload, err := encodeLargestInvertedPostingBlock(mode, postings, invertedPostingBlockPayloadMax)
		if err != nil {
			return nil, err
		}
		blockPostings := postings[:n]
		blocks = append(blocks, postingBlockFromPostings(mode, blockPostings, payload))
		postings = postings[n:]
	}
	return blocks, nil
}

// postingBlockFromPostings builds block metadata around an encoded posting list.
func postingBlockFromPostings(mode invertedPostingMode, postings []invertedPosting, payload []byte) invertedPostingBlock {
	return invertedPostingBlock{
		Payload:      payload,
		FirstRowID:   postings[0].RowID,
		LastRowID:    postings[len(postings)-1].RowID,
		PostingCount: countInvertedPostings(mode, postings),
		CodecVersion: invertedPostingCodecVersion,
	}
}

// replaceInvertedPostingBlocks swaps one block for one or more replacement blocks.
func replaceInvertedPostingBlocks(blocks []invertedPostingBlock, idx int, replacements []invertedPostingBlock) []invertedPostingBlock {
	updated := make([]invertedPostingBlock, 0, len(blocks)-1+len(replacements))
	updated = append(updated, blocks[:idx]...)
	updated = append(updated, replacements...)
	updated = append(updated, blocks[idx+1:]...)
	return updated
}

// encodeLargestInvertedPostingBlock finds the largest prefix that fits one block.
func encodeLargestInvertedPostingBlock(mode invertedPostingMode, postings []invertedPosting, maxPayload int) (int, []byte, error) {
	if len(postings) == 0 {
		return 0, nil, fmt.Errorf("cannot encode empty inverted posting block")
	}
	firstPayload, err := encodeGroupedInvertedPostingList(mode, postings[:1])
	if err != nil {
		return 0, nil, err
	}
	if len(firstPayload) > maxPayload {
		return 1, firstPayload, nil
	}

	lo, hi := 1, len(postings)
	bestN := 1
	bestPayload := firstPayload
	for lo <= hi {
		mid := lo + (hi-lo)/2
		payload, err := encodeGroupedInvertedPostingList(mode, postings[:mid])
		if err != nil {
			return 0, nil, err
		}
		if len(payload) <= maxPayload {
			bestN = mid
			bestPayload = payload
			lo = mid + 1
			continue
		}
		hi = mid - 1
	}
	return bestN, bestPayload, nil
}

// groupInvertedPostingBlocksIntoPages packs posting blocks into posting pages.
func groupInvertedPostingBlocksIntoPages(blocks []invertedPostingBlock, level byte) ([][]invertedPostingBlock, error) {
	var groups [][]invertedPostingBlock
	for _, block := range blocks {
		if len(groups) == 0 {
			groups = append(groups, []invertedPostingBlock{block})
			continue
		}
		lastIdx := len(groups) - 1
		candidate := append(append([]invertedPostingBlock(nil), groups[lastIdx]...), block)
		page := NewInvertedPostingPage(level)
		page.Blocks = candidate
		if err := ensureInvertedPostingPageFits(page, PageSize); err == nil {
			groups[lastIdx] = candidate
			continue
		}
		page.Blocks = []invertedPostingBlock{block}
		if err := ensureInvertedPostingPageFits(page, PageSize); err != nil {
			return nil, err
		}
		groups = append(groups, []invertedPostingBlock{block})
	}
	return groups, nil
}

// newPostingIterator creates the lookup iterator for inline or tree-backed postings.
func (idx *dedicatedInvertedIndex) newPostingIterator(ctx context.Context, cell invertedEntryCell) (invertedPostingIterator, error) {
	switch cell.PostingKind {
	case invertedPostingKindInline:
		block, err := inlineInvertedPostingBlock(cell)
		if err != nil {
			return nil, err
		}
		return &singleBlockInvertedPostingIterator{block: block, hasBlock: true}, nil
	case invertedPostingKindTree:
		return &postingTreeInvertedPostingIterator{
			pager:    idx.pager,
			nextPage: cell.Child,
		}, nil
	default:
		return nil, fmt.Errorf("inverted index %s term %q uses unsupported posting kind %d", idx.name, cell.Term, cell.PostingKind)
	}
}

// countInvertedPostings counts row IDs or positions depending on index mode.
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

// removeInvertedPosting removes one row or selected positions from postings.
// Postings must already be sorted by RowID (i.e. as returned by the codec).
func removeInvertedPosting(mode invertedPostingMode, postings []invertedPosting, remove invertedPosting) []invertedPosting {
	grouped := postings
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

// ensureInvertedEntryPageFits verifies an entry page can marshal into pageSize.
func ensureInvertedEntryPageFits(page *invertedEntryPage, pageSize int) error {
	if invertedEntryPageUsedBytes(page) > uint64(pageSize) {
		return errInvertedIndexEntryPageFull
	}
	return nil
}

// invertedEntryPageUnderfull reports whether a page should be repaired after delete.
func invertedEntryPageUnderfull(page *invertedEntryPage, pageSize int) bool {
	if len(page.Cells) == 0 {
		return true
	}
	return invertedEntryPageUsedBytes(page) < uint64(pageSize)/3
}

// invertedEntryPageCanSpare reports whether a sibling can lend one entry.
func invertedEntryPageCanSpare(page *invertedEntryPage, pageSize int) bool {
	if len(page.Cells) < 2 {
		return false
	}
	return invertedEntryPageUsedBytes(page) > uint64(pageSize)/2
}

// invertedEntryPageUsedBytes estimates occupied bytes including header and slots.
func invertedEntryPageUsedBytes(page *invertedEntryPage) uint64 {
	used := page.Header.size() + uint64(len(page.Cells))*2
	for _, cell := range page.Cells {
		used += cell.size()
	}
	return used
}

// ensureInvertedPostingPageFits verifies a posting page can marshal into pageSize.
func ensureInvertedPostingPageFits(page *invertedPostingPage, pageSize int) error {
	if invertedPostingPageUsedBytes(page) > uint64(pageSize) {
		return fmt.Errorf("inverted posting page has insufficient free space")
	}
	return nil
}

// invertedPostingPageUnderfull reports whether a posting page should be repaired after delete.
func invertedPostingPageUnderfull(page *invertedPostingPage, pageSize int) bool {
	if len(page.Blocks) == 0 {
		return true
	}
	return invertedPostingPageUsedBytes(page) < uint64(pageSize)/3
}

// invertedPostingPageCanSpare reports whether a posting sibling can lend one block.
func invertedPostingPageCanSpare(page *invertedPostingPage, pageSize int) bool {
	if len(page.Blocks) < 2 {
		return false
	}
	return invertedPostingPageUsedBytes(page) > uint64(pageSize)/2
}

// invertedPostingPageUsedBytes estimates occupied bytes including header and slots.
func invertedPostingPageUsedBytes(page *invertedPostingPage) uint64 {
	used := page.Header.size() + uint64(len(page.Blocks))*2
	for _, block := range page.Blocks {
		used += block.size()
	}
	return used
}

// invertedPageBodySize returns usable bytes for root and non-root inverted pages.
func invertedPageBodySize(pageIdx PageIndex) int {
	if pageIdx == 0 {
		return PageSize - RootPageConfigSize
	}
	return PageSize
}

// boolToInt converts found flags into slice-bound offsets.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// cloneInvertedEntryCells deep-copies entry cells and payload bytes.
func cloneInvertedEntryCells(cells []invertedEntryCell) []invertedEntryCell {
	clone := make([]invertedEntryCell, len(cells))
	for i, cell := range cells {
		clone[i] = cloneInvertedEntryCell(cell)
	}
	return clone
}

// cloneInvertedEntryCell deep-copies one entry cell payload.
func cloneInvertedEntryCell(cell invertedEntryCell) invertedEntryCell {
	clone := cell
	clone.Payload = append([]byte(nil), cell.Payload...)
	return clone
}

// inlineInvertedPostingBlock wraps an inline payload in iterator block metadata.
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

type postingTreeInvertedPostingIterator struct {
	pager      TxPager
	nextPage   PageIndex
	blocks     []invertedPostingBlock
	blockIndex int
}

// NextBlock returns the next compressed block from the posting tree leaf chain.
func (it *postingTreeInvertedPostingIterator) NextBlock(ctx context.Context) (invertedPostingBlock, bool, error) {
	for it.blockIndex >= len(it.blocks) {
		if it.nextPage == 0 {
			return invertedPostingBlock{}, false, nil
		}
		page, err := it.leftmostPostingLeaf(ctx, it.nextPage)
		if err != nil {
			return invertedPostingBlock{}, false, err
		}
		it.blocks = page.InvertedPostPage.Blocks
		it.blockIndex = 0
		it.nextPage = page.InvertedPostPage.Header.NextLeaf
	}
	block := it.blocks[it.blockIndex]
	it.blockIndex += 1
	return block, true, nil
}

// leftmostPostingLeaf follows internal routing pages to the first leaf page.
func (it *postingTreeInvertedPostingIterator) leftmostPostingLeaf(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	for {
		page, err := it.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return nil, fmt.Errorf("read inverted posting page %d: %w", pageIdx, err)
		}
		if page.InvertedPostPage == nil {
			return nil, fmt.Errorf("inverted posting page %d is not a posting page", pageIdx)
		}
		postingPage := page.InvertedPostPage
		if postingPage.Header.Level == 0 {
			return page, nil
		}
		if len(postingPage.Blocks) == 0 {
			return nil, fmt.Errorf("inverted posting internal page %d has no routing blocks", pageIdx)
		}
		childIdx := postingPage.Blocks[0].Child
		if childIdx == 0 {
			return nil, fmt.Errorf("inverted posting internal page %d has zero child", pageIdx)
		}
		pageIdx = childIdx
	}
}
