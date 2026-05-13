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

// invertedPostingIterator streams compressed posting blocks for a single term.
type invertedPostingIterator interface {
	NextBlock(context.Context) (invertedPostingBlock, bool, error)
}

// invertedIndex is the storage interface for dedicated full-text/JSON inverted indexes.
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

	separator, rightPageIdx, split, err := idx.insertEntry(ctx, idx.rootPageIdx, term, posting)
	if err != nil {
		return err
	}
	if !split {
		return nil
	}

	return idx.splitRootEntryPage(ctx, separator, rightPageIdx)
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

// insertEntry recursively inserts a term posting and returns a split separator if needed.
func (idx *dedicatedInvertedIndex) insertEntry(ctx context.Context, pageIdx PageIndex, term string, posting invertedPosting) (string, PageIndex, bool, error) {
	page, err := idx.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return "", 0, false, fmt.Errorf("read inverted entry page %d: %w", pageIdx, err)
	}
	if page.InvertedEntryPage == nil {
		return "", 0, false, fmt.Errorf("inverted entry page %d is not an entry page", pageIdx)
	}
	if page.InvertedEntryPage.Header.IsLeaf {
		return idx.insertEntryLeaf(ctx, pageIdx, term, posting)
	}
	return idx.insertEntryInternal(ctx, pageIdx, term, posting)
}

// insertEntryLeaf inserts or updates one leaf cell, splitting the leaf on overflow.
func (idx *dedicatedInvertedIndex) insertEntryLeaf(ctx context.Context, pageIdx PageIndex, term string, posting invertedPosting) (string, PageIndex, bool, error) {
	page, err := idx.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return "", 0, false, fmt.Errorf("modify inverted entry leaf %d: %w", pageIdx, err)
	}
	entryPage := page.InvertedEntryPage
	if entryPage == nil || !entryPage.Header.IsLeaf {
		return "", 0, false, fmt.Errorf("inverted entry page %d is not a leaf", pageIdx)
	}

	cellIdx, found := findInvertedEntryCell(entryPage.Cells, term)
	postings := []invertedPosting{posting}
	if found {
		var err error
		postings, err = idx.decodeInvertedEntryCell(ctx, entryPage.Cells[cellIdx])
		if err != nil {
			return "", 0, false, err
		}
		postings = append(postings, posting)
	}

	cell, err := idx.makeInvertedEntryCell(ctx, term, postings, entryPage.Cells[cellIdx:cellIdx+boolToInt(found)])
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
func (idx *dedicatedInvertedIndex) insertEntryInternal(ctx context.Context, pageIdx PageIndex, term string, posting invertedPosting) (string, PageIndex, bool, error) {
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

	separator, rightChildIdx, split, err := idx.insertEntry(ctx, childIdx, term, posting)
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
	grouped := groupInvertedPostings(idx.mode, postings)
	payload, err := encodeInvertedPostingList(idx.mode, grouped)
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
	return groupInvertedPostings(idx.mode, postings), nil
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

// makeInvertedPostingBlocks groups postings into bounded compressed blocks.
func makeInvertedPostingBlocks(mode invertedPostingMode, postings []invertedPosting) ([]invertedPostingBlock, error) {
	grouped := groupInvertedPostings(mode, postings)
	blocks := make([]invertedPostingBlock, 0)
	for len(grouped) > 0 {
		n, payload, err := encodeLargestInvertedPostingBlock(mode, grouped, invertedPostingBlockPayloadMax)
		if err != nil {
			return nil, err
		}
		blockPostings := grouped[:n]
		blocks = append(blocks, invertedPostingBlock{
			Payload:      payload,
			FirstRowID:   blockPostings[0].RowID,
			LastRowID:    blockPostings[len(blockPostings)-1].RowID,
			PostingCount: countInvertedPostings(mode, blockPostings),
			CodecVersion: invertedPostingCodecVersion,
		})
		grouped = grouped[n:]
	}
	return blocks, nil
}

// encodeLargestInvertedPostingBlock finds the largest prefix that fits one block.
func encodeLargestInvertedPostingBlock(mode invertedPostingMode, postings []invertedPosting, maxPayload int) (int, []byte, error) {
	var (
		bestPayload []byte
		bestN       int
	)
	for n := 1; n <= len(postings); n++ {
		payload, err := encodeInvertedPostingList(mode, postings[:n])
		if err != nil {
			return 0, nil, err
		}
		if len(payload) > maxPayload && n > 1 {
			break
		}
		bestPayload = payload
		bestN = n
		if len(payload) > maxPayload {
			break
		}
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

// ensureInvertedEntryPageFits verifies an entry page can marshal into pageSize.
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
	buf := make([]byte, pageSize)
	return page.Marshal(buf)
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
	it.blockIndex++
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
