package minisql

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type Pager interface {
	GetPage(context.Context, *Table, uint32) (*Page, error)
	TotalPages() uint32
	Flush(context.Context, uint32, int64) error
}

type Table struct {
	Name        string
	Columns     []Column
	RootPageIdx uint32
	RowSize     uint64
	pager       Pager
	maxICells   uint32
	writeLock   *sync.RWMutex
	logger      *zap.Logger
}

func NewTable(logger *zap.Logger, name string, columns []Column, pager Pager, rootPageIdx uint32) *Table {
	return &Table{
		Name:        name,
		Columns:     columns,
		RootPageIdx: rootPageIdx,
		RowSize:     Row{Columns: columns}.Size(),
		pager:       pager,
		maxICells:   InternalNodeMaxCells,
		writeLock:   new(sync.RWMutex),
		logger:      logger,
	}
}

func (t *Table) ColumnByName(name string) (Column, bool) {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return t.Columns[i], true
		}
	}
	return Column{}, false
}

// SeekNextRowID returns cursor pointing at the position after the last row ID
// plus a new row ID to insert
func (t *Table) SeekNextRowID(ctx context.Context, pageIdx uint32) (*Cursor, uint64, error) {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return nil, 0, fmt.Errorf("seek next row ID: %w", err)
	}
	if aPage.LeafNode == nil {
		return t.SeekNextRowID(ctx, aPage.InternalNode.Header.RightChild)
	}
	if aPage.LeafNode.Header.NextLeaf != 0 {
		return t.SeekNextRowID(ctx, aPage.LeafNode.Header.NextLeaf)
	}
	maxKey, err := t.GetMaxKey(ctx, aPage)
	nextRowID := maxKey
	if err == nil {
		nextRowID = maxKey + 1
	}
	return &Cursor{
		Table:   t,
		PageIdx: pageIdx,
		CellIdx: aPage.LeafNode.Header.Cells,
	}, nextRowID, nil
}

func (t *Table) SeekFirst(ctx context.Context) (*Cursor, error) {
	pageIdx := t.RootPageIdx
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek first: %w", err)
	}
	for aPage.LeafNode == nil {
		pageIdx = aPage.InternalNode.ICells[0].Child
		aPage, err = t.pager.GetPage(ctx, t, pageIdx)
		if err != nil {
			return nil, fmt.Errorf("seek first: %w", err)
		}
	}
	return &Cursor{
		Table:      t,
		PageIdx:    pageIdx,
		CellIdx:    0,
		EndOfTable: aPage.LeafNode.Header.Cells == 0,
	}, nil
}

// Seek the cursor for a key, if it does not exist then return the cursor
// for the page and cell where it should be inserted
func (t *Table) Seek(ctx context.Context, key uint64) (*Cursor, error) {
	aRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}
	if aRootPage.LeafNode != nil {
		return t.leafNodeSeek(t.RootPageIdx, aRootPage, key)
	} else if aRootPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, aRootPage, key)
	}
	return nil, fmt.Errorf("root page type")
}

func (t *Table) leafNodeSeek(pageIdx uint32, aPage *Page, key uint64) (*Cursor, error) {
	var (
		minIdx uint32
		maxIdx = aPage.LeafNode.Header.Cells

		aCursor = Cursor{
			Table:   t,
			PageIdx: pageIdx,
		}
	)

	// Search the Btree
	for i := maxIdx; i != minIdx; {
		index := (minIdx + i) / 2
		keyIdx := aPage.LeafNode.Cells[index].Key
		if key == keyIdx {
			aCursor.CellIdx = index
			return &aCursor, nil
		}
		if key < keyIdx {
			i = index
		} else {
			minIdx = index + 1
		}
	}

	aCursor.CellIdx = minIdx

	return &aCursor, nil
}

func (t *Table) internalNodeSeek(ctx context.Context, aPage *Page, key uint64) (*Cursor, error) {
	childIdx := aPage.InternalNode.IndexOfChild(key)
	childPageIdx, err := aPage.InternalNode.Child(childIdx)
	if err != nil {
		return nil, err
	}

	aChildPage, err := t.pager.GetPage(ctx, t, childPageIdx)
	if err != nil {
		return nil, fmt.Errorf("internal node seek: %w", err)
	}

	if aChildPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, aChildPage, key)
	}
	return t.leafNodeSeek(childPageIdx, aChildPage, key)
}

// Handle splitting the root.
// Old root copied to new page, becomes left child.
// Address of right child passed in.
// Re-initialize root page to contain the new root node.
// New root node points to two children.
func (t *Table) CreateNewRoot(ctx context.Context, rightChildPageIdx uint32) (*Page, error) {
	oldRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("create new root: %w", err)
	}

	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return nil, fmt.Errorf("create new root: %w", err)
	}

	leftChildPageIdx := t.pager.TotalPages()
	leftChildPage, err := t.pager.GetPage(ctx, t, leftChildPageIdx)
	if err != nil {
		return nil, fmt.Errorf("create new root: %w", err)
	}

	t.logger.Sugar().With(
		"left_child_index", int(leftChildPageIdx),
		"right_child_index", int(rightChildPageIdx),
	).Debug("create new root")

	// Copy all node contents to left child
	if oldRootPage.LeafNode != nil {
		*leftChildPage.LeafNode = *oldRootPage.LeafNode
		leftChildPage.LeafNode.Header.IsRoot = false
	} else if oldRootPage.InternalNode != nil {
		// New pages by default are leafs so we need to reset left child page
		// as an internal node here
		leftChildPage.InternalNode = NewInternalNode()
		leftChildPage.LeafNode = nil
		*leftChildPage.InternalNode = *oldRootPage.InternalNode
		leftChildPage.InternalNode.Header.IsRoot = false
		// Update parent for all child pages
		for i := 0; i < int(leftChildPage.InternalNode.Header.KeysNum); i++ {
			aChildPage, err := t.pager.GetPage(ctx, t, leftChildPage.InternalNode.ICells[i].Child)
			if err != nil {
				return nil, fmt.Errorf("create new root: %w", err)
			}
			aChildPage.setParent(leftChildPageIdx)
		}
	}

	// Change root node to a new internal node
	newRootNode := NewInternalNode()
	oldRootPage.LeafNode = nil
	oldRootPage.InternalNode = newRootNode
	newRootNode.Header.IsRoot = true
	newRootNode.Header.KeysNum = 1

	// Set left and right child
	newRootNode.Header.RightChild = rightChildPageIdx
	if err := newRootNode.SetChild(0, leftChildPageIdx); err != nil {
		return nil, err
	}
	leftChildMaxKey, err := t.GetMaxKey(ctx, leftChildPage)
	if err != nil {
		return nil, fmt.Errorf("create new root: %w", err)
	}
	newRootNode.ICells[0].Key = leftChildMaxKey

	// Set parent for both left and right child
	leftChildPage.setParent(t.RootPageIdx)
	rightChildPage.setParent(t.RootPageIdx)

	return leftChildPage, nil
}

// Add a new child/key pair to parent that corresponds to child
func (t *Table) InternalNodeInsert(ctx context.Context, parentPageIdx, childPageIdx uint32) error {
	aParentPage, err := t.pager.GetPage(ctx, t, parentPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}

	aChildPage, err := t.pager.GetPage(ctx, t, childPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	aChildPage.setParent(parentPageIdx)

	childMaxKey, err := t.GetMaxKey(ctx, aChildPage)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	var (
		index            = aParentPage.InternalNode.IndexOfChild(childMaxKey)
		originalKeyCount = aParentPage.InternalNode.Header.KeysNum
	)

	if aParentPage.InternalNode.Header.KeysNum >= t.maxICells {
		return t.InternalNodeSplitInsert(ctx, parentPageIdx, childPageIdx)
	}

	/*
	  An internal node with a right child of INVALID_PAGE_NUM is empty
	*/
	if aParentPage.InternalNode.Header.RightChild == RIGHT_CHILD_NOT_SET {
		aParentPage.InternalNode.Header.RightChild = childPageIdx
		return nil
	}

	/*
	  If we are already at the max number of cells for a node, we cannot increment
	  before splitting. Incrementing without inserting a new key/child pair
	  and immediately calling internal_node_split_and_insert has the effect
	  of creating a new key at (max_cells + 1) with an uninitialized value
	*/
	aParentPage.InternalNode.Header.KeysNum += 1

	rightChildPageIdx := aParentPage.InternalNode.Header.RightChild
	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}

	rightChildMaxKey, err := t.GetMaxKey(ctx, rightChildPage)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	if childMaxKey > rightChildMaxKey {
		// Replace right child
		aParentPage.InternalNode.SetChild(originalKeyCount, rightChildPageIdx)
		aParentPage.InternalNode.ICells[originalKeyCount].Key = rightChildMaxKey
		aParentPage.InternalNode.Header.RightChild = childPageIdx
		return nil
	}

	// Make room for the new cell
	for i := originalKeyCount; i > index; i-- {
		aParentPage.InternalNode.ICells[i] = aParentPage.InternalNode.ICells[i-1]
	}
	aParentPage.InternalNode.SetChild(index, childPageIdx)
	aParentPage.InternalNode.ICells[index].Key = childMaxKey

	return nil
}

// Splits internal node. First, create a sibling node to store (n-1)/2 keys,
// move these keys from the original internal node to the sibling. Second,
// update original node's parent to reflect its new max key after splitting.
// Insert the sibling node into the parent, this could cause parent
// to be split as well. If the original node is root, create new root.
func (t *Table) InternalNodeSplitInsert(ctx context.Context, pageIdx, childPageIdx uint32) error {
	aSplitPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	splittingRoot := aSplitPage.InternalNode.Header.IsRoot
	oldMaxKey, err := t.GetMaxKey(ctx, aSplitPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	childPage, err := t.pager.GetPage(ctx, t, childPageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	childMaxKey, err := t.GetMaxKey(ctx, childPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	newPageIdx := t.pager.TotalPages()
	// Create a new page, it will be on the same level as original node and to the right of it
	aNewPage, err := t.pager.GetPage(ctx, t, newPageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	// Make sure the new page is an internal node
	aNewPage.InternalNode = NewInternalNode()
	aNewPage.LeafNode = nil

	t.logger.Sugar().With(
		"page_index", int(pageIdx),
		"new_page_index", int(newPageIdx),
	).Debug("internal node split insert")

	if splittingRoot {
		/*
		   If we are splitting the root, we need to update old_node to point
		   to the new root's left child, new_page_num will already point to
		   the new root's right child
		*/
		aSplitPage, err = t.CreateNewRoot(ctx, newPageIdx)
		if err != nil {
			return err
		}
	}
	aNewPage.InternalNode.Header.Parent = aSplitPage.InternalNode.Header.Parent

	var maxCells = t.maxICells

	// First put right child into new node and set right child of old node to invalid page number
	aNewPage.InternalNode.Header.RightChild = aSplitPage.InternalNode.Header.RightChild
	newPageRightChild, err := t.pager.GetPage(ctx, t, aNewPage.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	newPageRightChild.setParent(newPageIdx)
	aSplitPage.InternalNode.Header.RightChild = RIGHT_CHILD_NOT_SET

	// For each key until you get to the middle key, move the key and the child to the new node
	for i := maxCells - 1; i > maxCells/2; i-- {
		if err := t.InternalNodeInsert(ctx, newPageIdx, aSplitPage.InternalNode.ICells[i].Child); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		aSplitPage.InternalNode.ICells[i] = ICell{}
		aSplitPage.InternalNode.Header.KeysNum -= 1
	}

	// Set child before middle key, which is now the highest key, to be node's right child,
	// and decrement number of keys
	aSplitPage.InternalNode.Header.RightChild, _ = aSplitPage.InternalNode.Child(aSplitPage.InternalNode.Header.KeysNum - 1)
	aSplitPage.InternalNode.RemoveLastCell()

	maxAfterSplit, err := t.GetMaxKey(ctx, aSplitPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	// Determine which of the two nodes after the split should contain the child to be inserted,
	// and insert the child
	if childMaxKey < maxAfterSplit {
		if err := t.InternalNodeInsert(ctx, pageIdx, childPageIdx); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		childPage.setParent(pageIdx)
	} else {
		if err := t.InternalNodeInsert(ctx, newPageIdx, childPageIdx); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		childPage.setParent(newPageIdx)
	}

	aParentPage, err := t.pager.GetPage(ctx, t, aSplitPage.InternalNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	aParentPage.InternalNode.ICells[aParentPage.InternalNode.IndexOfChild(oldMaxKey)].Key = maxAfterSplit

	if splittingRoot {
		return nil
	}

	return t.InternalNodeInsert(ctx, aSplitPage.InternalNode.Header.Parent, newPageIdx)
}

func (t *Table) GetMaxKey(ctx context.Context, aPage *Page) (uint64, error) {
	if aPage.LeafNode != nil {
		if aPage.LeafNode.Header.Cells == 0 {
			return 0, fmt.Errorf("get max key: leaf node has no cells")
		}
		return aPage.LeafNode.Cells[aPage.LeafNode.Header.Cells-1].Key, nil
	}
	rightChild, err := t.pager.GetPage(ctx, t, aPage.InternalNode.Header.RightChild)
	if err != nil {
		return 0, err
	}
	return t.GetMaxKey(ctx, rightChild)
}

// DeleteKey deletes a key from the table, when this is called, you should already
// have located the leaf that contains the key and pass its page and cell index here.
// The deletion process starts at the leaf and then recursively bubbles up the tree.
func (t *Table) DeleteKey(ctx context.Context, pageIdx uint32, key uint64) error {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	if aPage.LeafNode == nil {
		return fmt.Errorf("DeleteKey called on non-leaf node")
	}

	// Remove key
	aPage.LeafNode.Delete(key)

	// Check for underflow
	if aPage.LeafNode.AtLeastHalfFull() {
		return nil
	}

	// Rebalance leaf node
	if err := t.rebalanceLeaf(ctx, aPage, key); err != nil {
		return err
	}

	return nil
}

func (t *Table) rebalanceLeaf(ctx context.Context, aPage *Page, key uint64) error {
	aLeafNode := aPage.LeafNode

	if aLeafNode.Header.IsRoot {
		return nil
	}

	if aLeafNode.Header.Cells == 0 {
		return nil
	}

	aParentPage, err := t.pager.GetPage(ctx, t, aLeafNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("rebalance leaf: %w", err)
	}
	myPositionInParent := aParentPage.InternalNode.IndexOfChild(key)

	var (
		left  *Page
		right *Page
	)
	if myPositionInParent > 0 {
		left, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.ICells[myPositionInParent-1].Child)
		if err != nil {
			return fmt.Errorf("rebalance leaf: %w", err)
		}
	} else {
		right, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.GetRightChildByIndex(myPositionInParent))
		if err != nil {
			return fmt.Errorf("rebalance leaf: %w", err)
		}
	}

	if right != nil && right.LeafNode.MoreThanHalfFull() {
		return t.borrowFromRightLeaf(
			aParentPage.InternalNode,
			aLeafNode,
			right.LeafNode,
			myPositionInParent,
		)
	}

	if left != nil && left.LeafNode.MoreThanHalfFull() {
		return t.borrowFromLeftLeaf(
			aParentPage.InternalNode,
			aLeafNode,
			left.LeafNode,
			myPositionInParent-1,
		)
	}

	if right != nil && int(right.LeafNode.Header.Cells+aLeafNode.Header.Cells) <= len(aLeafNode.Cells) {
		return t.mergeLeaves(
			ctx,
			aParentPage,
			aPage,
			right,
			myPositionInParent,
		)
	}

	if left != nil && int(left.LeafNode.Header.Cells+aLeafNode.Header.Cells) <= len(aLeafNode.Cells) {
		return t.mergeLeaves(
			ctx,
			aParentPage,
			left,
			aPage,
			myPositionInParent-1,
		)
	}

	return nil
}

// borrowFromLeftLeaf borrows a key from the left neighbor of the given leaf node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftLeaf(aParent *InternalNode, aNode, left *LeafNode, idx uint32) error {
	aCellToRotate := left.LastCell()
	left.RemoveLastCell()
	aNode.PrependCell(aCellToRotate)

	aParent.ICells[idx].Key = left.LastCell().Key

	return nil
}

// borrowFromRightLeaf borrows a key from the right neighbor of the given leaf node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightLeaf(aParent *InternalNode, aNode, right *LeafNode, idx uint32) error {
	aCellToRotate := right.FirstCell()
	right.RemoveFirstCell()
	aNode.AppendCells(aCellToRotate)

	aParent.ICells[idx].Key = right.FirstCell().Key

	return nil
}

// mergeLeaves merges two leaf nodes and deletes the key from the parent node.
func (t *Table) mergeLeaves(ctx context.Context, aParent, left, right *Page, idx uint32) error {
	left.LeafNode.AppendCells(right.LeafNode.Cells[0:right.LeafNode.Header.Cells]...)
	left.LeafNode.Header.NextLeaf = right.LeafNode.Header.NextLeaf
	// Remove key from parent plus the right child pointer
	aParent.InternalNode.DeleteKeyByIndex(idx)

	if aParent.InternalNode.Header.IsRoot && aParent.InternalNode.Header.KeysNum == 0 {
		aRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
		if err != nil {
			return fmt.Errorf("merge leaves: %w", err)
		}
		aRootPage.InternalNode = nil
		aRootPage.LeafNode = left.LeafNode
		left.LeafNode.Header.IsRoot = true
		left.LeafNode.Header.Parent = 0
		left.LeafNode.Header.NextLeaf = 0
		return nil
	}

	// Check for underflow
	if aParent.InternalNode.AtLeastHalfFull(int(t.maxICells)) {
		return nil
	}

	return t.rebalanceInternal(ctx, aParent)
}

func (t *Table) rebalanceInternal(ctx context.Context, aPage *Page) error {
	aNode := aPage.InternalNode
	if aNode.Header.IsRoot {
		if aNode.Header.KeysNum == 0 {
			aRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
			if err != nil {
				return fmt.Errorf("rebalance internal: %w", err)
			}
			firstChildPage, err := t.pager.GetPage(ctx, t, aNode.ICells[0].Child)
			if err != nil {
				return fmt.Errorf("rebalance internal: %w", err)
			}
			aRootPage.InternalNode = firstChildPage.InternalNode
		}
		return nil
	}

	aParentPage, err := t.pager.GetPage(ctx, t, aNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("rebalance internal: %w", err)
	}

	myPositionInParent, err := aParentPage.InternalNode.IndexOfPage(aPage.Index)
	if err != nil {
		return fmt.Errorf("rebalance internal: %w", err)
	}

	var (
		left  *Page
		right *Page
	)
	if myPositionInParent > 0 {
		left, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.ICells[myPositionInParent-1].Child)
		if err != nil {
			return fmt.Errorf("rebalance internal: %w", err)
		}
	} else {
		right, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.GetRightChildByIndex(myPositionInParent))
		if err != nil {
			return fmt.Errorf("rebalance internal: %w", err)
		}
	}

	if right != nil && right.InternalNode.MoreThanHalfFull(int(t.maxICells)) {
		return t.borrowFromRightInternal(
			aParentPage.InternalNode,
			aPage.InternalNode,
			right.InternalNode,
			myPositionInParent,
		)
	}

	if left != nil && left.InternalNode.MoreThanHalfFull(int(t.maxICells)) {
		return t.borrowFromLeftInternal(
			aParentPage.InternalNode,
			aPage.InternalNode,
			left.InternalNode,
			myPositionInParent-1,
		)
	}

	if right != nil && int(right.InternalNode.Header.KeysNum+aNode.Header.KeysNum) <= int(t.maxICells) {
		return t.mergeInternalNodes(
			ctx,
			aParentPage,
			aPage,
			right,
			myPositionInParent,
		)
	}

	if left != nil && int(left.InternalNode.Header.KeysNum+aNode.Header.KeysNum) <= int(t.maxICells) {
		return t.mergeInternalNodes(
			ctx,
			aParentPage,
			left,
			aPage,
			myPositionInParent-1,
		)
	}

	return nil
}

// borrowFromLeftInternal borrows a key from the left neighbor of the given internal node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftInternal(aParent, right, left *InternalNode, idx uint32) error {
	aCellToRotate := left.LastCell()
	left.RemoveLastCell()
	right.PrependCell(aCellToRotate)

	aParent.ICells[idx].Key = aCellToRotate.Key

	return nil
}

// borrowFromRightInternal borrows a key from the right neighbor of the given internal node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightInternal(aParent, left, right *InternalNode, idx uint32) error {
	aCellToRotate := right.FirstCell()
	right.RemoveFirstCell()

	left.AppendCells(ICell{
		Child: left.Header.RightChild,
		Key:   aParent.ICells[idx].Key,
	})
	left.Header.RightChild = aCellToRotate.Child

	aParent.ICells[idx].Key = aCellToRotate.Key

	return nil
}

// mergeInternalNodes merges two internal nodes and deletes the key from the parent node.
func (t *Table) mergeInternalNodes(ctx context.Context, aParent, left, right *Page, idx uint32) error {
	var (
		leftCells = left.InternalNode.Header.KeysNum
		leftIndex = left.Index
	)
	if aParent.InternalNode.Header.IsRoot && aParent.InternalNode.Header.KeysNum == 1 {
		leftIndex = t.RootPageIdx
	}

	// Update parent of all cells we are moving to the left node
	cellsToMoveLeft := right.InternalNode.ICells[0:right.InternalNode.Header.KeysNum]
	for _, iCell := range cellsToMoveLeft {
		movedPage, err := t.pager.GetPage(ctx, t, iCell.Child)
		if err != nil {
			return fmt.Errorf("merge internal nodes: %w", err)
		}
		movedPage.setParent(leftIndex)
	}
	newRightChildPage, err := t.pager.GetPage(ctx, t, right.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("merge internal nodes: %w", err)
	}
	newRightChildPage.setParent(leftIndex)

	// Do not lose right most child of the left node in the process
	oldRightChildPage, err := t.pager.GetPage(ctx, t, left.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("merge internal nodes: %w", err)
	}
	maxKey, err := t.GetMaxKey(ctx, oldRightChildPage)
	if err != nil {
		return fmt.Errorf("merge internal nodes: %w", err)
	}
	iCell := ICell{
		Child: left.InternalNode.Header.RightChild,
		Key:   maxKey,
	}
	left.InternalNode.AppendCells(append([]ICell{iCell}, cellsToMoveLeft...)...)
	left.InternalNode.Header.RightChild = right.InternalNode.Header.RightChild

	aParent.InternalNode.DeleteKeyByIndex(idx)

	// If root has no keys, make left the new root
	if aParent.InternalNode.Header.IsRoot && aParent.InternalNode.Header.KeysNum == 0 {
		aRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
		if err != nil {
			return fmt.Errorf("merge internal nodes: %w", err)
		}
		aRootPage.InternalNode = left.InternalNode
		aRootPage.LeafNode = nil
		left.InternalNode.Header.IsRoot = true
		left.InternalNode.Header.Parent = 0
		for idx := range leftCells {
			childPage, err := t.pager.GetPage(ctx, t, left.InternalNode.ICells[idx].Child)
			if err != nil {
				return fmt.Errorf("merge internal nodes: %w", err)
			}
			childPage.setParent(leftIndex)
		}
		oldRightChildPage.setParent(leftIndex)
		return nil
	}

	// Check for underflow
	if aParent.InternalNode.AtLeastHalfFull(int(t.maxICells)) {
		return nil
	}

	return t.rebalanceInternal(ctx, aParent)
}

type callback func(page *Page)

func (t *Table) BFS(f callback) error {

	rootPage, err := t.pager.GetPage(context.Background(), t, t.RootPageIdx)
	if err != nil {
		return err
	}

	// Create a queue and enqueue the root node
	queue := make([]*Page, 0, 1)
	queue = append(queue, rootPage)

	// Repeat until queue is empty
	for len(queue) > 0 {
		// Get the first node in the queue
		current := queue[0]

		// Dequeue
		queue = queue[1:]

		f(current)

		if current.InternalNode != nil {
			for i := range current.InternalNode.Header.KeysNum {
				iCell := current.InternalNode.ICells[i]
				aPage, err := t.pager.GetPage(context.Background(), t, iCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
			if current.InternalNode.Header.RightChild != RIGHT_CHILD_NOT_SET {
				aPage, err := t.pager.GetPage(context.Background(), t, current.InternalNode.Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
		}
	}

	return nil
}

func printTree(aTable *Table) error {
	return aTable.BFS(func(aPage *Page) {
		if aPage.InternalNode != nil {
			fmt.Println("Internal node,", "page:", aPage.Index, "number of keys:", aPage.InternalNode.Header.KeysNum, "parent:", aPage.InternalNode.Header.Parent)
			fmt.Println("Keys:", aPage.InternalNode.Keys())
			fmt.Println("Children:", aPage.InternalNode.Children())
		} else {
			fmt.Println("Leaf node,", "page:", aPage.Index, "number of cells:", aPage.LeafNode.Header.Cells, "parent:", aPage.LeafNode.Header.Parent, "next leaf:", aPage.LeafNode.Header.NextLeaf)
			fmt.Println("Keys:", aPage.LeafNode.Keys())
		}
		fmt.Println("---------")
	})
}
