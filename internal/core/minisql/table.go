package minisql

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

type Table struct {
	Name          string
	Columns       []Column
	RootPageIdx   uint32
	RowSize       uint64
	maximumICells uint32
	logger        *zap.Logger
	pager         TxPager
	txManager     *TransactionManager
}

func NewTable(logger *zap.Logger, pager TxPager, txManager *TransactionManager, name string, columns []Column, rootPageIdx uint32) *Table {
	return &Table{
		Name:          name,
		Columns:       columns,
		RootPageIdx:   rootPageIdx,
		RowSize:       Row{Columns: columns}.Size(),
		maximumICells: InternalNodeMaxCells,
		logger:        logger,
		pager:         pager,
		txManager:     txManager,
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
	aPage, err := t.pager.ReadPage(ctx, pageIdx)
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
	aPage, err := t.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek first: %w", err)
	}
	for aPage.LeafNode == nil {
		pageIdx = aPage.InternalNode.ICells[0].Child
		aPage, err = t.pager.ReadPage(ctx, pageIdx)
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
	aRootPage, err := t.pager.ReadPage(ctx, t.RootPageIdx)
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

	aChildPage, err := t.pager.ReadPage(ctx, childPageIdx)
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
	oldRootPage, err := t.pager.ModifyPage(ctx, t.RootPageIdx)
	if err != nil {
		return nil, fmt.Errorf("get old root page: %w", err)
	}
	rightChildPage, err := t.pager.ModifyPage(ctx, rightChildPageIdx)
	if err != nil {
		return nil, fmt.Errorf("get right child page: %w", err)
	}

	// Use recycled page if available, otherwise create new one
	leftChildPage, err := t.pager.GetFreePage(ctx)
	if err != nil {
		return nil, fmt.Errorf("get left child page: %w", err)
	}

	t.logger.Sugar().With(
		"left_child_index", int(leftChildPage.Index),
		"right_child_index", int(rightChildPageIdx),
	).Debug("create new root")

	// Copy all node contents to left child
	if oldRootPage.LeafNode != nil {
		leftChildPage.LeafNode = oldRootPage.LeafNode.Clone()
		leftChildPage.LeafNode.Header.IsRoot = false
	} else if oldRootPage.InternalNode != nil {
		// New pages by default are leafs so we need to reset left child page
		// as an internal node here
		leftChildPage.LeafNode = nil
		leftChildPage.InternalNode = oldRootPage.InternalNode.Clone()
		leftChildPage.InternalNode.Header.IsRoot = false
		// Update parent for all child pages
		for i := 0; i < int(leftChildPage.InternalNode.Header.KeysNum); i++ {
			aChildPage, err := t.pager.ModifyPage(ctx, leftChildPage.InternalNode.ICells[i].Child)
			if err != nil {
				return nil, fmt.Errorf("get child page: %w", err)
			}
			aChildPage.setParent(leftChildPage.Index)
		}
		// Don't forget right child
		aChildPage, err := t.pager.ModifyPage(ctx, leftChildPage.InternalNode.Header.RightChild)
		if err != nil {
			return nil, fmt.Errorf("get right child page: %w", err)
		}
		aChildPage.setParent(leftChildPage.Index)
	}

	// Change root node to a new internal node
	newRootNode := NewInternalNode()
	oldRootPage.LeafNode = nil
	oldRootPage.InternalNode = newRootNode
	newRootNode.Header.IsRoot = true
	newRootNode.Header.KeysNum = 1

	// Set left and right child
	newRootNode.Header.RightChild = rightChildPageIdx
	if err := newRootNode.SetChild(0, leftChildPage.Index); err != nil {
		return nil, err
	}
	leftChildMaxKey, err := t.GetMaxKey(ctx, leftChildPage)
	if err != nil {
		return nil, fmt.Errorf("get max key: %w", err)
	}
	newRootNode.ICells[0].Key = leftChildMaxKey

	// Set parent for both left and right child
	leftChildPage.setParent(t.RootPageIdx)
	rightChildPage.setParent(t.RootPageIdx)

	return leftChildPage, nil
}

// Add a new child/key pair to parent that corresponds to child
func (t *Table) InternalNodeInsert(ctx context.Context, parentPageIdx, childPageIdx uint32) error {
	aParentPage, err := t.pager.ModifyPage(ctx, parentPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}

	aChildPage, err := t.pager.ModifyPage(ctx, childPageIdx)
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

	if aParentPage.InternalNode.Header.KeysNum >= uint32(t.maxICells(parentPageIdx)) {
		return t.InternalNodeSplitInsert(ctx, parentPageIdx, childPageIdx)
	}

	/*
	  An internal node with a right child of RIGHT_CHILD_NOT_SET is empty
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
	rightChildPage, err := t.pager.ModifyPage(ctx, rightChildPageIdx)
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
	aSplitPage, err := t.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	splittingRoot := aSplitPage.InternalNode.Header.IsRoot
	oldMaxKey, err := t.GetMaxKey(ctx, aSplitPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	childPage, err := t.pager.ModifyPage(ctx, childPageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	childMaxKey, err := t.GetMaxKey(ctx, childPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	// Create a new page, it will be on the same level as original node and to the right of it
	// Use recycled page if available, otherwise create new one
	aNewPage, err := t.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}
	// Make sure the new page is an internal node
	aNewPage.InternalNode = NewInternalNode()
	aNewPage.LeafNode = nil

	t.logger.Sugar().With(
		"page_index", int(pageIdx),
		"new_page_index", int(aNewPage.Index),
	).Debug("internal node split insert")

	if splittingRoot {
		/*
		   If we are splitting the root, we need to update old_node to point
		   to the new root's left child, new_page_num will already point to
		   the new root's right child
		*/
		aSplitPage, err = t.CreateNewRoot(ctx, aNewPage.Index)
		if err != nil {
			return fmt.Errorf("create new root: %w", err)
		}
	}
	aNewPage.InternalNode.Header.Parent = aSplitPage.InternalNode.Header.Parent

	maxICells := t.maxICells(pageIdx)

	// First put right child into new node and set right child of old node to invalid page number
	aNewPage.InternalNode.Header.RightChild = aSplitPage.InternalNode.Header.RightChild
	newPageRightChild, err := t.pager.ModifyPage(ctx, aNewPage.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	newPageRightChild.setParent(aNewPage.Index)
	aSplitPage.InternalNode.Header.RightChild = RIGHT_CHILD_NOT_SET

	// For each key until you get to the middle key, move the key and the child to the new node
	for i := maxICells - 1; i > maxICells/2; i-- {
		if err := t.InternalNodeInsert(ctx, aNewPage.Index, aSplitPage.InternalNode.ICells[i].Child); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		aSplitPage.InternalNode.ICells[i] = ICell{}
		aSplitPage.InternalNode.Header.KeysNum -= 1
	}

	// Set child before middle key, which is now the highest key, to be node's right child,
	// and decrement number of keys
	aSplitPage.InternalNode.Header.RightChild, err = aSplitPage.InternalNode.Child(aSplitPage.InternalNode.Header.KeysNum - 1)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
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
		if err := t.InternalNodeInsert(ctx, aNewPage.Index, childPageIdx); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		childPage.setParent(aNewPage.Index)
	}

	aParentPage, err := t.pager.ModifyPage(ctx, aSplitPage.InternalNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	aParentPage.InternalNode.ICells[aParentPage.InternalNode.IndexOfChild(oldMaxKey)].Key = maxAfterSplit

	if splittingRoot {
		return nil
	}

	return t.InternalNodeInsert(ctx, aSplitPage.InternalNode.Header.Parent, aNewPage.Index)
}

func (t *Table) GetMaxKey(ctx context.Context, aPage *Page) (uint64, error) {
	if aPage.LeafNode != nil {
		if aPage.LeafNode.Header.Cells == 0 {
			return 0, fmt.Errorf("get max key: leaf node has no cells")
		}
		return aPage.LeafNode.Cells[aPage.LeafNode.Header.Cells-1].Key, nil
	}
	rightChild, err := t.pager.ReadPage(ctx, aPage.InternalNode.Header.RightChild)
	if err != nil {
		return 0, err
	}
	return t.GetMaxKey(ctx, rightChild)
}

// DeleteKey deletes a key from the table, when this is called, you should already
// have located the leaf that contains the key and pass its page and cell index here.
// The deletion process starts at the leaf and then recursively bubbles up the tree.
func (t *Table) DeleteKey(ctx context.Context, pageIdx uint32, key uint64) error {
	aPage, err := t.pager.ModifyPage(ctx, pageIdx)
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

	aParentPage, err := t.pager.ModifyPage(ctx, aLeafNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("rebalance leaf: %w", err)
	}
	myPositionInParent := aParentPage.InternalNode.IndexOfChild(key)

	var (
		left  *Page
		right *Page
	)
	if myPositionInParent > 0 {
		left, err = t.pager.ModifyPage(ctx, aParentPage.InternalNode.ICells[myPositionInParent-1].Child)
		if err != nil {
			return fmt.Errorf("rebalance leaf: %w", err)
		}
	} else {
		right, err = t.pager.ModifyPage(ctx, aParentPage.InternalNode.GetRightChildByIndex(myPositionInParent))
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
		if err := t.mergeLeaves(
			ctx,
			aParentPage,
			aPage,
			right,
			myPositionInParent,
		); err != nil {
			return err
		}

		return t.pager.AddFreePage(ctx, right.Index)
	}

	if left != nil && int(left.LeafNode.Header.Cells+aLeafNode.Header.Cells) <= len(aLeafNode.Cells) {
		if err := t.mergeLeaves(
			ctx,
			aParentPage,
			left,
			aPage,
			myPositionInParent-1,
		); err != nil {
			return err
		}

		return t.pager.AddFreePage(ctx, aPage.Index)
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
		aRootPage, err := t.pager.ModifyPage(ctx, t.RootPageIdx)
		if err != nil {
			return fmt.Errorf("get root page: %w", err)
		}
		aRootPage.InternalNode = nil
		aRootPage.LeafNode = NewLeafNode(t.RowSize)
		*aRootPage.LeafNode = *left.LeafNode
		aRootPage.LeafNode.Header.IsRoot = true
		aRootPage.LeafNode.Header.Parent = 0
		aRootPage.LeafNode.Header.NextLeaf = 0
		return t.pager.AddFreePage(ctx, left.Index)
	}

	// Check for underflow
	if aParent.InternalNode.AtLeastHalfFull(t.maxICells(aParent.Index)) {
		return nil
	}

	return t.rebalanceInternal(ctx, aParent)
}

func (t *Table) rebalanceInternal(ctx context.Context, aPage *Page) error {
	aNode := aPage.InternalNode
	if aNode.Header.IsRoot {
		if aNode.Header.KeysNum == 0 {
			aRootPage, err := t.pager.ModifyPage(ctx, t.RootPageIdx)
			if err != nil {
				return fmt.Errorf("rebalance internal: %w", err)
			}
			firstChildPage, err := t.pager.ModifyPage(ctx, aNode.ICells[0].Child)
			if err != nil {
				return fmt.Errorf("rebalance internal: %w", err)
			}
			*aRootPage.InternalNode = *firstChildPage.InternalNode
			return t.pager.AddFreePage(ctx, firstChildPage.Index)
		}
		return nil
	}

	aParentPage, err := t.pager.ModifyPage(ctx, aNode.Header.Parent)
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
		left, err = t.pager.ModifyPage(ctx, aParentPage.InternalNode.ICells[myPositionInParent-1].Child)
		if err != nil {
			return fmt.Errorf("get left internal page: %w", err)
		}
	} else {
		right, err = t.pager.ModifyPage(ctx, aParentPage.InternalNode.GetRightChildByIndex(myPositionInParent))
		if err != nil {
			return fmt.Errorf("get right internal page: %w", err)
		}
	}

	if right != nil && right.InternalNode.MoreThanHalfFull(t.maxICells(right.Index)) {
		if err := t.borrowFromRightInternal(
			ctx,
			aParentPage,
			aPage,
			right,
			myPositionInParent,
		); err != nil {
			return fmt.Errorf("borrow from right internal: %w", err)
		}
		return nil
	}

	if left != nil && left.InternalNode.MoreThanHalfFull(t.maxICells(left.Index)) {
		if err := t.borrowFromLeftInternal(
			ctx,
			aParentPage,
			aPage,
			left,
			myPositionInParent-1,
		); err != nil {
			return fmt.Errorf("borrow from left internal: %w", err)
		}
		return nil
	}

	if right != nil && int(right.InternalNode.Header.KeysNum+aNode.Header.KeysNum) <= t.maxICells(right.Index) {
		if err := t.mergeInternalNodes(
			ctx,
			aParentPage,
			aPage,
			right,
			myPositionInParent,
		); err != nil {
			return fmt.Errorf("merge internal node with right: %w", err)
		}

		return t.pager.AddFreePage(ctx, right.Index)
	}

	if left != nil && int(left.InternalNode.Header.KeysNum+aNode.Header.KeysNum) <= t.maxICells(left.Index) {
		if err := t.mergeInternalNodes(
			ctx,
			aParentPage,
			left,
			aPage,
			myPositionInParent-1,
		); err != nil {
			return fmt.Errorf("merge internal node with left: %w", err)
		}

		return t.pager.AddFreePage(ctx, aPage.Index)
	}

	return nil
}

// borrowFromLeftInternal borrows a key from the left neighbor of the given internal node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftInternal(ctx context.Context, aParent, aPage, left *Page, idx uint32) error {
	aPage.InternalNode.PrependCell(ICell{
		Key:   aParent.InternalNode.ICells[idx-1].Key,
		Child: left.InternalNode.Header.RightChild,
	})

	aParent.InternalNode.ICells[idx-1].Key = left.InternalNode.LastCell().Key

	childPage, err := t.pager.ModifyPage(ctx, left.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childPage.setParent(aPage.Index)

	left.InternalNode.RemoveLastCell()

	return nil
}

// borrowFromRightInternal borrows a key from the right neighbor of the given internal node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightInternal(ctx context.Context, aParent, aPage, right *Page, idx uint32) error {
	aPage.InternalNode.AppendCells(ICell{
		Child: aPage.InternalNode.Header.RightChild,
		Key:   aParent.InternalNode.ICells[idx].Key,
	})
	aPage.InternalNode.Header.RightChild = right.InternalNode.FirstCell().Child

	childPage, err := t.pager.ModifyPage(ctx, aPage.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childPage.setParent(aPage.Index)

	aParent.InternalNode.ICells[idx].Key = right.InternalNode.FirstCell().Key

	right.InternalNode.RemoveFirstCell()

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
		movedPage, err := t.pager.ModifyPage(ctx, iCell.Child)
		if err != nil {
			return fmt.Errorf("get moved page: %w", err)
		}
		movedPage.setParent(leftIndex)
	}
	newRightChildPage, err := t.pager.ModifyPage(ctx, right.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get new right child page: %w", err)
	}
	newRightChildPage.setParent(leftIndex)

	// Do not lose right most child of the left node in the process
	oldRightChildPage, err := t.pager.ModifyPage(ctx, left.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get old right child page: %w", err)
	}
	maxKey, err := t.GetMaxKey(ctx, oldRightChildPage)
	if err != nil {
		return fmt.Errorf("get max key: %w", err)
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
		aRootPage, err := t.pager.ModifyPage(ctx, t.RootPageIdx)
		if err != nil {
			return fmt.Errorf("get root page: %w", err)
		}
		*aRootPage.InternalNode = *left.InternalNode
		aRootPage.LeafNode = nil
		aRootPage.InternalNode.Header.IsRoot = true
		aRootPage.InternalNode.Header.Parent = 0
		for idx := range leftCells {
			childPage, err := t.pager.ModifyPage(ctx, left.InternalNode.ICells[idx].Child)
			if err != nil {
				return fmt.Errorf("get child page: %w", err)
			}
			childPage.setParent(0)
		}
		oldRightChildPage.setParent(leftIndex)
		return t.pager.AddFreePage(ctx, left.Index)
	}

	// Check for underflow
	if aParent.InternalNode.AtLeastHalfFull(t.maxICells(aParent.Index)) {
		return nil
	}

	return t.rebalanceInternal(ctx, aParent)
}

func (t *Table) maxICells(pageIdx uint32) int {
	maxICells := t.maximumICells
	if maxICells == InternalNodeMaxCells && pageIdx == 0 {
		maxICells = maxICells - uint32(RootPageConfigSize/ICellSize) - 1 // root page has less space
	}
	return int(maxICells)
}

type callback func(page *Page)

func (t *Table) BFS(f callback) error {
	rootPage, err := t.pager.ReadPage(context.Background(), t.RootPageIdx)
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
				aPage, err := t.pager.ReadPage(context.Background(), iCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
			if current.InternalNode.Header.RightChild != RIGHT_CHILD_NOT_SET {
				aPage, err := t.pager.ReadPage(context.Background(), current.InternalNode.Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
		}
	}

	return nil
}

func (t *Table) print() error {
	return t.BFS(func(aPage *Page) {
		if aPage.InternalNode != nil {
			fmt.Println("Internal node,", "page:", aPage.Index, "root:", aPage.InternalNode.Header.IsRoot, "number of keys:", aPage.InternalNode.Header.KeysNum, "parent:", aPage.InternalNode.Header.Parent)
			fmt.Println("Keys:", aPage.InternalNode.Keys())
			fmt.Println("Children:", aPage.InternalNode.Children())
		} else {
			fmt.Println("Leaf node,", "page:", aPage.Index, "number of cells:", aPage.LeafNode.Header.Cells, "parent:", aPage.LeafNode.Header.Parent, "next leaf:", aPage.LeafNode.Header.NextLeaf)
			fmt.Println("Keys:", aPage.LeafNode.Keys())
		}
		fmt.Println("---------")
	})
}
