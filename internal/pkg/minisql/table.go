package minisql

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

var (
	errMaximumPagesReached = fmt.Errorf("maximum pages reached")
	errTableDoesNotExist   = fmt.Errorf("table does not exist")
	errTableAlreadyExists  = fmt.Errorf("table already exists")
)

type Pager interface {
	GetPage(context.Context, *Table, uint32) (*Page, error)
	// ListPages() []*Page
	TotalPages() uint32
	Flush(context.Context, uint32, int64) error
}

type Table struct {
	Name        string
	Columns     []Column
	RootPageIdx uint32
	RowSize     uint64
	pager       Pager
	logger      *zap.Logger
}

func NewTable(logger *zap.Logger, name string, columns []Column, pager Pager, rootPageIdx uint32) *Table {
	return &Table{
		Name:        name,
		Columns:     columns,
		RootPageIdx: rootPageIdx,
		RowSize:     Row{Columns: columns}.Size(),
		pager:       pager,
		logger:      logger,
	}
}

// SeekNextRowID returns cursor pointing at the position after the last row ID
// plus a new row ID to insert
func (t *Table) SeekNextRowID(ctx context.Context, pageIdx uint32) (*Cursor, uint64, error) {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return nil, 0, err
	}
	if aPage.LeafNode == nil {
		return t.SeekNextRowID(ctx, aPage.InternalNode.Header.RightChild)
	}
	if aPage.LeafNode.Header.NextLeaf != 0 {
		return t.SeekNextRowID(ctx, aPage.LeafNode.Header.NextLeaf)
	}
	maxKey, ok := aPage.GetMaxKey()
	nextRowID := maxKey
	if ok {
		nextRowID += 1
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
		return nil, err
	}
	for aPage.LeafNode == nil {
		pageIdx = aPage.InternalNode.ICells[0].Child
		aPage, err = t.pager.GetPage(ctx, t, pageIdx)
		if err != nil {
			return nil, err
		}
	}
	return &Cursor{
		Table:      t,
		PageIdx:    pageIdx,
		CellIdx:    0,
		EndOfTable: aPage.LeafNode.Header.Cells == 1,
	}, nil
}

// Seek the cursor for a key, if it does not exist then return the cursor
// for the page and cell where it should be inserted
func (t *Table) Seek(ctx context.Context, key uint64) (*Cursor, error) {
	aRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return nil, err
	}
	if aRootPage.LeafNode != nil {
		return t.leafNodeSeek(ctx, t.RootPageIdx, aRootPage, key)
	} else if aRootPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, aRootPage, key)
	}
	return nil, fmt.Errorf("root page type")
}

func (t *Table) leafNodeSeek(ctx context.Context, pageIdx uint32, aPage *Page, key uint64) (*Cursor, error) {
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
		return nil, err
	}

	if aChildPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, aChildPage, key)
	}
	return t.leafNodeSeek(ctx, childPageIdx, aChildPage, key)
}

// Handle splitting the root.
// Old root copied to new page, becomes left child.
// Address of right child passed in.
// Re-initialize root page to contain the new root node.
// New root node points to two children.
func (t *Table) CreateNewRoot(ctx context.Context, rightChildPageIdx uint32) (*Page, error) {
	oldRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return nil, err
	}

	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return nil, err
	}

	leftChildPageIdx := t.pager.TotalPages()
	leftChildPage, err := t.pager.GetPage(ctx, t, leftChildPageIdx)
	if err != nil {
		return nil, err
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
				return nil, err
			}
			if aChildPage.LeafNode != nil {
				aChildPage.LeafNode.Header.Parent = leftChildPageIdx
			} else if aChildPage.InternalNode != nil {
				aChildPage.InternalNode.Header.Parent = leftChildPageIdx
			}
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
	if err := newRootNode.SetChildIdx(0, leftChildPageIdx); err != nil {
		return nil, err
	}
	leftChildMaxKey, _ := leftChildPage.GetMaxKey()
	newRootNode.ICells[0].Key = leftChildMaxKey

	// Set parent for both left and right child
	if leftChildPage.LeafNode != nil {
		leftChildPage.LeafNode.Header.Parent = t.RootPageIdx
	} else if leftChildPage.InternalNode != nil {
		leftChildPage.InternalNode.Header.Parent = t.RootPageIdx
	}
	if rightChildPage.LeafNode != nil {
		rightChildPage.LeafNode.Header.Parent = t.RootPageIdx
	} else if rightChildPage.InternalNode != nil {
		rightChildPage.InternalNode.Header.Parent = t.RootPageIdx
	}

	return leftChildPage, nil
}

// Add a new child/key pair to parent that corresponds to child
func (t *Table) InternalNodeInsert(ctx context.Context, parentPageIdx, childPageIdx uint32) error {
	aParentPage, err := t.pager.GetPage(ctx, t, parentPageIdx)
	if err != nil {
		return err
	}

	aChildPage, err := t.pager.GetPage(ctx, t, childPageIdx)
	if err != nil {
		return err
	}
	aChildPage.LeafNode.Header.Parent = parentPageIdx

	var (
		childMaxKey, _   = aChildPage.GetMaxKey()
		index            = aParentPage.InternalNode.IndexOfChild(childMaxKey)
		originalKeyCount = aParentPage.InternalNode.Header.KeysNum
	)

	if aParentPage.InternalNode.Header.KeysNum >= InternalNodeMaxCells {
		return t.InternalNodeSplitInsert(ctx, parentPageIdx, childPageIdx)
	}

	aParentPage.InternalNode.Header.KeysNum += 1

	rightChildPageIdx := aParentPage.InternalNode.Header.RightChild
	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return err
	}

	rightChildMaxKey, _ := rightChildPage.GetMaxKey()
	if childMaxKey > rightChildMaxKey {
		// Replace right child
		aParentPage.InternalNode.SetChildIdx(originalKeyCount, rightChildPageIdx)
		aParentPage.InternalNode.ICells[originalKeyCount].Key = rightChildMaxKey
		aParentPage.InternalNode.Header.RightChild = childPageIdx
		return nil
	}

	// Make room for the new cell
	for i := originalKeyCount; i > index; i-- {
		aParentPage.InternalNode.ICells[i] = aParentPage.InternalNode.ICells[i-1]
	}
	aParentPage.InternalNode.SetChildIdx(index, childPageIdx)
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
		return err
	}
	originalMaxKey, _ := aSplitPage.GetMaxKey()

	// Create a new sibling page, it will be on the same level as original node and to the right of it
	siblingPageIdx := t.pager.TotalPages()
	aSiblingPage, err := t.pager.GetPage(ctx, t, siblingPageIdx)
	if err != nil {
		return err
	}

	// Make sure the sibling page is an internal node
	aSiblingPage.InternalNode = NewInternalNode()
	aSiblingPage.LeafNode = nil
	// Set the sibling node parent
	aSiblingPage.InternalNode.Header.Parent = aSplitPage.InternalNode.Header.Parent

	t.logger.Sugar().With(
		"page_index", int(pageIdx),
		"sibling_page_index", int(siblingPageIdx),
	).Debug("internal node split insert")

	var (
		maxCells        = InternalNodeMaxCells       // 340
		rightSplitCount = (maxCells - 1) / 2         // 339/2 = 169
		leftSplitCount  = maxCells - rightSplitCount // 340-169 = 171
	)

	// Keep half of the keys on the original node, move another half to the sibling
	for i := leftSplitCount; i < maxCells; i++ {
		aCell := aSplitPage.InternalNode.ICells[i]
		aSiblingPage.InternalNode.ICells[i-leftSplitCount] = aCell
		aSplitPage.InternalNode.ICells[i] = ICell{}

		aSiblingPage.InternalNode.Header.KeysNum += 1
		aSplitPage.InternalNode.Header.KeysNum -= 1

		// Update all pages we are moving to the sibling node on the right with new parent
		movedPage, err := t.pager.GetPage(ctx, t, aCell.Child)
		if err != nil {
			return err
		}

		movedPage.LeafNode.Header.Parent = siblingPageIdx
	}
	// Since the split page was full, it had right child leaf set,
	// add it as extra cell into the sibling page
	aRightLeafPage, err := t.pager.GetPage(ctx, t, aSplitPage.InternalNode.Header.RightChild)
	if err != nil {
		return err
	}
	aRightLeafPage.LeafNode.Header.Parent = siblingPageIdx
	aRightLeafMaxKey, _ := aRightLeafPage.GetMaxKey()
	aSiblingPage.InternalNode.ICells[aSiblingPage.InternalNode.Header.KeysNum] = ICell{
		Key:   aRightLeafMaxKey,
		Child: aSplitPage.InternalNode.Header.RightChild,
	}
	aSiblingPage.InternalNode.Header.KeysNum += 1

	// Save child now in the sibling page
	if err := t.InternalNodeInsert(ctx, siblingPageIdx, childPageIdx); err != nil {
		return err
	}

	if aSplitPage.InternalNode.Header.IsRoot {
		aSplitPage, err = t.CreateNewRoot(ctx, siblingPageIdx)
		if err != nil {
			return err
		}

		// Set right child on the split page and remove the last key
		// (it has moved to the new root page)
		aSplitPage.InternalNode.RemoveLastCell()
	} else {
		// Update parent to reflect new max split page key
		parentPageIdx := aSplitPage.InternalNode.Header.Parent
		aParentPage, err := t.pager.GetPage(ctx, t, parentPageIdx)
		if err != nil {
			return err
		}
		oldChildIdx := aParentPage.InternalNode.IndexOfChild(originalMaxKey)
		newMaxKey, _ := aSplitPage.GetMaxKey()
		aParentPage.InternalNode.ICells[oldChildIdx].Key = newMaxKey
	}

	return nil
}

// DeleteKey deletes a key from the table, when this is called, you should already
// have located the leaf that contains the key and pass its page and cell index here.
// The deletion process starts at the leaf and then recursively bubbles up the tree.
func (t *Table) DeleteKey(ctx context.Context, pageIdx uint32, key uint64) error {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return err
	}

	// First call we will start with a leaf node, then we will recursively call DeleteKey
	// for all parent nodes until we reach the root
	firstCell := ICell{}
	if aNode := aPage.LeafNode; aNode != nil {
		var err error
		if err = t.removeFromLeaf(ctx, aNode, key); err != nil {
			return err
		}
	} else if aNode := aPage.InternalNode; aNode != nil {
		firstCell = aNode.FirstCell()
		if err := t.removeFromInternal(ctx, aNode, key); err != nil {
			return err
		}
	}

	if isPageLessThanHalfFull(aPage) {
		if aNode, ok := isInternal(aPage); ok && aNode.Header.IsRoot {
			if aNode.Header.KeysNum == 0 && firstCell != emptyICell {
				aNewRoot, err := t.pager.GetPage(ctx, t, firstCell.Child)
				if err != nil {
					return err
				}
				aNewRoot.InternalNode.Header.IsRoot = true
				aNewRoot.InternalNode.Header.Parent = 0
			}
			return nil
		} else if aNode, ok := isLeaf(aPage); ok {
			aParentPage, err := t.pager.GetPage(ctx, t, aNode.Header.Parent)
			if err != nil {
				return err
			}
			idx := aParentPage.InternalNode.IndexOfChild(key)

			var (
				leftSibling  *Page
				rightSibling *Page
			)
			if idx < aParentPage.InternalNode.Header.KeysNum-1 {
				rightSibling, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.GetRightChildByIndex(idx))
				if err != nil {
					return err
				}
			} else if idx > 0 {
				leftSibling, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.ICells[idx-1].Child)
				if err != nil {
					return err
				}
			}

			if rightSibling != nil && rightSibling.LeafNode.MoreThanHalfFull() {
				if err := t.borrowFromRightLeaf(
					aParentPage.InternalNode,
					aPage.LeafNode,
					rightSibling.LeafNode,
					idx,
				); err != nil {
					return err
				}
			} else if leftSibling != nil && leftSibling.LeafNode.MoreThanHalfFull() {
				if err := t.borrowFromLeftLeaf(
					aParentPage.InternalNode,
					aPage.LeafNode,
					leftSibling.LeafNode,
					idx,
				); err != nil {
					return err
				}
			} else if rightSibling != nil && !rightSibling.LeafNode.AtLeastHalfFull() {
				if err := t.mergeLeafs(
					aParentPage.InternalNode,
					aPage.LeafNode,
					rightSibling.LeafNode,
					idx,
				); err != nil {
					return err
				}
			} else if leftSibling != nil && !leftSibling.LeafNode.AtLeastHalfFull() {
				if err := t.mergeLeafs(
					aParentPage.InternalNode,
					leftSibling.LeafNode,
					aPage.LeafNode,
					idx,
				); err != nil {
					return err
				}
			}
		} else if aNode, ok := isInternal(aPage); ok {
			aParentPage, err := t.pager.GetPage(ctx, t, aNode.Header.Parent)
			if err != nil {
				return err
			}

			idx, ok := aParentPage.InternalNode.IndexOfPage(pageIdx)

			var (
				leftSibling  *Page
				rightSibling *Page
			)
			if ok && idx < aParentPage.InternalNode.Header.KeysNum-1 {
				rightSibling, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.GetRightChildByIndex(uint32(idx)))
				if err != nil {
					return err
				}
			} else if ok && idx > 0 {
				leftSibling, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.ICells[idx-1].Child)
				if err != nil {
					return err
				}
			}

			if rightSibling != nil && rightSibling.InternalNode.MoreThanHalfFull() {
				if err := t.borrowFromRightInternal(
					aParentPage.InternalNode,
					aPage.InternalNode,
					rightSibling.InternalNode,
					idx,
				); err != nil {
					return err
				}
			} else if leftSibling != nil && leftSibling.InternalNode.MoreThanHalfFull() {
				if err := t.borrowFromLeftInternal(
					aParentPage.InternalNode,
					aPage.InternalNode,
					leftSibling.InternalNode,
					idx,
				); err != nil {
					return err
				}
			} else if rightSibling != nil && !rightSibling.LeafNode.MoreThanHalfFull() {
				if err := t.mergeInternalNodes(
					aParentPage.InternalNode,
					aPage.InternalNode,
					rightSibling.InternalNode,
					idx,
				); err != nil {
					return err
				}
			} else if leftSibling != nil && !leftSibling.LeafNode.MoreThanHalfFull() {
				if err := t.mergeInternalNodes(
					aParentPage.InternalNode,
					leftSibling.InternalNode,
					aPage.InternalNode,
					idx,
				); err != nil {
					return err
				}
			}
		}
	}

	if aParent, ok := hasParent(aPage); ok {
		return t.DeleteKey(ctx, aParent, key)
	}

	return nil
}

func isInternal(aPage *Page) (*InternalNode, bool) {
	if aPage.InternalNode == nil {
		return nil, false
	}
	return aPage.InternalNode, true
}

func isLeaf(aPage *Page) (*LeafNode, bool) {
	if aPage.LeafNode == nil {
		return nil, false
	}
	return aPage.LeafNode, true
}

func hasParent(aPage *Page) (uint32, bool) {
	if aNode, ok := isLeaf(aPage); ok {
		return aNode.Header.Parent, true
	}

	if aNode, ok := isInternal(aPage); ok {
		if !aNode.Header.IsRoot {
			return aNode.Header.Parent, true
		}
	}

	return 0, false
}

func isPageLessThanHalfFull(aPage *Page) bool {
	if aPage.LeafNode != nil && !aPage.LeafNode.AtLeastHalfFull() {
		return true
	}
	if aPage.InternalNode != nil && !aPage.InternalNode.AtLeastHalfFull() {
		return true
	}
	return false
}

// removeFromLeaf removes the given key from the given leaf node. It removes the associated cell,
// and updates the key in the parent node if necessary.
func (t *Table) removeFromLeaf(ctx context.Context, aNode *LeafNode, key uint64) error {
	aNode.Delete(key)

	if aNode.Header.IsRoot {
		return nil
	}

	if aNode.Header.Cells == 0 {
		return nil
	}

	aParentPage, err := t.pager.GetPage(ctx, t, aNode.Header.Parent)
	if err != nil {
		return err
	}
	idx := aParentPage.InternalNode.IndexOfChild(key)

	aParentPage.InternalNode.ICells[idx-1].Key = aNode.Cells[0].Key

	return nil
}

// removeFromInternal removes the given key from the given internal node. If the key found in the node,
// it replaces it with the smallest key from the rightmost child.
func (t *Table) removeFromInternal(ctx context.Context, aNode *InternalNode, key uint64) error {
	idx, ok := aNode.IndexOfKey(key)
	if !ok {
		return nil
	}

	leftMostLeaf, err := t.pager.GetPage(ctx, t, aNode.GetRightChildByIndex(idx))
	if err != nil {
		return err
	}
	for leftMostLeaf.LeafNode == nil {
		leftMostLeaf, err = t.pager.GetPage(ctx, t, aNode.ICells[0].Child)
		if err != nil {
			return err
		}
	}
	aNode.ICells[idx].Key = leftMostLeaf.LeafNode.Cells[0].Key

	return nil
}

// borrowFromLeftLeaf borrows a key from the left neighbor of the given leaf node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftLeaf(aParent *InternalNode, aNode, leftSibling *LeafNode, idx uint32) error {
	aCellToRotate := leftSibling.LastCell()
	leftSibling.RemoveLastCell()
	aNode.PrependCell(aCellToRotate)

	aParent.ICells[idx-1].Key = aNode.FirstCell().Key

	return nil
}

// borrowFromRightLeaf borrows a key from the right neighbor of the given leaf node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightLeaf(aParent *InternalNode, aNode, rightSibling *LeafNode, idx uint32) error {
	aCellToRotate := rightSibling.FirstCell()
	rightSibling.RemoveFirstCell()
	aNode.AppendCells(aCellToRotate)

	aParent.ICells[idx].Key = rightSibling.FirstCell().Key

	return nil
}

// mergeLeafs merges two leaf nodes and deletes the key from the parent node.
func (t *Table) mergeLeafs(aParent *InternalNode, aPredecessor, aSuccessor *LeafNode, idx uint32) error {
	aPredecessor.AppendCells(aSuccessor.Cells...)
	aPredecessor.Header.NextLeaf = aSuccessor.Header.NextLeaf
	aParent.DeleteKeyByIndex(idx)

	return nil
}

// borrowFromLeftInternal borrows a key from the left neighbor of the given internal node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftInternal(aParent, aNode, leftSibling *InternalNode, idx uint32) error {
	aCellToRotate := leftSibling.LastCell()
	leftSibling.RemoveLastCell()
	aNode.PrependCell(aCellToRotate)

	aParent.ICells[idx-1].Key = aNode.FirstCell().Key

	return nil
}

// borrowFromRightInternal borrows a key from the right neighbor of the given internal node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightInternal(aParent, aNode, rightSibling *InternalNode, idx uint32) error {
	aCellToRotate := rightSibling.FirstCell()
	rightSibling.RemoveFirstCell()
	aNode.AppendCells(aCellToRotate)
	aNode.Header.RightChild = rightSibling.FirstCell().Child

	aParent.ICells[idx].Key = rightSibling.FirstCell().Key

	return nil
}

// mergeLeafs merges two internal nodes and deletes the key from the parent node.
func (t *Table) mergeInternalNodes(aParent, aPredecessor, aSuccessor *InternalNode, idx uint32) error {
	aPredecessor.AppendCells(aSuccessor.ICells[:]...)
	aPredecessor.Header.RightChild = aSuccessor.Header.RightChild
	aParent.DeleteKeyByIndex(idx)

	return nil
}
