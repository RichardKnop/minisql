package minisql

import (
	"context"
	"fmt"
)

var (
	errMaximumPagesReached = fmt.Errorf("maximum pages reached")
	errTableDoesNotExist   = fmt.Errorf("table does not exist")
	errTableAlreadyExists  = fmt.Errorf("table already exists")
)

type Table struct {
	Name        string
	Columns     []Column
	RootPageIdx uint32
	RowSize     uint64
	pager       Pager
}

func NewTable(name string, columns []Column, pager Pager, rootPageIdx uint32) *Table {
	return &Table{
		Name:        name,
		Columns:     columns,
		RootPageIdx: rootPageIdx,
		RowSize:     Row{Columns: columns}.Size(),
		pager:       pager,
	}
}

// SeekNextRowID returns cursor pointing at the position after the last row ID
// plus a new row ID to insert
func (t *Table) SeekNextRowID(ctx context.Context, pageIdx uint32) (*Cursor, uint64, error) {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return nil, 0, err
	}
	if aPage.LeafNode != nil {
		if aPage.LeafNode.Header.NextLeaf != 0 {
			return t.SeekNextRowID(ctx, aPage.LeafNode.Header.NextLeaf)
		}
		maxKey, ok := aPage.GetMaxKey()
		nextRow := maxKey
		if ok {
			nextRow += 1
		}
		return &Cursor{
			Table:   t,
			PageIdx: pageIdx,
			CellIdx: aPage.LeafNode.Header.Cells,
		}, nextRow, nil
	}
	return t.SeekNextRowID(ctx, aPage.InternalNode.Header.RightChild)
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
	childIdx := aPage.InternalNode.FindChildByKey(key)
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

	logger.Sugar().With(
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

	var (
		childMaxKey, _   = aChildPage.GetMaxKey()
		index            = aParentPage.InternalNode.FindChildByKey(childMaxKey)
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

	logger.Sugar().With(
		"page_index", int(pageIdx),
		"sibling_page_index", int(siblingPageIdx),
	).Debug("internal node split insert")

	var (
		maxCells        = InternalNodeMaxCells       // 340
		rightSplitCount = (maxCells - 1) / 2         // 339/2 = 169
		leftSplitCount  = maxCells - rightSplitCount // 340-169 = 171
	)

	// Keep half of the keys on the original node, move another half to the sibling
	// fmt.Println("left split count", int(leftSplitCount))
	// fmt.Println("right split count", int(rightSplitCount))
	for i := leftSplitCount; i < maxCells; i++ {
		aSiblingPage.InternalNode.ICells[i-leftSplitCount] = aSplitPage.InternalNode.ICells[i]
		aSplitPage.InternalNode.ICells[i] = ICell{}

		aSiblingPage.InternalNode.Header.KeysNum += 1
		aSplitPage.InternalNode.Header.KeysNum -= 1

		// Update all pages we are moving to the sibling node on the right with new parent
		movedPage, err := t.pager.GetPage(ctx, t, aSiblingPage.InternalNode.ICells[i-leftSplitCount].Child)
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
	aRightLeafMaxKey, _ := aRightLeafPage.GetMaxKey()
	aSiblingPage.InternalNode.ICells[aSiblingPage.InternalNode.Header.KeysNum] = ICell{
		Key:   aRightLeafMaxKey,
		Child: aSplitPage.InternalNode.Header.RightChild,
	}
	aSiblingPage.InternalNode.Header.KeysNum += 1

	if aSplitPage.InternalNode.Header.IsRoot {
		aSplitPage, err = t.CreateNewRoot(ctx, siblingPageIdx)
		if err != nil {
			return err
		}

		// Set right child on the split page and remove the last key
		// (it has moved to the new root page)
		idx := aSplitPage.InternalNode.Header.KeysNum - 1
		aSplitPage.InternalNode.Header.RightChild = aSplitPage.InternalNode.ICells[idx].Child
		aSplitPage.InternalNode.ICells[idx] = ICell{}
		aSplitPage.InternalNode.Header.KeysNum -= 1
	} else {
		// Update parent to reflect new max split page key
		parentPageIdx := aSplitPage.InternalNode.Header.Parent
		aParentPage, err := t.pager.GetPage(ctx, t, parentPageIdx)
		if err != nil {
			return err
		}
		oldChildIdx := aParentPage.InternalNode.FindChildByKey(originalMaxKey)
		newMaxKey, _ := aSplitPage.GetMaxKey()
		aParentPage.InternalNode.ICells[oldChildIdx].Key = newMaxKey
	}

	// Save child now in the sibling page
	if err := t.InternalNodeInsert(ctx, siblingPageIdx, childPageIdx); err != nil {
		return err
	}

	return nil
}
