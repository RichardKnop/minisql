package minisql

import (
	"context"
	"fmt"
	"slices"

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

	return nil
}

func countKeys(aPage *Page) int {
	if aPage.InternalNode != nil {
		return int(aPage.InternalNode.Header.KeysNum)
	}
	return int(aPage.LeafNode.Header.Cells)
}

func getKey(aPage *Page, i int) uint64 {
	if aPage.InternalNode != nil {
		return aPage.InternalNode.ICells[i].Key
	}
	return aPage.LeafNode.Cells[i].Key
}

// func (t *Table) DeleteKey(ctx context.Context, key uint64) error {
// 	aCurrentPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
// 	if err != nil {
// 		return err
// 	}
// 	found := false
// 	i := 0
// 	for i < countKeys(aCurrentPage) {
// 		nodeKey := getKey(aCurrentPage, i)
// 		if key == nodeKey {
// 			found = true
// 			break
// 		} else if key < nodeKey {
// 			break
// 		}
// 		i += 1
// 	}

// 	if found {
// 		if aCurrentPage.LeafNode != nil {
// 			aCurrentPage.LeafNode.Delete(i)
// 		} else {
// 			// aPredecessor, err := t.pager.GetPage(ctx, t, aCurrentPage.InternalNode.ICells[i].Child)
// 			// if err != nil {
// 			// 	return err
// 			// }
// 			// if countKeys(aPredecessor) >=
// 		}
// 		// if curr.leaf:
// 		// 	curr.keys.pop(i)
// 		// else:
// 		// 	pred = curr.values[i]
// 		// 	if len(pred.keys) >= self.degree:
// 		// 		pred_key = self.get_max_key(pred)
// 		// 		curr.keys[i] = pred_key
// 		// 		self.delete_from_leaf(pred_key, pred)
// 		// 	else:
// 		// 		succ = curr.values[i + 1]
// 		// 		if len(succ.keys) >= self.degree:
// 		// 			succ_key = self.get_min_key(succ)
// 		// 			curr.keys[i] = succ_key
// 		// 			self.delete_from_leaf(succ_key, succ)
// 		// 		else:
// 		// 			self.merge(curr, i, pred, succ)
// 		// 			self.delete_from_leaf(key, pred)
// 		// if curr == self.root and not curr.keys:
// 		// 	self.root = curr.values[0]
// 	} else {

// 	}

// 	return fmt.Errorf("not implemented")
// }

func (t *Table) InternalNodeDelete(ctx context.Context, pageIdx uint32, key uint64) error {
	return fmt.Errorf("not implemented")
}

func (t *Table) LeafNodeDelete(ctx context.Context, pageIdx, cellIdx uint32, key uint64) error {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return err
	}
	if aPage.LeafNode == nil {
		return fmt.Errorf("error deleting key from a non leaf node, key %d", key)
	}

	aPage.LeafNode.Delete(cellIdx)

	if aPage.LeafNode.Header.IsRoot || aPage.LeafNode.AtLeastHalfFull() {
		return nil
	}

	aParentPage, err := t.pager.GetPage(ctx, t, aPage.LeafNode.Header.Parent)
	if err != nil {
		return err
	}
	idx := aParentPage.InternalNode.FindChildByKey(key)

	var (
		leftSibling  *Page
		rightSibling *Page
	)
	if idx > 0 {
		leftSibling, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.ICells[idx-1].Child)
		if err != nil {
			return err
		}
	} else {
		rightSibling, err = t.pager.GetPage(ctx, t, aParentPage.InternalNode.GetRightChildByIndex(idx))
		if err != nil {
			return err
		}
	}

	if idx > 0 && leftSibling.LeafNode.MoreThanHalfFull() {
		if err := t.rotateRight(ctx, aParentPage, aPage, leftSibling, idx); err != nil {
			return err
		}
	} else if idx < aPage.InternalNode.Header.KeysNum && rightSibling.LeafNode.MoreThanHalfFull() {
		if err := t.rotateLeft(ctx, aParentPage, aPage, rightSibling, idx); err != nil {
			return err
		}
	} else {
		if err := t.merge(aParentPage, idx, aPage, rightSibling); err != nil {
			return err
		}
	}

	return t.InternalNodeDelete(ctx, aPage.LeafNode.Header.Parent, key)
}

func (t *Table) rotateRight(ctx context.Context, aParent, aPage, leftSibling *Page, idx uint32) error {
	aCellToRotate := leftSibling.LeafNode.Cells[leftSibling.LeafNode.Header.Cells-1]
	leftSibling.LeafNode.Cells = leftSibling.LeafNode.Cells[0 : leftSibling.LeafNode.Header.Cells-1]
	leftSibling.LeafNode.Header.Cells -= 1

	aPage.LeafNode.Cells = slices.Insert(aPage.LeafNode.Cells, 0, aCellToRotate)
	aPage.LeafNode.Header.Cells += 1

	aParent.InternalNode.ICells[idx-1].Key = aCellToRotate.Key

	return nil
}

func (t *Table) rotateLeft(ctx context.Context, aParent, aPage, rightSibling *Page, idx uint32) error {
	aCellToRotate := rightSibling.LeafNode.Cells[0]
	rightSibling.LeafNode.Cells = rightSibling.LeafNode.Cells[1:rightSibling.LeafNode.Header.Cells]
	rightSibling.LeafNode.Header.Cells -= 1

	aPage.LeafNode.Cells = append(aPage.LeafNode.Cells, aCellToRotate)
	aPage.LeafNode.Header.Cells += 1

	aParent.InternalNode.ICells[idx].Key = rightSibling.LeafNode.Cells[0].Key

	return nil
}

func (t *Table) merge(aParent *Page, idx uint32, aPredecessor, aSuccessor *Page) error {
	aPredecessor.LeafNode.Cells = append(aPredecessor.LeafNode.Cells, aSuccessor.LeafNode.Cells...)
	aPredecessor.LeafNode.Header.Cells += aSuccessor.LeafNode.Header.Cells
	aParent.InternalNode.DeleteKeyByIndex(idx)

	return nil
}

func (t *Table) rotateRightInternal(ctx context.Context, aParent, leftSibling *Page, idx uint32) error {
	aPage, err := t.pager.GetPage(ctx, t, idx)
	if err != nil {
		return err
	}

	aCellToRotate := leftSibling.InternalNode.ICells[leftSibling.InternalNode.Header.KeysNum-1]
	leftSibling.InternalNode.ICells[leftSibling.InternalNode.Header.KeysNum-1] = ICell{}
	leftSibling.InternalNode.Header.KeysNum -= 1

	for i := int(aPage.InternalNode.Header.KeysNum); i > 0; i-- {
		aPage.InternalNode.ICells[i] = aPage.InternalNode.ICells[i-1]
	}
	aPage.InternalNode.ICells[0] = aCellToRotate
	aPage.InternalNode.Header.KeysNum += 1

	aParent.InternalNode.ICells[idx-1].Key = aCellToRotate.Key

	return nil
}

func (t *Table) rotateLeftInternal(ctx context.Context, aParent, rightSibling *Page, idx uint32) error {
	aPage, err := t.pager.GetPage(ctx, t, idx)
	if err != nil {
		return err
	}

	aCellToRotate := rightSibling.InternalNode.ICells[0]
	for i := 0; i < int(rightSibling.InternalNode.Header.KeysNum); i++ {
		rightSibling.InternalNode.ICells[i] = rightSibling.InternalNode.ICells[i+1]
	}
	rightSibling.InternalNode.ICells[rightSibling.InternalNode.Header.KeysNum-1] = ICell{}
	rightSibling.InternalNode.Header.KeysNum -= 1

	aPage.InternalNode.ICells[aPage.InternalNode.Header.KeysNum] = aCellToRotate
	aPage.InternalNode.Header.KeysNum += 1

	aParent.InternalNode.ICells[idx].Key = rightSibling.InternalNode.ICells[0].Key

	return nil
}
