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

// SeekMaxKey returns max key stored in a tree, starting from page index
func (t *Table) SeekMaxKey(ctx context.Context, pageIdx uint32) (uint64, bool, error) {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return 0, false, err
	}

	if aPage.LeafNode != nil {
		if aPage.LeafNode.Header.NextLeaf == 0 {
			maxKey, ok := aPage.GetMaxKey()
			return maxKey, ok, nil
		}
		return t.SeekMaxKey(ctx, aPage.LeafNode.Header.NextLeaf)
	}

	return t.SeekMaxKey(ctx, aPage.InternalNode.Header.RightChild)
}

// Seek the cursor for a key, if it does not exist then return the cursor
// for the page and cell where it should be inserted
func (t *Table) Seek(ctx context.Context, key uint64) (*Cursor, error) {
	rootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return nil, err
	}
	if rootPage.LeafNode != nil {
		return t.leafNodeSeek(ctx, t.RootPageIdx, rootPage, key)
	} else if rootPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, rootPage, key)
	}
	return nil, fmt.Errorf("root page type")
}

func (t *Table) leafNodeSeek(ctx context.Context, pageIdx uint32, aPage *Page, key uint64) (*Cursor, error) {
	var minIdx, maxIdx uint32

	maxIdx = aPage.LeafNode.Header.Cells

	aCursor := Cursor{
		Table:   t,
		PageIdx: pageIdx,
	}

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

	childPage, err := t.pager.GetPage(ctx, t, childPageIdx)
	if err != nil {
		return nil, err
	}

	if childPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, childPage, key)
	}
	return t.leafNodeSeek(ctx, childPageIdx, childPage, key)
}

// Handle splitting the root.
// Old root copied to new page, becomes left child.
// Address of right child passed in.
// Re-initialize root page to contain the new root node.
// New root node points to two children.
func (t *Table) CreateNewRoot(ctx context.Context, rightChildPageIdx uint32) error {
	oldRootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return err
	}

	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return err
	}

	leftChildPageIdx := t.pager.TotalPages()
	leftChildPage, err := t.pager.GetPage(ctx, t, leftChildPageIdx)
	if err != nil {
		return err
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
		return err
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

	return nil
}

// Add a new child/key pair to parent that corresponds to child
func (t *Table) InternalNodeInsert(ctx context.Context, parentPageIdx, childPageIdx uint32) error {
	parentPage, err := t.pager.GetPage(ctx, t, parentPageIdx)
	if err != nil {
		return err
	}
	childPage, err := t.pager.GetPage(ctx, t, childPageIdx)
	if err != nil {
		return err
	}
	childMaxKey, _ := childPage.GetMaxKey()
	index := parentPage.InternalNode.FindChildByKey(childMaxKey)
	originalKeyCnt := parentPage.InternalNode.Header.KeysNum
	parentPage.InternalNode.Header.KeysNum += 1

	if parentPage.InternalNode.Header.KeysNum > InternalNodeMaxCells {
		return fmt.Errorf("exceeded internal node max cells during internal node insert")
	}

	rightChildPageIdx := parentPage.InternalNode.Header.RightChild
	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return err
	}

	rightChildMaxKey, _ := rightChildPage.GetMaxKey()
	if childMaxKey > rightChildMaxKey {
		/* Replace right child */
		parentPage.InternalNode.SetChildIdx(originalKeyCnt, rightChildPageIdx)
		parentPage.InternalNode.ICells[originalKeyCnt].Key = rightChildMaxKey
		parentPage.InternalNode.Header.RightChild = childPageIdx
		return nil
	}

	/* Make room for the new cell */
	for i := originalKeyCnt; i > index; i-- {
		parentPage.InternalNode.ICells[i] = parentPage.InternalNode.ICells[i-1]
	}
	parentPage.InternalNode.SetChildIdx(index, childPageIdx)
	parentPage.InternalNode.ICells[index].Key = childMaxKey

	return nil
}
