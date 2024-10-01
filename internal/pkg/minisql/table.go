package minisql

import (
	"context"
	"fmt"

	"github.com/RichardKnop/minisql/internal/pkg/node"
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
	RowSize     uint32
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

func (t *Table) SeekMaxKey(ctx context.Context, pageIdx uint32) (uint32, bool, error) {
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

// Seek the page of key, if not exist then return the place key should be
// for the later INSERT.
func (t *Table) Seek(ctx context.Context, key uint32) (*Cursor, error) {
	rootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return nil, err
	}
	if rootPage.LeafNode != nil {
		return t.leafNodeSeek(ctx, t.RootPageIdx, key)
	} else if rootPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, t.RootPageIdx, key)
	}
	return nil, fmt.Errorf("root page type")
}

func (t *Table) leafNodeSeek(ctx context.Context, pageIdx, key uint32) (*Cursor, error) {
	var (
		minIdx, maxIdx, i uint32
	)

	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return nil, err
	}
	maxIdx = aPage.LeafNode.Header.Cells

	aCursor := Cursor{
		Table:      t,
		PageIdx:    pageIdx,
		EndOfTable: false,
	}

	// Search the Btree
	for i = maxIdx; i != minIdx; {
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

func (t *Table) internalNodeSeek(ctx context.Context, pageIdx, key uint32) (*Cursor, error) {
	aPage, err := t.pager.GetPage(ctx, t, pageIdx)
	if err != nil {
		return nil, err
	}

	nodeIdx := aPage.InternalNode.FindChildByKey(key)
	childIdx, err := aPage.InternalNode.Child(nodeIdx)
	if err != nil {
		return nil, err
	}

	childPage, err := t.pager.GetPage(ctx, t, childIdx)
	if err != nil {
		return nil, err
	}

	if childPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, childIdx, key)
	}
	return t.leafNodeSeek(ctx, childIdx, key)
}

func (t *Table) CreateNewRoot(ctx context.Context, rightChildPageIdx uint32) error {
	/*
	  Handle splitting the root.
	  Old root copied to new page, becomes left child.
	  Address of right child passed in.
	  Re-initialize root page to contain the new root node.
	  New root node points to two children.
	*/

	rootPage, err := t.pager.GetPage(ctx, t, t.RootPageIdx)
	if err != nil {
		return err
	}

	rightChildPage, err := t.pager.GetPage(ctx, t, rightChildPageIdx)
	if err != nil {
		return err
	}
	leftChildPageIdx := t.pager.TotalPages(t)
	leftChildPage, err := t.pager.GetPage(ctx, t, leftChildPageIdx)
	if err != nil {
		return err
	}

	// copy whatever kind of node to leftChildPage, and set nonRoot
	if rootPage.LeafNode != nil {
		*leftChildPage.LeafNode = *rootPage.LeafNode
		leftChildPage.LeafNode.Header.IsRoot = false
	} else if rootPage.InternalNode != nil {
		*leftChildPage.InternalNode = *rootPage.InternalNode
		leftChildPage.InternalNode.Header.IsRoot = false
	}

	rootPage.LeafNode = nil
	rootPage.InternalNode = new(node.InternalNode)
	rootNode := rootPage.InternalNode
	// change root node to internal node
	rootNode.Header.IsRoot = false
	rootNode.Header.IsInternal = true
	rootNode.Header.KeysNum = 1
	rootNode.Header.Parent = 0
	rootNode.Header.RightChild = 0

	if err := rootNode.SetChildIdx(0, leftChildPageIdx); err != nil {
		return err
	}

	leftChildMaxKey, _ := leftChildPage.GetMaxKey()
	rootNode.ICells[0].Key = leftChildMaxKey
	rootNode.Header.RightChild = rightChildPageIdx

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

func (t *Table) InternalNodeInsert(ctx context.Context, parentPageIdx, childPageIdx uint32) error {
	/*
	  Add a new child/key pair to parent that corresponds to child
	*/
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

	if parentPage.InternalNode.Header.KeysNum > node.InternalNodeMaxCells {
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
