package minisql

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

const MaxIndexKeySize = 255

type IndexKey interface {
	int8 | int32 | int64 | float32 | float64 | string
}

type UniqueIndex[T IndexKey] struct {
	logger      *zap.Logger
	Name        string
	Column      Column
	rootPageIdx PageIndex
	pager       TxPager
	txManager   *TransactionManager
	writeLock   sync.RWMutex
	maximumKeys uint32
}

func NewUniqueIndex[T IndexKey](logger *zap.Logger, txManager *TransactionManager, name string, aColumn Column, pager TxPager, rootPageIdx PageIndex) (*UniqueIndex[T], error) {
	if aColumn.Kind == Text {
		return nil, fmt.Errorf("unique index does not support text columns")
	}
	if aColumn.Kind == Varchar && aColumn.Size > MaxIndexKeySize {
		return nil, fmt.Errorf("unique index does not support varchar columns larger than %d", MaxIndexKeySize)
	}
	return &UniqueIndex[T]{
		logger:      logger,
		Name:        name,
		Column:      aColumn,
		rootPageIdx: rootPageIdx,
		pager:       pager,
		txManager:   txManager,
	}, nil
}

func (ui *UniqueIndex[T]) GetRootPageIdx() PageIndex {
	return ui.rootPageIdx
}

func (ui *UniqueIndex[T]) Insert(ctx context.Context, keyAny any, rowID uint64) error {
	key, ok := keyAny.(T)
	if !ok {
		return fmt.Errorf("invalid key type: %T", keyAny)
	}

	aRootPage, err := ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}
	aRootNode := aRootPage.IndexNode.(*IndexNode[T])

	// Root is empty, insert the first key
	if aRootNode.Header.Keys == 0 {
		aRootNode.Cells[0] = IndexCell[T]{
			Key:   key,
			RowID: rowID,
		}
		aRootNode.Header.IsRoot = true
		aRootNode.Header.IsLeaf = true
		aRootNode.Header.Keys += 1
		return nil
	}

	// Check for duplicate key
	_, ok, err = ui.Seek(ctx, aRootPage, key)
	if err != nil {
		return fmt.Errorf("seek key: %w", err)
	}
	if ok {
		return ErrDuplicateKey
	}

	// Root is not full, insert new key
	if ui.hasSpaceForKey(aRootNode, key) {
		return ui.insertNotFull(ctx, ui.GetRootPageIdx(), key, rowID)
	}

	// Root is full, need to split. Old root page will become left child
	newLeftChild, err := ui.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new left child page: %w", err)
	}
	newLeftChildNode := NewIndexNode[T]()
	*newLeftChildNode = *aRootNode
	newLeftChildNode.Header.Parent = aRootPage.Index
	newLeftChildNode.Header.IsRoot = false
	// Update children to set new parent
	for _, childIdx := range newLeftChildNode.Children() {
		aChildPage, err := ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		aChildNode := aChildPage.IndexNode.(*IndexNode[T])
		aChildNode.Header.Parent = newLeftChild.Index
	}
	newLeftChild.IndexNode = newLeftChildNode

	aRootNode = NewIndexNode[T]()
	aRootNode.Header.IsRoot = true
	aRootNode.Header.IsLeaf = false
	aRootNode.Header.Keys = 0
	aRootNode.Header.RightChild = RIGHT_CHILD_NOT_SET
	aRootNode.Cells[0].Child = newLeftChild.Index
	aRootPage.IndexNode = aRootNode

	if err := ui.splitChild(ctx, aRootPage, newLeftChild, 0); err != nil {
		return fmt.Errorf("split child: %w", err)
	}
	i := uint32(0)
	if aRootNode.Cells[0].Key < key {
		i += 1
	}
	childIdx, err := aRootNode.Child(i)
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	if err := ui.insertNotFull(ctx, childIdx, key, rowID); err != nil {
		return fmt.Errorf("insert not full: %w", err)
	}
	return nil
}

func (idx *UniqueIndex[T]) hasSpaceForKey(aNode *IndexNode[T], key T) bool {
	if idx.maximumKeys != 0 {
		return aNode.Header.Keys < idx.maximumKeys
	}
	return aNode.HasSpaceForKey(key)
}

func (idx *UniqueIndex[T]) atLeastHalfFull(aNode *IndexNode[T]) bool {
	if idx.maximumKeys != 0 {
		return aNode.Header.Keys >= (idx.maximumKeys+1)/2
	}
	return aNode.AtLeastHalfFull()
}

var ErrDuplicateKey = fmt.Errorf("duplicate key")

func (ui *UniqueIndex[T]) insertNotFull(ctx context.Context, pageIdx PageIndex, key T, rowID uint64) error {
	aPage, err := ui.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])

	i := int(aNode.Header.Keys) - 1

	if aNode.Header.IsLeaf {
		aNode.Cells = append(aNode.Cells, IndexCell[T]{})
		for i >= 0 && aNode.Cells[i].Key > key {
			aNode.Cells[i+1].Key = aNode.Cells[i].Key
			aNode.Cells[i+1].RowID = aNode.Cells[i].RowID
			i -= 1
		}
		aNode.Cells[i+1].Key = key
		aNode.Cells[i+1].RowID = rowID
		aNode.Header.Keys += 1
		return nil
	}

	// Find the child which is going to have the new key
	for i >= 0 && aNode.Cells[i].Key > key {
		i -= 1
	}

	childIdx, err := aNode.Child(uint32(i + 1))
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childNode := childPage.IndexNode.(*IndexNode[T])
	if !ui.hasSpaceForKey(childNode, key) {
		if err := ui.splitChild(ctx, aPage, childPage, uint32(i+1)); err != nil {
			return fmt.Errorf("split child: %w", err)
		}
		if aNode.Cells[i+1].Key < key {
			i += 1
		}
	}

	// Recurse to child node
	childIdx, err = aNode.Child(uint32(i + 1))
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	return ui.insertNotFull(ctx, childIdx, key, rowID)
}

// Split a child node into two nodes and move the median key up to the parent node
func (ui *UniqueIndex[T]) splitChild(ctx context.Context, parentPage, splitPage *Page, indexInParent uint32) error {
	parentNode := parentPage.IndexNode.(*IndexNode[T])
	splitNode := splitPage.IndexNode.(*IndexNode[T])

	newPage, err := ui.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}
	newNode := NewIndexNode[T]()
	newNode.Header.Parent = splitNode.Header.Parent
	newNode.Header.IsLeaf = splitNode.Header.IsLeaf
	newPage.IndexNode = newNode

	// Move smaller half to the new node
	var (
		rightCount = (splitNode.Header.Keys+1)/2 - 1
		leftCount  = splitNode.Header.Keys - rightCount
	)
	newNode.Header.Keys = rightCount
	// We will be moving rightmost cell (previous media cell) to the parent,
	// that's why we are substracting one from the key count
	splitNode.Header.Keys = leftCount - 1

	// Move everything to the right of median to new node,
	newNode.Header.RightChild = splitNode.Header.RightChild
	splitNode.Header.RightChild = splitNode.Cells[leftCount-1].Child
	for j := int(0); j < int(rightCount); j++ {
		if len(newNode.Cells) <= j {
			newNode.Cells = append(newNode.Cells, IndexCell[T]{})
		}
		newNode.Cells[j] = splitNode.Cells[j+int(leftCount)]
		splitNode.Cells[j+int(leftCount)] = IndexCell[T]{}
	}
	// Update children to set new parent
	for _, childIdx := range newNode.Children() {
		aChildPage, err := ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		aChildNode := aChildPage.IndexNode.(*IndexNode[T])
		aChildNode.Header.Parent = newPage.Index
	}

	// Update parent
	rowIDToMoveUp := splitNode.Cells[leftCount-1].RowID
	if len(parentNode.Cells) < int(parentNode.Header.Keys+1) {
		parentNode.Cells = append(parentNode.Cells, IndexCell[T]{})
	}
	for j := int(parentNode.Header.Keys) - 1; j >= int(indexInParent); j-- {
		parentNode.Cells[j+1].Key = parentNode.Cells[j].Key
		parentNode.Cells[j+1].RowID = parentNode.Cells[j].RowID
	}
	parentNode.Header.Keys += 1
	if len(parentNode.Cells) < int(indexInParent) {
		parentNode.Cells = append(parentNode.Cells, IndexCell[T]{})
	}
	parentNode.Cells[indexInParent].Key = splitNode.Cells[leftCount-1].Key
	parentNode.Cells[indexInParent].RowID = rowIDToMoveUp
	splitNode.Cells[leftCount] = IndexCell[T]{}

	for j := int(parentNode.Header.Keys) - 1; j > int(indexInParent); j-- {
		if j+1 >= int(parentNode.Header.Keys) {
			continue
		}
		if err := parentNode.SetChild(uint32(j+1), parentNode.Cells[j].Child); err != nil {
			return fmt.Errorf("set child 1: %w", err)
		}
	}
	if err := parentNode.SetChild(indexInParent, splitPage.Index); err != nil {
		return fmt.Errorf("set child 2: %w", err)
	}
	if err := parentNode.SetChild(indexInParent+1, newPage.Index); err != nil {
		return fmt.Errorf("set child 2: %w", err)
	}

	return nil
}

type indexCallback func(page *Page)

func (ui *UniqueIndex[T]) BFS(ctx context.Context, f indexCallback) error {
	rootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
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

		if current.IndexNode != nil {
			for i := range current.IndexNode.(*IndexNode[T]).Header.Keys {
				idxCell := current.IndexNode.(*IndexNode[T]).Cells[i]
				if idxCell.Child == 0 {
					continue
				}
				aPage, err := ui.pager.ReadPage(ctx, idxCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
			if current.IndexNode.(*IndexNode[T]).Header.RightChild > 0 && current.IndexNode.(*IndexNode[T]).Header.RightChild != RIGHT_CHILD_NOT_SET {
				aPage, err := ui.pager.ReadPage(ctx, current.IndexNode.(*IndexNode[T]).Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
		}
	}

	return nil
}

func (ui *UniqueIndex[T]) print() error {
	return ui.BFS(context.Background(), func(aPage *Page) {
		aNode := aPage.IndexNode.(*IndexNode[T])
		fmt.Println("Index node,", "page:", aPage.Index, "leaf", aNode.Header.IsLeaf, "number of keys:", aNode.Header.Keys, "parent:", aNode.Header.Parent, "right child:", aNode.Header.RightChild)
		fmt.Println("Keys:", aNode.Keys())
		fmt.Println("Row IDs:", aNode.RowIDs())
		fmt.Println("Children:", aNode.Children())
		fmt.Println("---------")
	})
}

func (ui *UniqueIndex[T]) Delete(ctx context.Context, keyAny any) error {
	key, ok := keyAny.(T)
	if !ok {
		return fmt.Errorf("invalid key type: %T", keyAny)
	}

	aRootPage, err := ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}

	if err := ui.remove(ctx, aRootPage, key); err != nil {
		return fmt.Errorf("remove key: %w", err)
	}

	aRootNode := aRootPage.IndexNode.(*IndexNode[T])
	if aRootNode.Header.Keys == 0 {
		if aRootNode.Header.IsLeaf {
			aRootNode = NewIndexNode[T]()
			aRootNode.Header.IsRoot = true
			aRootNode.Header.IsLeaf = false
			aRootNode.Header.Keys = 0
			aRootNode.Header.RightChild = RIGHT_CHILD_NOT_SET
			return nil
		}

		// Root has no keys, make first child the new root
		firstChildIdx, err := aRootNode.Child(0)
		if err != nil {
			return fmt.Errorf("get first child: %w", err)
		}
		firstChildPage, err := ui.pager.ModifyPage(ctx, firstChildIdx)
		if err != nil {
			return fmt.Errorf("get new root page: %w", err)
		}
		firstChildNode := firstChildPage.IndexNode.(*IndexNode[T])
		*aRootNode = *firstChildNode
		aRootNode.Header.Parent = 0
		aRootNode.Header.IsRoot = true
		// Update children to set new parent
		for _, childIdx := range aRootNode.Children() {
			aChildPage, err := ui.pager.ModifyPage(ctx, childIdx)
			if err != nil {
				return fmt.Errorf("get child page: %w", err)
			}
			aChildNode := aChildPage.IndexNode.(*IndexNode[T])
			aChildNode.Header.Parent = ui.GetRootPageIdx()
		}
		if err := ui.pager.AddFreePage(ctx, firstChildIdx); err != nil {
			return fmt.Errorf("add free page: %w", err)
		}
	}

	return nil
}

// remove a key from the sub-tree rooted with this node
func (ui *UniqueIndex[T]) remove(ctx context.Context, aPage *Page, key T) error {
	aNode := aPage.IndexNode.(*IndexNode[T])

	// Find index of the first key greater than or equal to key
	idx := 0
	for idx < int(aNode.Header.Keys) && aNode.Cells[idx].Key < key {
		idx += 1
	}

	if idx < int(aNode.Header.Keys) && aNode.Cells[idx].Key == key {
		// If the key is in this node and this is a leaf node, just remove the key
		if aNode.Header.IsLeaf {
			aNode.DeleteKeyByIndex(uint32(idx))
			return nil
		}

		// Otherwise call recursive function to remove the key from internal node
		if err := ui.removeFromInternal(ctx, aPage, idx); err != nil {
			return fmt.Errorf("remove from internal: %w", err)
		}

		return nil
	}

	if aNode.Header.IsLeaf {
		return fmt.Errorf("the key %v does not exist in the tree", key)
	}

	flag := idx == int(aNode.Header.Keys)

	childIdx, err := aNode.Child(uint32(idx))
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}

	childNode := childPage.IndexNode.(*IndexNode[T])
	if !ui.atLeastHalfFull(childNode) {
		if err := ui.fill(ctx, aPage, childPage, idx); err != nil {
			return fmt.Errorf("fill child: %w", err)
		}
	}

	if flag && idx > int(aNode.Header.Keys) {
		prevChildIdx, err := aNode.Child(uint32(idx - 1))
		if err != nil {
			return fmt.Errorf("get prev child: %w", err)
		}
		prevChildPage, err := ui.pager.ModifyPage(ctx, prevChildIdx)
		if err != nil {
			return fmt.Errorf("get prev child page: %w", err)
		}
		return ui.remove(ctx, prevChildPage, key)
	}

	return ui.remove(ctx, childPage, key)
}

func (ui *UniqueIndex[T]) removeFromInternal(ctx context.Context, aPage *Page, idx int) error {
	var (
		aNode = aPage.IndexNode.(*IndexNode[T])
		key   = aNode.Cells[idx].Key
	)

	leftChildPage, err := ui.pager.ModifyPage(ctx, aNode.Cells[idx].Child)
	if err != nil {
		return fmt.Errorf("get left child page: %w", err)
	}
	leftChildNode := leftChildPage.IndexNode.(*IndexNode[T])

	rightPageIdx, err := aNode.Child(uint32(idx + 1))
	if err != nil {
		return fmt.Errorf("get right child index: %w", err)
	}
	rightChildPage, err := ui.pager.ModifyPage(ctx, rightPageIdx)
	if err != nil {
		return fmt.Errorf("get right child page: %w", err)
	}
	rightChildNode := rightChildPage.IndexNode.(*IndexNode[T])

	if ui.atLeastHalfFull(leftChildNode) {
		predecessor, err := ui.getPred(ctx, aNode, idx)
		if err != nil {
			return fmt.Errorf("get predecessor key: %w", err)
		}

		aNode.Cells[idx].Key = predecessor.Key
		aNode.Cells[idx].RowID = predecessor.RowID
		if err := ui.remove(ctx, leftChildPage, predecessor.Key); err != nil {
			return fmt.Errorf("remove predecessor key: %w", err)
		}
	} else if ui.atLeastHalfFull(rightChildNode) {
		successor, err := ui.getSucc(ctx, aNode, idx)
		if err != nil {
			return fmt.Errorf("get successor key: %w", err)
		}

		aNode.Cells[idx].Key = successor.Key
		aNode.Cells[idx].RowID = successor.RowID
		if err := ui.remove(ctx, rightChildPage, successor.Key); err != nil {
			return fmt.Errorf("remove successor key: %w", err)
		}
	} else {
		if err := ui.merge(ctx, aPage, leftChildPage, rightChildPage, uint32(idx)); err != nil {
			return fmt.Errorf("merge children: %w", err)
		}
		if err := ui.pager.AddFreePage(ctx, rightChildPage.Index); err != nil {
			return fmt.Errorf("add free page: %w", err)
		}
		if err := ui.remove(ctx, leftChildPage, key); err != nil {
			return fmt.Errorf("remove key from merged child: %w", err)
		}
	}

	return nil
}

// getPred finds predecessor of the key at the idx-th position in the node
func (ui *UniqueIndex[T]) getPred(ctx context.Context, aNode *IndexNode[T], idx int) (IndexCell[T], error) {
	curPage, err := ui.pager.ModifyPage(ctx, aNode.Cells[idx].Child)
	if err != nil {
		return *new(IndexCell[T]), fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		aPage, err := ui.pager.ModifyPage(ctx, cur.Header.RightChild)
		if err != nil {
			return *new(IndexCell[T]), fmt.Errorf("get page: %w", err)
		}
		cur = aPage.IndexNode.(*IndexNode[T])
	}

	return cur.LastCell(), nil
}

// A function to get the successor of the key at the idx-th position in the node
func (ui *UniqueIndex[T]) getSucc(ctx context.Context, aNode *IndexNode[T], idx int) (IndexCell[T], error) {
	curIdx, err := aNode.Child(uint32(idx + 1))
	if err != nil {
		return *new(IndexCell[T]), fmt.Errorf("get child index: %w", err)
	}
	curPage, err := ui.pager.ModifyPage(ctx, curIdx)
	if err != nil {
		return *new(IndexCell[T]), fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		aPage, err := ui.pager.ModifyPage(ctx, cur.Cells[0].Child)
		if err != nil {
			return *new(IndexCell[T]), fmt.Errorf("get page: %w", err)
		}
		cur = aPage.IndexNode.(*IndexNode[T])
	}

	return cur.FirstCell(), nil
}

func (ui *UniqueIndex[T]) fill(ctx context.Context, aParent, aPage *Page, idx int) error {
	var (
		parentNode = aParent.IndexNode.(*IndexNode[T])
		left       *Page
		right      *Page
		leftNode   *IndexNode[T]
		rightNode  *IndexNode[T]
	)

	var err error
	if idx != 0 {
		left, err = ui.pager.ModifyPage(ctx, parentNode.Cells[idx-1].Child)
		if err != nil {
			return fmt.Errorf("get left page: %w", err)
		}
		leftNode = left.IndexNode.(*IndexNode[T])
	}
	if idx != int(parentNode.Header.Keys) {
		right, err = ui.pager.ModifyPage(ctx, parentNode.GetRightChildByIndex(uint32(idx)))
		if err != nil {
			return fmt.Errorf("get right page: %w", err)
		}
		rightNode = right.IndexNode.(*IndexNode[T])
	}

	if left != nil && ui.atLeastHalfFull(leftNode) {
		return ui.borrowFromLeft(ctx, aParent, aPage, left, uint32(idx))
	}

	if right != nil && ui.atLeastHalfFull(rightNode) {
		return ui.borrowFromRight(ctx, aParent, aPage, right, uint32(idx))
	}

	if idx != int(parentNode.Header.Keys) {
		if err := ui.merge(ctx, aParent, aPage, right, uint32(idx)); err != nil {
			return fmt.Errorf("merge with left: %w", err)
		}

		return ui.pager.AddFreePage(ctx, right.Index)
	}

	if err := ui.merge(ctx, aParent, left, aPage, uint32(idx-1)); err != nil {
		return fmt.Errorf("merge with right: %w", err)
	}
	return ui.pager.AddFreePage(ctx, aPage.Index)
}

// A function to borrow a key from left sibling
func (ui *UniqueIndex[T]) borrowFromLeft(ctx context.Context, aParent, aPage, left *Page, idx uint32) error {
	var (
		parentNode = aParent.IndexNode.(*IndexNode[T])
		aNode      = aPage.IndexNode.(*IndexNode[T])
		leftNode   = left.IndexNode.(*IndexNode[T])
	)

	aNode.PrependCell(IndexCell[T]{
		Key:   parentNode.Cells[idx-1].Key,
		RowID: parentNode.Cells[idx-1].RowID,
		Child: leftNode.Header.RightChild,
	})

	if !leftNode.Header.IsLeaf {
		childPage, err := ui.pager.ModifyPage(ctx, leftNode.Header.RightChild)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childPage.IndexNode.(*IndexNode[T]).setParent(aPage.Index)
	}

	parentNode.Cells[idx-1].Key = leftNode.LastCell().Key
	parentNode.Cells[idx-1].RowID = leftNode.LastCell().RowID

	leftNode.RemoveLastCell()

	return nil
}

// A function to borrow a key from right sibling
func (ui *UniqueIndex[T]) borrowFromRight(ctx context.Context, aParent, aPage, right *Page, idx uint32) error {
	var (
		parentNode = aParent.IndexNode.(*IndexNode[T])
		aNode      = aPage.IndexNode.(*IndexNode[T])
		rightNode  = right.IndexNode.(*IndexNode[T])
	)

	aNode.AppendCells(IndexCell[T]{
		Key:   parentNode.Cells[idx].Key,
		RowID: parentNode.Cells[idx].RowID,
		Child: aNode.Header.RightChild,
	})
	aNode.Header.RightChild = rightNode.FirstCell().Child

	if !aNode.Header.IsLeaf {
		childPage, err := ui.pager.ModifyPage(ctx, aNode.Header.RightChild)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childPage.IndexNode.(*IndexNode[T]).setParent(aPage.Index)
	}

	parentNode.Cells[idx].Key = rightNode.FirstCell().Key
	parentNode.Cells[idx].RowID = rightNode.FirstCell().RowID

	rightNode.RemoveFirstCell()

	return nil
}

// A function to merge right child node into left child node
func (ui *UniqueIndex[T]) merge(ctx context.Context, aParent, left, right *Page, idx uint32) error {
	var (
		parentNode = aParent.IndexNode.(*IndexNode[T])
		leftNode   = left.IndexNode.(*IndexNode[T])
		rightNode  = right.IndexNode.(*IndexNode[T])
		leftIndex  = left.Index
	)
	if parentNode.Header.IsRoot && parentNode.Header.Keys == 1 {
		leftIndex = ui.GetRootPageIdx()
	}

	// Update parent of all cells we are moving to the left node
	cellsToMoveLeft := rightNode.Cells[0:rightNode.Header.Keys]

	if !rightNode.Header.IsLeaf {
		for _, iCell := range cellsToMoveLeft {
			movedPage, err := ui.pager.ModifyPage(ctx, iCell.Child)
			if err != nil {
				return fmt.Errorf("get moved page: %w", err)
			}
			movedPage.IndexNode.(*IndexNode[T]).setParent(leftIndex)
		}
	}
	if !leftNode.Header.IsLeaf {
		newRightChildPage, err := ui.pager.ModifyPage(ctx, rightNode.Header.RightChild)
		if err != nil {
			return fmt.Errorf("get new right child page: %w", err)
		}
		newRightChildPage.IndexNode.(*IndexNode[T]).setParent(leftIndex)
	}

	aCell := IndexCell[T]{
		Child: leftNode.Header.RightChild,
		Key:   parentNode.Cells[idx].Key,
		RowID: parentNode.Cells[idx].RowID,
	}
	leftNode.AppendCells(append([]IndexCell[T]{aCell}, cellsToMoveLeft...)...)
	leftNode.Header.RightChild = rightNode.Header.RightChild

	parentNode.DeleteKeyByIndex(idx)

	return nil
}
