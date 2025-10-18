package minisql

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type UniqueIndex[T int8 | int32 | int64 | float32 | float64 | string] struct {
	Name        string
	Column      Column
	RootPageIdx uint32
	pager       Pager
	writeLock   *sync.RWMutex
	logger      *zap.Logger
	maximumKeys uint32
	debug       bool
}

func NewUniqueIndex[T int8 | int32 | int64 | float32 | float64 | string](logger *zap.Logger, name string, column Column, pager Pager, rootPageIdx uint32) *UniqueIndex[T] {
	return &UniqueIndex[T]{
		Name:        name,
		Column:      column,
		RootPageIdx: rootPageIdx,
		pager:       pager,
		writeLock:   new(sync.RWMutex),
		logger:      logger,
	}
}

func (ui *UniqueIndex[T]) Insert(ctx context.Context, key T, rowID uint64) error {
	aRootPage, err := ui.pager.GetPage(ctx, ui.RootPageIdx)
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
	_, ok, err := ui.Seek(ctx, aRootPage, key)
	if err != nil {
		return fmt.Errorf("seek key: %w", err)
	}
	if ok {
		return ErrDuplicateKey
	}

	// Root is not full, insert new key
	if aRootNode.Header.Keys < ui.maxIndexKeys(aRootNode.KeySize) {
		return ui.InsertNotFull(ctx, ui.RootPageIdx, key, rowID)
	}

	// Root is full, need to split. Old root page will become left child
	newLeftChild, err := ui.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new left child page: %w", err)
	}
	newLeftChildNode := NewIndexNode[T](aRootPage.IndexNode.(*IndexNode[T]).KeySize)
	*newLeftChildNode = *aRootNode
	newLeftChildNode.Header.Parent = aRootPage.Index
	newLeftChildNode.Header.IsRoot = false
	// Update children to set new parent
	for _, childIdx := range newLeftChildNode.Children() {
		aChildPage, err := ui.pager.GetPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		aChildNode := aChildPage.IndexNode.(*IndexNode[T])
		aChildNode.Header.Parent = newLeftChild.Index
	}
	newLeftChild.IndexNode = newLeftChildNode

	aRootNode = NewIndexNode[T](aRootPage.IndexNode.(*IndexNode[T]).KeySize)
	aRootNode.Header.IsRoot = true
	aRootNode.Header.IsLeaf = false
	aRootNode.Header.Keys = 0
	aRootNode.Header.RightChild = RIGHT_CHILD_NOT_SET
	aRootNode.Cells[0].Child = newLeftChild.Index
	aRootPage.IndexNode = aRootNode

	if err := ui.SplitChild(ctx, aRootPage, newLeftChild, 0); err != nil {
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
	if err := ui.InsertNotFull(ctx, childIdx, key, rowID); err != nil {
		return fmt.Errorf("insert not full: %w", err)
	}
	return nil
}

func (idx *UniqueIndex[T]) maxIndexKeys(keySize uint64) uint32 {
	if idx.maximumKeys != 0 {
		return idx.maximumKeys
	}
	// index header = 14
	// each cell = keySize + 8 + 4
	return uint32((PageSize - 14) / (keySize + 8 + 4))
}

var ErrDuplicateKey = fmt.Errorf("duplicate key")

func (ui *UniqueIndex[T]) InsertNotFull(ctx context.Context, pageIdx uint32, key T, rowID uint64) error {
	aPage, err := ui.pager.GetPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])

	i := int(aNode.Header.Keys) - 1

	if aNode.Header.IsLeaf {
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
	childPage, err := ui.pager.GetPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childNode := childPage.IndexNode.(*IndexNode[T])
	if childNode.Header.Keys == ui.maxIndexKeys(childNode.KeySize) {
		if err := ui.SplitChild(ctx, aPage, childPage, uint32(i+1)); err != nil {
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
	return ui.InsertNotFull(ctx, childIdx, key, rowID)
}

// Split a child node into two nodes and move the median key up to the parent node
func (ui *UniqueIndex[T]) SplitChild(ctx context.Context, parentPage, splitPage *Page, indexInParent uint32) error {
	parentNode := parentPage.IndexNode.(*IndexNode[T])
	splitNode := splitPage.IndexNode.(*IndexNode[T])

	newPage, err := ui.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}
	newNode := NewIndexNode[T](splitNode.KeySize)
	newNode.Header.Parent = splitNode.Header.Parent
	newNode.Header.IsLeaf = splitNode.Header.IsLeaf
	newPage.IndexNode = newNode

	// Move smaller half to the new node
	var (
		maxKeys     = ui.maxIndexKeys(splitNode.KeySize)
		maxChildren = maxKeys + 1
		rightCount  = maxChildren/2 - 1
		leftCount   = maxKeys - rightCount
	)
	newNode.Header.Keys = rightCount
	// We will be moving rightmost cell (previous media cell) to the parent,
	// that's why we are substracting one from the key count
	splitNode.Header.Keys = leftCount - 1

	// Move everything to the right of median to new node,
	newNode.Header.RightChild = splitNode.Header.RightChild
	splitNode.Header.RightChild = splitNode.Cells[leftCount-1].Child
	for j := int(0); j < int(rightCount); j++ {
		newNode.Cells[j] = splitNode.Cells[j+int(leftCount)]
		splitNode.Cells[j+int(leftCount)] = IndexCell[T]{}
	}
	// Update children to set new parent
	for _, childIdx := range newNode.Children() {
		aChildPage, err := ui.pager.GetPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		aChildNode := aChildPage.IndexNode.(*IndexNode[T])
		aChildNode.Header.Parent = newPage.Index
	}

	// Update parent
	rowIDToMoveUp := splitNode.Cells[leftCount-1].RowID
	for j := int(parentNode.Header.Keys) - 1; j >= int(indexInParent); j-- {
		parentNode.Cells[j+1].Key = parentNode.Cells[j].Key
		parentNode.Cells[j+1].RowID = parentNode.Cells[j].RowID
	}
	parentNode.Header.Keys += 1
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

type indexCallback[T int8 | int32 | int64 | float32 | float64 | string] func(page *Page)

func (ui *UniqueIndex[T]) BFS(f indexCallback[T]) error {

	rootPage, err := ui.pager.GetPage(context.Background(), ui.RootPageIdx)
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
				aPage, err := ui.pager.GetPage(context.Background(), idxCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
			if current.IndexNode.(*IndexNode[T]).Header.RightChild > 0 && current.IndexNode.(*IndexNode[T]).Header.RightChild != RIGHT_CHILD_NOT_SET {
				aPage, err := ui.pager.GetPage(context.Background(), current.IndexNode.(*IndexNode[T]).Header.RightChild)
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
	return ui.BFS(func(aPage *Page) {
		aNode := aPage.IndexNode.(*IndexNode[T])
		fmt.Println("Index node,", "page:", aPage.Index, "number of keys:", aNode.Header.Keys, "parent:", aNode.Header.Parent, "right child:", aNode.Header.RightChild)
		fmt.Println("Keys:", aNode.Keys())
		fmt.Println("Row IDs:", aNode.RowIDs())
		fmt.Println("Children:", aNode.Children())
		fmt.Println("---------")
	})
}

func (ui *UniqueIndex[T]) Delete(ctx context.Context, key T) error {
	aRootPage, err := ui.pager.GetPage(ctx, ui.RootPageIdx)
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}

	if err := ui.remove(ctx, aRootPage, key); err != nil {
		return fmt.Errorf("remove key: %w", err)
	}

	return nil
}

// remove a key from the sub-tree rooted with this node
func (ui *UniqueIndex[T]) remove(ctx context.Context, aPage *Page, key T) error {
	if ui.debug {
		fmt.Println("Removing key:", key, "page index:", aPage.Index)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])

	// Find index of the first key greater than or equal to key
	idx := 0
	for idx < int(aNode.Header.Keys) && aNode.Cells[idx].Key < key {
		idx++
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
	childPage, err := ui.pager.GetPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}

	var (
		childNode = childPage.IndexNode.(*IndexNode[T])
		maxCells  = int(ui.maxIndexKeys(childNode.KeySize))
	)

	if !childNode.AtLeastHalfFull(maxCells) {
		fmt.Println("Filling child", "parent page:", aPage.Index, "child page:", childPage.Index, "idx in parent:", idx, "keys", childNode.Header.Keys)
		if err := ui.fill(ctx, aPage, childPage, idx); err != nil {
			return fmt.Errorf("fill child: %w", err)
		}
	}

	if flag && idx > int(aNode.Header.Keys) {
		prevChildIdx, err := aNode.Child(uint32(idx - 1))
		if err != nil {
			return fmt.Errorf("get prev child: %w", err)
		}
		prevChildPage, err := ui.pager.GetPage(ctx, prevChildIdx)
		if err != nil {
			return fmt.Errorf("get prev child page: %w", err)
		}
		return ui.remove(ctx, prevChildPage, key)
	}

	return ui.remove(ctx, childPage, key)
}

// A function to remove the idx-th key from this node, which is a non-leaf node
// - If the child y that precedes k in node x has at least t keys, then find
// the predecessor k0 of k in the sub-tree rooted at y. Recursively delete k0,
// and replace k with k0 in x. (We can find k0 and delete it in a single downward pass.)
// - If y has fewer than t keys, then, symmetrically, examine the child z
// that follows k in node x. If z has at least t keys, then find the successor k0 of k
// in the subtree rooted at z. Recursively delete k0, and replace k with k0 in x.
// (We can find k0 and delete it in a single downward pass.)
// - Otherwise, if both y and z have only t-1 keys, merge k and all of z into y,
// so that x loses both k and the pointer to z, and y now contains 2t-1 keys.
// Then free z and recursively delete k from y.
func (ui *UniqueIndex[T]) removeFromInternal(ctx context.Context, aPage *Page, idx int) error {
	var (
		aNode    = aPage.IndexNode.(*IndexNode[T])
		key      = aNode.Cells[idx].Key
		maxCells = int(ui.maxIndexKeys(aNode.KeySize))
	)

	leftChildPage, err := ui.pager.GetPage(ctx, aNode.Cells[idx].Child)
	if err != nil {
		return fmt.Errorf("get left child page: %w", err)
	}
	leftChildNode := leftChildPage.IndexNode.(*IndexNode[T])

	rightPageIdx, err := aNode.Child(uint32(idx + 1))
	if err != nil {
		return fmt.Errorf("get right child index: %w", err)
	}
	rightChildPage, err := ui.pager.GetPage(ctx, rightPageIdx)
	if err != nil {
		return fmt.Errorf("get right child page: %w", err)
	}
	rightChildNode := rightChildPage.IndexNode.(*IndexNode[T])

	if leftChildNode.AtLeastHalfFull(maxCells) {
		predKey, err := ui.getPred(ctx, aNode, idx)
		if err != nil {
			return fmt.Errorf("get predecessor key: %w", err)
		}
		if ui.debug {
			fmt.Println("Pred key left:", predKey)
		}

		aNode.Cells[idx].Key = predKey
		if err := ui.remove(ctx, leftChildPage, predKey); err != nil {
			return fmt.Errorf("remove predecessor key: %w", err)
		}
	} else if rightChildNode.AtLeastHalfFull(maxCells) {
		succKey, err := ui.getSucc(ctx, aNode, idx)
		if err != nil {
			return fmt.Errorf("get successor key: %w", err)
		}
		if ui.debug {
			fmt.Println("Successor key right:", succKey)
		}

		aNode.Cells[idx].Key = succKey
		if err := ui.remove(ctx, rightChildPage, succKey); err != nil {
			return fmt.Errorf("remove successor key: %w", err)
		}
	} else {
		if err := ui.merge(ctx, aPage, leftChildPage, rightChildPage, uint32(idx)); err != nil {
			return fmt.Errorf("merge children: %w", err)
		}
		if err := ui.remove(ctx, leftChildPage, key); err != nil {
			return fmt.Errorf("remove key from merged child: %w", err)
		}
	}

	return nil
}

// getPred finds predecessor of the key at the idx-th position in the node
func (ui *UniqueIndex[T]) getPred(ctx context.Context, aNode *IndexNode[T], idx int) (T, error) {
	curPage, err := ui.pager.GetPage(ctx, aNode.Cells[idx].Child)
	if err != nil {
		return *new(T), fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		aPage, err := ui.pager.GetPage(ctx, cur.Header.RightChild)
		if err != nil {
			return *new(T), fmt.Errorf("get page: %w", err)
		}
		cur = aPage.IndexNode.(*IndexNode[T])
	}

	return cur.LastCell().Key, nil
}

// A function to get the successor of the key at the idx-th position in the node
func (ui *UniqueIndex[T]) getSucc(ctx context.Context, aNode *IndexNode[T], idx int) (T, error) {
	curIdx, err := aNode.Child(uint32(idx + 1))
	if err != nil {
		return *new(T), fmt.Errorf("get child index: %w", err)
	}
	curPage, err := ui.pager.GetPage(ctx, curIdx)
	if err != nil {
		return *new(T), fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		aPage, err := ui.pager.GetPage(ctx, cur.Cells[0].Child)
		if err != nil {
			return *new(T), fmt.Errorf("get page: %w", err)
		}
		cur = aPage.IndexNode.(*IndexNode[T])
	}

	return cur.FirstCell().Key, nil
}

func (ui *UniqueIndex[T]) fill(ctx context.Context, aParent, aPage *Page, idx int) error {
	var (
		parentNode = aParent.IndexNode.(*IndexNode[T])
		aNode      = aPage.IndexNode.(*IndexNode[T])
		maxCells   = ui.maxIndexKeys(aNode.KeySize)
		left       *Page
		right      *Page
		leftNode   *IndexNode[T]
		rightNode  *IndexNode[T]
	)

	var err error
	if idx != 0 {
		left, err = ui.pager.GetPage(ctx, parentNode.Cells[idx-1].Child)
		if err != nil {
			return fmt.Errorf("get left page: %w", err)
		}
		leftNode = left.IndexNode.(*IndexNode[T])
	} else if idx != int(parentNode.Header.Keys) {
		right, err = ui.pager.GetPage(ctx, parentNode.GetRightChildByIndex(uint32(idx)))
		if err != nil {
			return fmt.Errorf("get right page: %w", err)
		}
		rightNode = right.IndexNode.(*IndexNode[T])
	}

	if left != nil && leftNode.MoreThanHalfFull(int(maxCells)) {
		if ui.debug {
			fmt.Println("Borrow from left", "parent page:", aParent.Index, "left page:", aPage.Index, "left page:", left.Index, "idx in parent:", idx)
		}

		return ui.borrowFromLeft(aNode, aNode, leftNode, uint32(idx))
	}

	if right != nil && rightNode.MoreThanHalfFull(int(maxCells)) {
		if ui.debug {
			fmt.Println("Borrow from right", "parent page:", aParent.Index, "left page:", aPage.Index, "right page:", right.Index, "idx in parent:", idx)
		}

		return ui.borrowFromRight(aNode, aNode, rightNode, uint32(idx))
	}

	if idx != int(parentNode.Header.Keys) {
		if ui.debug {
			fmt.Println(int(maxCells), rightNode.Header.Keys, aNode.Header.Keys)
			fmt.Println("Merge with left", "parent page:", aParent.Index, "left page:", aPage.Index, "right page:", right.Index, "idx in parent:", idx)
		}

		if err := ui.merge(ctx, aParent, aPage, right, uint32(idx)); err != nil {
			return fmt.Errorf("merge with left: %w", err)
		}

		return ui.pager.AddFreePage(ctx, right.Index)
	}

	if ui.debug {
		fmt.Println("Merge with right", "parent page:", aParent.Index, "left page:", left.Index, "right page:", aPage.Index, "idx in parent:", idx)
	}

	if err := ui.merge(ctx, aParent, left, aPage, uint32(idx-1)); err != nil {
		return fmt.Errorf("merge with right: %w", err)
	}
	return ui.pager.AddFreePage(ctx, aPage.Index)
}

// A function to borrow a key from C[idx-1] and insert it into C[idx]
func (ui *UniqueIndex[T]) borrowFromLeft(aParent, aNode, left *IndexNode[T], idx uint32) error {
	aNode.PrependCell(IndexCell[T]{
		Key:   aParent.Cells[idx-1].Key,
		RowID: aParent.Cells[idx-1].RowID,
		Child: left.Header.RightChild,
	})

	aParent.Cells[idx-1].Key = left.LastCell().Key
	aParent.Cells[idx-1].RowID = left.LastCell().RowID

	left.RemoveLastCell()

	return nil
}

// A function to borrow a key from C[idx+1] and place it in C[idx]
func (ui *UniqueIndex[T]) borrowFromRight(aParent, aNode, right *IndexNode[T], idx uint32) error {
	aNode.AppendCells(IndexCell[T]{
		Child: aNode.Header.RightChild,
		Key:   aParent.Cells[idx].Key,
	})
	aNode.Header.RightChild = right.FirstCell().Child

	aParent.Cells[idx].Key = right.FirstCell().Key
	aParent.Cells[idx].RowID = right.FirstCell().RowID

	right.RemoveFirstCell()

	return nil
}

// A function to merge right child node into left child node
func (ui *UniqueIndex[T]) merge(ctx context.Context, aParent, aPage, aSibling *Page, idx uint32) error {
	var (
		parentNode = aParent.IndexNode.(*IndexNode[T])
		leftNode   = aPage.IndexNode.(*IndexNode[T])
		rightNode  = aSibling.IndexNode.(*IndexNode[T])
		leftIndex  = aPage.Index
		maxCells   = ui.maxIndexKeys(leftNode.KeySize)
		t          = (maxCells + 1) / 2
	)

	// Move the median key from parent to left child
	leftNode.Cells[t-1].Key = parentNode.Cells[idx].Key
	leftNode.Cells[t-1].RowID = parentNode.Cells[idx].RowID
	leftNode.Header.Keys += 1

	for i := uint32(0); i < rightNode.Header.Keys; i++ {
		leftNode.Cells[i+t] = rightNode.Cells[i]
	}
	leftNode.Header.Keys += rightNode.Header.Keys

	if !leftNode.Header.IsLeaf {
		for i := uint32(0); i < rightNode.Header.Keys+1; i++ {
			childIdx, err := rightNode.Child(i)
			if err != nil {
				return fmt.Errorf("get child: %w", err)
			}
			if err := leftNode.SetChild(i+t, childIdx); err != nil {
				return fmt.Errorf("set child: %w", err)
			}
			movedPage, err := ui.pager.GetPage(ctx, childIdx)
			if err != nil {
				return fmt.Errorf("get moved page: %w", err)
			}
			movedPage.IndexNode.(*IndexNode[T]).Header.Parent = leftIndex
		}
	}

	for i := int(idx + 1); i < int(parentNode.Header.Keys); i++ {
		parentNode.Cells[i-1].Key = parentNode.Cells[i].Key
		parentNode.Cells[i-1].RowID = parentNode.Cells[i].RowID
	}

	for i := int(idx + 2); i < int(parentNode.Header.Keys)+1; i++ {
		childIdx, err := parentNode.Child(uint32(i))
		if err != nil {
			return fmt.Errorf("get parent child: %w", err)
		}
		if err := parentNode.SetChild(uint32(i-1), childIdx); err != nil {
			return fmt.Errorf("set parent child: %w", err)
		}
	}

	parentNode.Header.Keys -= 1

	return nil
}
