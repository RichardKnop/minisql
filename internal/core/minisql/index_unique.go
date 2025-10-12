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

func (idx *UniqueIndex[T]) Insert(ctx context.Context, key T) error {
	aRootPage, err := idx.pager.GetPage(ctx, idx.RootPageIdx)
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}
	aRootNode := aRootPage.IndexNode.(*IndexNode[T])

	// Root is empty, insert the first key
	if aRootNode.Header.Keys == 0 {
		aRootNode.Cells[0] = IndexCell[T]{
			Key: key,
			// TODO - set RowID
		}
		aRootNode.Header.IsRoot = true
		aRootNode.Header.IsLeaf = true
		aRootNode.Header.Keys += 1
		return nil
	}

	// Root is not full, insert new key
	if aRootNode.Header.Keys < idx.maxIndexKeys(aRootNode.KeySize) {
		return idx.InsertNotFull(ctx, idx.RootPageIdx, key)
	}

	// Root is full, need to split. Old root page will become left child
	newLeftChild, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new left child page: %w", err)
	}
	newLeftChildNode := NewIndexNode[T](aRootPage.IndexNode.(*IndexNode[T]).KeySize)
	*newLeftChildNode = *aRootNode
	newLeftChildNode.Header.Parent = aRootPage.Index
	newLeftChildNode.Header.IsRoot = false
	// Update children to set new parent
	for _, childIdx := range newLeftChildNode.Children() {
		aChildPage, err := idx.pager.GetPage(ctx, childIdx)
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

	if err := idx.SplitChild(ctx, aRootPage, newLeftChild, 0); err != nil {
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
	if err := idx.InsertNotFull(ctx, childIdx, key); err != nil {
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

func (idx *UniqueIndex[T]) InsertNotFull(ctx context.Context, pageIdx uint32, key T) error {
	aPage, err := idx.pager.GetPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])

	i := int(aNode.Header.Keys) - 1

	if aNode.Header.IsLeaf {
		for i >= 0 && aNode.Cells[i].Key > key {
			aNode.Cells[i+1].Key = aNode.Cells[i].Key
			i -= 1
		}
		aNode.Cells[i+1].Key = key
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
	childPage, err := idx.pager.GetPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childNode := childPage.IndexNode.(*IndexNode[T])
	if childNode.Header.Keys == idx.maxIndexKeys(childNode.KeySize) {
		if err := idx.SplitChild(ctx, aPage, childPage, uint32(i+1)); err != nil {
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
	return idx.InsertNotFull(ctx, childIdx, key)
}

// Split a child node into two nodes and move the median key up to the parent node
func (idx *UniqueIndex[T]) SplitChild(ctx context.Context, parentPage, splitPage *Page, indexInParent uint32) error {
	parentNode := parentPage.IndexNode.(*IndexNode[T])
	splitNode := splitPage.IndexNode.(*IndexNode[T])

	newPage, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}
	newNode := NewIndexNode[T](splitNode.KeySize)
	newNode.Header.Parent = splitNode.Header.Parent
	newNode.Header.IsLeaf = splitNode.Header.IsLeaf
	newPage.IndexNode = newNode

	// Move smaller half to the new node
	var (
		maxKeys     = idx.maxIndexKeys(splitNode.KeySize)
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
		aChildPage, err := idx.pager.GetPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		aChildNode := aChildPage.IndexNode.(*IndexNode[T])
		aChildNode.Header.Parent = newPage.Index
	}

	// Update parent
	for j := int(parentNode.Header.Keys) - 1; j >= int(indexInParent); j-- {
		parentNode.Cells[j+1].Key = parentNode.Cells[j].Key
		// parentNode.Cells[j+1].Child = parentNode.Cells[j].Child
	}
	parentNode.Cells[indexInParent].Key = splitNode.Cells[leftCount-1].Key

	// parentNode.Cells[indexInParent+1].Child = newPage.Index
	parentNode.Cells[indexInParent].Child = splitPage.Index
	splitNode.Cells[leftCount] = IndexCell[T]{}

	parentNode.Header.Keys += 1

	for j := int(parentNode.Header.Keys) - 1; j > int(indexInParent); j-- {
		if j+1 >= int(maxKeys) {
			continue
		}
		if err := parentNode.SetChild(uint32(j+1), parentNode.Cells[j].Child); err != nil {
			return fmt.Errorf("set child: %w", err)
		}
	}
	if err := parentNode.SetChild(indexInParent+1, newPage.Index); err != nil {
		return fmt.Errorf("set child: %w", err)
	}

	return nil
}

type indexCallback[T int8 | int32 | int64 | float32 | float64 | string] func(page *Page)

func (idx *UniqueIndex[T]) BFS(f indexCallback[T]) error {

	rootPage, err := idx.pager.GetPage(context.Background(), idx.RootPageIdx)
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
				aPage, err := idx.pager.GetPage(context.Background(), idxCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
			if current.IndexNode.(*IndexNode[T]).Header.RightChild > 0 && current.IndexNode.(*IndexNode[T]).Header.RightChild != RIGHT_CHILD_NOT_SET {
				aPage, err := idx.pager.GetPage(context.Background(), current.IndexNode.(*IndexNode[T]).Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
		}
	}

	return nil
}

func (idx *UniqueIndex[T]) print() error {
	return idx.BFS(func(aPage *Page) {
		aNode := aPage.IndexNode.(*IndexNode[T])
		fmt.Println("Index node,", "page:", aPage.Index, "number of keys:", aNode.Header.Keys, "parent:", aNode.Header.Parent, "right child:", aNode.Header.RightChild)
		fmt.Println("Keys:", aNode.Keys())
		fmt.Println("Children:", aNode.Children())
		fmt.Println("---------")
	})
}
