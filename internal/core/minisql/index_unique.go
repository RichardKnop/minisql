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
}

func NewIndex[T int8 | int32 | int64 | float32 | float64 | string](logger *zap.Logger, name string, column Column, pager Pager, rootPageIdx uint32) *UniqueIndex[T] {
	return &UniqueIndex[T]{
		Name:        name,
		Column:      column,
		RootPageIdx: rootPageIdx,
		pager:       pager,
		writeLock:   new(sync.RWMutex),
		logger:      logger,
	}
}

func (idx *UniqueIndex[T]) InsertNotFull(ctx context.Context, pageIdx uint32, key T) error {
	_, aNode, err := idx.getPageNode(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}

	i := int(aNode.Header.Keys) // number of keys
	if aNode.Header.IsLeaf {
		for i >= 0 && aNode.Cells[i].Key > key {
			aNode.Cells[i+1] = aNode.Cells[i]
			i -= 1
		}
		aNode.Cells[i+1] = IndexCell[T]{
			Key: key,
			// TODO - set RowID
		}
		aNode.Header.Keys += 1
		return nil
	}

	for i >= 0 && aNode.Cells[i].Key > key {
		i -= 1
	}
	_, aChildNode, err := idx.getPageNode(ctx, aNode.Cells[i+1].Child)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	if aChildNode.Header.Keys == maxIndexCells(aNode.KeySize) {
		if err := idx.SplitChild(ctx, aChildNode, uint32(i+1)); err != nil {
			return fmt.Errorf("split child: %w", err)
		}
		if aNode.Cells[i+1].Key < key {
			i += 1
		}
	}

	// Recurse to child node
	return idx.InsertNotFull(ctx, aNode.Cells[i+1].Child, key)
}

// A utility function to split the child y of this node. i is index of y in
//
//	child array C[].  The Child y must be full when this function is called
func (idx *UniqueIndex[T]) SplitChild(ctx context.Context, aChildNode *IndexNode[T], indexInParent uint32) error {

	// TODO - implement
	return nil
}

// def __init__(self, t, leaf):
//     self.keys = [None] * (2 * t - 1) # An array of keys
//     self.t = t # Minimum degree (defines the range for number of keys)
//     self.C = [None] * (2 * t) # An array of child pointers
//     self.n = 0 # Current number of keys
//     self.leaf = leaf # Is true when node is leaf. Otherwise false

// # A utility function to split the child y of this node. i is index of y in
// # child array C[].  The Child y must be full when this function is called
// def splitChild(self, i, y):
//     z = BTreeNode(y.t, y.leaf)
//     z.n = self.t - 1
//     for j in range(self.t - 1):
//         z.keys[j] = y.keys[j + self.t]
//     if not y.leaf:
//         for j in range(self.t):
//             z.C[j] = y.C[j + self.t]
//     y.n = self.t - 1
//     for j in range(self.n, i, -1):
//         self.C[j + 1] = self.C[j]
//     self.C[i + 1] = z
//     for j in range(self.n - 1, i - 1, -1):
//         self.keys[j + 1] = self.keys[j]
//     self.keys[i] = y.keys[self.t - 1]
//     self.n += 1

// # The main function that inserts a new key in this B-Tree
// def insert(self, k):
//     if self.root == None:
//         self.root = BTreeNode(self.t, True)
//         self.root.keys[0] = k # Insert key
//         self.root.n = 1
//     else:
//         if self.root.n == 2 * self.t - 1:
//             s = BTreeNode(self.t, False)
//             s.C[0] = self.root
//             s.splitChild(0, self.root)
//             i = 0
//             if s.keys[0] < k:
//                 i += 1
//             s.C[i].insertNonFull(k)
//             self.root = s
//         else:
//             self.root.insertNonFull(k)

func (idx *UniqueIndex[T]) Insert(ctx context.Context, key T) error {
	_, aRootNode, err := idx.getPageNode(ctx, idx.RootPageIdx)
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}

	if aRootNode.Header.Keys == 0 {
		aRootNode.Cells[0] = IndexCell[T]{
			Key: key,
			// TODO - set RowID
		}
		aRootNode.Header.Keys += 1
		return nil
	}

	if aRootNode.Header.Keys == maxIndexCells(aRootNode.KeySize) {
		// newRootPage, err := idx.pager.GetFreePage(ctx)
		// if err != nil {
		// 	return fmt.Errorf("get new root page: %w", err)
		// }

		newRootNode := NewIndexNode[T](aRootNode.KeySize)
		newRootNode.Cells[0].Child = idx.RootPageIdx
		newRootNode.Header.IsRoot = true
		newRootNode.Header.IsLeaf = false
		if err := idx.SplitChild(ctx, aRootNode, 0); err != nil {
			return fmt.Errorf("split child: %w", err)
		}
		i := 0
		if newRootNode.Cells[0].Key < key {
			i += 1
		}
		if err := idx.InsertNotFull(ctx, newRootNode.Cells[i].Child, key); err != nil {
			return fmt.Errorf("insert not full: %w", err)
		}
		return nil
	}

	return idx.InsertNotFull(ctx, idx.RootPageIdx, key)
}

func (idx *UniqueIndex[T]) getPageNode(ctx context.Context, pageIdx uint32) (*Page, *IndexNode[T], error) {
	page, err := idx.pager.GetPage(ctx, pageIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("get page: %w", err)
	}
	node, ok := page.IndexNode.(*IndexNode[T])
	if !ok {
		return nil, nil, fmt.Errorf("unexpected index node type")
	}
	return page, node, nil
}
