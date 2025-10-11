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

func (idx *UniqueIndex[T]) Insert(ctx context.Context, key T) error {
	aRooTPage, err := idx.getPage(ctx, idx.RootPageIdx)
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}

	if aRooTPage.Node.Header.Keys == 0 {
		aRooTPage.Node.Cells[0] = IndexCell[T]{
			Key: key,
			// TODO - set RowID
		}
		aRooTPage.Node.Header.Keys += 1
		return nil
	}

	if aRooTPage.Node.Header.Keys < maxIndexKeys(aRooTPage.Node.KeySize) {
		return idx.InsertNotFull(ctx, idx.RootPageIdx, key)
	}

	newRootPage, err := idx.getFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new root page: %w", err)
	}

	newRootPage.Node.Cells[0].Child = idx.RootPageIdx
	newRootPage.Node.Header.IsRoot = true
	newRootPage.Node.Header.IsLeaf = false
	if err := idx.SplitChild(ctx, aRooTPage.Node, 0); err != nil {
		return fmt.Errorf("split child: %w", err)
	}
	i := 0
	if newRootPage.Node.Cells[0].Key < key {
		i += 1
	}
	if err := idx.InsertNotFull(ctx, newRootPage.Node.Cells[i].Child, key); err != nil {
		return fmt.Errorf("insert not full: %w", err)
	}
	return nil
}

func (idx *UniqueIndex[T]) InsertNotFull(ctx context.Context, pageIdx uint32, key T) error {
	aPage, err := idx.getPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}

	i := int(aPage.Node.Header.Keys)

	if aPage.Node.Header.IsLeaf {
		for i >= 0 && aPage.Node.Cells[i].Key > key {
			aPage.Node.Cells[i+1] = aPage.Node.Cells[i]
			i -= 1
		}
		aPage.Node.Cells[i+1] = IndexCell[T]{
			Key: key,
			// TODO - set RowID
		}
		aPage.Node.Header.Keys += 1
		return nil
	}

	for i >= 0 && aPage.Node.Cells[i].Key > key {
		i -= 1
	}
	childPage, err := idx.getPage(ctx, aPage.Node.Cells[i+1].Child)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	if childPage.Node.Header.Keys == maxIndexKeys(aPage.Node.KeySize) {
		if err := idx.SplitChild(ctx, childPage.Node, uint32(i+1)); err != nil {
			return fmt.Errorf("split child: %w", err)
		}
		if aPage.Node.Cells[i+1].Key < key {
			i += 1
		}
	}

	// Recurse to child node
	return idx.InsertNotFull(ctx, aPage.Node.Cells[i+1].Child, key)
}

// A utility function to split the child y of this node. i is index of y in
//
//	child array C[].  The Child y must be full when this function is called
func (idx *UniqueIndex[T]) SplitChild(ctx context.Context, aChildNode *IndexNode[T], indexInParent uint32) error {
	newPage, err := idx.getFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}

	masKeys := maxIndexKeys(aChildNode.KeySize)
	maxChildren := masKeys + 1

	// Move smaller half to the new node
	newPage.Node.Header.IsLeaf = aChildNode.Header.IsLeaf
	newPage.Node.Header.Keys = maxChildren/2 - 1
	aChildNode.Header.Keys = maxChildren / 2
	newPage.Node.Header.RightChild = aChildNode.Header.RightChild

	// Move everything to the right of median to new node,
	// median key will move to parent

	for j := uint32(0); j < maxChildren/2-1; j++ {
		newPage.Node.Cells[j] = aChildNode.Cells[j+maxChildren/2]
	}

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
// def splitChild(self, i, child):
//     z = BTreeNode(child.t, child.leaf)
//     z.n = self.t - 1
//     for j in range(self.t - 1):
//         z.keys[j] = child.keys[j + self.t]
//     if not child.leaf:
//         for j in range(self.t):
//             z.C[j] = child.C[j + self.t]
//     child.n = self.t - 1
//     for j in range(self.n, i, -1):
//         self.C[j + 1] = self.C[j]
//     self.C[i + 1] = z
//     for j in range(self.n - 1, i - 1, -1):
//         self.keys[j + 1] = self.keys[j]
//     self.keys[i] = child.keys[self.t - 1]
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

type IndexPage[T int8 | int32 | int64 | float32 | float64 | string] struct {
	Node *IndexNode[T]
}

func (idx *UniqueIndex[T]) getFreePage(ctx context.Context) (*IndexPage[T], error) {
	page, err := idx.pager.GetFreePage(ctx)
	if err != nil {
		return nil, fmt.Errorf("get free page: %w", err)
	}
	node, ok := page.IndexNode.(*IndexNode[T])
	if !ok {
		return nil, fmt.Errorf("unexpected index node type")
	}
	return &IndexPage[T]{Node: node}, nil
}

func (idx *UniqueIndex[T]) getPage(ctx context.Context, pageIdx uint32) (*IndexPage[T], error) {
	page, err := idx.pager.GetPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("get page: %w", err)
	}
	node, ok := page.IndexNode.(*IndexNode[T])
	if !ok {
		return nil, fmt.Errorf("unexpected index node type")
	}
	return &IndexPage[T]{Node: node}, nil
}
