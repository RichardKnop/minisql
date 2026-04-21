package minisql

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// MaxIndexKeySize is the maximum number of bytes allowed for a single index key.
const MaxIndexKeySize = 255

// IndexKey ...
type IndexKey interface {
	int8 | int32 | int64 | float32 | float64 | string | CompositeKey
}

// Index ...
type Index[T IndexKey] struct {
	logger      *zap.Logger
	Name        string
	Columns     []Column
	unique      bool
	rootPageIdx PageIndex
	pager       TxPager
	txManager   *TransactionManager
	maximumKeys uint32
}

// NewUniqueIndex ...
func NewUniqueIndex[T IndexKey](logger *zap.Logger, txManager *TransactionManager, name string, columns []Column, pager TxPager, rootPageIdx PageIndex) (*Index[T], error) {
	return newIndex[T](true, logger, txManager, name, columns, pager, rootPageIdx)
}

// NewNonUniqueIndex ...
func NewNonUniqueIndex[T IndexKey](logger *zap.Logger, txManager *TransactionManager, name string, columns []Column, pager TxPager, rootPageIdx PageIndex) (*Index[T], error) {
	return newIndex[T](false, logger, txManager, name, columns, pager, rootPageIdx)
}

func newIndex[T IndexKey](unique bool, logger *zap.Logger, txManager *TransactionManager, name string, columns []Column, pager TxPager, rootPageIdx PageIndex) (*Index[T], error) {
	for _, col := range columns {
		if col.Kind == Text {
			return nil, errors.New("unique index does not support text columns")
		}
		if col.Kind == Varchar && col.Size > MaxIndexKeySize {
			return nil, fmt.Errorf("unique index does not support varchar columns larger than %d", MaxIndexKeySize)
		}
		// TODO - check total index size
	}
	return &Index[T]{
		logger:      logger,
		Name:        name,
		Columns:     columns,
		unique:      unique,
		rootPageIdx: rootPageIdx,
		pager:       pager,
		txManager:   txManager,
	}, nil
}

// GetRootPageIdx ...
func (ui *Index[T]) GetRootPageIdx() PageIndex {
	return ui.rootPageIdx
}

// Insert ...
func (ui *Index[T]) Insert(ctx context.Context, keyAny any, rowID RowID) error {
	key, ok := keyAny.(T)
	if !ok {
		return fmt.Errorf("invalid key type: %T", keyAny)
	}

	rootPage, err := ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}
	rootNode := rootPage.IndexNode.(*IndexNode[T])

	// Root is empty, insert the first key
	if rootNode.Header.Keys == 0 {
		rootNode.Cells = append(rootNode.Cells, NewIndexCell[T](ui.unique))
		rootNode.Cells[0].Key = key
		if ui.unique {
			rootNode.Cells[0].UniqueRowID = rowID
		} else {
			rootNode.Cells[0].InlineRowIDs = 1
			rootNode.Cells[0].RowIDs = append(rootNode.Cells[0].RowIDs, rowID)
		}
		rootNode.Header.IsRoot = true
		rootNode.Header.IsLeaf = true
		rootNode.Header.Keys += 1
		return nil
	}

	if ui.unique {
		// In case of unique index, we cannot insert duplicate keys
		_, ok, err := ui.Seek(ctx, rootPage, key)
		if err != nil {
			return fmt.Errorf("seek key: %w", err)
		}
		if ok {
			return ErrDuplicateKey
		}
	}

	// Root is not full, insert new key
	if ui.hasSpaceForKey(rootNode, key) {
		return ui.insertNotFull(ctx, ui.GetRootPageIdx(), key, rowID)
	}

	// Root is full, need to split. Old root page will become left child
	newLeftChild, err := ui.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new left child page: %w", err)
	}
	newLeftChildNode := NewIndexNode[T](ui.unique)
	*newLeftChildNode = *rootNode
	newLeftChildNode.Header.Parent = rootPage.Index
	newLeftChildNode.Header.IsRoot = false

	// Update children to set new parent
	for _, childIdx := range newLeftChildNode.Children() {
		childPage, err := ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childNode := childPage.IndexNode.(*IndexNode[T])
		childNode.Header.Parent = newLeftChild.Index
	}
	newLeftChild.IndexNode = newLeftChildNode

	rootNode = NewIndexNode[T](ui.unique)
	rootNode.Header.IsRoot = true
	rootNode.Header.IsLeaf = false
	rootNode.Header.Keys = 0
	rootNode.Header.RightChild = RightChildNotSet
	rootNode.Cells[0].Child = newLeftChild.Index
	rootPage.IndexNode = rootNode

	if err := ui.splitChild(ctx, rootPage, newLeftChild, 0); err != nil {
		return fmt.Errorf("split child: %w", err)
	}
	i := uint32(0)
	if compare(rootNode.Cells[0].Key, key) < 0 {
		i += 1
	}
	childIdx, err := rootNode.Child(i)
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	if err := ui.insertNotFull(ctx, childIdx, key, rowID); err != nil {
		return fmt.Errorf("insert not full: %w", err)
	}
	return nil
}

func (ui *Index[T]) hasSpaceForKey(node *IndexNode[T], key T) bool {
	if ui.maximumKeys != 0 {
		return node.Header.Keys < ui.maximumKeys
	}
	return node.HasSpaceForKey(key)
}

func (ui *Index[T]) atLeastHalfFull(node *IndexNode[T]) bool {
	if ui.maximumKeys != 0 {
		return node.Header.Keys >= (ui.maximumKeys+1)/2
	}
	return node.AtLeastHalfFull()
}

// ErrDuplicateKey ...
var ErrDuplicateKey = errors.New("duplicate key")

func (ui *Index[T]) insertNotFull(ctx context.Context, pageIdx PageIndex, key T, rowID RowID) error {
	page, err := ui.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}
	node := page.IndexNode.(*IndexNode[T])

	if !ui.unique {
		// For non-unique index, find existing key and append row ID in case it exists
		for cellIdx := uint32(0); cellIdx < node.Header.Keys; cellIdx++ {
			if compare(node.Cells[cellIdx].Key, key) != 0 {
				continue
			}
			if err := appendRowID(ctx, ui.pager, node, cellIdx, rowID); err != nil {
				return fmt.Errorf("error appending row ID to existing key: %w", err)
			}
			return nil
		}
		// Otherwise we will fall through to insert new key and row ID
	}

	if node.Header.IsLeaf {
		// Find location to insert new key, shift keys to make space
		i := int(node.Header.Keys) - 1
		node.Cells = append(node.Cells, NewIndexCell[T](ui.unique))
		for i >= 0 && compare(node.Cells[i].Key, key) > 0 {
			node.Cells[i+1].Key = node.Cells[i].Key
			node.Cells[i+1].InlineRowIDs = node.Cells[i].InlineRowIDs
			node.Cells[i+1].RowIDs = node.Cells[i].RowIDs
			node.Cells[i+1].UniqueRowID = node.Cells[i].UniqueRowID
			node.Cells[i+1].Overflow = node.Cells[i].Overflow
			i -= 1
		}
		node.Cells[i+1].Key = key
		if ui.unique {
			node.Cells[i+1].UniqueRowID = rowID
		} else {
			node.Cells[i+1].InlineRowIDs = 1
			node.Cells[i+1].RowIDs = []RowID{rowID}
		}
		node.Header.Keys += 1
		return nil
	}

	// Find the child which is going to have the new key
	i := int(node.Header.Keys) - 1
	for i >= 0 && compare(node.Cells[i].Key, key) > 0 {
		i -= 1
	}

	childIdx, err := node.Child(uint32(i + 1))
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childNode := childPage.IndexNode.(*IndexNode[T])
	if !ui.hasSpaceForKey(childNode, key) {
		if err := ui.splitChild(ctx, page, childPage, uint32(i+1)); err != nil {
			return fmt.Errorf("split child: %w", err)
		}
		if compare(node.Cells[i+1].Key, key) < 0 {
			i += 1
		}
	}

	// Recurse to child node
	childIdx, err = node.Child(uint32(i + 1))
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	return ui.insertNotFull(ctx, childIdx, key, rowID)
}

// Split a child node into two nodes and move the median key up to the parent node
func (ui *Index[T]) splitChild(ctx context.Context, parentPage, splitPage *Page, indexInParent uint32) error {
	var (
		parentNode = parentPage.IndexNode.(*IndexNode[T])
		splitNode  = splitPage.IndexNode.(*IndexNode[T])
	)

	// Split page is left child, create new right child
	newPage, err := ui.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}
	newNode := NewIndexNode[T](ui.unique)
	newNode.Header.Parent = splitNode.Header.Parent
	newNode.Header.IsLeaf = splitNode.Header.IsLeaf
	newPage.IndexNode = newNode

	// Move smaller half to the new node
	// For non unique index, we calculate split point based on cell size
	// because cells can be of different sizes.
	leftCount, rightCount := splitNode.SplitInHalves(ui.unique)

	// Move everything to the right of median to new node
	newNode.Header.RightChild = splitNode.Header.RightChild
	splitNode.Header.RightChild = splitNode.Cells[leftCount-1].Child
	newNode.AppendCells(splitNode.Cells[leftCount : rightCount+leftCount]...)
	for range rightCount {
		splitNode.RemoveLastCell()
	}
	// We will be moving rightmost cell (previous median cell) to the parent
	cellToMoveToParent := splitNode.RemoveLastCell()

	for _, childIdx := range newNode.Children() {
		childPage, err := ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childNode := childPage.IndexNode.(*IndexNode[T])
		childNode.Header.Parent = newPage.Index
	}

	// Update parent node
	if len(parentNode.Cells) < int(parentNode.Header.Keys+1) {
		parentNode.Cells = append(parentNode.Cells, NewIndexCell[T](ui.unique))
	}
	for j := int(parentNode.Header.Keys) - 1; j >= int(indexInParent); j-- {
		parentNode.Cells[j+1].Key = parentNode.Cells[j].Key
		parentNode.Cells[j+1].InlineRowIDs = parentNode.Cells[j].InlineRowIDs
		parentNode.Cells[j+1].RowIDs = parentNode.Cells[j].RowIDs
		parentNode.Cells[j+1].UniqueRowID = parentNode.Cells[j].UniqueRowID
		parentNode.Cells[j+1].Overflow = parentNode.Cells[j].Overflow
	}
	parentNode.Header.Keys += 1
	if len(parentNode.Cells) < int(indexInParent) {
		parentNode.Cells = append(parentNode.Cells, NewIndexCell[T](ui.unique))
	}
	parentNode.Cells[indexInParent].Key = cellToMoveToParent.Key
	parentNode.Cells[indexInParent].InlineRowIDs = cellToMoveToParent.InlineRowIDs
	parentNode.Cells[indexInParent].RowIDs = cellToMoveToParent.RowIDs
	parentNode.Cells[indexInParent].UniqueRowID = cellToMoveToParent.UniqueRowID
	parentNode.Cells[indexInParent].Overflow = cellToMoveToParent.Overflow
	splitNode.Cells[leftCount] = NewIndexCell[T](ui.unique)

	for j := int(parentNode.Header.Keys) - 1; j > int(indexInParent); j-- {
		if j+1 >= int(parentNode.Header.Keys) {
			continue
		}
		if err := parentNode.SetChild(uint32(j+1), parentNode.Cells[j].Child); err != nil {
			return fmt.Errorf("set child: %w", err)
		}
	}
	if err := parentNode.SetChild(indexInParent, splitPage.Index); err != nil {
		return fmt.Errorf("set child: %w", err)
	}
	if err := parentNode.SetChild(indexInParent+1, newPage.Index); err != nil {
		return fmt.Errorf("set child: %w", err)
	}

	return nil
}

// Delete ...
func (ui *Index[T]) Delete(ctx context.Context, keyAny any, rowID RowID) error {
	key, ok := keyAny.(T)
	if !ok {
		return fmt.Errorf("invalid key type: %T", keyAny)
	}

	rootPage, err := ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}

	if err := ui.remove(ctx, rootPage, key, rowID); err != nil {
		return fmt.Errorf("remove key: %w", err)
	}

	rootNode := rootPage.IndexNode.(*IndexNode[T])
	if rootNode.Header.Keys > 0 {
		return nil
	}

	if rootNode.Header.IsLeaf {
		rootNode = NewIndexNode[T](ui.unique)
		rootNode.Header.IsRoot = true
		rootNode.Header.IsLeaf = false
		rootNode.Header.Keys = 0
		rootNode.Header.RightChild = RightChildNotSet
		return nil
	}

	// Root has no keys, make first child the new root
	firstChildIdx, err := rootNode.Child(0)
	if err != nil {
		return fmt.Errorf("get first child: %w", err)
	}
	firstChildPage, err := ui.pager.ModifyPage(ctx, firstChildIdx)
	if err != nil {
		return fmt.Errorf("get new root page: %w", err)
	}
	firstChildNode := firstChildPage.IndexNode.(*IndexNode[T])
	*rootNode = *firstChildNode
	rootNode.Header.Parent = 0
	rootNode.Header.IsRoot = true

	// Update children to set new parent
	for _, childIdx := range rootNode.Children() {
		childPage, err := ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childNode := childPage.IndexNode.(*IndexNode[T])
		childNode.Header.Parent = ui.GetRootPageIdx()
	}
	if err := ui.pager.AddFreePage(ctx, firstChildIdx); err != nil {
		return fmt.Errorf("add free page: %w", err)
	}

	return nil
}

// remove a key from the sub-tree rooted with this node
func (ui *Index[T]) remove(ctx context.Context, page *Page, key T, rowID RowID) error {
	node := page.IndexNode.(*IndexNode[T])

	// Find index of the first key greater than or equal to key
	idx := 0
	for idx < int(node.Header.Keys) && compare(node.Cells[idx].Key, key) < 0 {
		idx += 1
	}

	if idx < int(node.Header.Keys) && compare(node.Cells[idx].Key, key) == 0 {
		// If the key is in this node and this is a leaf node
		if node.Header.IsLeaf {
			// If unique index, just remove the key.
			// Or if there is only one row ID for non-unique index.
			if ui.unique || node.Cells[idx].InlineRowIDs == 1 {
				return node.DeleteKeyAndRightChild(uint32(idx))
			}

			// For non-unique index with multiple row IDs, remove specific row ID
			if err := removeRowID(ctx, ui.pager, node, uint32(idx), key, rowID); err != nil {
				return fmt.Errorf("remove row ID: %w", err)
			}

			return nil
		}
		// For non unique index internal node with multiple row IDs,
		// remove specific row ID.
		if !ui.unique && node.Cells[idx].InlineRowIDs > 1 {
			if err := removeRowID(ctx, ui.pager, node, uint32(idx), key, rowID); err != nil {
				return fmt.Errorf("remove row ID: %w", err)
			}
			return nil
		}

		// Otherwise call recursive function to remove the key from internal node
		if err := ui.removeFromInternal(ctx, page, idx, rowID); err != nil {
			return fmt.Errorf("remove from internal: %w", err)
		}

		return nil
	}

	if node.Header.IsLeaf {
		return fmt.Errorf("the key %v does not exist in the tree", key)
	}

	flag := idx == int(node.Header.Keys)

	childIdx, err := node.Child(uint32(idx))
	if err != nil {
		return fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ModifyPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}

	childNode := childPage.IndexNode.(*IndexNode[T])
	if !ui.atLeastHalfFull(childNode) {
		if err := ui.fill(ctx, page, childPage, idx); err != nil {
			return fmt.Errorf("fill child: %w", err)
		}
	}

	if flag && idx > int(node.Header.Keys) {
		prevChildIdx, err := node.Child(uint32(idx - 1))
		if err != nil {
			return fmt.Errorf("get prev child: %w", err)
		}
		prevChildPage, err := ui.pager.ModifyPage(ctx, prevChildIdx)
		if err != nil {
			return fmt.Errorf("get prev child page: %w", err)
		}
		return ui.remove(ctx, prevChildPage, key, rowID)
	}

	return ui.remove(ctx, childPage, key, rowID)
}

func (ui *Index[T]) removeFromInternal(ctx context.Context, page *Page, idx int, rowID RowID) error {
	var (
		node = page.IndexNode.(*IndexNode[T])
		key  = node.Cells[idx].Key
	)

	leftChildPage, err := ui.pager.ModifyPage(ctx, node.Cells[idx].Child)
	if err != nil {
		return fmt.Errorf("get left child page: %w", err)
	}
	leftChildNode := leftChildPage.IndexNode.(*IndexNode[T])

	rightPageIdx, err := node.Child(uint32(idx + 1))
	if err != nil {
		return fmt.Errorf("get right child index: %w", err)
	}
	rightChildPage, err := ui.pager.ModifyPage(ctx, rightPageIdx)
	if err != nil {
		return fmt.Errorf("get right child page: %w", err)
	}
	rightChildNode := rightChildPage.IndexNode.(*IndexNode[T])

	switch {
	case ui.atLeastHalfFull(leftChildNode):
		predecessor, err := ui.getPred(ctx, node, idx)
		if err != nil {
			return fmt.Errorf("get predecessor key: %w", err)
		}

		node.Cells[idx].Key = predecessor.Key
		node.Cells[idx].InlineRowIDs = predecessor.InlineRowIDs
		node.Cells[idx].RowIDs = predecessor.RowIDs
		node.Cells[idx].UniqueRowID = predecessor.UniqueRowID
		node.Cells[idx].Overflow = predecessor.Overflow
		if err := ui.remove(ctx, leftChildPage, predecessor.Key, rowID); err != nil {
			return fmt.Errorf("remove predecessor key: %w", err)
		}
	case ui.atLeastHalfFull(rightChildNode):
		successor, err := ui.getSucc(ctx, node, idx)
		if err != nil {
			return fmt.Errorf("get successor key: %w", err)
		}

		node.Cells[idx].Key = successor.Key
		node.Cells[idx].InlineRowIDs = successor.InlineRowIDs
		node.Cells[idx].RowIDs = successor.RowIDs
		node.Cells[idx].UniqueRowID = successor.UniqueRowID
		node.Cells[idx].Overflow = successor.Overflow
		if err := ui.remove(ctx, rightChildPage, successor.Key, rowID); err != nil {
			return fmt.Errorf("remove successor key: %w", err)
		}
	default:
		if err := ui.merge(ctx, page, leftChildPage, rightChildPage, uint32(idx)); err != nil {
			return fmt.Errorf("merge children: %w", err)
		}
		if err := ui.pager.AddFreePage(ctx, rightChildPage.Index); err != nil {
			return fmt.Errorf("add free page: %w", err)
		}
		if err := ui.remove(ctx, leftChildPage, key, rowID); err != nil {
			return fmt.Errorf("remove key from merged child: %w", err)
		}
	}

	return nil
}

// getPred finds predecessor of the key at the idx-th position in the node
func (ui *Index[T]) getPred(ctx context.Context, node *IndexNode[T], idx int) (IndexCell[T], error) {
	curPage, err := ui.pager.ModifyPage(ctx, node.Cells[idx].Child)
	if err != nil {
		return IndexCell[T]{}, fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		page, err := ui.pager.ModifyPage(ctx, cur.Header.RightChild)
		if err != nil {
			return IndexCell[T]{}, fmt.Errorf("get page: %w", err)
		}
		cur = page.IndexNode.(*IndexNode[T])
	}

	return cur.LastCell(), nil
}

// A function to get the successor of the key at the idx-th position in the node
func (ui *Index[T]) getSucc(ctx context.Context, node *IndexNode[T], idx int) (IndexCell[T], error) {
	curIdx, err := node.Child(uint32(idx + 1))
	if err != nil {
		return IndexCell[T]{}, fmt.Errorf("get child index: %w", err)
	}
	curPage, err := ui.pager.ModifyPage(ctx, curIdx)
	if err != nil {
		return IndexCell[T]{}, fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		page, err := ui.pager.ModifyPage(ctx, cur.Cells[0].Child)
		if err != nil {
			return IndexCell[T]{}, fmt.Errorf("get page: %w", err)
		}
		cur = page.IndexNode.(*IndexNode[T])
	}

	return cur.FirstCell(), nil
}

func (ui *Index[T]) fill(ctx context.Context, parent, page *Page, idx int) error {
	var (
		parentNode = parent.IndexNode.(*IndexNode[T])
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
		return ui.borrowFromLeft(ctx, parent, page, left, uint32(idx))
	}

	if right != nil && ui.atLeastHalfFull(rightNode) {
		return ui.borrowFromRight(ctx, parent, page, right, uint32(idx))
	}

	if idx != int(parentNode.Header.Keys) {
		if err := ui.merge(ctx, parent, page, right, uint32(idx)); err != nil {
			return fmt.Errorf("merge with left: %w", err)
		}

		return ui.pager.AddFreePage(ctx, right.Index)
	}

	if err := ui.merge(ctx, parent, left, page, uint32(idx-1)); err != nil {
		return fmt.Errorf("merge with right: %w", err)
	}
	return ui.pager.AddFreePage(ctx, page.Index)
}

// A function to borrow a key from left sibling
func (ui *Index[T]) borrowFromLeft(ctx context.Context, parent, page, left *Page, idx uint32) error {
	var (
		parentNode = parent.IndexNode.(*IndexNode[T])
		node       = page.IndexNode.(*IndexNode[T])
		leftNode   = left.IndexNode.(*IndexNode[T])
	)

	node.PrependCell(IndexCell[T]{
		Key:          parentNode.Cells[idx-1].Key,
		InlineRowIDs: parentNode.Cells[idx-1].InlineRowIDs,
		RowIDs:       parentNode.Cells[idx-1].RowIDs,
		UniqueRowID:  parentNode.Cells[idx-1].UniqueRowID,
		Overflow:     parentNode.Cells[idx-1].Overflow,
		Child:        leftNode.Header.RightChild,
		unique:       ui.unique,
	})

	if !leftNode.Header.IsLeaf {
		childPage, err := ui.pager.ModifyPage(ctx, leftNode.Header.RightChild)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childPage.IndexNode.(*IndexNode[T]).setParent(page.Index)
	}

	parentNode.Cells[idx-1].Key = leftNode.LastCell().Key
	parentNode.Cells[idx-1].InlineRowIDs = leftNode.LastCell().InlineRowIDs
	parentNode.Cells[idx-1].RowIDs = leftNode.LastCell().RowIDs
	parentNode.Cells[idx-1].UniqueRowID = leftNode.LastCell().UniqueRowID
	parentNode.Cells[idx-1].Overflow = leftNode.LastCell().Overflow

	leftNode.RemoveLastCell()

	return nil
}

// A function to borrow a key from right sibling
func (ui *Index[T]) borrowFromRight(ctx context.Context, parent, page, right *Page, idx uint32) error {
	var (
		parentNode = parent.IndexNode.(*IndexNode[T])
		node       = page.IndexNode.(*IndexNode[T])
		rightNode  = right.IndexNode.(*IndexNode[T])
	)

	node.AppendCells(IndexCell[T]{
		Key:          parentNode.Cells[idx].Key,
		InlineRowIDs: parentNode.Cells[idx].InlineRowIDs,
		RowIDs:       parentNode.Cells[idx].RowIDs,
		UniqueRowID:  parentNode.Cells[idx].UniqueRowID,
		Overflow:     parentNode.Cells[idx].Overflow,
		Child:        node.Header.RightChild,
		unique:       ui.unique,
	})
	node.Header.RightChild = rightNode.FirstCell().Child

	if !node.Header.IsLeaf {
		childPage, err := ui.pager.ModifyPage(ctx, node.Header.RightChild)
		if err != nil {
			return fmt.Errorf("get child page: %w", err)
		}
		childPage.IndexNode.(*IndexNode[T]).setParent(page.Index)
	}

	parentNode.Cells[idx].Key = rightNode.FirstCell().Key
	parentNode.Cells[idx].InlineRowIDs = rightNode.FirstCell().InlineRowIDs
	parentNode.Cells[idx].RowIDs = rightNode.FirstCell().RowIDs
	parentNode.Cells[idx].UniqueRowID = rightNode.FirstCell().UniqueRowID
	parentNode.Cells[idx].Overflow = rightNode.FirstCell().Overflow

	rightNode.RemoveFirstCell()

	return nil
}

// A function to merge right child node into left child node
func (ui *Index[T]) merge(ctx context.Context, parent, left, right *Page, idx uint32) error {
	var (
		parentNode = parent.IndexNode.(*IndexNode[T])
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

	cell := IndexCell[T]{
		Child:        leftNode.Header.RightChild,
		Key:          parentNode.Cells[idx].Key,
		InlineRowIDs: parentNode.Cells[idx].InlineRowIDs,
		RowIDs:       parentNode.Cells[idx].RowIDs,
		UniqueRowID:  parentNode.Cells[idx].UniqueRowID,
		Overflow:     parentNode.Cells[idx].Overflow,
		unique:       ui.unique,
	}
	leftNode.AppendCells(append([]IndexCell[T]{cell}, cellsToMoveLeft...)...)
	leftNode.Header.RightChild = rightNode.Header.RightChild

	return parentNode.DeleteKeyAndRightChild(idx)
}

func compare[T IndexKey](a, b T) int {
	return compareAny(any(a), any(b))
}
