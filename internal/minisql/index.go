package minisql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

// MaxIndexKeySize is the maximum number of bytes allowed for a single index key.
const MaxIndexKeySize = 255

// IndexKey ...
type IndexKey interface {
	int8 | int32 | int64 | float32 | float64 | string | CompositeKey | UUIDValue
}

// Index ...
type Index[T IndexKey] struct {
	pager         TxPager
	logger        *zap.Logger
	txManager     *TransactionManager
	Name          string
	Columns       []Column
	rightmostLeaf atomic.Int64  // cached rightmost leaf page index; -1 = invalid/unpopulated
	lastTxID      atomic.Uint64 // transaction that last wrote rightmostLeaf; 0 = none
	rootPageIdx   PageIndex
	maximumKeys   uint32
	unique        bool
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
	idx := &Index[T]{
		logger:      logger,
		Name:        name,
		Columns:     columns,
		unique:      unique,
		rootPageIdx: rootPageIdx,
		pager:       pager,
		txManager:   txManager,
	}
	idx.rightmostLeaf.Store(-1)
	return idx, nil
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

	// Invalidate the rightmost-leaf cache when a new transaction begins.
	// After a rollback (explicit or OCC conflict), the tree reverts to its
	// pre-transaction state but rightmostLeaf may still point to a stale page
	// from the aborted transaction.  Checking the transaction ID here ensures
	// each new transaction starts with a cold cache rather than a potentially
	// wrong hint from a previous (possibly rolled-back) transaction.
	if tx := TxFromContext(ctx); tx != nil {
		if uint64(tx.ID) != ui.lastTxID.Load() {
			ui.rightmostLeaf.Store(-1)
			ui.lastTxID.Store(uint64(tx.ID))
		}
	}

	// Start with a read-only peek at the root — only upgrade to ModifyPage when we know
	// we must write (empty root, full root requiring a split).
	rootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}
	rootNode := rootPage.IndexNode.(*IndexNode[T])

	// Root is empty, insert the first key — must upgrade to write.
	if rootNode.Header.Keys == 0 {
		rootPage, err = ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
		if err != nil {
			return fmt.Errorf("modify root page for first insert: %w", err)
		}
		rootNode = rootPage.IndexNode.(*IndexNode[T])
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
		rootNode.freeBytes -= rootNode.Cells[0].Size()
		// Seed the cache: the root leaf is the rightmost (and only) leaf.
		ui.rightmostLeaf.Store(int64(ui.GetRootPageIdx()))
		return nil
	}

	if ui.unique {
		// In case of unique index, we cannot insert duplicate keys (read-only check).
		_, ok, err := ui.Seek(ctx, rootPage, key)
		if err != nil {
			return fmt.Errorf("seek key: %w", err)
		}
		if ok {
			return ErrDuplicateKey
		}
	}

	// Root is not full — insertNotFull will upgrade only the pages it actually writes.
	if ui.hasSpaceForKey(rootNode, key) {
		// Fast path: when root has room and we hold a rightmost-leaf hint, skip the
		// tree traversal for strictly-increasing (autoincrement) keys.
		// The root-has-space guard is essential: a full root triggers a proactive split
		// in the normal path that restructures the tree; the fast path cannot replicate
		// that, so it must only fire when no root split is required.
		if cached := ui.rightmostLeaf.Load(); cached >= 0 {
			inserted, err := ui.tryInsertIntoRightmostLeaf(ctx, PageIndex(cached), key, rowID)
			if err != nil {
				return err
			}
			if inserted {
				return nil
			}
			// Miss (leaf full, stale, or key out-of-order): invalidate and fall through.
			ui.rightmostLeaf.Store(-1)
		}

		leafIdx, isRightmost, err := ui.insertNotFull(ctx, ui.GetRootPageIdx(), key, rowID)
		if err != nil {
			return err
		}
		if isRightmost {
			ui.rightmostLeaf.Store(int64(leafIdx))
		} else {
			ui.rightmostLeaf.Store(-1)
		}
		return nil
	}

	// Root is full, need to split — upgrade root to write set now.
	rootPage, err = ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("modify root page for split: %w", err)
	}
	rootNode = rootPage.IndexNode.(*IndexNode[T])

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
	// isRightmostAtRoot: after split the root has exactly 1 key; going to child
	// index 1 (== Keys) means we took the rightmost branch at the root level.
	isRightmostAtRoot := i == rootNode.Header.Keys
	leafIdx, isRightmostBelow, err := ui.insertNotFull(ctx, childIdx, key, rowID)
	if err != nil {
		return fmt.Errorf("insert not full: %w", err)
	}
	if isRightmostAtRoot && isRightmostBelow {
		ui.rightmostLeaf.Store(int64(leafIdx))
	} else {
		ui.rightmostLeaf.Store(-1)
	}
	return nil
}

// tryInsertIntoRightmostLeaf attempts to insert key directly into the cached
// rightmost leaf without traversing the tree. It succeeds when:
//   - the cached page is still a valid leaf node of the correct type,
//   - key is strictly greater than the last key in the leaf (monotonic insert), and
//   - the leaf has room for one more key.
//
// Returns (true, nil) on success, (false, nil) on any cache miss, and
// (false, err) on an unexpected I/O error.
func (ui *Index[T]) tryInsertIntoRightmostLeaf(ctx context.Context, leafIdx PageIndex, key T, rowID RowID) (bool, error) {
	leafPage, err := ui.pager.ReadPage(ctx, leafIdx)
	if err != nil {
		return false, fmt.Errorf("read cached rightmost leaf %d: %w", leafIdx, err)
	}
	leafNode, ok := leafPage.IndexNode.(*IndexNode[T])
	if !ok || leafNode == nil || !leafNode.Header.IsLeaf || leafNode.Header.Keys == 0 {
		return false, nil // stale or wrong type
	}
	// Key must land after the current last key (rightmost position).
	if compare(leafNode.LastCell().Key, key) >= 0 {
		return false, nil // out-of-order or duplicate
	}
	if !ui.hasSpaceForKey(leafNode, key) {
		return false, nil // leaf is full — caller will fall back to a split
	}

	// Upgrade to write set and re-read the node (COW).
	leafPage, err = ui.pager.ModifyPage(ctx, leafIdx)
	if err != nil {
		return false, fmt.Errorf("modify cached rightmost leaf %d: %w", leafIdx, err)
	}
	leafNode = leafPage.IndexNode.(*IndexNode[T])

	// Append at the end — no shifting required since key > all existing keys.
	i := int(leafNode.Header.Keys)
	leafNode.Cells = append(leafNode.Cells, NewIndexCell[T](ui.unique))
	leafNode.Cells[i].Key = key
	if ui.unique {
		leafNode.Cells[i].UniqueRowID = rowID
	} else {
		leafNode.Cells[i].InlineRowIDs = 1
		leafNode.Cells[i].RowIDs = []RowID{rowID}
	}
	leafNode.Header.Keys++
	leafNode.freeBytes -= leafNode.Cells[i].Size()
	return true, nil
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

// insertNotFull inserts key/rowID into the subtree rooted at pageIdx.
// It returns the leaf page index where the key landed, a bool indicating
// whether every traversal step in this call chose the rightmost child
// (so the caller can determine if the leaf is the global rightmost), and
// any error.
func (ui *Index[T]) insertNotFull(ctx context.Context, pageIdx PageIndex, key T, rowID RowID) (PageIndex, bool, error) {
	// Read the page first — only upgrade to ModifyPage when we know we will write.
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return 0, false, fmt.Errorf("get page: %w", err)
	}
	node := page.IndexNode.(*IndexNode[T])

	if !ui.unique {
		// For non-unique index, binary-search for an existing key to append the row ID.
		pos := sort.Search(int(node.Header.Keys), func(i int) bool {
			return compare(node.Cells[i].Key, key) >= 0
		})
		if pos < int(node.Header.Keys) && compare(node.Cells[pos].Key, key) == 0 {
			// Upgrade to write set before modifying.
			page, err = ui.pager.ModifyPage(ctx, pageIdx)
			if err != nil {
				return 0, false, fmt.Errorf("modify page for append row ID: %w", err)
			}
			node = page.IndexNode.(*IndexNode[T])
			if err := appendRowID(ctx, ui.pager, node, uint32(pos), rowID); err != nil {
				return 0, false, fmt.Errorf("error appending row ID to existing key: %w", err)
			}
			// Appending to an existing key — not a rightmost-leaf insert.
			return pageIdx, false, nil
		}
		// Otherwise fall through to insert a new key and row ID.
	}

	if node.Header.IsLeaf {
		// Upgrade to write set before inserting into the leaf.
		page, err = ui.pager.ModifyPage(ctx, pageIdx)
		if err != nil {
			return 0, false, fmt.Errorf("modify leaf page for insert: %w", err)
		}
		node = page.IndexNode.(*IndexNode[T])

		// Binary search for the insertion position.
		pos := sort.Search(int(node.Header.Keys), func(i int) bool {
			return compare(node.Cells[i].Key, key) >= 0
		})
		// Extend the slice by one, then shift cells [pos, Keys-1] right to make room.
		node.Cells = append(node.Cells, NewIndexCell[T](ui.unique))
		for j := int(node.Header.Keys) - 1; j >= pos; j-- {
			node.Cells[j+1] = node.Cells[j]
		}
		node.Cells[pos] = NewIndexCell[T](ui.unique)
		node.Cells[pos].Key = key
		if ui.unique {
			node.Cells[pos].UniqueRowID = rowID
		} else {
			node.Cells[pos].InlineRowIDs = 1
			node.Cells[pos].RowIDs = []RowID{rowID}
		}
		node.Header.Keys += 1
		node.freeBytes -= node.Cells[pos].Size()
		// The leaf itself is trivially "rightmost at leaf level"; whether it is the
		// global rightmost depends on the path taken above (callers check their own
		// level's direction and AND it with this return value).
		return pageIdx, true, nil
	}

	// Internal node — binary search for the child that will receive the new key.
	// pos = first index where Cells[pos].Key > key; child to descend into is pos.
	pos := sort.Search(int(node.Header.Keys), func(i int) bool {
		return compare(node.Cells[i].Key, key) > 0
	})

	childIdx, err := node.Child(uint32(pos))
	if err != nil {
		return 0, false, fmt.Errorf("get child: %w", err)
	}

	// Peek at the child with ReadPage to decide if a split is needed.
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return 0, false, fmt.Errorf("get child page: %w", err)
	}
	childNode := childPage.IndexNode.(*IndexNode[T])

	if !ui.hasSpaceForKey(childNode, key) {
		// Child is full — upgrade both parent and child to write set before splitting.
		page, err = ui.pager.ModifyPage(ctx, pageIdx)
		if err != nil {
			return 0, false, fmt.Errorf("modify parent page for split: %w", err)
		}
		node = page.IndexNode.(*IndexNode[T])
		childPage, err = ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return 0, false, fmt.Errorf("modify child page for split: %w", err)
		}
		if err := ui.splitChild(ctx, page, childPage, uint32(pos)); err != nil {
			return 0, false, fmt.Errorf("split child: %w", err)
		}
		if compare(node.Cells[pos].Key, key) < 0 {
			pos += 1
		}
		// Re-read the child index from the now-updated parent node.
		childIdx, err = node.Child(uint32(pos))
		if err != nil {
			return 0, false, fmt.Errorf("get child after split: %w", err)
		}
	}

	// isRightmostAtThisLevel: we chose node.Child(Keys) — the RightChild pointer.
	isRightmostAtThisLevel := uint32(pos) == node.Header.Keys
	leafIdx, isRightmostBelow, err := ui.insertNotFull(ctx, childIdx, key, rowID)
	return leafIdx, isRightmostAtThisLevel && isRightmostBelow, err
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
	parentNode.freeBytes -= cellToMoveToParent.Size()
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

	// Invalidate the rightmost-leaf cache: a delete may trigger merges or borrows
	// that restructure the rightmost path, making the cached page index stale.
	ui.rightmostLeaf.Store(-1)

	// Start with a read-only peek at the root; remove will upgrade pages as needed.
	rootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("get root page: %w", err)
	}

	if err := ui.remove(ctx, rootPage, key, rowID); err != nil {
		return fmt.Errorf("remove key: %w", err)
	}

	// Re-read root — it may have been promoted to the write set by remove.
	rootPage, err = ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("re-read root page: %w", err)
	}
	rootNode := rootPage.IndexNode.(*IndexNode[T])
	if rootNode.Header.Keys > 0 {
		return nil
	}

	if rootNode.Header.IsLeaf {
		// Tree is now empty. The root-leaf with Keys=0 is a valid empty state;
		// the next Insert will handle Keys==0 and reinitialise the node.
		return nil
	}

	// Root has no keys — collapse: make first child the new root.
	firstChildIdx, err := rootNode.Child(0)
	if err != nil {
		return fmt.Errorf("get first child: %w", err)
	}
	firstChildPage, err := ui.pager.ModifyPage(ctx, firstChildIdx)
	if err != nil {
		return fmt.Errorf("get new root page: %w", err)
	}
	firstChildNode := firstChildPage.IndexNode.(*IndexNode[T])

	// Upgrade root to write set before overwriting its content.
	rootPage, err = ui.pager.ModifyPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return fmt.Errorf("modify root page for collapse: %w", err)
	}
	rootNode = rootPage.IndexNode.(*IndexNode[T])
	*rootNode = *firstChildNode
	rootNode.Header.Parent = 0
	rootNode.Header.IsRoot = true

	// Update children to set new parent.
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

// remove a key from the sub-tree rooted with this node.
// page may be either a ReadPage or ModifyPage result; this function upgrades it
// to the write set only when an actual modification is required.
func (ui *Index[T]) remove(ctx context.Context, page *Page, key T, rowID RowID) error {
	node := page.IndexNode.(*IndexNode[T])

	// Binary search for the first index where Cells[idx].Key >= key.
	idx := sort.Search(int(node.Header.Keys), func(i int) bool {
		return compare(node.Cells[i].Key, key) >= 0
	})

	if idx < int(node.Header.Keys) && compare(node.Cells[idx].Key, key) == 0 {
		// Key is in this node.
		if node.Header.IsLeaf {
			// Upgrade page to write set before modifying the leaf.
			writePage, err := ui.pager.ModifyPage(ctx, page.Index)
			if err != nil {
				return fmt.Errorf("modify leaf page for delete: %w", err)
			}
			writeNode := writePage.IndexNode.(*IndexNode[T])

			if ui.unique || writeNode.Cells[idx].InlineRowIDs == 1 {
				return writeNode.DeleteKeyAndRightChild(uint32(idx))
			}
			if err := removeRowID(ctx, ui.pager, writeNode, uint32(idx), key, rowID); err != nil {
				return fmt.Errorf("remove row ID: %w", err)
			}
			return nil
		}

		// Internal node with multiple row IDs for a non-unique index.
		if !ui.unique && node.Cells[idx].InlineRowIDs > 1 {
			writePage, err := ui.pager.ModifyPage(ctx, page.Index)
			if err != nil {
				return fmt.Errorf("modify internal page for remove row ID: %w", err)
			}
			writeNode := writePage.IndexNode.(*IndexNode[T])
			if err := removeRowID(ctx, ui.pager, writeNode, uint32(idx), key, rowID); err != nil {
				return fmt.Errorf("remove row ID: %w", err)
			}
			return nil
		}

		// Structural removal from an internal node — must be in write set.
		writePage, err := ui.pager.ModifyPage(ctx, page.Index)
		if err != nil {
			return fmt.Errorf("modify internal page for removeFromInternal: %w", err)
		}
		if err := ui.removeFromInternal(ctx, writePage, idx, rowID); err != nil {
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

	// Peek at the child with ReadPage — no write yet.
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}

	childNode := childPage.IndexNode.(*IndexNode[T])
	if !ui.atLeastHalfFull(childNode) {
		// Child is underfull — upgrade both parent and child before fill.
		writePage, err := ui.pager.ModifyPage(ctx, page.Index)
		if err != nil {
			return fmt.Errorf("modify parent page for fill: %w", err)
		}
		childPage, err = ui.pager.ModifyPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("modify child page for fill: %w", err)
		}

		if err := ui.fill(ctx, writePage, childPage, idx); err != nil {
			return fmt.Errorf("fill child: %w", err)
		}
		// Refresh node reference after fill may have altered the parent's cells.
		node = writePage.IndexNode.(*IndexNode[T])
	}

	if flag && idx > int(node.Header.Keys) {
		prevChildIdx, err := node.Child(uint32(idx - 1))
		if err != nil {
			return fmt.Errorf("get prev child: %w", err)
		}
		prevChildPage, err := ui.pager.ReadPage(ctx, prevChildIdx)
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

// getPred finds the predecessor (rightmost leaf key in left subtree) of the key
// at the idx-th position in the node.  All traversal is read-only; the actual
// deletion of the predecessor is handled by the subsequent remove call.
func (ui *Index[T]) getPred(ctx context.Context, node *IndexNode[T], idx int) (IndexCell[T], error) {
	curPage, err := ui.pager.ReadPage(ctx, node.Cells[idx].Child)
	if err != nil {
		return IndexCell[T]{}, fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		page, err := ui.pager.ReadPage(ctx, cur.Header.RightChild)
		if err != nil {
			return IndexCell[T]{}, fmt.Errorf("get page: %w", err)
		}
		cur = page.IndexNode.(*IndexNode[T])
	}

	return cur.LastCell(), nil
}

// getSucc finds the successor (leftmost leaf key in right subtree) of the key
// at the idx-th position in the node.  All traversal is read-only; the actual
// deletion of the successor is handled by the subsequent remove call.
func (ui *Index[T]) getSucc(ctx context.Context, node *IndexNode[T], idx int) (IndexCell[T], error) {
	curIdx, err := node.Child(uint32(idx + 1))
	if err != nil {
		return IndexCell[T]{}, fmt.Errorf("get child index: %w", err)
	}
	curPage, err := ui.pager.ReadPage(ctx, curIdx)
	if err != nil {
		return IndexCell[T]{}, fmt.Errorf("get child page: %w", err)
	}
	cur := curPage.IndexNode.(*IndexNode[T])
	for !cur.Header.IsLeaf {
		page, err := ui.pager.ReadPage(ctx, cur.Cells[0].Child)
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

	// Capture size before the in-place key replacement; then delta-adjust
	// freeBytes. For fixed-width types (int64 etc.) old == new, so this is
	// effectively a no-op arithmetic-wise, but avoids the O(n) recompute.
	oldCellSize := parentNode.Cells[idx-1].Size()
	last := leftNode.LastCell()
	parentNode.Cells[idx-1].Key = last.Key
	parentNode.Cells[idx-1].InlineRowIDs = last.InlineRowIDs
	parentNode.Cells[idx-1].RowIDs = last.RowIDs
	parentNode.Cells[idx-1].UniqueRowID = last.UniqueRowID
	parentNode.Cells[idx-1].Overflow = last.Overflow
	newCellSize := parentNode.Cells[idx-1].Size()
	if newCellSize > oldCellSize {
		parentNode.freeBytes -= newCellSize - oldCellSize
	} else {
		parentNode.freeBytes += oldCellSize - newCellSize
	}

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

	// Capture size before the in-place key replacement; delta-adjust freeBytes.
	oldCellSize := parentNode.Cells[idx].Size()
	first := rightNode.FirstCell()
	parentNode.Cells[idx].Key = first.Key
	parentNode.Cells[idx].InlineRowIDs = first.InlineRowIDs
	parentNode.Cells[idx].RowIDs = first.RowIDs
	parentNode.Cells[idx].UniqueRowID = first.UniqueRowID
	parentNode.Cells[idx].Overflow = first.Overflow
	newCellSize := parentNode.Cells[idx].Size()
	if newCellSize > oldCellSize {
		parentNode.freeBytes -= newCellSize - oldCellSize
	} else {
		parentNode.freeBytes += oldCellSize - newCellSize
	}

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

// compare compares two index keys of the same type without routing through
// compareAny.  Inlining the type switch here eliminates the external function
// call and lets the Go compiler inline compare[T] into its call sites.
// a and b are always the same concrete type because T is a single-type
// instantiation of IndexKey — the cross-type branches in compareAny are never
// reachable here.
func compare[T IndexKey](a, b T) int {
	switch va := any(a).(type) {
	case int8:
		vb := any(b).(int8)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case int32:
		vb := any(b).(int32)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case int64:
		vb := any(b).(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float32:
		vb := any(b).(float32)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float64:
		vb := any(b).(float64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		return strings.Compare(va, any(b).(string))
	case CompositeKey:
		return bytes.Compare(va.Comparison, any(b).(CompositeKey).Comparison)
	case UUIDValue:
		vb := any(b).(UUIDValue)
		return bytes.Compare(va[:], vb[:])
	}
	return 0
}
