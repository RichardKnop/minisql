package minisql

import (
	"context"
	"fmt"
)

type indexScanner func(key any, rowID RowID) error

// ScanAll iterates over all keys in the index in order using in-order traversal
func (ui *UniqueIndex[T]) ScanAll(ctx context.Context, reverse bool, callback indexScanner) error {
	if reverse {
		return ui.scanDescending(ctx, ui.GetRootPageIdx(), callback)
	}
	return ui.scanAscending(ctx, ui.GetRootPageIdx(), callback)
}

// scanAscending performs in-order traversal (ascending order)
// Structure: child[0] key[0] child[1] key[1] ... child[n-1] key[n-1] child[n]
// where child[i] is stored in Cells[i].Child and child[n] is RightChild
func (ui *UniqueIndex[T]) scanAscending(ctx context.Context, pageIdx PageIndex, callback indexScanner) error {
	aPage, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}

	aNode := aPage.IndexNode.(*UniqueIndexNode[T])

	if aNode.Header.IsLeaf {
		// Leaf node: just visit all keys in order
		for i := uint32(0); i < aNode.Header.Keys; i++ {
			aCell := aNode.Cells[i]
			if err := callback(aCell.Key, aCell.RowID); err != nil {
				return err
			}
		}
		return nil
	}

	// Internal node: in-order traversal
	for i := uint32(0); i < aNode.Header.Keys; i++ {
		aCell := aNode.Cells[i]

		// Visit left child of this key
		if aCell.Child != 0 {
			if err := ui.scanAscending(ctx, aCell.Child, callback); err != nil {
				return err
			}
		}

		// Visit the key itself
		if err := callback(aCell.Key, aCell.RowID); err != nil {
			return err
		}
	}

	// Visit the rightmost child
	if aNode.Header.RightChild != 0 {
		if err := ui.scanAscending(ctx, aNode.Header.RightChild, callback); err != nil {
			return err
		}
	}

	return nil
}

// scanDescending performs reverse in-order traversal (descending order)
func (ui *UniqueIndex[T]) scanDescending(ctx context.Context, pageIdx PageIndex, callback indexScanner) error {
	aPage, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}

	aNode := aPage.IndexNode.(*UniqueIndexNode[T])

	if aNode.Header.IsLeaf {
		// Leaf node: visit all keys in reverse order
		for i := int(aNode.Header.Keys) - 1; i >= 0; i-- {
			aCell := aNode.Cells[i]
			if err := callback(aCell.Key, aCell.RowID); err != nil {
				return err
			}
		}
		return nil
	}

	// Internal node: reverse in-order traversal
	// Visit rightmost child first
	if aNode.Header.RightChild != 0 {
		if err := ui.scanDescending(ctx, aNode.Header.RightChild, callback); err != nil {
			return err
		}
	}

	// Visit keys in reverse order
	for i := int(aNode.Header.Keys) - 1; i >= 0; i-- {
		aCell := aNode.Cells[i]

		// Visit the key
		if err := callback(aCell.Key, aCell.RowID); err != nil {
			return err
		}

		// Visit left child of this key
		if aCell.Child != 0 {
			if err := ui.scanDescending(ctx, aCell.Child, callback); err != nil {
				return err
			}
		}
	}

	return nil
}

// ScanRange scans keys within the specified range using in-order traversal with range checks.
func (ui *UniqueIndex[T]) ScanRange(ctx context.Context, rangeCondition RangeCondition, callback indexScanner) error {
	// If lower bound exists, seek to it
	if rangeCondition.Lower != nil {
		aPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
		if err != nil {
			return err
		}

		aCursor, _, err := ui.Seek(ctx, aPage, rangeCondition.Lower.Value.(T))
		if err != nil {
			return err
		}
		return ui.scanRangeFrom(ctx, aCursor.PageIdx, aCursor.CellIdx, rangeCondition, callback)
	}
	// Otherwise, scan from the beginning
	return ui.scanRangeRecursive(ctx, ui.GetRootPageIdx(), rangeCondition, callback)
}

// scanRangeFrom scans in in-order traversal starting from a given page and cell index (for B-tree).
func (ui *UniqueIndex[T]) scanRangeFrom(
	ctx context.Context,
	pageIdx PageIndex,
	cellIdx uint32,
	rangeCondition RangeCondition,
	callback indexScanner,
) error {
	aPage, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}
	aNode := aPage.IndexNode.(*UniqueIndexNode[T])

	// Start at cellIdx, traverse in-order
	for i := cellIdx; i < aNode.Header.Keys; i++ {
		// Visit left child from from the second key onwards
		// Child of the first key contains keys lower than the first key (outside range)
		if !aNode.Header.IsLeaf && aNode.Cells[i].Child != 0 && i > cellIdx {
			if err := ui.scanRangeRecursive(ctx, aNode.Cells[i].Child, rangeCondition, callback); err != nil {
				return err
			}
		}

		var (
			aCell = aNode.Cells[i]
			key   = aCell.Key
		)

		// Range checks
		if rangeCondition.Lower != nil {
			cmp := compareAny(key, rangeCondition.Lower.Value.(T))
			if cmp < 0 || (cmp == 0 && !rangeCondition.Lower.Inclusive) {
				continue // Key is below lower bound
			}
		}
		if rangeCondition.Upper != nil {
			cmp := compareAny(key, rangeCondition.Upper.Value.(T))
			if cmp > 0 || (cmp == 0 && !rangeCondition.Upper.Inclusive) {
				return nil // Key is above upper bound, stop traversal
			}
		}

		// Key is within range
		if err := callback(key, aCell.RowID); err != nil {
			return err
		}
	}

	// After last key, visit rightmost child
	if !aNode.Header.IsLeaf && aNode.Header.RightChild != 0 {
		if err := ui.scanRangeRecursive(ctx, aNode.Header.RightChild, rangeCondition, callback); err != nil {
			return err
		}
	}

	if aNode.Header.IsRoot {
		return nil

	}

	// Recurse to the parent to find all succeeding keys
	var (
		startPageIdx = pageIdx
		parentIdx    = aNode.Header.Parent
		reachedRoot  = false
	)
	for !reachedRoot {
		parentPage, err := ui.pager.ReadPage(ctx, parentIdx)
		if err != nil {
			return fmt.Errorf("read parent page: %w", err)
		}
		parentNode := parentPage.IndexNode.(*UniqueIndexNode[T])
		if parentNode.Header.IsRoot {
			reachedRoot = true
		} else {
			parentIdx = parentNode.Header.Parent
		}

		// Find the index of the current page in the parent
		parentCellIdx := -1
		for j := uint32(0); j < parentNode.Header.Keys; j++ {
			childIdx, err := parentNode.Child(j)
			if err != nil {
				return fmt.Errorf("get child index: %w", err)
			}
			if childIdx == startPageIdx {
				parentCellIdx = int(j)
				break
			}
		}
		startPageIdx = parentPage.Index

		if parentCellIdx == -1 || parentCellIdx == int(parentNode.Header.Keys) {
			// No more succeding keys in parent, we're done but check parent until root
			continue
		}

		for idx := parentCellIdx; idx < int(parentNode.Header.Keys); idx++ {
			var (
				aCell = parentNode.Cells[idx]
				key   = aCell.Key
			)

			if err := callback(key, aCell.RowID); err != nil {
				return err
			}

			nextChildIdx, err := parentNode.Child(uint32(idx) + 1)
			if err != nil {
				return fmt.Errorf("get next child index: %w", err)
			}
			if err := ui.scanRangeRecursive(ctx, nextChildIdx, rangeCondition, callback); err != nil {
				return err
			}
		}
	}

	return nil
}

// scanRangeRecursive traverses the tree in-order, applying range checks at each step.
func (ui *UniqueIndex[T]) scanRangeRecursive(ctx context.Context, pageIdx PageIndex, rangeCondition RangeCondition, callback indexScanner) error {
	aPage, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}
	aNode := aPage.IndexNode.(*UniqueIndexNode[T])

	for i := uint32(0); i < aNode.Header.Keys; i++ {
		// Visit left child before key[i]
		if !aNode.Header.IsLeaf && aNode.Cells[i].Child != 0 {
			if err := ui.scanRangeRecursive(ctx, aNode.Cells[i].Child, rangeCondition, callback); err != nil {
				return err
			}
		}

		var (
			aCell = aNode.Cells[i]
			key   = aCell.Key
		)

		// Range checks
		if rangeCondition.Lower != nil {
			cmp := compareAny(key, rangeCondition.Lower.Value.(T))
			if cmp < 0 || (cmp == 0 && !rangeCondition.Lower.Inclusive) {
				continue // Key is below lower bound
			}
		}

		if rangeCondition.Upper != nil {
			cmp := compareAny(key, rangeCondition.Upper.Value.(T))
			if cmp > 0 || (cmp == 0 && !rangeCondition.Upper.Inclusive) {
				return nil // Key is above upper bound, stop traversal
			}
		}

		// Key is within range
		if err := callback(key, aCell.RowID); err != nil {
			return err
		}
	}

	// Visit rightmost child after all keys
	if !aNode.Header.IsLeaf && aNode.Header.RightChild != 0 {
		if err := ui.scanRangeRecursive(ctx, aNode.Header.RightChild, rangeCondition, callback); err != nil {
			return err
		}
	}

	return nil
}
