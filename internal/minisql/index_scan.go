package minisql

import (
	"context"
	"fmt"
)

type indexScanner func(key any, rowID RowID) error

// ScanAll iterates over all keys in the index in order using in-order traversal
func (ui *Index[T]) ScanAll(ctx context.Context, reverse bool, callback indexScanner) error {
	if reverse {
		return ui.scanDescending(ctx, ui.GetRootPageIdx(), callback)
	}
	return ui.scanAscending(ctx, ui.GetRootPageIdx(), callback)
}

// scanAscending performs in-order traversal (ascending order)
// Structure: child[0] key[0] child[1] key[1] ... child[n-1] key[n-1] child[n]
// where child[i] is stored in Cells[i].Child and child[n] is RightChild
func (ui *Index[T]) scanAscending(ctx context.Context, pageIdx PageIndex, callback indexScanner) error {
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}

	node := page.IndexNode.(*IndexNode[T])

	if node.Header.IsLeaf {
		// Leaf node: just visit all keys in order
		for i := uint32(0); i < node.Header.Keys; i++ {
			cell := node.Cells[i]
			for _, rowID := range cell.RowIDs {
				if err := callback(cell.Key, rowID); err != nil {
					return err
				}
			}
			if cell.Overflow != 0 {
				rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
				if err != nil {
					return err
				}
				for _, rowID := range rowIDs {
					if err := callback(cell.Key, rowID); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// Internal node: in-order traversal
	for i := uint32(0); i < node.Header.Keys; i++ {
		cell := node.Cells[i]

		// Visit left child of this key
		if cell.Child != 0 {
			if err := ui.scanAscending(ctx, cell.Child, callback); err != nil {
				return err
			}
		}

		// Visit the key itself
		for _, rowID := range cell.RowIDs {
			if err := callback(cell.Key, rowID); err != nil {
				return err
			}
		}
		if cell.Overflow != 0 {
			rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
			if err != nil {
				return err
			}
			for _, rowID := range rowIDs {
				if err := callback(cell.Key, rowID); err != nil {
					return err
				}
			}
		}
	}

	// Visit the rightmost child
	if node.Header.RightChild != 0 {
		if err := ui.scanAscending(ctx, node.Header.RightChild, callback); err != nil {
			return err
		}
	}

	return nil
}

// scanDescending performs reverse in-order traversal (descending order)
func (ui *Index[T]) scanDescending(ctx context.Context, pageIdx PageIndex, callback indexScanner) error {
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}

	node := page.IndexNode.(*IndexNode[T])

	if node.Header.IsLeaf {
		// Leaf node: visit all keys in reverse order
		for i := int(node.Header.Keys) - 1; i >= 0; i-- {
			cell := node.Cells[i]
			for _, rowID := range cell.RowIDs {
				if err := callback(cell.Key, rowID); err != nil {
					return err
				}
			}
			if cell.Overflow != 0 {
				rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
				if err != nil {
					return err
				}
				for _, rowID := range rowIDs {
					if err := callback(cell.Key, rowID); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// Internal node: reverse in-order traversal
	// Visit rightmost child first
	if node.Header.RightChild != 0 {
		if err := ui.scanDescending(ctx, node.Header.RightChild, callback); err != nil {
			return err
		}
	}

	// Visit keys in reverse order
	for i := int(node.Header.Keys) - 1; i >= 0; i-- {
		cell := node.Cells[i]

		// Visit the key
		for _, rowID := range cell.RowIDs {
			if err := callback(cell.Key, rowID); err != nil {
				return err
			}
		}
		if cell.Overflow != 0 {
			rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
			if err != nil {
				return err
			}
			for _, rowID := range rowIDs {
				if err := callback(cell.Key, rowID); err != nil {
					return err
				}
			}
		}

		// Visit left child of this key
		if cell.Child != 0 {
			if err := ui.scanDescending(ctx, cell.Child, callback); err != nil {
				return err
			}
		}
	}

	return nil
}

// ScanRange scans keys within the specified range using in-order traversal with range checks.
func (ui *Index[T]) ScanRange(ctx context.Context, rangeCondition RangeCondition, reverse bool, callback indexScanner) error {
	if reverse {
		return ui.scanRangeReverse(ctx, rangeCondition, callback)
	}

	// If lower bound exists, seek to it
	if rangeCondition.Lower != nil {
		page, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
		if err != nil {
			return err
		}

		// If type is CompositeKey and lower bound is a prefix, use SeekWithPrefix
		cv, isComposite := any(rangeCondition.Lower.Value).(CompositeKey)
		if isComposite && len(cv.Columns) < len(ui.Columns) {
			cursor, ok, err := ui.SeekWithPrefix(ctx, page, rangeCondition.Lower.Value, len(cv.Columns))
			if err != nil {
				return err
			}
			if ok {
				return ui.scanRangeFrom(ctx, cursor.PageIdx, cursor.CellIdx, rangeCondition, callback)
			}
		} else {
			cursor, ok, err := ui.Seek(ctx, page, rangeCondition.Lower.Value.(T))
			if err != nil {
				return err
			}
			if ok {
				return ui.scanRangeFrom(ctx, cursor.PageIdx, cursor.CellIdx, rangeCondition, callback)
			}
		}
	}
	// Otherwise, scan from the beginning
	return ui.scanRangeRecursive(ctx, ui.GetRootPageIdx(), rangeCondition, callback)
}

// scanRangeReverse scans keys within the specified range in reverse order (descending).
func (ui *Index[T]) scanRangeReverse(ctx context.Context, rangeCondition RangeCondition, callback indexScanner) error {
	// Simply use recursive reverse scan from root - range checks happen at each step
	return ui.scanRangeRecursiveReverse(ctx, ui.GetRootPageIdx(), rangeCondition, callback)
}

// scanRangeFrom scans in in-order traversal starting from a given page and cell index (for B-tree).
func (ui *Index[T]) scanRangeFrom(
	ctx context.Context,
	pageIdx PageIndex,
	cellIdx uint32,
	rangeCondition RangeCondition,
	callback indexScanner,
) error {
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}
	node := page.IndexNode.(*IndexNode[T])

	// Start at cellIdx, traverse in-order
	for i := cellIdx; i < node.Header.Keys; i++ {
		// Visit left child from from the second key onwards
		// Child of the first key contains keys lower than the first key (outside range)
		if !node.Header.IsLeaf && node.Cells[i].Child != 0 && i > cellIdx {
			if err := ui.scanRangeRecursive(ctx, node.Cells[i].Child, rangeCondition, callback); err != nil {
				return err
			}
		}

		var (
			cell = node.Cells[i]
			key   = cell.Key
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
		for _, rowID := range cell.RowIDs {
			if err := callback(key, rowID); err != nil {
				return err
			}
		}
		if cell.Overflow != 0 {
			rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
			if err != nil {
				return err
			}
			for _, rowID := range rowIDs {
				if err := callback(key, rowID); err != nil {
					return err
				}
			}
		}
	}

	// After last key, visit rightmost child
	if !node.Header.IsLeaf && node.Header.RightChild != 0 {
		if err := ui.scanRangeRecursive(ctx, node.Header.RightChild, rangeCondition, callback); err != nil {
			return err
		}
	}

	if node.Header.IsRoot {
		return nil
	}

	// Recurse to the parent to find all succeeding keys
	var (
		startPageIdx = pageIdx
		parentIdx    = node.Header.Parent
		reachedRoot  = false
	)
	for !reachedRoot {
		parentPage, err := ui.pager.ReadPage(ctx, parentIdx)
		if err != nil {
			return fmt.Errorf("read parent page: %w", err)
		}
		parentNode := parentPage.IndexNode.(*IndexNode[T])
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
				cell = parentNode.Cells[idx]
				key   = cell.Key
			)

			for _, rowID := range cell.RowIDs {
				if err := callback(key, rowID); err != nil {
					return err
				}
			}
			if cell.Overflow != 0 {
				rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
				if err != nil {
					return err
				}
				for _, rowID := range rowIDs {
					if err := callback(key, rowID); err != nil {
						return err
					}
				}
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
func (ui *Index[T]) scanRangeRecursive(ctx context.Context, pageIdx PageIndex, rangeCondition RangeCondition, callback indexScanner) error {
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}
	node := page.IndexNode.(*IndexNode[T])

	for i := uint32(0); i < node.Header.Keys; i++ {
		// Visit left child before key[i]
		if !node.Header.IsLeaf && node.Cells[i].Child != 0 {
			if err := ui.scanRangeRecursive(ctx, node.Cells[i].Child, rangeCondition, callback); err != nil {
				return err
			}
		}

		var (
			cell = node.Cells[i]
			key   = cell.Key
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
		for _, rowID := range cell.RowIDs {
			if err := callback(key, rowID); err != nil {
				return err
			}
		}
		if cell.Overflow != 0 {
			rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
			if err != nil {
				return err
			}
			for _, rowID := range rowIDs {
				if err := callback(key, rowID); err != nil {
					return err
				}
			}
		}
	}

	// Visit rightmost child after all keys
	if !node.Header.IsLeaf && node.Header.RightChild != 0 {
		if err := ui.scanRangeRecursive(ctx, node.Header.RightChild, rangeCondition, callback); err != nil {
			return err
		}
	}

	return nil
}

// scanRangeFromReverse does not actually exist - we use scanRangeRecursiveReverse directly

// scanRangeRecursiveReverse traverses the tree in reverse in-order, applying range checks at each step.
func (ui *Index[T]) scanRangeRecursiveReverse(ctx context.Context, pageIdx PageIndex, rangeCondition RangeCondition, callback indexScanner) error {
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}
	node := page.IndexNode.(*IndexNode[T])

	// Visit rightmost child first
	if !node.Header.IsLeaf && node.Header.RightChild != 0 {
		if err := ui.scanRangeRecursiveReverse(ctx, node.Header.RightChild, rangeCondition, callback); err != nil {
			return err
		}
	}

	// Visit keys in reverse order
	for i := int(node.Header.Keys) - 1; i >= 0; i-- {
		var (
			cell = node.Cells[i]
			key   = cell.Key
		)

		// Check if this key should be included
		includeKey := true
		if rangeCondition.Upper != nil {
			cmp := compareAny(key, rangeCondition.Upper.Value.(T))
			if cmp > 0 || (cmp == 0 && !rangeCondition.Upper.Inclusive) {
				includeKey = false
			}
		}
		if rangeCondition.Lower != nil {
			cmp := compareAny(key, rangeCondition.Lower.Value.(T))
			if cmp < 0 || (cmp == 0 && !rangeCondition.Lower.Inclusive) {
				// Key is below lower bound, all remaining keys will be too
				// Visit left child first, then stop
				if !node.Header.IsLeaf && node.Cells[i].Child != 0 {
					if err := ui.scanRangeRecursiveReverse(ctx, node.Cells[i].Child, rangeCondition, callback); err != nil {
						return err
					}
				}
				return nil
			}
		}

		// Emit key if it's within range
		if includeKey {
			for _, rowID := range cell.RowIDs {
				if err := callback(key, rowID); err != nil {
					return err
				}
			}
			if cell.Overflow != 0 {
				rowIDs, err := readOverflowRowIDs[T](ctx, ui.pager, cell.Overflow)
				if err != nil {
					return err
				}
				for _, rowID := range rowIDs {
					if err := callback(key, rowID); err != nil {
						return err
					}
				}
			}
		}

		// Always visit left child (child contains keys less than current key)
		if !node.Header.IsLeaf && node.Cells[i].Child != 0 {
			if err := ui.scanRangeRecursiveReverse(ctx, node.Cells[i].Child, rangeCondition, callback); err != nil {
				return err
			}
		}
	}

	return nil
}

type indexCallback func(page *Page)

// BFS ...
func (ui *Index[T]) BFS(ctx context.Context, f indexCallback) error {
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
				page, err := ui.pager.ReadPage(ctx, idxCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, page)
			}
			if current.IndexNode.(*IndexNode[T]).Header.RightChild > 0 && current.IndexNode.(*IndexNode[T]).Header.RightChild != RightChildNotSet {
				page, err := ui.pager.ReadPage(ctx, current.IndexNode.(*IndexNode[T]).Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, page)
			}
		}
	}

	return nil
}
