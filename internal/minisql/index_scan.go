package minisql

import (
	"context"
	"io"
)

type indexScanner func(key any, rowID RowID) error

// ScanAll iterates over all keys in the index in order using in-order traversal
func (ui *UniqueIndex[T]) ScanAll(ctx context.Context, reverse bool, callback indexScanner) error {

	pageIdx := ui.GetRootPageIdx()

	if reverse {
		return ui.scanDescending(ctx, pageIdx, callback)
	}
	return ui.scanAscending(ctx, pageIdx, callback)
}

// scanAscending performs in-order traversal (ascending order)
// Structure: child[0] key[0] child[1] key[1] ... child[n-1] key[n-1] child[n]
// where child[i] is stored in Cells[i].Child and child[n] is RightChild
func (ui *UniqueIndex[T]) scanAscending(ctx context.Context, pageIdx PageIndex, callback indexScanner) error {

	aPage, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return err
	}

	aNode := aPage.IndexNode.(*IndexNode[T])

	if aNode.Header.IsLeaf {
		// Leaf node: just visit all keys in order
		for i := uint32(0); i < aNode.Header.Keys; i++ {
			aCell := aNode.Cells[i]
			if err := callback(aCell.Key, aCell.RowID); err != nil {
				if err == io.EOF {
					return nil
				}
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
			if err == io.EOF {
				return nil
			}
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

	aNode := aPage.IndexNode.(*IndexNode[T])

	if aNode.Header.IsLeaf {
		// Leaf node: visit all keys in reverse order
		for i := int(aNode.Header.Keys) - 1; i >= 0; i-- {
			aCell := aNode.Cells[i]
			if err := callback(aCell.Key, aCell.RowID); err != nil {
				if err == io.EOF {
					return nil
				}
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
			if err == io.EOF {
				return nil
			}
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
