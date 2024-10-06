package minisql

import (
	"context"
	"fmt"
)

type Cursor struct {
	Table   *Table
	PageIdx uint32
	CellIdx uint32
}

func (c *Cursor) LeafNodeInsert(ctx context.Context, key uint64, aRow *Row) error {
	aPage, err := c.Table.pager.GetPage(ctx, c.Table, c.PageIdx)
	if err != nil {
		return err
	}
	if aPage.LeafNode == nil {
		return fmt.Errorf("error inserting row to a non leaf node, key %d", key)
	}

	cells := aPage.LeafNode.Header.Cells
	if cells >= aRow.MaxCells() {
		// Split leaf node
		return c.LeafNodeSplitInsert(ctx, key, aRow)
	}

	if c.CellIdx < cells {
		// Need make room for new cell
		for i := cells; i > c.CellIdx; i-- {
			aPage.LeafNode.Cells[i] = aPage.LeafNode.Cells[i-1]
		}
	}
	aPage.LeafNode.Header.Cells += 1

	aCell := aPage.LeafNode.Cells[c.CellIdx]
	return saveToCell(ctx, &aCell, key, aRow)
}

// Create a new node and move half the cells over.
// Insert the new value in one of the two nodes.
// Update parent or create a new parent.
func (c *Cursor) LeafNodeSplitInsert(ctx context.Context, key uint64, aRow *Row) error {
	aPager := c.Table.pager

	oldPage, err := aPager.GetPage(ctx, c.Table, c.PageIdx)
	if err != nil {
		return err
	}

	oldMaxKey, _ := oldPage.GetMaxKey()
	newPageNum := aPager.TotalPages(c.Table)

	// Append new page at the end
	// TODO: Page recycle
	newPage, err := aPager.GetPage(ctx, c.Table, newPageNum)
	if err != nil {
		return err
	}

	newPage.LeafNode = NewLeafNode(uint64(c.Table.RowSize))
	newPage.LeafNode.Header.Parent = oldPage.LeafNode.Header.Parent

	newPage.LeafNode.Header.NextLeaf = oldPage.LeafNode.Header.NextLeaf
	oldPage.LeafNode.Header.NextLeaf = newPageNum

	/*
	  All existing keys plus new key should should be divided
	  evenly between old (left) and new (right) nodes.
	  Starting from the right, move each key to correct position.
	*/
	var (
		leafNodeMaxCells = uint32(len(newPage.LeafNode.Cells))
		rightSplitCount  = (leafNodeMaxCells + 1) / 2
		leftSplitCount   = leafNodeMaxCells + 1 - rightSplitCount
	)
	for i := leafNodeMaxCells; ; i-- {
		if i+1 == 0 {
			break
		}
		var (
			destPage *Page
			isLeft   = i < leftSplitCount
		)

		if !isLeft {
			destPage = newPage // right
		} else {
			destPage = oldPage // left
		}
		cellIdx := i % leftSplitCount
		destCell := &destPage.LeafNode.Cells[cellIdx]

		if i == c.CellIdx {
			if err := saveToCell(ctx, destCell, key, aRow); err != nil {
				return err
			}
		} else if i > c.CellIdx {
			*destCell = oldPage.LeafNode.Cells[i-1]
		} else {
			*destCell = oldPage.LeafNode.Cells[i]
		}
	}

	/* Update cell count on both leaf nodes */
	oldPage.LeafNode.Header.Cells = leftSplitCount
	newPage.LeafNode.Header.Cells = rightSplitCount

	if oldPage.LeafNode.Header.IsRoot {
		return c.Table.CreateNewRoot(ctx, newPageNum)
	}

	parentPageIdx := oldPage.LeafNode.Header.Parent
	parentPage, err := aPager.GetPage(ctx, c.Table, parentPageIdx)
	if err != nil {
		return err
	}

	// parent page is an internal node
	oldChildIdx := parentPage.InternalNode.FindChildByKey(oldMaxKey)

	if oldChildIdx >= InternalNodeMaxCells {
		return fmt.Errorf("exceeded internal node max cells during splitting")
	}
	parentPage.InternalNode.ICells[oldChildIdx].Key, _ = oldPage.GetMaxKey()
	return c.Table.InternalNodeInsert(ctx, parentPageIdx, newPageNum)
}

func saveToCell(ctx context.Context, cell *Cell, key uint64, aRow *Row) error {
	rowBuf, err := aRow.Marshal()
	if err != nil {
		return err
	}
	cell.Key = key
	copy(cell.Value[:], rowBuf)
	return nil
}
