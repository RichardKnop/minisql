package minisql

import (
	"context"
	"fmt"
)

type Cursor struct {
	Table      *Table
	PageIdx    uint32
	CellIdx    uint32
	EndOfTable bool
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

	if err := saveToCell(&aPage.LeafNode.Cells[c.CellIdx], key, aRow); err != nil {
		return err
	}
	aPage.LeafNode.Header.Cells += 1

	return nil
}

// Create a new node and move half the cells over.
// Insert the new value in one of the two nodes.
// Update parent or create a new parent.
func (c *Cursor) LeafNodeSplitInsert(ctx context.Context, key uint64, aRow *Row) error {
	aPager := c.Table.pager

	aSplitPage, err := aPager.GetPage(ctx, c.Table, c.PageIdx)
	if err != nil {
		return err
	}

	originalMaxKey, _ := aSplitPage.GetMaxKey()
	newPageIdx := aPager.TotalPages()

	c.Table.logger.Sugar().With(
		"key", int(key),
		"old_max_key", int(originalMaxKey),
		"new_page_index", int(newPageIdx),
	).Debug("leaf node split insert")

	// Append new page at the end
	// TODO: Page recycle
	aNewPage, err := aPager.GetPage(ctx, c.Table, newPageIdx)
	if err != nil {
		return err
	}

	aNewPage.LeafNode = NewLeafNode(uint64(c.Table.RowSize))
	aNewPage.LeafNode.Header.Parent = aSplitPage.LeafNode.Header.Parent

	aNewPage.LeafNode.Header.NextLeaf = aSplitPage.LeafNode.Header.NextLeaf
	aSplitPage.LeafNode.Header.NextLeaf = newPageIdx

	// All existing keys plus new key should should be divided
	// evenly between old (left) and new (right) nodes.
	// Starting from the right, move each key to correct position.
	var (
		leafNodeMaxCells = uint32(len(aNewPage.LeafNode.Cells))
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
			destPage = aNewPage // right
		} else {
			destPage = aSplitPage // left
		}
		cellIdx := i % leftSplitCount
		destCell := &destPage.LeafNode.Cells[cellIdx]

		if i == c.CellIdx {
			if err := saveToCell(destCell, key, aRow); err != nil {
				return err
			}
		} else if i > c.CellIdx {
			*destCell = aSplitPage.LeafNode.Cells[i-1]
		} else {
			*destCell = aSplitPage.LeafNode.Cells[i]
		}
	}

	// Update cell count on both leaf nodes
	aSplitPage.LeafNode.Header.Cells = leftSplitCount
	aNewPage.LeafNode.Header.Cells = rightSplitCount

	if aSplitPage.LeafNode.Header.IsRoot {
		_, err := c.Table.CreateNewRoot(ctx, newPageIdx)
		return err
	}

	parentPageIdx := aSplitPage.LeafNode.Header.Parent
	aParentPage, err := aPager.GetPage(ctx, c.Table, parentPageIdx)
	if err != nil {
		return err
	}

	// If we won't need to split the internal node,
	// update parent to reflect new max key
	oldChildIdx := aParentPage.InternalNode.IndexOfChild(originalMaxKey)
	if oldChildIdx < InternalNodeMaxCells {
		oldPageNewMaxKey, _ := aSplitPage.GetMaxKey()
		aParentPage.InternalNode.ICells[oldChildIdx].Key = oldPageNewMaxKey
	}

	return c.Table.InternalNodeInsert(ctx, parentPageIdx, newPageIdx)
}

func (c *Cursor) fetchRow(ctx context.Context) (Row, error) {
	aPage, err := c.Table.pager.GetPage(ctx, c.Table, c.PageIdx)
	if err != nil {
		return Row{}, err
	}
	aRow := NewRow(c.Table.Columns)

	if err := UnmarshalRow(aPage.LeafNode.Cells[c.CellIdx].Value[:], &aRow); err != nil {
		return Row{}, err
	}

	// There are still more cells in the page, move cursor to next cell and return
	if c.CellIdx < aPage.LeafNode.Header.Cells-1 {
		c.CellIdx += 1
		return aRow, nil
	}

	// If there is no leaf page to the right, set end of table flag and return
	if aPage.LeafNode.Header.NextLeaf == 0 {
		c.EndOfTable = true
		return aRow, nil
	}

	// Otherwise, we try to move the cursor to the next leaf page
	c.PageIdx = aPage.LeafNode.Header.NextLeaf
	c.CellIdx = 0

	return aRow, nil
}

func (c *Cursor) update(ctx context.Context, aRow *Row) error {
	aPage, err := c.Table.pager.GetPage(ctx, c.Table, c.PageIdx)
	if err != nil {
		return err
	}

	rowBuf, err := aRow.Marshal()
	if err != nil {
		return err
	}

	cell := &aPage.LeafNode.Cells[c.CellIdx]
	copy(cell.Value[:], rowBuf)
	return nil
}

func saveToCell(cell *Cell, key uint64, aRow *Row) error {
	rowBuf, err := aRow.Marshal()
	if err != nil {
		return err
	}
	cell.Key = key
	copy(cell.Value[:], rowBuf)
	return nil
}
