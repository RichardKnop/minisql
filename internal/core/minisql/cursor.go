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
	aPage, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}
	if aPage.LeafNode == nil {
		return fmt.Errorf("error inserting row to a non leaf node, key %d", key)
	}

	maxCells := aRow.MaxCells()
	if aPage.Index == 0 {
		maxCells = aRow.MaxRootCells()
	}
	if aPage.LeafNode.Header.Cells >= maxCells {
		// Split leaf node
		if err := c.LeafNodeSplitInsert(ctx, key, aRow); err != nil {
			return fmt.Errorf("leaf node split insert: %w", err)
		}
		return nil
	}

	if c.CellIdx < aPage.LeafNode.Header.Cells {
		// Need make room for new cell
		for i := aPage.LeafNode.Header.Cells; i > c.CellIdx; i-- {
			aPage.LeafNode.Cells[i] = aPage.LeafNode.Cells[i-1]
		}
	}

	if err := c.saveToCell(&aPage.LeafNode.Cells[c.CellIdx], key, aRow); err != nil {
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

	aSplitPage, err := aPager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}

	originalMaxKey, err := c.Table.GetMaxKey(ctx, aSplitPage)
	if err != nil {
		return fmt.Errorf("get original max key: %w", err)
	}
	// Use recycled page if available, otherwise create new one
	aNewPage, err := aPager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}

	c.Table.logger.Sugar().With(
		"key", int(key),
		"old_max_key", int(originalMaxKey),
		"new_page_index", int(aNewPage.Index),
	).Debug("leaf node split insert")

	aNewPage.LeafNode = NewLeafNode(c.Table.RowSize)
	aNewPage.LeafNode.Header.Parent = aSplitPage.LeafNode.Header.Parent

	aNewPage.LeafNode.Header.NextLeaf = aSplitPage.LeafNode.Header.NextLeaf
	aSplitPage.LeafNode.Header.NextLeaf = aNewPage.Index

	// All existing keys plus new key should should be divided
	// evenly between old (left) and new (right) nodes.
	// Starting from the right, move each key to correct position.
	var (
		leafNodeMaxCells = uint32(aSplitPage.LeafNode.Header.Cells)
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
			if err := c.saveToCell(destCell, key, aRow); err != nil {
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
		_, err := c.Table.CreateNewRoot(ctx, aNewPage.Index)
		return err
	}

	parentPageIdx := aSplitPage.LeafNode.Header.Parent
	aParentPage, err := aPager.ModifyPage(ctx, parentPageIdx)
	if err != nil {
		return fmt.Errorf("get parent page %w", err)
	}

	// If we won't need to split the internal node,
	// update parent to reflect new max key
	oldChildIdx := aParentPage.InternalNode.IndexOfChild(originalMaxKey)
	if int(oldChildIdx) < c.Table.maxICells(aSplitPage.Index) {
		oldPageNewMaxKey, err := c.Table.GetMaxKey(ctx, aSplitPage)
		if err != nil {
			return fmt.Errorf("get old page max key %w", err)
		}
		aParentPage.InternalNode.ICells[oldChildIdx].Key = oldPageNewMaxKey
	}

	return c.Table.InternalNodeInsert(ctx, parentPageIdx, aNewPage.Index)
}

func (c *Cursor) fetchRow(ctx context.Context) (Row, error) {
	aPage, err := c.Table.pager.ReadPage(ctx, c.PageIdx)
	if err != nil {
		return Row{}, fmt.Errorf("fetch row: %w", err)
	}
	aRow := NewRow(c.Table.Columns)

	if err := UnmarshalRow(aPage.LeafNode.Cells[c.CellIdx], &aRow); err != nil {
		return Row{}, err
	}
	aRow.Key = aPage.LeafNode.Cells[c.CellIdx].Key

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

func (c *Cursor) saveToCell(cell *Cell, key uint64, aRow *Row) error {
	rowBuf, err := aRow.Marshal()
	if err != nil {
		return fmt.Errorf("save to cell: %w", err)
	}

	cell.NullBitmask = aRow.NullBitmask()
	cell.Key = key
	copy(cell.Value[:], rowBuf)

	return nil
}

func (c *Cursor) update(ctx context.Context, aRow *Row) error {
	aPage, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	cell := &aPage.LeafNode.Cells[c.CellIdx]

	rowBuf, err := aRow.Marshal()
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	cell.NullBitmask = aRow.NullBitmask()
	copy(cell.Value[:], rowBuf)

	return nil
}

func (c *Cursor) delete(ctx context.Context) error {
	aPage, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	key := aPage.LeafNode.Cells[c.CellIdx].Key
	err = c.Table.DeleteKey(ctx, c.PageIdx, key)

	return err
}
