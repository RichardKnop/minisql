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

	if !aPage.HasSpaceForRow(aRow) {
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

	if err := c.saveToCell(ctx, aPage.LeafNode, c.CellIdx, key, aRow); err != nil {
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

	aNewPage.LeafNode = NewLeafNode()
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

		if i == c.CellIdx {
			if err := c.saveToCell(ctx, destPage.LeafNode, cellIdx, key, aRow); err != nil {
				return err
			}
		} else if i > c.CellIdx {
			destPage.LeafNode.Cells[cellIdx] = aSplitPage.LeafNode.Cells[i-1]
		} else {
			destPage.LeafNode.Cells[cellIdx] = aSplitPage.LeafNode.Cells[i]
		}
	}

	// Update cell count on both leaf nodes
	aSplitPage.LeafNode.Header.Cells = leftSplitCount
	aNewPage.LeafNode.Header.Cells = rightSplitCount

	if aSplitPage.LeafNode.Header.IsRoot {
		_, err := c.Table.createNewRoot(ctx, aNewPage.Index)
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

	if err := aRow.Unmarshal(aPage.LeafNode.Cells[c.CellIdx]); err != nil {
		return Row{}, err
	}
	aRow.Key = aPage.LeafNode.Cells[c.CellIdx].Key

	if err := unwrapTextPointers(ctx, c.Table.pager, &aRow); err != nil {
		return Row{}, fmt.Errorf("unwrap text pointers: %w", err)
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

func (c *Cursor) saveToCell(ctx context.Context, aNode *LeafNode, cellIdx uint32, key uint64, aRow *Row) error {
	if err := wrapTextValues(ctx, c.Table.pager, aRow); err != nil {
		return fmt.Errorf("wrap text columns in pointers: %w", err)
	}

	rowBuf, err := aRow.Marshal()
	if err != nil {
		return fmt.Errorf("save to cell: %w", err)
	}

	// When splitting nodes, we will be iterating over all cells from last cell to first,
	// and assigning them to either the original node or the new node. Thus, we expand the
	// cell slice here as it will be filled from end to start.
	if cellIdx >= uint32(len(aNode.Cells)) {
		for i := uint32(len(aNode.Cells)); i <= cellIdx; i++ {
			aNode.Cells = append(aNode.Cells, Cell{})
		}
	}

	aCell := &aNode.Cells[cellIdx]
	aCell.NullBitmask = aRow.NullBitmask()
	aCell.Key = key
	aCell.Value = rowBuf

	return nil
}

func (c *Cursor) update(ctx context.Context, stmt Statement, aRow *Row) (bool, error) {
	var (
		oldRow        = aRow.Clone()
		oldPkValue, _ = oldRow.GetValue(c.Table.PrimaryKey.Column.Name)
		changedValues = map[string]struct{}{}
	)
	for name, value := range stmt.Updates {
		found, changed := aRow.SetValue(name, value)
		if found && changed {
			changedValues[name] = struct{}{}
		}
	}

	if len(changedValues) == 0 {
		// No changes
		return false, nil
	}

	aPage, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return false, fmt.Errorf("update: %w", err)
	}

	// In case the new row is larger than available space (after removing old row),
	// we need to delete the old row and re-insert the new row. This will likely cause
	// a split, but that's better than trying to move rows around in the page. Since we
	// use internal row IDs as keys, we will reinsert to the same page.
	if aRow.Size() > aPage.AvailableSpace()-oldRow.Size() {
		// Delete the row
		if err := c.delete(ctx); err != nil {
			return false, fmt.Errorf("update delete old row: %w", err)
		}
		// Reinsert primary key if applicable
		if c.Table.HasPrimaryKey() {
			if c.Table.PrimaryKey.Index == nil {
				return false, fmt.Errorf("table %s has primary key but no index", c.Table.Name)
			}

			pkValue, ok := stmt.Updates[c.Table.PrimaryKey.Name]
			if !ok {
				return false, fmt.Errorf("failed to get value for primary key %s", c.Table.PrimaryKey.Name)
			}

			_, err := c.Table.insertPrimaryKey(ctx, pkValue, aRow.Key)
			if err != nil {
				return false, err
			}
		}
		// Reinsert with the same row ID
		if err := c.LeafNodeInsert(ctx, aRow.Key, aRow); err != nil {
			return false, fmt.Errorf("update re-insert new row: %w", err)
		}
		return true, nil
	}

	if err := wrapTextValues(ctx, c.Table.pager, aRow); err != nil {
		return false, fmt.Errorf("wrap text columns in pointers: %w", err)
	}

	if c.Table.HasPrimaryKey() {
		// Only update primary key if it has changed
		_, ok := changedValues[c.Table.PrimaryKey.Column.Name]
		if ok {
			if err := c.Table.updatePrimaryKey(ctx, oldPkValue, aRow); err != nil {
				return false, err
			}
		}
	}

	cell := &aPage.LeafNode.Cells[c.CellIdx]

	rowBuf, err := aRow.Marshal()
	if err != nil {
		return false, fmt.Errorf("update: %w", err)
	}

	cell.NullBitmask = aRow.NullBitmask()
	copy(cell.Value[:], rowBuf)

	return true, nil
}

func (c *Cursor) delete(ctx context.Context) error {
	aPage, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	key := aPage.LeafNode.Cells[c.CellIdx].Key

	if err := c.deletePrimaryKey(ctx); err != nil {
		return fmt.Errorf("delete primary key: %w", err)
	}

	if err := c.Table.DeleteKey(ctx, c.PageIdx, key); err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	return nil
}

func (c *Cursor) deletePrimaryKey(ctx context.Context) error {
	if !c.Table.HasPrimaryKey() {
		return nil
	}

	aRow, err := c.fetchRow(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch row: %w", err)
	}
	primaryKeyValue, ok := aRow.GetValue(c.Table.PrimaryKey.Column.Name)
	if !ok {
		return fmt.Errorf("primary key %s not found in row", c.Table.PrimaryKey.Name)
	}
	if err := c.Table.PrimaryKey.Index.Delete(ctx, primaryKeyValue.Value); err != nil {
		return fmt.Errorf("failed to delete primary key %s: %w", c.Table.PrimaryKey.Name, err)
	}

	return nil
}
