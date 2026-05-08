package minisql

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// Cursor is a position within a table's B+ tree used to traverse or modify rows.
type Cursor struct {
	Table      *Table
	PageIdx    PageIndex
	CellIdx    uint32
	EndOfTable bool
}

// LeafNodeInsert inserts a row at the cursor's current position, splitting the leaf node if necessary.
func (c *Cursor) LeafNodeInsert(ctx context.Context, key RowID, row Row) error {
	page, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}
	if page.LeafNode == nil {
		return fmt.Errorf("error inserting row to a non leaf node, key %d", key)
	}

	if !page.LeafNode.HasSpaceForRow(row) {
		// Split leaf node
		if err := c.LeafNodeSplitInsert(ctx, key, row); err != nil {
			return fmt.Errorf("leaf node split insert: %w", err)
		}
		return nil
	}

	if c.CellIdx < page.LeafNode.Header.Cells {
		// Need make room for new cell
		for i := page.LeafNode.Header.Cells; i > c.CellIdx; i-- {
			page.LeafNode.PrepareModifyCell(i - 1)
			page.LeafNode.Cells[i] = page.LeafNode.Cells[i-1]
		}
	}

	if err := c.saveToCell(ctx, page.LeafNode, c.CellIdx, key, row); err != nil {
		return err
	}
	page.LeafNode.Header.Cells += 1

	return nil
}

// LeafNodeSplitInsert creates a new leaf node, moves half the cells over, inserts the new value,
// and updates or creates a parent internal node.
func (c *Cursor) LeafNodeSplitInsert(ctx context.Context, key RowID, row Row) error {
	pager := c.Table.pager

	splitPage, err := pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return fmt.Errorf("get page: %w", err)
	}

	originalMaxKey, err := c.Table.GetMaxKey(ctx, splitPage)
	if err != nil {
		return fmt.Errorf("get original max key: %w", err)
	}
	// Use recycled page if available, otherwise create new one
	newPage, err := pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}

	c.Table.logger.Debug("leaf node split insert",
		zap.Int("key", int(key)),
		zap.Int("old_max_key", int(originalMaxKey)),
		zap.Int("new_page_index", int(newPage.Index)),
	)

	newPage.LeafNode = NewLeafNode()
	newPage.LeafNode.Header.Parent = splitPage.LeafNode.Header.Parent

	newPage.LeafNode.Header.NextLeaf = splitPage.LeafNode.Header.NextLeaf
	splitPage.LeafNode.Header.NextLeaf = newPage.Index

	// Keep the rightmost-page hint current: if the new page is the last leaf
	// (NextLeaf == 0), it becomes the new rightmost page for future inserts.
	if newPage.LeafNode.Header.NextLeaf == 0 {
		c.Table.rightmostTablePage.Store(int64(newPage.Index))
	}

	var (
		leafNodeMaxCells = uint32(splitPage.LeafNode.Header.Cells)
		rightSplitCount  uint32
		leftSplitCount   uint32
	)

	if key > originalMaxKey {
		// Biased split: new key is the greatest; pack all existing cells on the left page
		// and place only the new key on the right page. Table RowIDs are engine-managed
		// and strictly monotone-increasing, so this is always safe for table row storage.
		leftSplitCount = leafNodeMaxCells
		rightSplitCount = 1
		if err := c.saveToCell(ctx, newPage.LeafNode, 0, key, row); err != nil {
			return err
		}
	} else {
		// Even split: divide existing keys plus new key evenly between left and right nodes.
		// Starting from the right, move each key to the correct position.
		rightSplitCount = (leafNodeMaxCells + 1) / 2
		leftSplitCount = leafNodeMaxCells + 1 - rightSplitCount
		// Pre-allocate right-page cells so the copy loop can assign directly by index
		// without relying on saveToCell being called first.
		for uint32(len(newPage.LeafNode.Cells)) < rightSplitCount {
			newPage.LeafNode.Cells = append(newPage.LeafNode.Cells, Cell{})
		}
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
				destPage = splitPage // left
			}
			cellIdx := i % leftSplitCount

			switch {
			case i == c.CellIdx:
				if err := c.saveToCell(ctx, destPage.LeafNode, cellIdx, key, row); err != nil {
					return err
				}
			case i > c.CellIdx:
				splitPage.LeafNode.PrepareModifyCell(i - 1)
				destPage.LeafNode.Cells[cellIdx] = splitPage.LeafNode.Cells[i-1]
			default:
				splitPage.LeafNode.PrepareModifyCell(i)
				destPage.LeafNode.Cells[cellIdx] = splitPage.LeafNode.Cells[i]
			}
		}
	}

	// Update cell count on both leaf nodes
	splitPage.LeafNode.Header.Cells = leftSplitCount
	newPage.LeafNode.Header.Cells = rightSplitCount

	if splitPage.LeafNode.Header.IsRoot {
		_, err := c.Table.createNewRoot(ctx, newPage.Index)
		return err
	}

	parentPageIdx := splitPage.LeafNode.Header.Parent
	parentPage, err := pager.ModifyPage(ctx, parentPageIdx)
	if err != nil {
		return fmt.Errorf("get parent page %w", err)
	}

	// If we won't need to split the internal node,
	// update parent to reflect new max key
	oldChildIdx := parentPage.InternalNode.IndexOfChild(originalMaxKey)
	if int(oldChildIdx) < c.Table.maxICells(splitPage.Index) {
		oldPageNewMaxKey, err := c.Table.GetMaxKey(ctx, splitPage)
		if err != nil {
			return fmt.Errorf("get old page max key %w", err)
		}
		parentPage.InternalNode.ICells[oldChildIdx].Key = oldPageNewMaxKey
	}

	return c.Table.InternalNodeInsert(ctx, parentPageIdx, newPage.Index)
}

func (c *Cursor) fetchRow(ctx context.Context, advance bool, selectedFields ...Field) (Row, error) {
	return c.fetchRowWithMask(ctx, advance, selectedColumnsMask(c.Table.Columns, selectedFields))
}

func (c *Cursor) fetchRowWithMask(ctx context.Context, advance bool, selectedMask []bool) (Row, error) {
	page, err := c.Table.pager.ReadPage(ctx, c.PageIdx)
	if err != nil {
		return Row{}, fmt.Errorf("read page: %w", err)
	}

	row := c.Table.newRow()
	if c.CellIdx > page.LeafNode.Header.Cells-1 || len(page.LeafNode.Cells) == 0 {
		return Row{}, fmt.Errorf("cell index %d out of bounds, max %d", c.CellIdx, page.LeafNode.Header.Cells-1)
	}
	row, err = row.UnmarshalWithMask(page.LeafNode.Cells[c.CellIdx], selectedMask)
	if err != nil {
		return Row{}, err
	}
	row.Key = page.LeafNode.Cells[c.CellIdx].Key

	row, err = row.readOverflowTexts(ctx, c.Table.pager)
	if err != nil {
		return Row{}, fmt.Errorf("read overflow texts: %w", err)
	}

	if !advance {
		return row, nil
	}

	// There are still more cells in the page, move cursor to next cell and return
	if c.CellIdx < page.LeafNode.Header.Cells-1 {
		c.CellIdx += 1
		return row, nil
	}

	// If there is no leaf page to the right, set end of table flag and return
	if page.LeafNode.Header.NextLeaf == 0 {
		c.EndOfTable = true
		return row, nil
	}

	c.PageIdx = page.LeafNode.Header.NextLeaf
	c.CellIdx = 0

	return row, nil
}

func (c *Cursor) saveToCell(ctx context.Context, node *LeafNode, cellIdx uint32, key RowID, row Row) error {
	var err error
	row, err = row.storeOverflowTexts(ctx, c.Table.pager)
	if err != nil {
		return fmt.Errorf("store overflow texts: %w", err)
	}

	rowBuf, err := row.Marshal()
	if err != nil {
		return fmt.Errorf("save to cell: %w", err)
	}

	// When splitting nodes, we will be iterating over all cells from last cell to first,
	// and assigning them to either the original node or the new node. Thus, we expand the
	// cell slice here as it will be filled from end to start.
	if cellIdx >= uint32(len(node.Cells)) {
		for i := uint32(len(node.Cells)); i <= cellIdx; i++ {
			node.Cells = append(node.Cells, Cell{})
		}
	}

	node.PrepareModifyCell(cellIdx)
	cell := &node.Cells[cellIdx]
	cell.NullBitmask = row.NullBitmask()
	cell.Key = key
	cell.Value = rowBuf

	return nil
}

func (c *Cursor) update(ctx context.Context, stmt Statement, row Row) (bool, error) {
	var (
		oldRow        = row.Clone()
		changedValues = map[string]Column{}
	)
	for name, value := range stmt.Updates {
		// Evaluate arithmetic expressions against the current row before applying.
		if expr, ok := value.Value.(*Expr); ok {
			result, err := expr.Eval(row)
			if err != nil {
				return false, fmt.Errorf("evaluating expression for column %q: %w", name, err)
			}
			if result == nil {
				value = OptionalValue{Valid: false}
			} else {
				value = OptionalValue{Value: result, Valid: true}
			}
		}
		col, idx := row.GetColumn(name)
		if idx < 0 {
			return false, fmt.Errorf("column '%s' not found", name)
		}
		var changed bool
		row, changed = row.SetValue(name, value)
		if changed {
			changedValues[name] = col
		}
	}

	if len(changedValues) == 0 {
		// No changes
		return false, nil
	}

	if err := validateCheckConstraints(c.Table.Columns, row); err != nil {
		return false, err
	}

	// Enforce parent FK on UPDATE: only when a referenced column is being changed.
	if c.Table.enforceParentFKOnUpdate != nil {
		for colName := range changedValues {
			if c.Table.referencedColumns[colName] {
				if err := c.Table.enforceParentFKOnUpdate(ctx, oldRow, row); err != nil {
					return false, err
				}
				break
			}
		}
	}

	// Check child FK: only when an outgoing-FK column is being updated.
	if c.Table.checkChildFK != nil {
		for colName := range changedValues {
			if c.Table.fkColumnSet[colName] {
				if err := c.Table.checkChildFK(ctx, row); err != nil {
					return false, err
				}
				break
			}
		}
	}

	page, err := c.Table.pager.ModifyPage(ctx, c.PageIdx)
	if err != nil {
		return false, fmt.Errorf("update: %w", err)
	}

	// In case the new row is larger than available space (after removing old row),
	// we need to delete the old row and re-insert the new row. This will likely cause
	// a split, but that's better than trying to move rows around in the page. Since we
	// use internal row IDs as keys, we will reinsert to the same page.
	if row.Size() > page.LeafNode.AvailableSpace()+oldRow.Size() {
		// Delete the row
		if err := c.delete(ctx, oldRow); err != nil {
			return false, fmt.Errorf("update delete old row: %w", err)
		}
		// Reinsert primary key if applicable
		if c.Table.HasPrimaryKey() {
			pkValues := make([]OptionalValue, 0, len(c.Table.PrimaryKey.Columns))
			for _, col := range c.Table.PrimaryKey.Columns {
				pkValue, ok := stmt.Updates[col.Name]
				if !ok {
					// Column not being updated — use the existing row value.
					pkValue, _ = oldRow.GetValue(col.Name)
				}
				pkValues = append(pkValues, pkValue)
			}

			_, err := c.Table.insertPrimaryKey(ctx, pkValues, row.Key)
			if err != nil {
				return false, err
			}
		}
		// Resinsert unique keys if applicable
		for _, uniqueIndex := range c.Table.UniqueIndexes {
			indexValues := make([]OptionalValue, 0, len(uniqueIndex.Columns))
			for _, col := range uniqueIndex.Columns {
				indexValue, ok := stmt.Updates[col.Name]
				if !ok {
					// Column not being updated — use the existing row value.
					indexValue, _ = oldRow.GetValue(col.Name)
				}
				indexValues = append(indexValues, indexValue)
			}

			if err := c.Table.insertUniqueIndexKey(ctx, uniqueIndex, indexValues, row.Key); err != nil {
				return false, err
			}
		}
		// Reinsert with the same row ID
		if err := c.LeafNodeInsert(ctx, row.Key, row); err != nil {
			return false, fmt.Errorf("update re-insert new row: %w", err)
		}
		// Resinsert secondary keys if applicable
		for _, secondaryIndex := range c.Table.SecondaryIndexes {
			indexValues := make([]OptionalValue, 0, len(secondaryIndex.Columns))
			for _, col := range secondaryIndex.Columns {
				indexValue, ok := stmt.Updates[col.Name]
				if !ok {
					// Column not being updated — use the existing row value.
					indexValue, _ = oldRow.GetValue(col.Name)
				}
				indexValues = append(indexValues, indexValue)
			}

			if err := c.Table.insertSecondaryIndexKey(ctx, secondaryIndex, indexValues, row.Key); err != nil {
				return false, err
			}
		}
		return true, nil
	}

	// Remove any overflow pages
	if overflowColumns := textOverflowColumns(c.Table.Columns...); len(overflowColumns) > 0 {
		// TODO - a more efficient implementation would be to try to reuse existing overflow pages
		// if possible. For example if text size didn't change much and fits into existing overflow pages.
		changedColumns := make([]Column, 0, len(changedValues))
		for _, col := range overflowColumns {
			_, ok := changedValues[col.Name]
			if !ok {
				continue
			}
			changedColumns = append(changedColumns, col)
		}
		if err := c.Table.freeOverflowPages(ctx, oldRow, changedColumns...); err != nil {
			return false, err
		}
	}

	row, err = row.storeOverflowTexts(ctx, c.Table.pager)
	if err != nil {
		return false, fmt.Errorf("store overflow texts: %w", err)
	}

	if c.Table.HasPrimaryKey() {
		// Only update primary key if it has changed
		var changed bool
		for _, col := range c.Table.PrimaryKey.Columns {
			if _, ok := changedValues[col.Name]; ok {
				changed = true
				break
			}
		}
		if changed {
			oldIndexKeys, ok := oldRow.GetValuesForColumns(c.Table.PrimaryKey.Columns)
			if !ok || len(oldIndexKeys) != len(c.Table.PrimaryKey.Columns) {
				return false, fmt.Errorf("failed to get old value for primary key %s", c.Table.PrimaryKey.Name)
			}
			if err := c.Table.updatePrimaryKey(ctx, oldIndexKeys, row); err != nil {
				return false, err
			}
		}
	}
	for _, uniqueIndex := range c.Table.UniqueIndexes {
		// Only update unique index key if it has changed
		var changed bool
		for _, col := range uniqueIndex.Columns {
			if _, ok := changedValues[col.Name]; ok {
				changed = true
				break
			}
		}
		if changed {
			oldIndexKeys, ok := oldRow.GetValuesForColumns(uniqueIndex.Columns)
			if !ok || len(oldIndexKeys) != len(uniqueIndex.Columns) {
				return false, fmt.Errorf("failed to get old value for unique index %s", uniqueIndex.Name)
			}
			if err := c.Table.updateUniqueIndexKey(ctx, uniqueIndex, oldIndexKeys, row); err != nil {
				return false, err
			}
		}
	}
	for _, secondaryIndex := range c.Table.SecondaryIndexes {
		// Only update secondary index key if it has changed
		var changed bool
		for _, col := range secondaryIndex.Columns {
			if _, ok := changedValues[col.Name]; ok {
				changed = true
				break
			}
		}
		if changed {
			oldIndexKeys, ok := oldRow.GetValuesForColumns(secondaryIndex.Columns)
			if !ok || len(oldIndexKeys) != len(secondaryIndex.Columns) {
				return false, fmt.Errorf("failed to get old value for secondary index %s", secondaryIndex.Name)
			}
			if err := c.Table.updateSecondaryIndexKey(ctx, secondaryIndex, oldIndexKeys, row); err != nil {
				return false, err
			}
		}
	}

	page.LeafNode.PrepareModifyCell(c.CellIdx)
	cell := &page.LeafNode.Cells[c.CellIdx]

	rowBuf, err := row.Marshal()
	if err != nil {
		return false, fmt.Errorf("update: %w", err)
	}

	cell.NullBitmask = row.NullBitmask()
	cell.Value = rowBuf

	return true, nil
}

func (c *Cursor) delete(ctx context.Context, row Row) error {
	if err := c.deletePrimaryKey(ctx, row); err != nil {
		return fmt.Errorf("delete primary key: %w", err)
	}

	if err := c.deleteUniqueIndexKeys(ctx, row); err != nil {
		return fmt.Errorf("delete unique index keys: %w", err)
	}

	if err := c.deleteSecondaryIndexKeys(ctx, row); err != nil {
		return fmt.Errorf("delete secondary index keys: %w", err)
	}

	if err := c.Table.DeleteKey(ctx, c.PageIdx, row.Key); err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	return nil
}

func (c *Cursor) deletePrimaryKey(ctx context.Context, row Row) error {
	if !c.Table.HasPrimaryKey() {
		return nil
	}

	if len(c.Table.PrimaryKey.Columns) > 1 {
		return c.deleteCompositePrimaryKey(ctx, row)
	}

	primaryKeyValue, ok := row.GetValue(c.Table.PrimaryKey.Columns[0].Name)
	if !ok {
		return fmt.Errorf("primary key %s not found in row", c.Table.PrimaryKey.Name)
	}

	castedValue, err := castKeyValue(c.Table.PrimaryKey.Columns[0], primaryKeyValue.Value)
	if err != nil {
		return fmt.Errorf("failed to cast key value for primary key %s: %w", c.Table.PrimaryKey.Name, err)
	}

	if err := c.Table.PrimaryKey.Index.Delete(ctx, castedValue, row.Key); err != nil {
		return fmt.Errorf("failed to delete primary key %s: %w", c.Table.PrimaryKey.Name, err)
	}

	return nil
}

func (c *Cursor) deleteCompositePrimaryKey(ctx context.Context, row Row) error {
	keyParts, ok := row.GetValuesForColumns(c.Table.PrimaryKey.Columns)
	if !ok || len(keyParts) != len(c.Table.PrimaryKey.Columns) {
		return fmt.Errorf("failed to get value for primary key %s", c.Table.PrimaryKey.Name)
	}

	keyValues := make([]any, 0, len(keyParts))
	for i, keyPart := range keyParts {
		if !keyPart.Valid {
			return fmt.Errorf("failed to get value for comnposite primary key %s", c.Table.PrimaryKey.Name)
		}
		castedKey, err := castKeyValue(c.Table.PrimaryKey.Columns[i], keyPart.Value)
		if err != nil {
			return fmt.Errorf("failed to cast old unique index value for %s: %w", c.Table.PrimaryKey.Name, err)
		}
		keyValues = append(keyValues, castedKey)
	}

	ck := NewCompositeKey(c.Table.PrimaryKey.Columns[0:len(keyValues)], keyValues...)
	if err := c.Table.PrimaryKey.Index.Delete(ctx, ck, row.Key); err != nil {
		return fmt.Errorf("failed to delete primary key %s: %w", c.Table.PrimaryKey.Name, err)
	}
	return nil
}

func (c *Cursor) deleteUniqueIndexKeys(ctx context.Context, row Row) error {
	if len(c.Table.UniqueIndexes) == 0 {
		return nil
	}

	for _, uniqueIndex := range c.Table.UniqueIndexes {
		if len(uniqueIndex.Columns) > 1 {
			if err := c.deleteCompositeUniqueIndexKey(ctx, row, uniqueIndex); err != nil {
				return err
			}
			continue
		}

		indexValue, ok := row.GetValue(uniqueIndex.Columns[0].Name)
		if !ok {
			return fmt.Errorf("unique index key %s not found in row", uniqueIndex.Name)
		}

		castedValue, err := castKeyValue(uniqueIndex.Columns[0], indexValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast key value for unique index %s: %w", uniqueIndex.Name, err)
		}

		if err := uniqueIndex.Index.Delete(ctx, castedValue, row.Key); err != nil {
			return fmt.Errorf("failed to delete unique index key %s: %w", uniqueIndex.Name, err)
		}
	}

	return nil
}

func (c *Cursor) deleteCompositeUniqueIndexKey(ctx context.Context, row Row, uniqueIndex UniqueIndex) error {
	keyParts, ok := row.GetValuesForColumns(uniqueIndex.Columns)
	if !ok || len(keyParts) != len(uniqueIndex.Columns) {
		return fmt.Errorf("failed to get value for unique index key %s", uniqueIndex.Name)
	}

	keyValues := make([]any, 0, len(keyParts))
	for i, keyPart := range keyParts {
		if !keyPart.Valid {
			// No need to delete as key should not be in the index (all columns are not non-NULL)
			return nil
		}
		castedKey, err := castKeyValue(uniqueIndex.Columns[i], keyPart.Value)
		if err != nil {
			return fmt.Errorf("failed to cast unique index value for %s: %w", uniqueIndex.Name, err)
		}
		keyValues = append(keyValues, castedKey)
	}

	ck := NewCompositeKey(uniqueIndex.Columns, keyValues...)
	if err := uniqueIndex.Index.Delete(ctx, ck, row.Key); err != nil {
		return fmt.Errorf("failed to delete unique index key %s: %w", uniqueIndex.Name, err)
	}

	return nil
}

func (c *Cursor) deleteSecondaryIndexKeys(ctx context.Context, row Row) error {
	if len(c.Table.SecondaryIndexes) == 0 {
		return nil
	}

	for _, secondaryIndex := range c.Table.SecondaryIndexes {
		if len(secondaryIndex.Columns) > 1 {
			if err := c.deleteCompositeSecondaryIndexKey(ctx, row, secondaryIndex); err != nil {
				return err
			}
			continue
		}

		indexValue, ok := row.GetValue(secondaryIndex.Columns[0].Name)
		if !ok {
			return fmt.Errorf("unique index key %s not found in row", secondaryIndex.Name)
		}

		castedValue, err := castKeyValue(secondaryIndex.Columns[0], indexValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast key value for secondary index %s: %w", secondaryIndex.Name, err)
		}

		if err := secondaryIndex.Index.Delete(ctx, castedValue, row.Key); err != nil {
			return fmt.Errorf("failed to delete secondary index key %s: %w", secondaryIndex.Name, err)
		}
	}

	return nil
}

func (c *Cursor) deleteCompositeSecondaryIndexKey(ctx context.Context, row Row, secondaryIndex SecondaryIndex) error {
	keyParts, ok := row.GetValuesForColumns(secondaryIndex.Columns)
	if !ok || len(keyParts) != len(secondaryIndex.Columns) {
		return fmt.Errorf("failed to get value for secondary index key %s", secondaryIndex.Name)
	}

	keyValues := make([]any, 0, len(keyParts))
	for i, keyPart := range keyParts {
		if !keyPart.Valid {
			// No need to delete as key should not be in the index (all columns are not non-NULL)
			return nil
		}
		castedKey, err := castKeyValue(secondaryIndex.Columns[i], keyPart.Value)
		if err != nil {
			return fmt.Errorf("failed to cast secondary index value for %s: %w", secondaryIndex.Name, err)
		}
		keyValues = append(keyValues, castedKey)
	}

	ck := NewCompositeKey(secondaryIndex.Columns, keyValues...)
	if err := secondaryIndex.Index.Delete(ctx, ck, row.Key); err != nil {
		return fmt.Errorf("failed to delete secondary index key %s: %w", secondaryIndex.Name, err)
	}

	return nil
}
