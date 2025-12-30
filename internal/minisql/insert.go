package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Insert(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if stmt.Kind != Insert {
		return StatementResult{}, fmt.Errorf("invalid statement kind for INSERT: %v", stmt.Kind)
	}

	aCursor, nextRowID, err := t.SeekNextRowID(ctx, t.GetRootPageIdx())
	if err != nil {
		return StatementResult{}, err
	}

	for insertIdx, values := range stmt.Inserts {
		if t.HasPrimaryKey() {
			if t.PrimaryKey.Index == nil {
				return StatementResult{}, fmt.Errorf("table %s has primary key but no Btree index instance", t.Name)
			}

			pkValue, ok := stmt.InsertValueForColumn(t.PrimaryKey.Columns[0].Name, insertIdx)
			if !ok {
				return StatementResult{}, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
			}

			insertedPrimaryKey, err := t.insertPrimaryKey(ctx, pkValue, nextRowID)
			if err != nil {
				return StatementResult{}, err
			}

			// Update statement with autoincremented primary key value
			pkIdx := stmt.ColumnIdx(t.PrimaryKey.Columns[0].Name)
			values[pkIdx] = OptionalValue{Value: insertedPrimaryKey, Valid: true}
		}

		for _, uniqueIndex := range t.UniqueIndexes {
			if uniqueIndex.Index == nil {
				return StatementResult{}, fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
			}

			indexValue, ok := stmt.InsertValueForColumn(uniqueIndex.Columns[0].Name, insertIdx)
			if !ok {
				return StatementResult{}, fmt.Errorf("failed to get value for unique index %s", uniqueIndex.Name)
			}

			if err := t.insertUniqueIndexKey(ctx, uniqueIndex, indexValue, nextRowID); err != nil {
				return StatementResult{}, err
			}
		}

		for _, secondaryIndex := range t.SecondaryIndexes {
			if secondaryIndex.Index == nil {
				return StatementResult{}, fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
			}

			indexValue, ok := stmt.InsertValueForColumn(secondaryIndex.Columns[0].Name, insertIdx)
			if !ok {
				return StatementResult{}, fmt.Errorf("failed to get value for secondary index %s", secondaryIndex.Name)
			}

			if err := t.insertSecondaryIndexKey(ctx, secondaryIndex, indexValue, nextRowID); err != nil {
				return StatementResult{}, err
			}
		}

		aRow := NewRow(t.Columns)
		aRow = aRow.AppendValues(stmt.Fields, values)

		aPage, err := t.pager.ModifyPage(ctx, aCursor.PageIdx)
		if err != nil {
			return StatementResult{}, fmt.Errorf("insert: %w", err)
		}

		// Must be leaf node
		if aPage.LeafNode == nil {
			return StatementResult{}, fmt.Errorf("trying to insert into non leaf node")
		}

		t.logger.Sugar().With(
			"page_index", int(aCursor.PageIdx),
			"cell_index", int(aCursor.CellIdx),
			"row_id", int(nextRowID),
		).Debug("inserting row")

		if aCursor.CellIdx < aPage.LeafNode.Header.Cells {
			if aPage.LeafNode.Cells[aCursor.CellIdx].Key == nextRowID {
				return StatementResult{}, fmt.Errorf("duplicate key %d", nextRowID)
			}
		}

		if err := aCursor.LeafNodeInsert(ctx, nextRowID, aRow); err != nil {
			return StatementResult{}, err
		}

		if insertIdx == len(stmt.Inserts)-1 {
			break
		}

		// Try to advance cursor to next position, if there is still space in the
		// current page, just increment cell index, otherwise call Seek to get
		// new cursor
		aCursor, nextRowID, err = t.SeekNextRowID(ctx, t.GetRootPageIdx())
		if err != nil {
			return StatementResult{}, err
		}
	}

	// TODO - set LastInsertId
	return StatementResult{RowsAffected: len(stmt.Inserts)}, nil
}
