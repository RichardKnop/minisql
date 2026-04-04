package minisql

import (
	"context"
	"errors"
	"fmt"
)

// Insert ...
func (t *Table) Insert(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if stmt.Kind != Insert {
		return StatementResult{}, fmt.Errorf("invalid statement kind for INSERT: %v", stmt.Kind)
	}

	cursor, nextRowID, err := t.SeekNextRowID(ctx, t.GetRootPageIdx())
	if err != nil {
		return StatementResult{}, err
	}

	for insertIdx, values := range stmt.Inserts {
		if t.HasPrimaryKey() {
			keyParts := stmt.InsertValuesForColumns(insertIdx, t.PrimaryKey.Columns...)
			if len(keyParts) != len(t.PrimaryKey.Columns) {
				return StatementResult{}, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
			}

			insertedPrimaryKey, err := t.insertPrimaryKey(ctx, keyParts, nextRowID)
			if err != nil {
				return StatementResult{}, err
			}

			if len(t.PrimaryKey.Columns) == 1 {
				// Update statement with autoincremented primary key value
				pkIdx := stmt.ColumnIdx(t.PrimaryKey.Columns[0].Name)
				values[pkIdx] = OptionalValue{Value: insertedPrimaryKey, Valid: true}
			}
		}

		for _, uniqueIndex := range t.UniqueIndexes {
			keyParts := stmt.InsertValuesForColumns(insertIdx, uniqueIndex.Columns...)
			if len(keyParts) != len(uniqueIndex.Columns) {
				return StatementResult{}, fmt.Errorf("failed to get value for unique index %s", uniqueIndex.Name)
			}

			if err := t.insertUniqueIndexKey(ctx, uniqueIndex, keyParts, nextRowID); err != nil {
				return StatementResult{}, err
			}
		}

		for _, secondaryIndex := range t.SecondaryIndexes {
			keyParts := stmt.InsertValuesForColumns(insertIdx, secondaryIndex.Columns...)
			if len(keyParts) != len(secondaryIndex.Columns) {
				return StatementResult{}, fmt.Errorf("failed to get value for secondary index %s", secondaryIndex.Name)
			}

			if err := t.insertSecondaryIndexKey(ctx, secondaryIndex, keyParts, nextRowID); err != nil {
				return StatementResult{}, err
			}
		}

		row := NewRow(t.Columns)
		row = row.AppendValues(stmt.Fields, values)

		page, err := t.pager.ModifyPage(ctx, cursor.PageIdx)
		if err != nil {
			return StatementResult{}, fmt.Errorf("insert: %w", err)
		}

		// Must be leaf node
		if page.LeafNode == nil {
			return StatementResult{}, errors.New("trying to insert into non leaf node")
		}

		t.logger.Sugar().With(
			"page_index", int(cursor.PageIdx),
			"cell_index", int(cursor.CellIdx),
			"row_id", int(nextRowID),
		).Debug("inserting row")

		if cursor.CellIdx < page.LeafNode.Header.Cells {
			if page.LeafNode.Cells[cursor.CellIdx].Key == nextRowID {
				return StatementResult{}, fmt.Errorf("duplicate key %d", nextRowID)
			}
		}

		if err := cursor.LeafNodeInsert(ctx, nextRowID, row); err != nil {
			return StatementResult{}, err
		}

		if insertIdx == len(stmt.Inserts)-1 {
			break
		}

		// Try to advance cursor to next position, if there is still space in the
		// current page, just increment cell index, otherwise call Seek to get
		// new cursor
		cursor, nextRowID, err = t.SeekNextRowID(ctx, t.GetRootPageIdx())
		if err != nil {
			return StatementResult{}, err
		}
	}

	// TODO - set LastInsertId
	return StatementResult{RowsAffected: len(stmt.Inserts)}, nil
}
