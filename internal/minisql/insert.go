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

	rowsInserted := 0
	for insertIdx, values := range stmt.Inserts {
		if stmt.ConflictAction == ConflictActionDoNothing {
			conflict, err := t.hasInsertConflict(ctx, stmt, insertIdx)
			if err != nil {
				return StatementResult{}, err
			}
			if conflict {
				continue
			}
		}

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

		rowsInserted++

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
	return StatementResult{RowsAffected: rowsInserted}, nil
}

// hasInsertConflict returns true if inserting the row at insertIdx would violate
// a primary key or unique index constraint.
func (t *Table) hasInsertConflict(ctx context.Context, stmt Statement, insertIdx int) (bool, error) {
	if t.HasPrimaryKey() && t.PrimaryKey.Index != nil {
		keyParts := stmt.InsertValuesForColumns(insertIdx, t.PrimaryKey.Columns...)
		if len(keyParts) == len(t.PrimaryKey.Columns) {
			key, err := buildIndexLookupKey(t.PrimaryKey.Columns, keyParts)
			if err != nil {
				return false, err
			}
			// key is nil when any part is NULL; for autoincrement that means the
			// value will be auto-generated and cannot conflict — skip the check.
			if key != nil {
				rowIDs, err := t.PrimaryKey.Index.FindRowIDs(ctx, key)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return false, err
				}
				if len(rowIDs) > 0 {
					return true, nil
				}
			}
		}
	}

	for _, uniqueIndex := range t.UniqueIndexes {
		if uniqueIndex.Index == nil {
			continue
		}
		keyParts := stmt.InsertValuesForColumns(insertIdx, uniqueIndex.Columns...)
		if len(keyParts) != len(uniqueIndex.Columns) {
			continue
		}
		key, err := buildIndexLookupKey(uniqueIndex.Columns, keyParts)
		if err != nil {
			return false, err
		}
		if key == nil {
			// NULL values don't participate in unique constraint checks
			continue
		}
		rowIDs, err := uniqueIndex.Index.FindRowIDs(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, err
		}
		if len(rowIDs) > 0 {
			return true, nil
		}
	}

	return false, nil
}

// buildIndexLookupKey builds the key value used to probe a BTree index.
// Returns nil if any key part is NULL (NULL values are not indexed).
func buildIndexLookupKey(columns []Column, keyParts []OptionalValue) (any, error) {
	for _, kp := range keyParts {
		if !kp.Valid {
			return nil, nil
		}
	}
	if len(columns) == 1 {
		return castKeyValue(columns[0], keyParts[0].Value)
	}
	keyValues := make([]any, 0, len(keyParts))
	for i, kp := range keyParts {
		castedKey, err := castKeyValue(columns[i], kp.Value)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, castedKey)
	}
	return NewCompositeKey(columns, keyValues...), nil
}
