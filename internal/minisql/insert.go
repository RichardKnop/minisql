package minisql

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
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

	// After prepareInsert, stmt.Fields[i].Name == t.Columns[i].Name for i < len(t.Columns),
	// so stmt.Inserts[insertIdx][i] directly corresponds to t.Columns[i].
	// No map is needed — values[i] is already the value for t.Columns[i].

	rowsInserted := 0
	// newRowsInserted counts only actual new rows added to the table (not DO UPDATE
	// hits, which update existing rows without changing the total count).
	newRowsInserted := 0
	var returningRows []Row
	for insertIdx, values := range stmt.Inserts {
		switch stmt.ConflictAction {
		case ConflictActionDoNothing:
			conflict, err := t.hasInsertConflict(ctx, stmt, insertIdx)
			if err != nil {
				return StatementResult{}, err
			}
			if conflict {
				continue
			}
		case ConflictActionDoUpdate:
			conflict, rowID, err := t.findInsertConflict(ctx, stmt, insertIdx)
			if err != nil {
				return StatementResult{}, err
			}
			if conflict {
				cursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return StatementResult{}, fmt.Errorf("insert on conflict do update seek: %w", err)
				}
				existingRow, err := cursor.fetchRow(ctx, false, fieldsFromColumns(t.Columns...)...)
				if err != nil {
					return StatementResult{}, fmt.Errorf("insert on conflict do update read: %w", err)
				}
				// Resolve any EXCLUDED.col references against the current proposed row.
				resolvedStmt := stmt.resolveExcludedRefs(insertIdx)
				changed, err := cursor.update(ctx, resolvedStmt, existingRow)
				if err != nil {
					return StatementResult{}, fmt.Errorf("insert on conflict do update: %w", err)
				}
				if changed {
					rowsInserted += 1
					// Row count unchanged — DO UPDATE replaces an existing row.
				}
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

		// Assemble row values in table-column order.
		// Fast path: after prepareInsert, values[i] == value for t.Columns[i] — just copy.
		// Slow path: direct Insert call without prepareInsert — use field-name lookup.
		rowValues := make([]OptionalValue, len(t.Columns))
		if len(values) == len(t.Columns) {
			// prepareInsert was called; values are already in column order.
			copy(rowValues, values)
		} else {
			// Direct call without prepareInsert; resolve by field name.
			fieldPositions := make(map[string]int, len(stmt.Fields))
			for i, f := range stmt.Fields {
				fieldPositions[f.Name] = i
			}
			for i, col := range t.Columns {
				if fi, ok := fieldPositions[col.Name]; ok && fi < len(values) {
					rowValues[i] = values[fi]
				}
			}
		}
		row := NewRowWithValues(t.Columns, rowValues)

		if err := validateCheckConstraints(t.Columns, row); err != nil {
			return StatementResult{}, err
		}

		if t.checkChildFK != nil {
			if err := t.checkChildFK(ctx, row); err != nil {
				return StatementResult{}, err
			}
		}

		// Collect the row for RETURNING before it is inserted (values are already
		// final at this point, including any autoincrement PK that was resolved above).
		if len(stmt.ReturningFields) > 0 {
			projected, err := projectReturning(row, stmt.ReturningFields)
			if err != nil {
				return StatementResult{}, err
			}
			returningRows = append(returningRows, projected)
		}

		page, err := t.pager.ModifyPage(ctx, cursor.PageIdx)
		if err != nil {
			return StatementResult{}, fmt.Errorf("insert: %w", err)
		}

		// Must be leaf node
		if page.LeafNode == nil {
			return StatementResult{}, errors.New("trying to insert into non leaf node")
		}

		t.logger.Debug("inserting row",
			zap.Int("page_index", int(cursor.PageIdx)),
			zap.Int("cell_index", int(cursor.CellIdx)),
			zap.Int("row_id", int(nextRowID)),
		)

		if cursor.CellIdx < page.LeafNode.Header.Cells {
			if page.LeafNode.Cells[cursor.CellIdx].Key == nextRowID {
				return StatementResult{}, fmt.Errorf("duplicate key %d", nextRowID)
			}
		}

		if err := cursor.LeafNodeInsert(ctx, nextRowID, row); err != nil {
			return StatementResult{}, err
		}

		rowsInserted += 1
		newRowsInserted += 1

		if insertIdx == len(stmt.Inserts)-1 {
			break
		}

		// Advance the cursor for the next insert without re-traversing the tree.
		//
		// Invariant: before every insert, the cursor points at the rightmost
		// leaf, which always has NextLeaf == 0.  After a split, LeafNodeSplitInsert
		// sets splitPage.NextLeaf = newPage.Index, so NextLeaf becomes non-zero.
		// Reading the page back is a write-set hit (O(1) map lookup) — no I/O.
		afterPage, err := t.pager.ReadPage(ctx, cursor.PageIdx)
		if err != nil {
			return StatementResult{}, fmt.Errorf("insert: read page after insert: %w", err)
		}
		if afterPage.LeafNode != nil && afterPage.LeafNode.Header.NextLeaf == 0 {
			// No split: this page is still the rightmost leaf.
			// Position the cursor past the last cell that was just written.
			cursor.CellIdx = afterPage.LeafNode.Header.Cells
			nextRowID += 1
		} else {
			// A split created a new right sibling — this page is no longer
			// rightmost. Find the actual rightmost leaf via a full seek.
			cursor, nextRowID, err = t.SeekNextRowID(ctx, t.GetRootPageIdx())
			if err != nil {
				return StatementResult{}, err
			}
		}
	}

	// Update the in-memory row-count cache (only for tables that have a getter,
	// i.e. user tables managed by the Database — system tables are excluded).
	if t.getRowCount != nil && newRowsInserted > 0 {
		if tx := TxFromContext(ctx); tx != nil {
			tx.AddRowCountDelta(t.Name, int64(newRowsInserted))
		}
	}

	result := StatementResult{RowsAffected: rowsInserted}
	if len(stmt.ReturningFields) > 0 {
		result.Columns = returningColumns(stmt.ReturningFields, t.Columns)
		result.Rows = NewSliceIterator(returningRows)
	}
	return result, nil
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

// findInsertConflict is like hasInsertConflict but also returns the RowID of the
// conflicting row so the caller can seek and update it.
func (t *Table) findInsertConflict(ctx context.Context, stmt Statement, insertIdx int) (bool, RowID, error) {
	if t.HasPrimaryKey() && t.PrimaryKey.Index != nil {
		keyParts := stmt.InsertValuesForColumns(insertIdx, t.PrimaryKey.Columns...)
		if len(keyParts) == len(t.PrimaryKey.Columns) {
			key, err := buildIndexLookupKey(t.PrimaryKey.Columns, keyParts)
			if err != nil {
				return false, 0, err
			}
			if key != nil {
				rowIDs, err := t.PrimaryKey.Index.FindRowIDs(ctx, key)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return false, 0, err
				}
				if len(rowIDs) > 0 {
					return true, rowIDs[0], nil
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
			return false, 0, err
		}
		if key == nil {
			continue
		}
		rowIDs, err := uniqueIndex.Index.FindRowIDs(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, 0, err
		}
		if len(rowIDs) > 0 {
			return true, rowIDs[0], nil
		}
	}

	return false, 0, nil
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
