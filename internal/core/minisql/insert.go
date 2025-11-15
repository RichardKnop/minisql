package minisql

import (
	"context"
	"fmt"
	"slices"
)

func (t *Table) Insert(ctx context.Context, stmt Statement) error {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	// First, we will make sure to add any nullable columns that are missing from the
	// insert statement, setting them to NULL
	for i, aColumn := range t.Columns {
		if !stmt.HasField(aColumn.Name) {
			stmt.Fields = slices.Insert(stmt.Fields, i, aColumn.Name)
			for j := range stmt.Inserts {
				stmt.Inserts[j] = slices.Insert(stmt.Inserts[j], i, OptionalValue{})
			}
		}
	}

	if err := stmt.Validate(t); err != nil {
		return err
	}

	aCursor, nextRowID, err := t.SeekNextRowID(ctx, t.GetRootPageIdx())
	if err != nil {
		return err
	}

	for i, values := range stmt.Inserts {
		if t.HasPrimaryKey() {
			insertedPrimaryKey, err := t.insertPrimaryKey(ctx, stmt, i, nextRowID)
			if err != nil {
				return err
			}
			// Update statement with autoincremented primary key value
			pkIdx := stmt.ColumnIdx(t.PrimaryKey.Column.Name)
			values[pkIdx] = OptionalValue{Value: insertedPrimaryKey, Valid: true}
		}

		aRow := Row{
			Columns: t.Columns,
			Values:  make([]OptionalValue, 0, len(t.Columns)),
		}
		aRow = aRow.appendValues(stmt.Fields, values)

		aPage, err := t.pager.ModifyPage(ctx, aCursor.PageIdx)
		if err != nil {
			return fmt.Errorf("insert: %w", err)
		}

		// Must be leaf node
		if aPage.LeafNode == nil {
			return fmt.Errorf("trying to insert into non leaf node")
		}

		t.logger.Sugar().With(
			"page_index", int(aCursor.PageIdx),
			"cell_index", int(aCursor.CellIdx),
			"row_id", int(nextRowID),
		).Debug("inserting row")

		if aCursor.CellIdx < aPage.LeafNode.Header.Cells {
			if aPage.LeafNode.Cells[aCursor.CellIdx].Key == nextRowID {
				return fmt.Errorf("duplicate key %d", nextRowID)
			}
		}

		if err := aCursor.LeafNodeInsert(ctx, nextRowID, &aRow); err != nil {
			return err
		}

		if i == len(stmt.Inserts)-1 {
			break
		}

		// Try to advance cursor to next position, if there is still space in the
		// current page, just increment cell index, otherwise call Seek to get
		// new cursor
		aCursor, nextRowID, err = t.SeekNextRowID(ctx, t.GetRootPageIdx())
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) insertPrimaryKey(ctx context.Context, stmt Statement, i int, rowID uint64) (any, error) {
	if t.PrimaryKey.Index == nil {
		return 0, fmt.Errorf("table %s has primary key but no index", t.Name)
	}
	pkValue, ok := stmt.InsertForColumn(t.PrimaryKey.Column.Name, i)
	if !ok {
		return 0, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
	}
	if !pkValue.Valid {
		if !t.PrimaryKey.Autoincrement {
			return 0, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
		}
		newPrimaryKey, err := t.insertAutoincrementedPrimaryKey(ctx, rowID)
		if err != nil {
			return 0, err
		}
		return newPrimaryKey, nil
	}
	castedValue, err := castPrimaryKeyValue(t.PrimaryKey.Column, pkValue.Value)
	if err != nil {
		return 0, fmt.Errorf("failed to cast primary key value for %s: %w", t.PrimaryKey.Name, err)
	}
	if err := t.PrimaryKey.Index.Insert(ctx, castedValue, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}
	return castedValue, nil
}

func (t *Table) insertAutoincrementedPrimaryKey(ctx context.Context, rowID uint64) (int64, error) {
	if t.PrimaryKey.Autoincrement && t.PrimaryKey.Column.Kind != Int8 {
		return 0, fmt.Errorf("autoincrement primary key %s must be of type INT8", t.PrimaryKey.Name)
	}
	lastKey, err := t.PrimaryKey.Index.SeekLastKey(ctx, t.PrimaryKey.Index.GetRootPageIdx())
	if err != nil {
		return 0, err
	}
	lastPrimaryKey, ok := lastKey.(int64)
	if !ok {
		return 0, fmt.Errorf("failed to cast last primary key value for autoincrement")
	}
	newPrimaryKey := lastPrimaryKey + 1
	if err := t.PrimaryKey.Index.Insert(ctx, newPrimaryKey, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}
	return newPrimaryKey, nil
}
