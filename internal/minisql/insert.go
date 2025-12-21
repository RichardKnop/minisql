package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Insert(ctx context.Context, stmt Statement) error {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Prepare(t.clock()); err != nil {
		return err
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
			if t.PrimaryKey.Index == nil {
				return fmt.Errorf("table %s has primary key but no index", t.Name)
			}

			pkValue, ok := stmt.InsertForColumn(t.PrimaryKey.Column.Name, i)
			if !ok {
				return fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
			}

			insertedPrimaryKey, err := t.insertPrimaryKey(ctx, pkValue, nextRowID)
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
		aRow.appendValues(stmt.Fields, values)

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
