package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Insert(ctx context.Context, stmt Statement) error {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Validate(t); err != nil {
		return err
	}

	// Write lock limits concurrent writes to the table
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

	aCursor, nextRowID, err := t.SeekNextRowID(ctx, t.RootPageIdx)
	if err != nil {
		return err
	}

	for i, values := range stmt.Inserts {
		aRow := Row{
			Columns: t.Columns,
			Values:  make([]OptionalValue, 0, len(t.Columns)),
		}
		aRow = aRow.appendValues(stmt.Fields, values)

		aPage, err := t.pager.GetPage(ctx, aCursor.PageIdx)
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
		aCursor, nextRowID, err = t.SeekNextRowID(ctx, t.RootPageIdx)
		if err != nil {
			return err
		}
	}

	return nil
}
