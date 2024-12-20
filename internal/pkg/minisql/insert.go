package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Insert(ctx context.Context, stmt Statement) error {
	aCursor, nextRowID, err := t.SeekNextRowID(ctx, t.RootPageIdx)
	if err != nil {
		return err
	}

	// TODO - lock row so parallel insert won't try to insert the same row ID?

	for i, values := range stmt.Inserts {
		aRow := Row{
			Columns: t.Columns,
			Values:  make([]any, 0, len(t.Columns)),
		}
		aRow = aRow.appendValues(stmt.Fields, values)

		aPage, err := t.pager.GetPage(ctx, t, aCursor.PageIdx)
		if err != nil {
			return err
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
