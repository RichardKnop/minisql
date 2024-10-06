package minisql

import (
	"context"
	"fmt"
)

func (d *Database) executeInsert(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	if err := aTable.Insert(ctx, stmt); err != nil {
		return StatementResult{}, err
	}

	return StatementResult{RowsAffected: len(stmt.Inserts)}, nil
}

func (t *Table) Insert(ctx context.Context, stmt Statement) error {
	newRowID, found, err := t.SeekMaxKey(ctx, t.RootPageIdx)
	if err != nil {
		return err
	}
	if found {
		newRowID = +1
	}

	for _, values := range stmt.Inserts {
		aRow := Row{
			Columns: t.Columns,
			Values:  make([]any, 0, len(t.Columns)),
		}
		aRow = aRow.appendValues(stmt.Fields, values)

		aCursor, err := t.Seek(ctx, newRowID)
		if err != nil {
			return err
		}

		aPage, err := t.pager.GetPage(ctx, t, aCursor.PageIdx)
		if err != nil {
			return err
		}

		// Must be leaf node
		if aPage.LeafNode == nil {
			return fmt.Errorf("trying to insert into non leaf node")
		}
		if aCursor.CellIdx < aPage.LeafNode.Header.Cells {
			if aPage.LeafNode.Cells[aCursor.CellIdx].Key == newRowID {
				return fmt.Errorf("duplicate key %d", newRowID)
			}
		}

		fmt.Println("inserting row", int(newRowID), "cursor", fmt.Sprintf("%+v", aCursor))

		if err := aCursor.LeafNodeInsert(ctx, newRowID, &aRow); err != nil {
			return err
		}

		newRowID += 1
	}

	return nil
}
