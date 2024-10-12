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
	maxRowID, found, err := t.SeekMaxKey(ctx, t.RootPageIdx)
	if err != nil {
		return err
	}
	nextRowID := maxRowID
	if found {
		nextRowID += 1
	}

	// rootPage, err := t.pager.GetPage(ctx, t, uint32(0))
	// if err != nil {
	// 	return err
	// }
	// child, err := t.pager.GetPage(ctx, t, uint32(1))
	// if err != nil {
	// 	return err
	// }
	// leftChild, err := t.pager.GetPage(ctx, t, uint32(2))
	// if err != nil {
	// 	return err
	// }
	// fmt.Println("Root Page Left Child", int(rootPage.InternalNode.ICells[0].Child))
	// fmt.Println("Root Page Right Child", int(rootPage.InternalNode.Header.RightChild))
	// fmt.Println("Root Page Keys")
	// for i := 0; i < int(rootPage.InternalNode.Header.KeysNum); i++ {
	// 	fmt.Println(int(rootPage.InternalNode.ICells[i].Key))
	// }
	// fmt.Println("Left child Keys")
	// for i := 0; i < int(leftChild.LeafNode.Header.Cells); i++ {
	// 	fmt.Println(int(leftChild.LeafNode.Cells[i].Key))
	// }
	// fmt.Println("Right child Keys")
	// for i := 0; i < int(child.LeafNode.Header.Cells); i++ {
	// 	fmt.Println(int(child.LeafNode.Cells[i].Key))
	// }

	for _, values := range stmt.Inserts {
		aRow := Row{
			Columns: t.Columns,
			Values:  make([]any, 0, len(t.Columns)),
		}
		aRow = aRow.appendValues(stmt.Fields, values)

		aCursor, err := t.Seek(ctx, nextRowID)
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

		logger.Sugar().With(
			"row_id", int(nextRowID),
			"found", found,
			"page_index", int(aCursor.PageIdx),
			"cell_index", int(aCursor.CellIdx),
		).Debug("inserting row")

		if aCursor.CellIdx < aPage.LeafNode.Header.Cells {
			if aPage.LeafNode.Cells[aCursor.CellIdx].Key == nextRowID {
				return fmt.Errorf("duplicate key %d", nextRowID)
			}
		}

		if err := aCursor.LeafNodeInsert(ctx, nextRowID, &aRow); err != nil {
			return err
		}

		nextRowID += 1
	}

	return nil
}
