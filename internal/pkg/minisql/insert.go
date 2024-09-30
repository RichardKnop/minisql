package minisql

import (
	"context"
)

func (d *Database) executeInsert(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	return aTable.Insert(ctx, stmt)
}

func (t *Table) Insert(ctx context.Context, stmt Statement) (StatementResult, error) {
	inserted := 0

	aCursor := TableEnd(t)
	for _, values := range stmt.Inserts {
		pageIdx, offset, err := aCursor.Value()
		if err != nil {
			return StatementResult{}, err
		}
		aPage, err := t.pager.GetPage(ctx, t.Name, pageIdx)
		if err != nil {
			return StatementResult{}, err
		}
		aRow := Row{
			Columns: t.Columns,
			Values:  make([]any, 0, len(t.Columns)),
		}
		aRow = aRow.appendValues(stmt.Fields, values)

		if err := aPage.Insert(ctx, offset, aRow); err != nil {
			// TODO - handle partial insert by deleting all previously inserted rows
			// if a row insert fails so we don't end up with inconsistent state
			return StatementResult{}, err
		}

		aCursor.Advance()
		inserted += 1
	}

	rowsAffected := inserted
	t.numRows += inserted
	return StatementResult{RowsAffected: rowsAffected}, nil
}
