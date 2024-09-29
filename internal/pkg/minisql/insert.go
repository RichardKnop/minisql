package minisql

import (
	"context"
)

func (d Database) executeInsert(ctx context.Context, stmt Statement) (StatementResult, error) {
	aTable, ok := d.tables[stmt.TableName]
	if !ok {
		return StatementResult{}, errTableDoesNotExist
	}

	rowNumber := aTable.numRows
	for _, values := range stmt.Inserts {
		aPage, offset, err := aTable.RowSlot(rowNumber)
		if err != nil {
			return StatementResult{}, err
		}
		aRow := Row{
			Columns: aTable.Columns,
			Values:  make([]any, 0, len(aTable.Columns)),
		}
		for _, aColumn := range aRow.Columns {
			var (
				found    = false
				fieldIdx = 0
			)
			for i, field := range stmt.Fields {
				if field == aColumn.Name {
					found = true
					fieldIdx = i
					break
				}
			}
			if found {
				aRow.Values = append(aRow.Values, values[fieldIdx])
			} else {
				// TODO - NULL values currently not handled properly
				aRow.Values = append(aRow.Values, nil)
			}
		}
		if err := aPage.Insert(ctx, offset, aRow); err != nil {
			// TODO - handle partial insert by deleting all previously inserted rows
			// if a row insert fails so we don't end up with inconsistent state
			return StatementResult{}, err
		}

		rowNumber += 1
	}

	rowsAffected := rowNumber - aTable.numRows
	aTable.numRows = rowNumber
	return StatementResult{RowsAffected: rowsAffected}, nil
}
