package minisql

type Cursor struct {
	Table      *Table
	RowNumber  int
	EndOfTable bool
}

func TableStart(aTable *Table) *Cursor {
	return &Cursor{
		Table:      aTable,
		RowNumber:  0,
		EndOfTable: aTable.numRows == 0,
	}
}

func TableEnd(aTable *Table) *Cursor {
	rowNumber := 0
	if aTable.numRows > 0 {
		rowNumber = aTable.numRows
	}
	return &Cursor{
		Table:      aTable,
		RowNumber:  rowNumber,
		EndOfTable: true,
	}
}

func TableAt(aTable *Table, rowNum int) *Cursor {
	return &Cursor{
		Table:      aTable,
		RowNumber:  rowNum,
		EndOfTable: rowNum == aTable.numRows-1,
	}
}

func (c *Cursor) Advance() {
	c.RowNumber += 1
	if c.RowNumber >= c.Table.numRows {
		c.EndOfTable = true
	}
}

func (c *Cursor) Value() (uint32, uint32, error) {
	rowsPerPage := PageSize / c.Table.rowSize
	rowNumber := c.RowNumber
	pageIdx := uint32(rowNumber / int(rowsPerPage))

	if pageIdx >= MaxPages {
		return uint32(0), uint32(0), errMaximumPagesReached
	}

	rowOffset := uint32(rowNumber % int(rowsPerPage))
	byteOffset := rowOffset * c.Table.rowSize

	return pageIdx, uint32(byteOffset), nil
}
