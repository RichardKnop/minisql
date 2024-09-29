package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabase_CreateTable(t *testing.T) {
	t.Parallel()

	aDatabase, err := NewDatabase("db")
	require.NoError(t, err)

	aTable, err := aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.NoError(t, err)
	assert.Equal(t, "foo", aTable.Name)
	assert.Equal(t, testColumns, aTable.Columns)
	assert.Empty(t, aTable.Pages)
	assert.Equal(t, 267, aTable.rowSize)
	assert.Equal(t, 0, aTable.numRows)
	assert.Len(t, aDatabase.tables, 1)

	aTable, err = aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTableAlreadyExists)
	assert.Len(t, aDatabase.tables, 1)
}

func TestDatabase_DropTable(t *testing.T) {
	t.Parallel()

	aDatabase, err := NewDatabase("db")
	require.NoError(t, err)

	err = aDatabase.DropTable(context.Background(), "foo")
	require.Error(t, err)
	assert.ErrorIs(t, err, errTableDoesNotExist)

	_, err = aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.NoError(t, err)
	assert.Len(t, aDatabase.tables, 1)

	err = aDatabase.DropTable(context.Background(), "foo")
	require.NoError(t, err)
	assert.Len(t, aDatabase.tables, 0)
}

func TestPage_Insert(t *testing.T) {
	t.Parallel()

	aPage := NewPage(0)

	// Row size is 267 bytes
	// 4096B page can fit 15 rows

	offset := 0
	for i := 0; i < 15; i++ {
		aRow := gen.Row()
		err := aPage.Insert(context.Background(), offset, aRow)
		require.NoError(t, err)
		offset += aRow.Size()
	}

	// When trying to insert 16th row, we should receive an error
	// explaining that there is not enough space left in the page
	err := aPage.Insert(context.Background(), offset, gen.Row())
	require.Error(t, err)
	assert.Equal(t, "error inserting 267 bytes into page at offset 4005, not enough space", err.Error())
}

func TestTable_RowSlot(t *testing.T) {
	t.Parallel()

	aDatabase, err := NewDatabase("db")
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.NoError(t, err)

	// Row size is 267 bytes
	// 15 rows will fit into each 4096 bytes page
	// There are max 100 pages right now (temporary limitation)

	testCases := []struct {
		Name      string
		RowNumber int
		Page      int
		Offset    int
		Err       error
	}{
		{
			Name:      "First row in the table",
			RowNumber: 0,
			Page:      0,
			Offset:    0,
		},
		{
			Name:      "Second row in the table",
			RowNumber: 1,
			Page:      0,
			Offset:    267,
		},
		{
			Name:      "Third row in the table",
			RowNumber: 2,
			Page:      0,
			Offset:    267 * 2,
		},
		{
			Name:      "16th row should be the first row of the second page",
			RowNumber: 15,
			Page:      1,
			Offset:    0,
		},
		{
			Name:      "1486th row should be the first row of the last 100th page",
			RowNumber: 1485,
			Page:      99,
			Offset:    0,
		},
		{
			Name:      "1500th row should be the last row of the last 100th page",
			RowNumber: 1499,
			Page:      99,
			Offset:    267 * 14,
		},
		{
			Name:      "1501th row should cause error",
			RowNumber: 1500,
			Err:       errMaximumPagesReached,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aPage, offset, err := aTable.RowSlot(aTestCase.RowNumber)
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Page, aPage.Number)
			assert.Equal(t, aTestCase.Offset, offset)
		})
	}
}
