package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_RowSlot(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: Int4,
			Size: 4,
			Name: "age",
		},
	}
	aDatabase, err := NewDatabase()
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(context.Background(), "foo", columns)
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
