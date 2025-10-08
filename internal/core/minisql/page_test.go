package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_PageRecycling(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := NewTablePager(aPager, Row{Columns: testMediumColumns}.Size())

	var (
		ctx     = context.Background()
		numRows = 100
		rows    = gen.MediumRows(numRows)
		aTable  = NewTable(testLogger, testTableName, testMediumColumns, tablePager, 0)
	)
	aTable.maximumICells = 5 // for testing purposes only, normally 340

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 0, int(aPager.dbHeader.FreePageCount))
	checkRows(ctx, t, aTable, rows)

	// Now delete all rows, this will free up 46 pages
	// but the root page will remain in use
	deleteResult, err := aTable.Delete(ctx, Statement{
		Kind: Delete,
	})
	require.NoError(t, err)
	assert.Equal(t, len(rows), deleteResult.RowsAffected)

	checkRows(ctx, t, aTable, nil)
	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 46, int(aPager.dbHeader.FreePageCount))

	// Now we reinsert the same rows again
	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	// We should still have the same number of pages in total
	// and no free pages
	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 0, int(aPager.dbHeader.FreePageCount))
	checkRows(ctx, t, aTable, rows)
}

func (p *pagerImpl) getFreePages(rowSize uint64) ([]int, error) {
	var freePages []int

	tablePager := NewTablePager(p, rowSize)

	nextFreePage := p.dbHeader.FirstFreePage
	for nextFreePage != 0 {
		freePages = append(freePages, int(nextFreePage))

		freePage, err := tablePager.GetPage(context.Background(), nextFreePage)
		if err != nil {
			return nil, err
		}

		nextFreePage = freePage.FreePage.NextFreePage
	}

	// sort.Ints(freePages)

	return freePages, nil
}
