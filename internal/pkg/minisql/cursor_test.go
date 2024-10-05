package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCursor_LeafNodeInsert_RootLeafEmptyTable(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		aRow           = gen.Row()
		cells, rowSize = 0, aRow.Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		aTable         = NewTable("foo", testColumns, pagerMock, 0)
		key            = uint64(0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)

	aCursor, err := aTable.Seek(ctx, key)
	require.NoError(t, err)

	err = aCursor.LeafNodeInsert(ctx, 0, &aRow)
	require.NoError(t, err)

	assert.Equal(t, 1, int(aRootPage.LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aRootPage.LeafNode.Cells[0].Key))

	actualRow := NewRow(aRow.Columns)
	err = UnmarshalRow(aRootPage.LeafNode.Cells[0].Value, &actualRow)
	require.NoError(t, err)
	assert.Equal(t, aRow, actualRow)
}
