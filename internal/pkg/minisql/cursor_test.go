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

func TestCursor_LeafNodeInsert_RootLeafFull(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		aRow           = gen.Row()
		cells, rowSize = aRow.MaxCells(), aRow.Size()
		aRootPage      = newRootLeafPageWithCells(int(cells), int(rowSize))
		rightChild     = &Page{LeafNode: NewLeafNode(aRow.Size())}
		leftChild      = &Page{LeafNode: NewLeafNode(aRow.Size())}
		aTable         = NewTable("foo", testColumns, pagerMock, 0)
		key            = uint64(cells)
	)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(rightChild, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leftChild, nil)

	// TotalPages is called twice, let's make sure the second time it's called,
	// it will return incremented value since we have created a new page already
	totalPages := uint32(1)
	pagerMock.On("TotalPages", aTable).Return(func(t *Table) uint32 {
		old := totalPages
		totalPages += 1
		return old
	}, nil)

	aCursor, err := aTable.Seek(ctx, key)
	require.NoError(t, err)

	err = aCursor.LeafNodeInsert(ctx, uint64(cells), &aRow)
	require.NoError(t, err)

	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, 2, int(aRootPage.InternalNode.ICells[0].Child))
	assert.Equal(t, 7, int(aRootPage.InternalNode.ICells[0].Key))

	// Assert right leaf
	assert.False(t, rightChild.LeafNode.Header.IsRoot)
	assert.False(t, rightChild.LeafNode.Header.IsInternal)
	assert.Equal(t, 7, int(rightChild.LeafNode.Header.Cells))
	assert.Equal(t, uint32(0), rightChild.LeafNode.Header.NextLeaf)
	// Assert keys in the right leaf
	assert.Equal(t, 8, int(rightChild.LeafNode.Cells[0].Key))
	assert.Equal(t, 9, int(rightChild.LeafNode.Cells[1].Key))
	assert.Equal(t, 10, int(rightChild.LeafNode.Cells[2].Key))
	assert.Equal(t, 11, int(rightChild.LeafNode.Cells[3].Key))
	assert.Equal(t, 12, int(rightChild.LeafNode.Cells[4].Key))
	assert.Equal(t, 13, int(rightChild.LeafNode.Cells[5].Key))
	assert.Equal(t, 14, int(rightChild.LeafNode.Cells[6].Key))

	// Assert left leaf
	assert.False(t, leftChild.LeafNode.Header.IsRoot)
	assert.False(t, leftChild.LeafNode.Header.IsInternal)
	assert.Equal(t, 8, int(leftChild.LeafNode.Header.Cells))
	assert.Equal(t, 1, int(leftChild.LeafNode.Header.NextLeaf))
	// Assert keys in the left leaf
	assert.Equal(t, 0, int(leftChild.LeafNode.Cells[0].Key))
	assert.Equal(t, 1, int(leftChild.LeafNode.Cells[1].Key))
	assert.Equal(t, 2, int(leftChild.LeafNode.Cells[2].Key))
	assert.Equal(t, 3, int(leftChild.LeafNode.Cells[3].Key))
	assert.Equal(t, 4, int(leftChild.LeafNode.Cells[4].Key))
	assert.Equal(t, 5, int(leftChild.LeafNode.Cells[5].Key))
	assert.Equal(t, 6, int(leftChild.LeafNode.Cells[6].Key))
	assert.Equal(t, 7, int(leftChild.LeafNode.Cells[7].Key))
}
