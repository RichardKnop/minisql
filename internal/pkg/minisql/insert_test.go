package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_Insert(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		aRow           = gen.Row()
		cells, rowSize = 0, aRow.Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		aTable         = NewTable("foo", testColumns, pagerMock, 0)
	)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
		Inserts:   [][]any{aRow.Values},
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	assert.Equal(t, 1, int(aRootPage.LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aRootPage.LeafNode.Cells[0].Key))

	actualRow := NewRow(aRow.Columns)
	err = UnmarshalRow(aRootPage.LeafNode.Cells[0].Value, &actualRow)
	require.NoError(t, err)
	assert.Equal(t, aRow, actualRow)
}

func TestTable_Insert_MultiInsert(t *testing.T) {
	t.Parallel()

	var (
		ctx                = context.Background()
		pagerMock          = new(MockPager)
		aRow, aRow2, aRow3 = gen.Row(), gen.Row(), gen.Row()
		cells, rowSize     = 0, aRow.Size()
		aRootPage          = newRootLeafPageWithCells(cells, int(rowSize))
		aTable             = NewTable("foo", testColumns, pagerMock, 0)
	)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
		Inserts:   [][]any{aRow.Values, aRow2.Values, aRow3.Values},
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	assert.Equal(t, 3, int(aRootPage.LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aRootPage.LeafNode.Cells[0].Key))
	assert.Equal(t, 1, int(aRootPage.LeafNode.Cells[1].Key))
	assert.Equal(t, 2, int(aRootPage.LeafNode.Cells[2].Key))

	actualRow := NewRow(aRow.Columns)
	err = UnmarshalRow(aRootPage.LeafNode.Cells[0].Value, &actualRow)
	require.NoError(t, err)
	assert.Equal(t, aRow, actualRow)

	actualRow2 := NewRow(aRow.Columns)
	err = UnmarshalRow(aRootPage.LeafNode.Cells[1].Value, &actualRow2)
	require.NoError(t, err)
	assert.Equal(t, aRow2, actualRow2)

	actualRow3 := NewRow(aRow.Columns)
	err = UnmarshalRow(aRootPage.LeafNode.Cells[2].Value, &actualRow3)
	require.NoError(t, err)
	assert.Equal(t, aRow3, actualRow3)
}

func TestTable_Insert_SplitRootLeaf(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		aRow           = gen.Row()
		cells, rowSize = maxCells(aRow.Size()), aRow.Size()
		aRootPage      = newRootLeafPageWithCells(int(cells), int(rowSize))
		rightChild     = &Page{LeafNode: NewLeafNode(rowSize)}
		leftChild      = &Page{LeafNode: NewLeafNode(rowSize)}
		aTable         = NewTable("foo", testColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(rightChild, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leftChild, nil)

	// TotalPages is called twice, let's make sure the second time it's called,
	// it will return incremented value since we have created a new page already
	totalPages := uint32(1)
	pagerMock.On("TotalPages").Return(func() uint32 {
		old := totalPages
		totalPages += 1
		return old
	}, nil)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
		Inserts:   [][]any{aRow.Values},
	}

	err := aTable.Insert(ctx, stmt)
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
	assert.Equal(t, 0, int(rightChild.LeafNode.Header.Parent))
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
	assert.Equal(t, 0, int(leftChild.LeafNode.Header.Parent))
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

	actualRow := NewRow(aRow.Columns)
	err = UnmarshalRow(rightChild.LeafNode.Cells[6].Value, &actualRow)
	require.NoError(t, err)
	assert.Equal(t, aRow, actualRow)
}

func TestTable_Insert_SplitLeaf(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		rows           = gen.BigRows(4)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		leaf1          = &Page{LeafNode: NewLeafNode(rowSize)}
		leaf2          = &Page{LeafNode: NewLeafNode(rowSize)}
		leaf3          = &Page{LeafNode: NewLeafNode(rowSize)}
		leaf4          = &Page{LeafNode: NewLeafNode(rowSize)}
		aTable         = NewTable("foo", testBigColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leaf1, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(leaf2, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(3)).Return(leaf3, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(4)).Return(leaf4, nil)

	// TotalPages is called 3 times, let's make sure each time it's called, it returns
	// an incremented value since we have created a new page in the meantime
	totalPages := uint32(1)
	pagerMock.On("TotalPages").Return(func() uint32 {
		old := totalPages
		totalPages += 1
		return old
	}, nil)

	// Insert test rows
	for _, aRow := range rows {
		stmt := Statement{
			Kind:      Insert,
			TableName: "foo",
			Fields:    []string{"id", "email", "name", "description"},
			Inserts:   [][]any{aRow.Values},
		}

		err := aTable.Insert(ctx, stmt)
		require.NoError(t, err)
	}

	// Assert root node
	assert.Equal(t, 3, int(aRootPage.InternalNode.Header.KeysNum))
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 4, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, 2, int(aRootPage.InternalNode.ICells[0].Child))
	assert.Equal(t, 0, int(aRootPage.InternalNode.ICells[0].Key))
	assert.Equal(t, 1, int(aRootPage.InternalNode.ICells[1].Child))
	assert.Equal(t, 1, int(aRootPage.InternalNode.ICells[1].Key))
	assert.Equal(t, 3, int(aRootPage.InternalNode.ICells[2].Child))
	assert.Equal(t, 2, int(aRootPage.InternalNode.ICells[2].Key))

	// Assert leaf nodes
	// in order from left to right
	leafPages := []*Page{
		leaf1,
		leaf2,
		leaf3,
		leaf4,
	}
	for i, aLeaf := range leafPages {
		assert.False(t, aLeaf.LeafNode.Header.IsRoot)
		assert.False(t, aLeaf.LeafNode.Header.IsInternal)
		assert.Equal(t, 0, int(aLeaf.LeafNode.Header.Parent))
		assert.Equal(t, 1, int(aLeaf.LeafNode.Header.Cells))
		assert.Equal(t, i, int(aLeaf.LeafNode.Cells[0].Key))
	}
}
