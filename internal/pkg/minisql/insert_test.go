package minisql

import (
	"context"
	"fmt"
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
		aTable         = NewTable(testLogger, "foo", testColumns, pagerMock, 0)
	)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testColumns...),
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
		aTable             = NewTable(testLogger, "foo", testColumns, pagerMock, 0)
	)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testColumns...),
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
		aTable         = NewTable(testLogger, "foo", testColumns, pagerMock, 0)
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
		Fields:    columnNames(testColumns...),
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
		aTable         = NewTable(testLogger, "foo", testBigColumns, pagerMock, 0)
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

	// Batch insert test rows
	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testBigColumns...),
		Inserts:   [][]any{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

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

func TestTable_Insert_SplitInternalNode_CreateNewRoot(t *testing.T) {
	t.Parallel()

	/*
		In this test we are trying to simulate an internal node split. We will create
		a new tree and start inserting big rows (each row is big enough to take all
		space in a page and will require a new leaf node).
		Internal node has maximum of 340 keys therefor it can have 341 children leafs.
		That's why we need to try to insert 342 new rows, 342th row should cause the
		root node to split into two child internal nodes, each inheriting half of leaf
		nodes.
	*/
	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		numRows        = InternalNodeMaxCells + 2
		rows           = gen.BigRows(numRows)
		cells, rowSize = 0, rows[0].Size()
		// numberOfLeafs  = numRows
		aRootPage = newRootLeafPageWithCells(cells, int(rowSize))
		leafs     = make([]*Page, 0, numRows)
		aTable    = NewTable(testLogger, "foo", testBigColumns, pagerMock, 0)
		// These two pages will be returned as leafs by the pager as default behaviour
		// for allocating a new page but will be converted to internal nodes
		aNewRightInternal = &Page{LeafNode: NewLeafNode(rowSize)}
		aNewLeftInternal  = &Page{LeafNode: NewLeafNode(rowSize)}
	)
	for i := 0; i < numRows; i++ {
		leafs = append(leafs, &Page{LeafNode: NewLeafNode(rowSize)})
	}

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leafs[0], nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(leafs[1], nil)
	for i := 3; i < numRows+1; i++ {
		pagerMock.On("GetPage", mock.Anything, aTable, uint32(i)).Return(leafs[i-1], nil)
	}
	// Splitting root internal node causes 2 more pages to be requested, one for
	// sibling internal node, one for new root node
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(343)).Return(aNewRightInternal, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(344)).Return(aNewLeftInternal, nil)

	totalPages := uint32(1)
	pagerMock.On("TotalPages").Return(func() uint32 {
		old := totalPages
		totalPages += 1
		return old
	}, nil)

	// Batch insert test rows
	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testBigColumns...),
		Inserts:   [][]any{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	// Assert root node
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.Equal(t, 343, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, 344, int(aRootPage.InternalNode.ICells[0].Child))
	assert.Equal(t, 169, int(aRootPage.InternalNode.ICells[0].Key))

	// New left internal node should have 171 cells (170 + right child page).
	// First two pages should be switched (2, 1) as a result of root leaf split
	// but after that it continues as 3, 4, ... 171. Keys are 0, 1, ... 170
	assert.Equal(t, 170, int(aNewLeftInternal.InternalNode.Header.KeysNum))
	assert.False(t, aNewLeftInternal.InternalNode.Header.IsRoot)
	assert.True(t, aNewLeftInternal.InternalNode.Header.IsInternal)
	assert.Equal(t, 0, int(aNewLeftInternal.InternalNode.ICells[0].Key))
	assert.Equal(t, 2, int(aNewLeftInternal.InternalNode.ICells[0].Child))
	assert.Equal(t, 1, int(aNewLeftInternal.InternalNode.ICells[1].Key))
	assert.Equal(t, 1, int(aNewLeftInternal.InternalNode.ICells[1].Child))
	for i := 2; i < 170; i++ {
		assert.Equal(t, i, int(aNewLeftInternal.InternalNode.ICells[i].Key))
		assert.Equal(t, i+1, int(aNewLeftInternal.InternalNode.ICells[i].Child))
	}
	assert.Equal(t, 171, int(aNewLeftInternal.InternalNode.Header.RightChild))

	// New right internal node will have 169 cells.
	// Children go from 172, 173, ... 342 and keys from 171, 172, ... 341
	assert.Equal(t, 170, int(aNewRightInternal.InternalNode.Header.KeysNum))
	assert.False(t, aNewRightInternal.InternalNode.Header.IsRoot)
	assert.True(t, aNewRightInternal.InternalNode.Header.IsInternal)
	var (
		firstRightKey   = 171
		firstRightChild = 172
	)
	for i := 0; i < 170; i++ {
		assert.Equal(t, firstRightKey, int(aNewRightInternal.InternalNode.ICells[i].Key))
		assert.Equal(t, firstRightChild, int(aNewRightInternal.InternalNode.ICells[i].Child))
		firstRightKey += 1
		firstRightChild += 1
	}
	assert.Equal(t, 340, int(aNewRightInternal.InternalNode.ICells[169].Key))
	assert.Equal(t, 341, int(aNewRightInternal.InternalNode.ICells[169].Child))
	assert.Equal(t, 342, int(aNewRightInternal.InternalNode.Header.RightChild))

	for i, aLeaf := range leafs {
		assert.Equal(t, 1, int(aLeaf.LeafNode.Header.Cells))
		assert.Equal(t, i, int(aLeaf.LeafNode.Cells[0].Key))
		if i < 171 {
			assert.Equal(t, 344, int(aLeaf.LeafNode.Header.Parent), fmt.Sprintf("parent not 344 %d", i))
		} else {
			assert.Equal(t, 343, int(aLeaf.LeafNode.Header.Parent), fmt.Sprintf("parent not 343 %d", i))
		}

	}
}
