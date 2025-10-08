package minisql

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_SeekNextRowID_EmptyTable(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockTablePager)
		cells, rowSize = 0, 270
		aRootPage      = newRootLeafPageWithCells(cells, rowSize)
		aTable         = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, uint32(0)).Return(aRootPage, nil).Once()

	aCursor, rowID, err := aTable.SeekNextRowID(ctx, aTable.RootPageIdx)
	require.NoError(t, err)
	assert.Equal(t, &Cursor{
		Table:   aTable,
		PageIdx: 0,
		CellIdx: 0,
	}, aCursor)
	assert.Equal(t, 0, int(rowID))

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_SeekNextRowID(t *testing.T) {
	t.Parallel()

	var (
		ctx                                 = context.Background()
		pagerMock                           = new(MockTablePager)
		aTable                              = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
		aRootPage, internalPages, leafPages = newTestBtree()
	)

	pagerMock.On("GetPage", mock.Anything, uint32(0)).Return(aRootPage, nil).Once()
	pagerMock.On("GetPage", mock.Anything, uint32(2)).Return(internalPages[1], nil).Once()
	pagerMock.On("GetPage", mock.Anything, uint32(6)).Return(leafPages[3], nil).Once()

	aCursor, rowID, err := aTable.SeekNextRowID(ctx, aTable.RootPageIdx)
	require.NoError(t, err)
	require.NoError(t, err)
	assert.Equal(t, &Cursor{
		Table:   aTable,
		PageIdx: 6,
		CellIdx: 1,
	}, aCursor)
	assert.Equal(t, 22, int(rowID))

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_Seek_EmptyTable(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockTablePager)
		cells, rowSize = 0, 270
		aRootPage      = newRootLeafPageWithCells(cells, rowSize)
		aTable         = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable.RootPageIdx).Return(aRootPage, nil)

	aCursor, err := aTable.Seek(ctx, uint64(0))
	require.NoError(t, err)
	assert.Equal(t, aTable, aCursor.Table)
	assert.Equal(t, 0, int(aCursor.PageIdx))
	assert.Equal(t, 0, int(aCursor.CellIdx))

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_Seek_RootLeafNode_SingleCell(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockTablePager)
		cells, rowSize = 1, 270
		aRootPage      = newRootLeafPageWithCells(cells, rowSize)
		aTable         = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable.RootPageIdx).Return(aRootPage, nil)

	// Seek key 0
	aCursor, err := aTable.Seek(ctx, uint64(0))
	require.NoError(t, err)
	assert.Equal(t, aTable, aCursor.Table)
	assert.Equal(t, 0, int(aCursor.PageIdx))
	assert.Equal(t, 0, int(aCursor.CellIdx))

	// Seek key 1 (doesn't exist, end of table)
	aCursor, err = aTable.Seek(ctx, uint64(1))
	require.NoError(t, err)
	assert.Equal(t, aTable, aCursor.Table)
	assert.Equal(t, 0, int(aCursor.PageIdx))
	assert.Equal(t, 1, int(aCursor.CellIdx))

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_Seek_RootLeafNode_Full(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockTablePager)
		aTable         = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
		cells, rowSize = maxCells(aTable.RowSize), aTable.RowSize
		aRootPage      = newRootLeafPageWithCells(int(cells), int(rowSize))
	)

	pagerMock.On("GetPage", mock.Anything, aTable.RootPageIdx).Return(aRootPage, nil)

	// Seek all existing keys
	for key := uint64(0); key < uint64(aRootPage.LeafNode.Header.Cells); key++ {
		aCursor, err := aTable.Seek(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, aTable, aCursor.Table)
		assert.Equal(t, 0, int(aCursor.PageIdx))
		assert.Equal(t, int(key), int(aCursor.CellIdx))
	}

	// Seek key 3 (does not exist, end of table)
	aCursor, err := aTable.Seek(ctx, uint64(cells))
	require.NoError(t, err)
	assert.Equal(t, aTable, aCursor.Table)
	assert.Equal(t, 0, int(aCursor.PageIdx))
	assert.Equal(t, int(cells), int(aCursor.CellIdx))

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_Seek_RootLeafNode_BiggerTree(t *testing.T) {
	t.Parallel()

	var (
		ctx                                 = context.Background()
		pagerMock                           = new(MockTablePager)
		aTable                              = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
		aRootPage, internalPages, leafPages = newTestBtree()
	)

	pagerMock.On("GetPage", mock.Anything, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, uint32(1)).Return(internalPages[0], nil)
	pagerMock.On("GetPage", mock.Anything, uint32(2)).Return(internalPages[1], nil)
	pagerMock.On("GetPage", mock.Anything, uint32(3)).Return(leafPages[0], nil)
	pagerMock.On("GetPage", mock.Anything, uint32(4)).Return(leafPages[1], nil)
	pagerMock.On("GetPage", mock.Anything, uint32(5)).Return(leafPages[2], nil)
	pagerMock.On("GetPage", mock.Anything, uint32(6)).Return(leafPages[3], nil)

	testCases := []struct {
		Name   string
		Key    uint64
		Cursor *Cursor
	}{
		{
			Name: "Cursor to key 1",
			Key:  1,
			Cursor: &Cursor{
				PageIdx: 3,
				CellIdx: 0,
			},
		},
		{
			Name: "Cursor to key 2",
			Key:  2,
			Cursor: &Cursor{
				PageIdx: 3,
				CellIdx: 1,
			},
		},
		{
			Name: "Cursor to key 5",
			Key:  5,
			Cursor: &Cursor{
				PageIdx: 4,
				CellIdx: 0,
			},
		},
		{
			Name: "Cursor to key 12",
			Key:  12,
			Cursor: &Cursor{
				PageIdx: 5,
				CellIdx: 0,
			},
		},
		{
			Name: "Cursor to key 18",
			Key:  18,
			Cursor: &Cursor{
				PageIdx: 5,
				CellIdx: 1,
			},
		},
		{
			Name: "Cursor to key 21",
			Key:  21,
			Cursor: &Cursor{
				PageIdx: 6,
				CellIdx: 0,
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aCursor, err := aTable.Seek(ctx, aTestCase.Key)
			require.NoError(t, err)
			assert.Equal(t, int(aTestCase.Cursor.PageIdx), int(aCursor.PageIdx))
			assert.Equal(t, int(aTestCase.Cursor.CellIdx), int(aCursor.CellIdx))
		})
	}

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_CreateNewRoot(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockTablePager)
		aRow           = gen.Row()
		cells, rowSize = aRow.MaxCells(), aRow.Size()
		aRootPage      = newRootLeafPageWithCells(int(cells), int(rowSize))
		newRightChild  = &Page{Index: 1, LeafNode: NewLeafNode(aRow.Size())}
		newLeftChild   = &Page{Index: 2, LeafNode: NewLeafNode(aRow.Size())}
		aTable         = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, uint32(1)).Return(newRightChild, nil)
	pagerMock.On("GetFreePage", mock.Anything).Return(newLeftChild, nil)

	_, err := aTable.CreateNewRoot(ctx, uint32(1))
	require.NoError(t, err)
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, ICell{
		Key:   uint64(cells - 1),
		Child: 2,
	}, aRootPage.InternalNode.ICells[0])

	assert.Equal(t, 0, int(newRightChild.LeafNode.Header.Cells))
	assert.Equal(t, int(cells), int(newLeftChild.LeafNode.Header.Cells))

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_InternalNodeInsert(t *testing.T) {
	t.Parallel()

	var (
		ctx                         = context.Background()
		pagerMock                   = new(MockTablePager)
		_, internalPages, leafPages = newTestBtree()
		aTable                      = NewTable(testLogger, testTableName, testColumns, pagerMock, 0)
		aNewLeaf                    = NewLeafNode(aTable.RowSize)
	)
	aNewLeaf.Header.Cells = 1
	aNewLeaf.Cells[0].Key = 25
	aNewLeaf.Cells[0].Value = bytes.Repeat([]byte{byte(7)}, 270)

	pagerMock.On("GetPage", mock.Anything, uint32(2)).Return(internalPages[1], nil).Once()
	pagerMock.On("GetPage", mock.Anything, uint32(6)).Return(leafPages[3], nil).Once()
	pagerMock.On("GetPage", mock.Anything, uint32(7)).Return(&Page{LeafNode: aNewLeaf}, nil).Once()

	/*
	   Original Btree:

	   		           +-------------------+
	   		           |       *,5,*       |
	   		           +-------------------+
	   		          /                     \
	   		     +-------+                  +--------+
	   		     | *,2,* |                  | *,18,* |
	   		     +-------+                  +--------+
	   		    /         \                /          \
	   	 +---------+     +-----+     +-----------+    +------+
	   	 | 1:c,2:d |     | 5:a |     | 12:b,18:f |    | 21:g |
	   	 +---------+     +-----+     +-----------+    +------+
	*/
	err := aTable.InternalNodeInsert(ctx, uint32(2), uint32(7))
	require.NoError(t, err)

	/*
	   Should become:

	   		           +-------------------+
	   		           |       *,5,*       |
	   		           +-------------------+
	   		          /                     \
	   		    +-------+                   +--------------+
	   		    | *,2,* |                   |   *,18,21,*  |
	   		    +-------+                   +--------------+
	   		   /         \                 /        |       \
	   	+---------+    +-----+   +-----------+  +------+  +------+
	   	| 1:c,2:d |    | 5:a |   | 12:b,18:f |  | 21:g |  | 25:h |
	   	+---------+    +-----+   +-----------+  +------+  +------+
	*/
	assert.Equal(t, 2, int(internalPages[1].InternalNode.Header.KeysNum))
	assert.Equal(t, ICell{
		Key:   18,
		Child: 5,
	}, internalPages[1].InternalNode.ICells[0])
	assert.Equal(t, ICell{
		Key:   21,
		Child: 6,
	}, internalPages[1].InternalNode.ICells[1])
	assert.Equal(t, 7, int(internalPages[1].InternalNode.Header.RightChild))

	mock.AssertExpectationsForObjects(t, pagerMock)
}
