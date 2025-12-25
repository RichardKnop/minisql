package minisql

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testTableName  = "test_table"
	testTableName2 = "test_table_2"
	testTableName3 = "test_table_3"
	testTableName4 = "test_table_4"
)

func TestNewTable_WithPrimaryKey(t *testing.T) {
	t.Parallel()

	var columns = []Column{
		{
			Kind:       Int8,
			Size:       8,
			Name:       "id",
			PrimaryKey: true,
		},
		{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     "email",
			Nullable: true,
		},
	}

	aTable := NewTable(testLogger, nil, nil, "tablename", columns, 0)
	assert.Equal(t, PrimaryKey{
		IndexInfo: IndexInfo{
			Name:   primaryKeyName("tablename"),
			Column: columns[0],
		},
	}, aTable.PrimaryKey)
}

func TestTable_SeekNextRowID(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		aTable    = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
	)

	t.Run("empty table", func(t *testing.T) {
		var (
			cells, rowSize = 0, 270
			aRootPage      = newRootLeafPageWithCells(cells, rowSize)
		)

		pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(aRootPage, nil).Once()

		aCursor, rowID, err := aTable.SeekNextRowID(ctx, aTable.GetRootPageIdx())
		require.NoError(t, err)
		assert.Equal(t, &Cursor{
			Table:   aTable,
			PageIdx: 0,
			CellIdx: 0,
		}, aCursor)
		assert.Equal(t, 0, int(rowID))

		mock.AssertExpectationsForObjects(t, pagerMock)
		resetMock(&pagerMock.Mock)
	})

	t.Run("table with rows", func(t *testing.T) {
		aRootPage, internalPages, leafPages := newTestBtree()

		pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(aRootPage, nil).Once()
		pagerMock.On("ReadPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil).Once()
		pagerMock.On("ReadPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil).Once()

		aCursor, rowID, err := aTable.SeekNextRowID(ctx, aTable.GetRootPageIdx())
		require.NoError(t, err)
		require.NoError(t, err)
		assert.Equal(t, &Cursor{
			Table:   aTable,
			PageIdx: 6,
			CellIdx: 1,
		}, aCursor)
		assert.Equal(t, 22, int(rowID))

		mock.AssertExpectationsForObjects(t, pagerMock)
	})
}

func TestTable_SeekFirst(t *testing.T) {
	t.Parallel()

	var (
		ctx                                 = context.Background()
		pagerMock                           = new(MockTxPager)
		aTable                              = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
		aRootPage, internalPages, leafPages = newTestBtree()
	)

	pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(aRootPage, nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(1)).Return(internalPages[0], nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(3)).Return(leafPages[0], nil).Once()

	aCursor, err := aTable.SeekFirst(ctx)
	require.NoError(t, err)
	assert.Equal(t, &Cursor{
		Table:   aTable,
		PageIdx: 3,
		CellIdx: 0,
	}, aCursor)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_SeekLast(t *testing.T) {
	t.Parallel()

	var (
		ctx                                 = context.Background()
		pagerMock                           = new(MockTxPager)
		aTable                              = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
		aRootPage, internalPages, leafPages = newTestBtree()
	)

	pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(aRootPage, nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil).Once()

	aCursor, err := aTable.SeekLast(ctx, aTable.GetRootPageIdx())
	require.NoError(t, err)
	assert.Equal(t, &Cursor{
		Table:   aTable,
		PageIdx: 6,
		CellIdx: 0,
	}, aCursor)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_Seek_EmptyTable(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		aTable    = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
	)

	t.Run("empty table", func(t *testing.T) {
		var (
			cells, rowSize = 0, 270
			aRootPage      = newRootLeafPageWithCells(cells, rowSize)
		)

		pagerMock.On("ReadPage", mock.Anything, aTable.GetRootPageIdx()).Return(aRootPage, nil).Once()

		aCursor, err := aTable.Seek(ctx, RowID(0))
		require.NoError(t, err)
		assert.Equal(t, aTable, aCursor.Table)
		assert.Equal(t, 0, int(aCursor.PageIdx))
		assert.Equal(t, 0, int(aCursor.CellIdx))

		mock.AssertExpectationsForObjects(t, pagerMock)
		resetMock(&pagerMock.Mock)
	})

	t.Run("root leaf node single cell", func(t *testing.T) {
		var (
			cells, rowSize = 1, 270
			aRootPage      = newRootLeafPageWithCells(cells, rowSize)
		)

		pagerMock.On("ReadPage", mock.Anything, aTable.GetRootPageIdx()).Return(aRootPage, nil)

		// Seek key 0
		aCursor, err := aTable.Seek(ctx, RowID(0))
		require.NoError(t, err)
		assert.Equal(t, aTable, aCursor.Table)
		assert.Equal(t, 0, int(aCursor.PageIdx))
		assert.Equal(t, 0, int(aCursor.CellIdx))

		// Seek key 1 (doesn't exist, end of table)
		aCursor, err = aTable.Seek(ctx, RowID(1))
		require.NoError(t, err)
		assert.Equal(t, aTable, aCursor.Table)
		assert.Equal(t, 0, int(aCursor.PageIdx))
		assert.Equal(t, 1, int(aCursor.CellIdx))

		mock.AssertExpectationsForObjects(t, pagerMock)
		resetMock(&pagerMock.Mock)
	})

	t.Run("root leaf node full", func(t *testing.T) {
		var (
			cells     = maxCells(testRowSize)
			aRootPage = newRootLeafPageWithCells(int(cells), int(testRowSize))
		)
		pagerMock.On("ReadPage", mock.Anything, aTable.GetRootPageIdx()).Return(aRootPage, nil)

		// Seek all existing keys
		for key := uint64(0); key < uint64(aRootPage.LeafNode.Header.Cells); key++ {
			aCursor, err := aTable.Seek(ctx, RowID(key))
			require.NoError(t, err)
			assert.Equal(t, aTable, aCursor.Table)
			assert.Equal(t, 0, int(aCursor.PageIdx))
			assert.Equal(t, int(key), int(aCursor.CellIdx))
		}

		// Seek key 3 (does not exist, end of table)
		aCursor, err := aTable.Seek(ctx, RowID(cells))
		require.NoError(t, err)
		assert.Equal(t, aTable, aCursor.Table)
		assert.Equal(t, 0, int(aCursor.PageIdx))
		assert.Equal(t, int(cells), int(aCursor.CellIdx))

		mock.AssertExpectationsForObjects(t, pagerMock)
	})
}

func TestTable_Seek_RootLeafNode_BiggerTree(t *testing.T) {
	t.Parallel()

	var (
		ctx                                 = context.Background()
		pagerMock                           = new(MockTxPager)
		aTable                              = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
		aRootPage, internalPages, leafPages = newTestBtree()
	)

	pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(aRootPage, nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(1)).Return(internalPages[0], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(3)).Return(leafPages[0], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(4)).Return(leafPages[1], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(5)).Return(leafPages[2], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil)

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
			aCursor, err := aTable.Seek(ctx, RowID(aTestCase.Key))
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
		ctx           = context.Background()
		pagerMock     = new(MockTxPager)
		aTable        = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
		cells         = maxCells(testRowSize)
		aRootPage     = newRootLeafPageWithCells(int(cells), int(testRowSize))
		newRightChild = &Page{Index: 1, LeafNode: NewLeafNode()}
		newLeftChild  = &Page{Index: 2, LeafNode: NewLeafNode()}
	)

	pagerMock.On("ModifyPage", mock.Anything, PageIndex(0)).Return(aRootPage, nil)
	pagerMock.On("ModifyPage", mock.Anything, PageIndex(1)).Return(newRightChild, nil)
	pagerMock.On("GetFreePage", mock.Anything).Return(newLeftChild, nil)

	_, err := aTable.createNewRoot(ctx, 1)
	require.NoError(t, err)
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, ICell{
		Key:   RowID(cells - 1),
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
		pagerMock                   = new(MockTxPager)
		_, internalPages, leafPages = newTestBtree()
		aTable                      = NewTable(testLogger, pagerMock, NewTransactionManager(zap.NewNop()), testTableName, testColumns, 0)
		aNewLeaf                    = NewLeafNode()
	)
	aNewLeaf.Header.Cells = 1
	aNewLeaf.Cells = append(aNewLeaf.Cells, Cell{
		Key:   25,
		Value: bytes.Repeat([]byte{byte(7)}, 270),
	})

	pagerMock.On("ModifyPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil).Once()
	pagerMock.On("ModifyPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil).Once()
	pagerMock.On("ModifyPage", mock.Anything, PageIndex(7)).Return(&Page{LeafNode: aNewLeaf}, nil).Once()

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
	err := aTable.InternalNodeInsert(ctx, 2, 7)
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
