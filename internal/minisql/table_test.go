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
	testTableName2 = "test_table_2"
	testTableName3 = "test_table_3"
)

func TestNewTable_WithPrimaryKey(t *testing.T) {
	t.Parallel()

	var (
		columns = []Column{
			{
				Kind: Int8,
				Size: 8,
				Name: "id",
			},
			{
				Kind:     Varchar,
				Size:     MaxInlineVarchar,
				Name:     "email",
				Nullable: true,
			},
		}
		pk = PrimaryKey{
			IndexInfo: IndexInfo{
				Name:    PrimaryKeyName(testTableName),
				Columns: columns[0:1],
			},
		}
	)

	table := NewTable(testLogger, nil, nil, testTableName, columns, 0, nil, WithPrimaryKey(pk))
	assert.Equal(t, pk, table.PrimaryKey)
}

func TestTable_SeekNextRowID(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table     = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
	)

	t.Run("empty table", func(t *testing.T) {
		var (
			cells, rowSize = 0, 270
			rootPage       = newRootLeafPageWithCells(cells, rowSize)
		)

		pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(rootPage, nil).Once()

		cursor, rowID, err := table.SeekNextRowID(ctx, table.GetRootPageIdx())
		require.NoError(t, err)
		assert.Equal(t, &Cursor{
			Table:   table,
			PageIdx: 0,
			CellIdx: 0,
		}, cursor)
		assert.Equal(t, 0, int(rowID))

		mock.AssertExpectationsForObjects(t, pagerMock)
		resetMocks(&pagerMock.Mock)
	})

	t.Run("table with rows", func(t *testing.T) {
		rootPage, internalPages, leafPages := newTestBtree()

		pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(rootPage, nil).Once()
		pagerMock.On("ReadPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil).Once()
		pagerMock.On("ReadPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil).Once()

		cursor, rowID, err := table.SeekNextRowID(ctx, table.GetRootPageIdx())
		require.NoError(t, err)
		require.NoError(t, err)
		assert.Equal(t, &Cursor{
			Table:   table,
			PageIdx: 6,
			CellIdx: 1,
		}, cursor)
		assert.Equal(t, 22, int(rowID))

		mock.AssertExpectationsForObjects(t, pagerMock)
	})
}

func TestTable_SeekFirst(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table     = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
	)
	rootPage, internalPages, leafPages := newTestBtree()

	pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(rootPage, nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(1)).Return(internalPages[0], nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(3)).Return(leafPages[0], nil).Once()

	cursor, err := table.SeekFirst(ctx)
	require.NoError(t, err)
	assert.Equal(t, &Cursor{
		Table:   table,
		PageIdx: 3,
		CellIdx: 0,
	}, cursor)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_SeekLast(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table     = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
	)
	rootPage, internalPages, leafPages := newTestBtree()

	pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(rootPage, nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil).Once()
	pagerMock.On("ReadPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil).Once()

	cursor, err := table.SeekLast(ctx, table.GetRootPageIdx())
	require.NoError(t, err)
	assert.Equal(t, &Cursor{
		Table:   table,
		PageIdx: 6,
		CellIdx: 0,
	}, cursor)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_Seek_EmptyTable(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table     = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
	)

	t.Run("empty table", func(t *testing.T) {
		var (
			cells, rowSize = 0, 270
			rootPage       = newRootLeafPageWithCells(cells, rowSize)
		)

		pagerMock.On("ReadPage", mock.Anything, table.GetRootPageIdx()).Return(rootPage, nil).Once()

		cursor, err := table.Seek(ctx, RowID(0))
		require.NoError(t, err)
		assert.Equal(t, table, cursor.Table)
		assert.Equal(t, 0, int(cursor.PageIdx))
		assert.Equal(t, 0, int(cursor.CellIdx))

		mock.AssertExpectationsForObjects(t, pagerMock)
		resetMocks(&pagerMock.Mock)
	})

	t.Run("root leaf node single cell", func(t *testing.T) {
		var (
			cells, rowSize = 1, 270
			rootPage       = newRootLeafPageWithCells(cells, rowSize)
		)

		pagerMock.On("ReadPage", mock.Anything, table.GetRootPageIdx()).Return(rootPage, nil)

		// Seek key 0
		cursor, err := table.Seek(ctx, RowID(0))
		require.NoError(t, err)
		assert.Equal(t, table, cursor.Table)
		assert.Equal(t, 0, int(cursor.PageIdx))
		assert.Equal(t, 0, int(cursor.CellIdx))

		// Seek key 1 (doesn't exist, end of table)
		cursor, err = table.Seek(ctx, RowID(1))
		require.NoError(t, err)
		assert.Equal(t, table, cursor.Table)
		assert.Equal(t, 0, int(cursor.PageIdx))
		assert.Equal(t, 1, int(cursor.CellIdx))

		mock.AssertExpectationsForObjects(t, pagerMock)
		resetMocks(&pagerMock.Mock)
	})

	t.Run("root leaf node full", func(t *testing.T) {
		var (
			cells    = maxCells(testRowSize)
			rootPage = newRootLeafPageWithCells(int(cells), int(testRowSize))
		)
		pagerMock.On("ReadPage", mock.Anything, table.GetRootPageIdx()).Return(rootPage, nil)

		// Seek all existing keys
		for key := uint64(0); key < uint64(rootPage.LeafNode.Header.Cells); key++ {
			cursor, err := table.Seek(ctx, RowID(key))
			require.NoError(t, err)
			assert.Equal(t, table, cursor.Table)
			assert.Equal(t, 0, int(cursor.PageIdx))
			assert.Equal(t, int(key), int(cursor.CellIdx))
		}

		// Seek key 3 (does not exist, end of table)
		cursor, err := table.Seek(ctx, RowID(cells))
		require.NoError(t, err)
		assert.Equal(t, table, cursor.Table)
		assert.Equal(t, 0, int(cursor.PageIdx))
		assert.Equal(t, int(cells), int(cursor.CellIdx))

		mock.AssertExpectationsForObjects(t, pagerMock)
	})
}

func TestTable_Seek_RootLeafNode_BiggerTree(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		pagerMock = new(MockTxPager)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table     = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
	)
	rootPage, internalPages, leafPages := newTestBtree()

	pagerMock.On("ReadPage", mock.Anything, PageIndex(0)).Return(rootPage, nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(1)).Return(internalPages[0], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(2)).Return(internalPages[1], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(3)).Return(leafPages[0], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(4)).Return(leafPages[1], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(5)).Return(leafPages[2], nil)
	pagerMock.On("ReadPage", mock.Anything, PageIndex(6)).Return(leafPages[3], nil)

	testCases := []struct {
		Cursor *Cursor
		Name   string
		Key    uint64
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
			cursor, err := table.Seek(ctx, RowID(aTestCase.Key))
			require.NoError(t, err)
			assert.Equal(t, int(aTestCase.Cursor.PageIdx), int(cursor.PageIdx))
			assert.Equal(t, int(aTestCase.Cursor.CellIdx), int(cursor.CellIdx))
		})
	}

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_CreateNewRoot(t *testing.T) {
	t.Parallel()

	var (
		ctx           = context.Background()
		pagerMock     = new(MockTxPager)
		txManager     = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table         = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
		cells         = maxCells(testRowSize)
		rootPage      = newRootLeafPageWithCells(int(cells), int(testRowSize))
		newRightChild = &Page{Index: 1, LeafNode: NewLeafNode()}
		newLeftChild  = &Page{Index: 2, LeafNode: NewLeafNode()}
	)

	pagerMock.On("ModifyPage", mock.Anything, PageIndex(0)).Return(rootPage, nil)
	pagerMock.On("ModifyPage", mock.Anything, PageIndex(1)).Return(newRightChild, nil)
	pagerMock.On("GetFreePage", mock.Anything).Return(newLeftChild, nil)

	_, err := table.createNewRoot(ctx, 1)
	require.NoError(t, err)
	assert.True(t, rootPage.InternalNode.Header.IsRoot)
	assert.True(t, rootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(rootPage.InternalNode.Header.KeysNum))
	assert.Equal(t, 1, int(rootPage.InternalNode.Header.RightChild))
	assert.Equal(t, ICell{
		Key:   RowID(cells - 1),
		Child: 2,
	}, rootPage.InternalNode.ICells[0])

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
		txManager                   = NewTransactionManager(zap.NewNop(), testDBName, nil, nil, nil)
		table                       = NewTable(testLogger, pagerMock, txManager, testTableName, testColumns, 0, nil)
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
	err := table.InternalNodeInsert(ctx, 2, 7)
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
