package minisql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_Delete_RootLeafNode(t *testing.T) {
	t.Parallel()

	/*
		In this test we will be deleting from a root leaf node only tree.
	*/
	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		numRows        = 5
		rows           = gen.MediumRows(numRows)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		aTable         = NewTable(testLogger, "foo", testMediumColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)

	pagerMock.On("TotalPages").Return(uint32(1), nil)

	// Batch insert test rows
	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testMediumColumns...),
		Inserts:   [][]any{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	t.Run("delete one row", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, id.(int64)),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, rows[1:])
	})

	t.Run("delete all rows", func(t *testing.T) {
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:      Delete,
			TableName: "foo",
		})
		require.NoError(t, err)
		assert.Equal(t, 4, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, nil)
	})
}

func TestTable_Delete_LeafNodeRebalancing(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		numRows        = 20
		rows           = gen.MediumRows(numRows)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		leafs          = make([]*Page, 0, 5)
		aTable         = NewTable(testLogger, "foo", testMediumColumns, pagerMock, 0)
	)
	for i := range numRows {
		leafs = append(leafs, &Page{LeafNode: NewLeafNode(rowSize)})
		leafs[i].Index = uint32(i + 1)
	}

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leafs[0], nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(leafs[1], nil)
	for i := 3; i < 7; i++ {
		pagerMock.On("GetPage", mock.Anything, aTable, uint32(i)).Return(leafs[i-1], nil)
	}

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
		Fields:    columnNames(testMediumColumns...),
		Inserts:   [][]any{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	/*
		Initial state of the tree:

		           +------------------------------------------------+
		           |   2,       5,       8,         11,        14   |
		           +------------------------------------------------+
		          /       /         /        /             /         \
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
		| 0,1,2 |  | 3,4,5 |  | 6,7,8 |  | 9,10,11 |  | 12,13,14 |  | 15,16,17,18,19 |
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
	*/

	printTree(t, aTable)
	assert.Equal(t, 5, int(aRootPage.InternalNode.Header.KeysNum))

	t.Run("delete first row to force merging of first two leaves", func(t *testing.T) {
		ids := rowIDs(rows[0])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, rows[1:])

		/*
				          +----------------------------------------------+
				          |      5,        8,         11,        14      |
				          +----------------------------------------------+
				         /           /          /            /           \
			+-----------+     +-------+    +---------+    +----------+     +----------------+
			| 1,2,3,4,5 |     | 6,7,8 |    | 9,10,11 |    | 12,13,14 |     | 15,16,17,18,19 |
			+-----------+     +-------+    +---------+    +----------+     +----------------+
		*/

		printTree(t, aTable)
		assert.Equal(t, 4, int(aRootPage.InternalNode.Header.KeysNum))
		assertLeafKeys(t, leafs[0].LeafNode, 1, 2, 3, 4, 5)
		// leafs[1] has been merged into leafs[0]
		assertLeafKeys(t, leafs[2].LeafNode, 6, 7, 8)
		assertLeafKeys(t, leafs[3].LeafNode, 9, 10, 11)
		assertLeafKeys(t, leafs[4].LeafNode, 12, 13, 14)
		assertLeafKeys(t, leafs[5].LeafNode, 15, 16, 17, 18, 19)
	})

	t.Run("delete last three rows to force merging of last two leaves", func(t *testing.T) {
		ids := rowIDs(rows[17], rows[18], rows[19])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 3, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, rows[1:17])

		/*
				          +------------------------------------+
				          |      5,        8,         11,      |
				          +------------------------------------+
				         /           /          /               \
			+-----------+     +-------+    +---------+    +----------------+
			| 1,2,3,4,5 |     | 6,7,8 |    | 9,10,11 |    | 12,13,14,15,16 |
			+-----------+     +-------+    +---------+    +----------------+
		*/

		printTree(t, aTable)
		assert.Equal(t, 3, int(aRootPage.InternalNode.Header.KeysNum))
		assertLeafKeys(t, leafs[0].LeafNode, 1, 2, 3, 4, 5)
		assertLeafKeys(t, leafs[2].LeafNode, 6, 7, 8)
		assertLeafKeys(t, leafs[3].LeafNode, 9, 10, 11)
		assertLeafKeys(t, leafs[4].LeafNode, 12, 13, 14, 15, 16)
	})

	t.Run("keep deleting more rows, another merge", func(t *testing.T) {
		ids := rowIDs(rows[2], rows[4], rows[6]) //, rows[6], rows[9], rows[11], rows[13])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 3, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[3], rows[5], rows[7], rows[8],
			rows[9], rows[10], rows[11],
			rows[12], rows[13], rows[14], rows[15], rows[16],
		})

		/*
			           +----------------------------+
			           |        8,         11,      |
			           +----------------------------+
			          /              /               \
			+-----------+      +---------+      +----------------+
			| 1,3,5,7,8 |      | 9,10,11 |      | 12,13,14,15,16 |
			+-----------+      +---------+      +----------------+
		*/

		printTree(t, aTable)
		assert.Equal(t, 2, int(aRootPage.InternalNode.Header.KeysNum))
		assertLeafKeys(t, leafs[0].LeafNode, 1, 3, 5, 7, 8)
		assertLeafKeys(t, leafs[3].LeafNode, 9, 10, 11)
		assertLeafKeys(t, leafs[4].LeafNode, 12, 13, 14, 15, 16)
	})

	t.Run("keep deleting more rows, no merge", func(t *testing.T) {
		ids := rowIDs(rows[9], rows[11], rows[13], rows[15])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 4, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[3], rows[5],
			rows[7], rows[8], rows[10],
			rows[12], rows[14], rows[16],
		})

		/*
			           +----------------------------+
			           |        8,         11,      |
			           +----------------------------+
			          /              /               \
			+--------+           +--------+          +----------+
			| 1,3,5, |           | 7,8,10 |          | 12,14,16 |
			+--------+           +--------+          +----------+
		*/

		printTree(t, aTable)
		assert.Equal(t, 2, int(aRootPage.InternalNode.Header.KeysNum))
		assertLeafKeys(t, leafs[0].LeafNode, 1, 3, 5)
		assertLeafKeys(t, leafs[3].LeafNode, 7, 8, 10)
		assertLeafKeys(t, leafs[4].LeafNode, 12, 14, 16)
	})

	t.Run("keep deleting more rows, another merge and borrow", func(t *testing.T) {
		ids := rowIDs(rows[3], rows[12], rows[5])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 3, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[7], rows[8],
			rows[10], rows[14], rows[16],
		})

		/*
		           +-------------+
		           |      8      |
		           +-------------+
		          /               \
		 +-------+                +----------+
		 | 1,7,8 |                | 10,14,16 |
		 +-------+                +----------+
		*/

		printTree(t, aTable)
		assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
		assertLeafKeys(t, leafs[0].LeafNode, 1, 7, 8)
		assertLeafKeys(t, leafs[4].LeafNode, 10, 14, 16)
	})

	t.Run("delete one more time, we are left with only root leaf node", func(t *testing.T) {
		ids := rowIDs(rows[14])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[7], rows[8],
			rows[10], rows[16],
		})

		/*
		   +-----------------+
		   | 1, 7, 8, 10, 16 |
		   +-----------------+
		*/

		printTree(t, aTable)
		assert.Nil(t, aRootPage.InternalNode)
		assert.Equal(t, 5, int(aRootPage.LeafNode.Header.Cells))
		assert.Equal(t, 0, int(aRootPage.LeafNode.Header.Parent))
		assert.Equal(t, 0, int(aRootPage.LeafNode.Header.NextLeaf))
		assertLeafKeys(t, leafs[0].LeafNode, 1, 7, 8, 10, 16)
	})

	t.Run("delete all remaining rows", func(t *testing.T) {
		ids := rowIDs(rows[1], rows[7], rows[8], rows[10], rows[16])
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, ids...),
		})
		require.NoError(t, err)
		assert.Equal(t, 5, deleteResult.RowsAffected)

		checkRows(ctx, t, aTable, nil)

		printTree(t, aTable)
		assert.Equal(t, 0, int(aRootPage.LeafNode.Header.Cells))
	})
}

func TestTable_Delete_InternalNodeRebalancing(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		numRows        = 81
		rows           = gen.MediumRows(numRows)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		leafs          = make([]*Page, 0, 10)
		aTable         = NewTable(testLogger, "foo", testMediumColumns, pagerMock, 0)
	)
	aTable.maxICells = 5 // for testing purposes only, normally 340
	for i := range numRows {
		leafs = append(leafs, &Page{LeafNode: NewLeafNode(rowSize)})
		leafs[i].Index = uint32(i + 1)
	}

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	for i := range numRows {
		pagerMock.On("GetPage", mock.Anything, aTable, uint32(i+1)).Return(leafs[i], nil)
	}

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
		Fields:    columnNames(testMediumColumns...),
		Inserts:   [][]any{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	/*
		Initial state of the tree:

		           +------------------------------------------------+
		           |   2,       5,       8,         11,        14   |
		           +------------------------------------------------+
		          /       /         /        /             /         \
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
		| 0,1,2 |  | 3,4,5 |  | 6,7,8 |  | 9,10,11 |  | 12,13,14 |  | 15,16,17,18,19 |
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
	*/

	printTree(t, aTable)

	checkRows(ctx, t, aTable, rows)

	// assert.True(t, false)
	// assert.Equal(t, 5, int(aRootPage.InternalNode.Header.KeysNum))
}

func rowIDs(rows ...Row) []any {
	ids := make([]any, 0, len(rows))
	for _, r := range rows {
		id, ok := r.GetValue("id")
		if ok {
			ids = append(ids, id.(int64))
		}
	}
	return ids
}

func assertLeafKeys(t *testing.T, aLeaf *LeafNode, expectedKeys ...uint64) {
	require.Equal(t, len(expectedKeys), int(aLeaf.Header.Cells))
	for i := 0; i < len(expectedKeys); i++ {
		assert.Equal(t, int(expectedKeys[i]), int(aLeaf.Cells[i].Key))
	}
}

func checkRows(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
	selectResult, err := aTable.Select(ctx, Statement{
		Kind:      Select,
		TableName: "foo",
		Fields:    columnNames(testColumns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.(int64)] = struct{}{}
	}

	actual := make([]Row, 0, 10)
	aRow, err := selectResult.Rows(ctx)
	for ; err == nil; aRow, err = selectResult.Rows(ctx) {
		actual = append(actual, aRow)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[aRow.Values[0].(int64)]
			assert.True(t, ok)
		}
	}
	assert.Len(t, actual, len(expectedRows))
}

type callback func(page *Page)

func (t *Table) BFS(f callback) error {

	rootPage, err := t.pager.GetPage(context.Background(), t, t.RootPageIdx)
	if err != nil {
		return err
	}

	// Create a queue and enqueue the root node
	queue := make([]*Page, 0, 1)
	queue = append(queue, rootPage)

	// Repeat until queue is empty
	for len(queue) > 0 {
		// Get the first node in the queue
		current := queue[0]

		// Dequeue
		queue = queue[1:]

		f(current)

		if current.InternalNode != nil {
			for i := range current.InternalNode.Header.KeysNum {
				icell := current.InternalNode.ICells[i]
				aPage, err := t.pager.GetPage(context.Background(), t, icell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
			if current.InternalNode.Header.RightChild != RIGHT_CHILD_NOT_SET {
				aPage, err := t.pager.GetPage(context.Background(), t, current.InternalNode.Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, aPage)
			}
		}
	}

	return nil
}

func printTree(t *testing.T, aTable *Table) {
	err := aTable.BFS(func(aPage *Page) {
		if aPage.InternalNode != nil {
			fmt.Println("Internal node,", "page:", aPage.Index, "number of keys:", aPage.InternalNode.Header.KeysNum, "parent:", aPage.InternalNode.Header.Parent)
			keys := make([]uint64, 0, aPage.InternalNode.Header.KeysNum)
			for i := range aPage.InternalNode.Header.KeysNum {
				keys = append(keys, aPage.InternalNode.ICells[i].Key)
			}
			fmt.Println("Keys:", keys)

			children := make([]uint32, 0, aPage.InternalNode.Header.KeysNum)
			for i := range aPage.InternalNode.Header.KeysNum {
				children = append(children, aPage.InternalNode.ICells[i].Child)
			}
			if aPage.InternalNode.Header.RightChild != RIGHT_CHILD_NOT_SET {
				children = append(children, aPage.InternalNode.Header.RightChild)
			}
			fmt.Println("Children:", children)
		} else {
			fmt.Println("Leaf node,", "page:", aPage.Index, "number of cells:", aPage.LeafNode.Header.Cells, "parent:", aPage.LeafNode.Header.Parent, "next leaf:", aPage.LeafNode.Header.NextLeaf)
			keys := make([]uint64, 0, aPage.LeafNode.Header.Cells)
			for i := uint32(0); i < aPage.LeafNode.Header.Cells; i++ {
				keys = append(keys, aPage.LeafNode.Cells[i].Key)
			}
			fmt.Println("Keys:", keys)
		}
		fmt.Println("---------")
	})
	require.NoError(t, err)
}
