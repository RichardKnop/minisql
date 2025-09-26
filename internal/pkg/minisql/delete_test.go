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

		checkRowsAfterDeletion(ctx, t, aTable, rows[1:])
	})

	t.Run("delete all rows", func(t *testing.T) {
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:      Delete,
			TableName: "foo",
		})
		require.NoError(t, err)
		assert.Equal(t, 4, deleteResult.RowsAffected)

		checkRowsAfterDeletion(ctx, t, aTable, nil)
	})
}

func TestTable_Delete_RootInternalNode_SecondLevelLeafs(t *testing.T) {
	t.Parallel()
	t.Skip()

	/*
		In this test we will be deleting from slightly less trivial tree with
		root internal node and 2nd level leaf nodes.

		           +------------------------------------------------+
		           | 2,     5,        8,       11,          14      |
		           +------------------------------------------------+
		          /       /         /        /             /         \
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
		| 0,1,2 |  | 3,4,5 |  | 6,7,8 |  | 9,10,11 |  | 12,13,14 |  | 15,16,17,18,19 |
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
	*/

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		numRows        = 20
		rows           = gen.MediumRows(numRows)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		leafs          = make([]*Page, 0, 5)
		aTable         = NewTable(testLogger, "foo", testMediumColumns, pagerMock, 0)
		// These two pages will be returned as leafs by the pager as default behaviour
		// for allocating a new page but will be converted to internal nodes
		// aNewRightInternal = &Page{LeafNode: NewLeafNode(rowSize)}
		// aNewLeftInternal  = &Page{LeafNode: NewLeafNode(rowSize)}
	)
	for i := 0; i < numRows; i++ {
		leafs = append(leafs, &Page{LeafNode: NewLeafNode(rowSize)})
	}

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leafs[0], nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(leafs[1], nil)
	for i := 3; i < 7; i++ {
		pagerMock.On("GetPage", mock.Anything, aTable, uint32(i)).Return(leafs[i-1], nil)
	}
	// // Splitting root internal node causes 2 more pages to be requested, one for
	// // sibling internal node, one for new root node
	// pagerMock.On("GetPage", mock.Anything, aTable, uint32(5)).Return(aNewRightInternal, nil)
	// pagerMock.On("GetPage", mock.Anything, aTable, uint32(6)).Return(aNewLeftInternal, nil)

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

	printTree(t, aTable)

	t.Run("delete first row", func(t *testing.T) {
		id1, ok := rows[0].GetValue("id")
		require.True(t, ok)
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, id1.(int64)),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		checkRowsAfterDeletion(ctx, t, aTable, rows[1:])
	})

	printTree(t, aTable)

	t.Run("delete second row", func(t *testing.T) {
		id1, ok := rows[1].GetValue("id")
		require.True(t, ok)
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, id1.(int64)),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		checkRowsAfterDeletion(ctx, t, aTable, rows[2:])
	})

	printTree(t, aTable)

	t.Run("delete third row", func(t *testing.T) {
		id1, ok := rows[2].GetValue("id")
		require.True(t, ok)
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:       Delete,
			TableName:  "foo",
			Conditions: FieldIsIn("id", Integer, id1.(int64)),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		// checkRowsAfterDeletion(ctx, t, aTable, rows[3:])
	})

	printTree(t, aTable)

	assert.True(t, false)
}

func checkRowsAfterDeletion(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
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
			aPage, err := t.pager.GetPage(context.Background(), t, current.InternalNode.Header.RightChild)
			if err != nil {
				return err
			}
			queue = append(queue, aPage)
		}
	}

	return nil
}

func printTree(t *testing.T, aTable *Table) {
	err := aTable.BFS(func(aPage *Page) {
		if aPage.InternalNode != nil {
			fmt.Println("Internal node, number of keys:", aPage.InternalNode.Header.KeysNum, "parent:", aPage.InternalNode.Header.Parent)
			keys := make([]uint64, 0, aPage.InternalNode.Header.KeysNum)
			for i := range aPage.InternalNode.Header.KeysNum {
				keys = append(keys, aPage.InternalNode.ICells[i].Key)
			}
			fmt.Println("Keys:", keys)
		} else {
			fmt.Println("Leaf node, number of cells:", aPage.LeafNode.Header.Cells, "parent:", aPage.LeafNode.Header.Parent, "next leaf:", aPage.LeafNode.Header.NextLeaf)
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
