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
		In this test we will test deleting from a root leaf node only tree.
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
			Kind:      Delete,
			TableName: "foo",
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  Field,
							Value: "id",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  Integer,
							Value: id.(int64), // delete first row
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, deleteResult.RowsAffected)

		selectResult, err := aTable.Select(ctx, Statement{
			Kind:      Select,
			TableName: "foo",
			Fields:    columnNames(testColumns...),
		})
		require.NoError(t, err)

		actual := []Row{}
		aRow, err := selectResult.Rows(ctx)
		for ; err == nil; aRow, err = selectResult.Rows(ctx) {
			actual = append(actual, aRow)
			assert.NotEqual(t, id, aRow.Values[0].(int64))
		}
		assert.Len(t, actual, numRows-1)
	})

	t.Run("delete all rows", func(t *testing.T) {
		deleteResult, err := aTable.Delete(ctx, Statement{
			Kind:      Delete,
			TableName: "foo",
		})
		require.NoError(t, err)
		assert.Equal(t, 4, deleteResult.RowsAffected)

		selectResult, err := aTable.Select(ctx, Statement{
			Kind:      Select,
			TableName: "foo",
			Fields:    columnNames(testColumns...),
		})
		require.NoError(t, err)

		actual := []Row{}
		aRow, err := selectResult.Rows(ctx)
		for ; err == nil; aRow, err = selectResult.Rows(ctx) {
			actual = append(actual, aRow)
		}
		assert.Empty(t, actual)
	})
}

func TestTable_Delete(t *testing.T) {
	t.Parallel()
	t.Skip()

	/*
		In this test we first need to create a big enough tree to have root node,
		2 internal nodes and many leaf nodes. This way we can use different deletion
		use cases.
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

	fmt.Println(aRootPage.InternalNode.Header.KeysNum, len(aRootPage.InternalNode.ICells))
	fmt.Println(aRootPage.InternalNode.ICells[0].Key)
	fmt.Println(aRootPage.InternalNode.ICells[1].Key)
	fmt.Println(aRootPage.InternalNode.ICells[2].Key)
	fmt.Println(aRootPage.InternalNode.ICells[3].Key)
	fmt.Println(aRootPage.InternalNode.ICells[4].Key)
	fmt.Println("---------")

	fmt.Println(leafs[0].LeafNode.Header.Cells, len(leafs[0].LeafNode.Cells))
	fmt.Println(leafs[0].LeafNode.Cells[0].Key)
	fmt.Println(leafs[0].LeafNode.Cells[1].Key)
	fmt.Println(leafs[0].LeafNode.Cells[2].Key)
	fmt.Println("---------")

	fmt.Println(leafs[1].LeafNode.Header.Cells, len(leafs[1].LeafNode.Cells))
	fmt.Println(leafs[1].LeafNode.Cells[0].Key)
	fmt.Println(leafs[1].LeafNode.Cells[1].Key)
	fmt.Println(leafs[1].LeafNode.Cells[2].Key)
	fmt.Println("---------")

	fmt.Println(leafs[2].LeafNode.Header.Cells, len(leafs[2].LeafNode.Cells))
	fmt.Println(leafs[2].LeafNode.Cells[0].Key)
	fmt.Println(leafs[2].LeafNode.Cells[1].Key)
	fmt.Println(leafs[2].LeafNode.Cells[2].Key)
	fmt.Println("---------")

	fmt.Println(leafs[3].LeafNode.Header.Cells, len(leafs[3].LeafNode.Cells))
	fmt.Println(leafs[3].LeafNode.Cells[0].Key)
	fmt.Println(leafs[3].LeafNode.Cells[1].Key)
	fmt.Println(leafs[3].LeafNode.Cells[2].Key)
	fmt.Println("---------")

	fmt.Println(leafs[4].LeafNode.Header.Cells, len(leafs[4].LeafNode.Cells))
	fmt.Println(leafs[4].LeafNode.Cells[0].Key)
	fmt.Println(leafs[4].LeafNode.Cells[1].Key)
	fmt.Println(leafs[4].LeafNode.Cells[2].Key)
	fmt.Println("---------")

	fmt.Println(leafs[5].LeafNode.Header.Cells, len(leafs[5].LeafNode.Cells))
	fmt.Println(leafs[5].LeafNode.Cells[0].Key)
	fmt.Println(leafs[5].LeafNode.Cells[1].Key)
	fmt.Println(leafs[5].LeafNode.Cells[2].Key)
	fmt.Println(leafs[5].LeafNode.Cells[3].Key)
	fmt.Println(leafs[5].LeafNode.Cells[4].Key)

	assert.True(t, false)
}
