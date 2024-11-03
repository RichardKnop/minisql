package minisql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_Delete(t *testing.T) {
	t.Parallel()

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

	// assert.True(t, false)
}
