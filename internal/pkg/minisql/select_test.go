package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_Select_SplitRootLeaf(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		rows           = gen.Rows(15)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
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

	// Insert test rows
	for _, aRow := range rows {
		stmt := Statement{
			Kind:      Insert,
			TableName: "foo",
			Fields:    []string{"id", "email", "age"},
			Inserts:   [][]any{aRow.Values},
		}

		err := aTable.Insert(ctx, stmt)
		require.NoError(t, err)
	}

	// Select all rows
	stmt := Statement{
		Kind:      Select,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
	}
	aResult, err := aTable.Select(ctx, stmt)

	require.NoError(t, err)
	assert.Equal(t, aTable.Columns, aResult.Columns)

	aRow, err := aResult.Rows(ctx)
	i := 0
	for ; err == nil; aRow, err = aResult.Rows(ctx) {
		assert.Equal(t, rows[i], aRow)
		i += 1
	}
}

func TestTable_Select_LeafNodeInsert(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		pagerMock      = new(MockPager)
		rows           = gen.Rows(38)
		cells, rowSize = 0, rows[0].Size()
		aRootPage      = newRootLeafPageWithCells(cells, int(rowSize))
		leaf1          = &Page{LeafNode: NewLeafNode(rowSize)}
		leaf2          = &Page{LeafNode: NewLeafNode(rowSize)}
		leaf3          = &Page{LeafNode: NewLeafNode(rowSize)}
		leaf4          = &Page{LeafNode: NewLeafNode(rowSize)}
		aTable         = NewTable("foo", testColumns, pagerMock, 0)
	)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(aRootPage, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(1)).Return(leaf2, nil)
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(leaf1, nil)
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
			Fields:    []string{"id", "email", "age"},
			Inserts:   [][]any{aRow.Values},
		}

		err := aTable.Insert(ctx, stmt)
		require.NoError(t, err)
	}

	// Select all rows
	stmt := Statement{
		Kind:      Select,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
	}
	aResult, err := aTable.Select(ctx, stmt)

	require.NoError(t, err)
	assert.Equal(t, aTable.Columns, aResult.Columns)

	aRow, err := aResult.Rows(ctx)
	i := 0
	for ; err == nil; aRow, err = aResult.Rows(ctx) {
		assert.Equal(t, rows[i], aRow)
		i += 1
	}
}
