package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_Select(t *testing.T) {
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
		aTable         = NewTable(testLogger, "foo", testColumns, pagerMock, 0)
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

	// Batch insert test rows
	insertStmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testColumns...),
		Inserts:   [][]any{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err := aTable.Insert(ctx, insertStmt)
	require.NoError(t, err)

	testCases := []struct {
		Name     string
		Stmt     Statement
		Expected []Row
	}{
		{
			"Select all rows",
			Statement{
				Kind:      Select,
				TableName: "foo",
				Fields:    columnNames(testColumns...),
			},
			rows,
		},
		{
			"Select no rows",
			Statement{
				Kind:      Select,
				TableName: "foo",
				Fields:    columnNames(testColumns...),
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{
								Type:  Field,
								Value: "email",
							},
							Operator: Eq,
							Operand2: Operand{
								Type:  QuotedString,
								Value: "bogus",
							},
						},
					},
				},
			},
			[]Row{},
		},
		{
			"Select single row",
			Statement{
				Kind:      Select,
				TableName: "foo",
				Fields:    columnNames(testColumns...),
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
								Value: rows[5].Values[0].(int64),
							},
						},
					},
				},
			},
			[]Row{rows[5]},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aResult, err := aTable.Select(ctx, aTestCase.Stmt)
			require.NoError(t, err)

			// Use iterator to collect all rows
			actual := []Row{}
			aRow, err := aResult.Rows(ctx)
			for ; err == nil; aRow, err = aResult.Rows(ctx) {
				actual = append(actual, aRow)
			}

			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}
