package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_Update(t *testing.T) {
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

	// Prepare expected rows for test cases
	singleUpdatedRow := make([]Row, 0, len(rows))
	for i, aRow := range rows {
		expectedRow := aRow.Clone()
		if i == 5 {
			expectedRow.SetValue("email", "updatedsingle@foo.bar")
		}

		singleUpdatedRow = append(singleUpdatedRow, expectedRow)
	}

	allUpdatedRows := make([]Row, 0, len(rows))
	for _, aRow := range rows {
		expectedRow := aRow.Clone()
		expectedRow.SetValue("email", "updatedall@foo.bar")
		allUpdatedRows = append(allUpdatedRows, expectedRow)
	}

	testCases := []struct {
		Name         string
		Stmt         Statement
		RowsAffected int
		Expected     []Row
	}{
		{
			"Update single row",
			Statement{
				Kind:      Update,
				TableName: "foo",
				Updates: map[string]any{
					"email": "updatedsingle@foo.bar",
				},
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
			1,
			singleUpdatedRow,
		},
		{
			"Update all rows",
			Statement{
				Kind:      Update,
				TableName: "foo",
				Updates: map[string]any{
					"email": "updatedall@foo.bar",
				},
			},
			38,
			allUpdatedRows,
		},
		{
			"Update now rows",
			Statement{
				Kind:      Update,
				TableName: "foo",
				Updates: map[string]any{
					"email": "updatednone@foo.bar",
				},
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
			0,
			allUpdatedRows,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aResult, err := aTable.Update(ctx, aTestCase.Stmt)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.RowsAffected, aResult.RowsAffected)

			aResult, err = aTable.Select(ctx, Statement{
				Kind:      Select,
				TableName: "foo",
				Fields:    columnNames(testColumns...),
			})
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
