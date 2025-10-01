package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Select(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, "minisql_main")
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		rows   = gen.Rows(38)
		aTable = NewTable(testLogger, "foo", testColumns, aPager, 0)
	)

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

	err = aTable.Insert(ctx, insertStmt)
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
