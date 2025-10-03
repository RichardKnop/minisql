package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Update(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
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
		Columns:   aTable.Columns,
		Fields:    columnNames(testColumns...),
		Inserts:   [][]OptionalValue{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, insertStmt)
	require.NoError(t, err)

	// Prepare expected rows for test cases
	singleUpdatedRow := make([]Row, 0, len(rows))
	for i, aRow := range rows {
		expectedRow := aRow.Clone()
		if i == 5 {
			expectedRow.SetValue("email", OptionalValue{Value: "updatedsingle@foo.bar", Valid: true})
		}

		singleUpdatedRow = append(singleUpdatedRow, expectedRow)
	}

	allUpdatedRows := make([]Row, 0, len(rows))
	for _, aRow := range rows {
		expectedRow := aRow.Clone()
		expectedRow.SetValue("email", OptionalValue{Value: "updatedall@foo.bar", Valid: true})
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
				Updates: map[string]OptionalValue{
					"email": {Value: "updatedsingle@foo.bar", Valid: true},
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
								Value: rows[5].Values[0].Value.(int64),
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
				Updates: map[string]OptionalValue{
					"email": {Value: "updatedall@foo.bar", Valid: true},
				},
			},
			38,
			allUpdatedRows,
		},
		{
			"Update no rows",
			Statement{
				Kind:      Update,
				TableName: "foo",
				Updates: map[string]OptionalValue{
					"email": {Value: "updatednone@foo.bar", Valid: true},
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
