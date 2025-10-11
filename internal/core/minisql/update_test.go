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
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := aPager.ForTable(Row{Columns: testColumns}.Size())

	var (
		ctx    = context.Background()
		rows   = gen.Rows(38)
		aTable = NewTable(testLogger, testTableName, testColumns, tablePager, 0)
	)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, insertStmt)
	require.NoError(t, err)

	t.Run("Update no rows", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
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
		}

		aResult, err := aTable.Update(ctx, stmt)
		require.NoError(t, err)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
	})

	t.Run("Update single row", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: "updatedsingle@foo.bar", Valid: true},
			},
			Conditions: FieldIsIn("id", Integer, rows[5].Values[0].Value.(int64)),
		}

		aResult, err := aTable.Update(ctx, stmt)
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		expected := make([]Row, 0, len(rows))
		for i, aRow := range rows {
			expectedRow := aRow.Clone()
			if i == 5 {
				expectedRow.SetValue("email", OptionalValue{Value: "updatedsingle@foo.bar", Valid: true})
			}

			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, aTable, expected)
	})

	t.Run("Update single row, set column to NULL", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Valid: false},
			},
			Conditions: FieldIsIn("id", Integer, rows[18].Values[0].Value.(int64)),
		}

		aResult, err := aTable.Update(ctx, stmt)
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		expected := make([]Row, 0, len(rows))
		for i, aRow := range rows {
			expectedRow := aRow.Clone()
			if i == 5 {
				expectedRow.SetValue("email", OptionalValue{Value: "updatedsingle@foo.bar", Valid: true})
			}
			if i == 18 {
				expectedRow.SetValue("email", OptionalValue{Valid: false})
			}

			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, aTable, expected)
	})

	t.Run("Update all rows", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: "updatedall@foo.bar", Valid: true},
			},
		}

		aResult, err := aTable.Update(ctx, stmt)
		require.NoError(t, err)
		assert.Equal(t, 38, aResult.RowsAffected)

		// Prepare expected rows with all rows updated
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expectedRow := aRow.Clone()
			expectedRow.SetValue("email", OptionalValue{Value: "updatedall@foo.bar", Valid: true})
			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, aTable, expected)
	})
}
