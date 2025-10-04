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
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		rows   = gen.Rows(38)
		aTable = NewTable(testLogger, testTableName, testColumns, aPager, 0)
	)

	// Set some values to NULL so we can test selecting/filtering on NULLs
	rows[5].Values[2] = OptionalValue{Valid: false}
	rows[21].Values[5] = OptionalValue{Valid: false}
	rows[32].Values[2] = OptionalValue{Valid: false}

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

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: columnNames(testColumns...),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Equal(t, rows, actual)
	})

	t.Run("Select no rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: columnNames(testColumns...),
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

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Empty(t, actual)
	})

	t.Run("Select single row", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: columnNames(testColumns...),
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
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select rows with NULL values when there are none", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     columnNames(testColumns...),
			Conditions: FieldIsNull("id"),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Empty(t, actual)
	})

	t.Run("Select rows with NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     columnNames(testColumns...),
			Conditions: FieldIsNull("age"),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		// rows[5] and rows[32] have NULL age values
		assert.Len(t, actual, 2)
		assert.Equal(t, []Row{rows[5], rows[32]}, actual)
	})

	t.Run("Select rows with NOT NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     columnNames(testColumns...),
			Conditions: FieldIsNotNull("age"),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		// rows[5] and rows[32] have NULL age values, so exclude them
		expected := append(rows[0:5], append(rows[6:32], rows[33:]...)...)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})
}
