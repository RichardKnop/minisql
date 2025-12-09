package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Select(t *testing.T) {
	aPager := initTest(t)

	var (
		ctx        = context.Background()
		rows       = gen.Rows(38)
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumns, 0)
	)

	// Set some values to NULL so we can test selecting/filtering on NULLs
	rows[5].Values[2] = OptionalValue{Valid: false}
	rows[21].Values[5] = OptionalValue{Valid: false}
	rows[32].Values[2] = OptionalValue{Valid: false}

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, insertStmt)
	}, aPager)
	require.NoError(t, err)

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
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

	t.Run("Select with LIMIT", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Limit:  OptionalValue{Value: int64(10), Valid: true},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Equal(t, rows[0:10], actual)
	})

	t.Run("Select with OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Offset: OptionalValue{Value: int64(10), Valid: true},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Equal(t, rows[10:], actual)
	})

	t.Run("Select with LIMIT and OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Limit:  OptionalValue{Value: int64(5), Valid: true},
			Offset: OptionalValue{Value: int64(10), Valid: true},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		assert.Equal(t, rows[10:15], actual)
	})

	t.Run("Select no rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
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
		id := rowIDs(rows[5])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: id,
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
			Fields:     fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{{FieldIsNull("id")}},
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
			Fields:     fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{{FieldIsNull("age")}},
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
			Fields:     fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{{FieldIsNotNull("age")}},
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
		expected := make([]Row, 0, len(rows)-2)
		for i, aRow := range rows {
			if i == 5 || i == 32 {
				continue
			}
			expected = append(expected, aRow)
		}
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select only some columns", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "id"}, {Name: "verified"}},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		expected := make([]Row, 0, len(rows))
		// Since we are only selecting id, name, filter out other columns and values
		for _, aRow := range rows {
			expected = append(expected, Row{
				Key:     aRow.Key,
				Columns: []Column{aRow.Columns[0], aRow.Columns[3]},
				Values:  []OptionalValue{aRow.Values[0], aRow.Values[3]},
			})
		}
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
		assert.Equal(t, []Column{testColumns[0], testColumns[3]}, aResult.Columns)
	})

	t.Run("Select only some columns with where condtition on unselected column", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "id"}, {Name: "email"}},
			Conditions: OneOrMore{{FieldIsNotNull("age")}},
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
		expected := make([]Row, 0, len(rows)-2)
		for i, aRow := range rows {
			if i == 5 || i == 32 {
				continue
			}
			// Since we are only selecting id, email, filter out other columns and values
			expectedRow := Row{
				Key:     aRow.Key,
				Columns: []Column{aRow.Columns[0], aRow.Columns[1]},
				Values:  []OptionalValue{aRow.Values[0], aRow.Values[1]},
			}
			expected = append(expected, expectedRow)

		}
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
		assert.Equal(t, []Column{testColumns[0], testColumns[1]}, aResult.Columns)
	})
}

func TestTable_Select_Overflow(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testOverflowColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testOverflowColumns, 0)
		rows   = gen.OverflowRows(3, []uint32{
			MaxInlineVarchar,          // inline text
			MaxInlineVarchar + 100,    // text overflows to 1 page
			MaxOverflowPageData + 100, // text overflows to multiple pages
		})
	)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testOverflowColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, insertStmt)
	}, aPager)
	require.NoError(t, err)

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testOverflowColumns...),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// Use iterator to collect all rows
		actual := []Row{}
		aRow, err := aResult.Rows(ctx)
		for ; err == nil; aRow, err = aResult.Rows(ctx) {
			actual = append(actual, aRow)
		}

		// Set expected first overflow pages on rows
		overflow1, _ := rows[1].GetValue("profile")
		tp1 := overflow1.Value.(TextPointer)
		tp1.FirstPage = 1
		overflow1.Value = tp1
		rows[1].SetValue("profile", overflow1)

		overflow2, _ := rows[2].GetValue("profile")
		tp2 := overflow2.Value.(TextPointer)
		tp2.FirstPage = 2
		overflow2.Value = tp2
		rows[2].SetValue("profile", overflow2)

		// And now we can assert
		assert.Equal(t, rows, actual)
	})
}
