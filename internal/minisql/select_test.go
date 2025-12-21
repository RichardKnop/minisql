package minisql

import (
	"context"
	"sort"
	"strings"
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

		assert.Equal(t, rows, aResult.CollectRows(ctx))
	})

	t.Run("Select with LIMIT", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Limit:  OptionalValue{Value: int64(10), Valid: true},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[0:10], aResult.CollectRows(ctx))
	})

	t.Run("Select with OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			Offset: OptionalValue{Value: int64(10), Valid: true},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[10:], aResult.CollectRows(ctx))
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

		assert.Equal(t, rows[10:15], aResult.CollectRows(ctx))
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
							Value: NewTextPointer([]byte("bogus")),
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Empty(t, aResult.CollectRows(ctx))
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

		actual := aResult.CollectRows(ctx)
		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select multiple rows with IN", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
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
						Operator: In,
						Operand2: Operand{
							Type:  OperandList,
							Value: ids,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, aRow := range rows {
			if i != 5 && i != 11 && i != 12 && i != 33 {
				continue
			}
			expected = append(expected, aRow)
		}
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Select multiple rows with NOT IN", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
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
						Operator: NotIn,
						Operand2: Operand{
							Type:  OperandList,
							Value: ids,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows other than 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, aRow := range rows {
			if i == 5 || i == 11 || i == 12 || i == 33 {
				continue
			}
			expected = append(expected, aRow)
		}
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Select rows with NULL values when there are none", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{{FieldIsNull("id")}},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Empty(t, aResult.CollectRows(ctx))
	})

	t.Run("Select rows with NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     fieldsFromColumns(testColumns...),
			Conditions: OneOrMore{{FieldIsNull("age")}},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// rows[5] and rows[32] have NULL age values
		actual := aResult.CollectRows(ctx)
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

		// rows[5] and rows[32] have NULL age values, so exclude them
		expected := make([]Row, 0, len(rows)-2)
		for i, aRow := range rows {
			if i == 5 || i == 32 {
				continue
			}
			expected = append(expected, aRow)
		}
		actual := aResult.CollectRows(ctx)
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

		// Since we are only selecting id, name, filter out other columns and values
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expected = append(expected, Row{
				Key:     aRow.Key,
				Columns: []Column{aRow.Columns[0], aRow.Columns[3]},
				Values:  []OptionalValue{aRow.Values[0], aRow.Values[3]},
			})
		}
		actual := aResult.CollectRows(ctx)
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
		actual := aResult.CollectRows(ctx)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
		assert.Equal(t, []Column{testColumns[0], testColumns[1]}, aResult.Columns)
	})

	t.Run("Select with order by sort in memory asc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "email"},
					Direction: Asc,
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by email ascending
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expected = append(expected, aRow)
		}
		sort.Slice(expected, func(i, j int) bool {
			email1, _ := expected[i].GetValue("email")
			email2, _ := expected[j].GetValue("email")
			return strings.Compare(email1.Value.(TextPointer).String(), email2.Value.(TextPointer).String()) < 0
		})
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Select with order by sort in memory desc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "email"},
					Direction: Desc,
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by email descending
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expected = append(expected, aRow)
		}
		sort.Slice(expected, func(i, j int) bool {
			email1, _ := expected[i].GetValue("email")
			email2, _ := expected[j].GetValue("email")
			return strings.Compare(email1.Value.(TextPointer).String(), email2.Value.(TextPointer).String()) > 0
		})
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Count all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "COUNT(*)"}},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)
		assert.Equal(t, len(rows), int(aResult.Count))
		assert.Nil(t, aResult.Rows)
	})

	t.Run("Count rows with condition", func(t *testing.T) {
		// Pick one of middle IDs and prepared expected count
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expected = append(expected, aRow)
		}
		sort.Slice(expected, func(i, j int) bool {
			id1, _ := expected[i].GetValue("id")
			id2, _ := expected[j].GetValue("id")
			return id1.Value.(int64) < id2.Value.(int64)
		})
		var (
			middleID      = expected[10].Values[0].Value.(int64)
			expectedCount int64
		)
		for _, aRow := range expected {
			idVal, _ := aRow.GetValue("id")
			if idVal.Value.(int64) > middleID {
				expectedCount += 1
			}
		}

		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "COUNT(*)"}},
			Conditions: OneOrMore{{FieldIsGreater("id", OperandInteger, middleID)}},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)
		assert.Equal(t, expectedCount, aResult.Count)
		assert.Nil(t, aResult.Rows)
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
		assert.Equal(t, rows, aResult.CollectRows(ctx))
	})
}
