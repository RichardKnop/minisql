package minisql

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Select(t *testing.T) {
	table, txManager, _ := newTestTable(t, testColumns)
	var (
		ctx  = context.Background()
		rows = gen.Rows(38)
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
	for _, row := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, row.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, insertStmt)
		return err
	})
	require.NoError(t, err)

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows, collectRows(ctx, result))
	})

	t.Run("Select with LIMIT", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Limit:  OptionalValue{Value: int64(10), Valid: true},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[0:10], collectRows(ctx, result))
	})

	t.Run("Select with OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Offset: OptionalValue{Value: int64(10), Valid: true},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[10:], collectRows(ctx, result))
	})

	t.Run("Select with LIMIT and OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Limit:  OptionalValue{Value: int64(5), Valid: true},
			Offset: OptionalValue{Value: int64(10), Valid: true},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[10:15], collectRows(ctx, result))
	})

	t.Run("Select no rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "email"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Empty(t, collectRows(ctx, result))
	})

	t.Run("Select single row", func(t *testing.T) {
		id := rowIDs(rows[5])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		actual := collectRows(ctx, result)
		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select multiple rows with IN", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, row := range rows {
			if i != 5 && i != 11 && i != 12 && i != 33 {
				continue
			}
			expected = append(expected, row)
		}
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select multiple rows with NOT IN", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows other than 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, row := range rows {
			if i == 5 || i == 11 || i == 12 || i == 33 {
				continue
			}
			expected = append(expected, row)
		}
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select rows with NULL values when there are none", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{{FieldIsNull(Field{Name: "id"})}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Empty(t, collectRows(ctx, result))
	})

	t.Run("Select rows with NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{{FieldIsNull(Field{Name: "age"})}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// rows[5] and rows[32] have NULL age values
		actual := collectRows(ctx, result)
		assert.Len(t, actual, 2)
		assert.Equal(t, []Row{rows[5], rows[32]}, actual)
	})

	t.Run("Select rows with NOT NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{{FieldIsNotNull(Field{Name: "age"})}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// rows[5] and rows[32] have NULL age values, so exclude them
		expected := make([]Row, 0, len(rows)-2)
		for i, row := range rows {
			if i == 5 || i == 32 {
				continue
			}
			expected = append(expected, row)
		}
		actual := collectRows(ctx, result)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select only some columns", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "id"}, {Name: "verified"}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// Since we are only selecting id, name, filter out other columns and values
		expected := make([]Row, 0, len(rows))
		for _, row := range rows {
			expectedRow := NewRowWithValues(
				[]Column{row.Columns[0], row.Columns[3]},
				[]OptionalValue{row.Values[0], row.Values[3]},
			)
			expectedRow.Key = row.Key
			expected = append(expected, expectedRow)
		}
		actual := collectRows(ctx, result)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
		assert.Equal(t, []Column{table.Columns[0], table.Columns[3]}, result.Columns)
	})

	t.Run("Select only some columns with where condtition on unselected column", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "id"}, {Name: "email"}},
			Conditions: OneOrMore{{FieldIsNotNull(Field{Name: "age"})}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// rows[5] and rows[32] have NULL age values, so exclude them
		expected := make([]Row, 0, len(rows)-2)
		for i, row := range rows {
			if i == 5 || i == 32 {
				continue
			}
			// Since we are only selecting id, email, filter out other columns and values
			expectedRow := NewRowWithValues(
				[]Column{row.Columns[0], row.Columns[1]},
				[]OptionalValue{row.Values[0], row.Values[1]},
			)
			expectedRow.Key = row.Key
			expected = append(expected, expectedRow)

		}
		actual := collectRows(ctx, result)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
		assert.Equal(t, []Column{testColumns[0], testColumns[1]}, result.Columns)
	})

	t.Run("Select with order by sort in memory asc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "email"},
					Direction: Asc,
				},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by email ascending
		expected := make([]Row, 0, len(rows))
		expected = append(expected, rows...)
		sort.Slice(expected, func(i, j int) bool {
			email1, _ := expected[i].GetValue("email")
			email2, _ := expected[j].GetValue("email")
			return email1.Value.(TextPointer).String() < email2.Value.(TextPointer).String()
		})
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select with order by sort in memory desc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "email"},
					Direction: Desc,
				},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by email descending
		expected := make([]Row, 0, len(rows))
		expected = append(expected, rows...)
		sort.Slice(expected, func(i, j int) bool {
			email1, _ := expected[i].GetValue("email")
			email2, _ := expected[j].GetValue("email")
			return email1.Value.(TextPointer).String() > email2.Value.(TextPointer).String()
		})
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select with multi-column order by (age ASC, email ASC)", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{Field: Field{Name: "age"}, Direction: Asc},
				{Field: Field{Name: "email"}, Direction: Asc},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		expected := make([]Row, len(rows))
		copy(expected, rows)
		sort.SliceStable(expected, func(i, j int) bool {
			ageI, _ := expected[i].GetValue("age")
			ageJ, _ := expected[j].GetValue("age")
			cmp := compareValues(ageI, ageJ)
			if cmp != 0 {
				return cmp < 0
			}
			emailI, _ := expected[i].GetValue("email")
			emailJ, _ := expected[j].GetValue("email")
			return emailI.Value.(TextPointer).String() < emailJ.Value.(TextPointer).String()
		})

		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select with multi-column order by (age ASC, email DESC)", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{Field: Field{Name: "age"}, Direction: Asc},
				{Field: Field{Name: "email"}, Direction: Desc},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		expected := make([]Row, len(rows))
		copy(expected, rows)
		sort.SliceStable(expected, func(i, j int) bool {
			ageI, _ := expected[i].GetValue("age")
			ageJ, _ := expected[j].GetValue("age")
			cmp := compareValues(ageI, ageJ)
			if cmp != 0 {
				return cmp < 0
			}
			emailI, _ := expected[i].GetValue("email")
			emailJ, _ := expected[j].GetValue("email")
			return emailI.Value.(TextPointer).String() > emailJ.Value.(TextPointer).String()
		})

		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select with three-column order by", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{Field: Field{Name: "age"}, Direction: Asc},
				{Field: Field{Name: "verified"}, Direction: Desc},
				{Field: Field{Name: "email"}, Direction: Asc},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		expected := make([]Row, len(rows))
		copy(expected, rows)
		sort.SliceStable(expected, func(i, j int) bool {
			ageI, _ := expected[i].GetValue("age")
			ageJ, _ := expected[j].GetValue("age")
			if cmp := compareValues(ageI, ageJ); cmp != 0 {
				return cmp < 0
			}
			verI, _ := expected[i].GetValue("verified")
			verJ, _ := expected[j].GetValue("verified")
			if cmp := compareValues(verI, verJ); cmp != 0 {
				return cmp > 0 // DESC
			}
			emailI, _ := expected[i].GetValue("email")
			emailJ, _ := expected[j].GetValue("email")
			return emailI.Value.(TextPointer).String() < emailJ.Value.(TextPointer).String()
		})

		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Count all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "COUNT(*)"}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, []Row{
			NewRowWithValues(
				[]Column{{Name: "COUNT(*)"}},
				[]OptionalValue{{Value: int64(len(rows)), Valid: true}},
			),
		}, collectRows(ctx, result))
	})

	t.Run("Count rows with condition", func(t *testing.T) {
		// Pick one of middle IDs and prepared expected count
		expected := make([]Row, 0, len(rows))
		expected = append(expected, rows...)
		sort.Slice(expected, func(i, j int) bool {
			id1, _ := expected[i].GetValue("id")
			id2, _ := expected[j].GetValue("id")
			return id1.Value.(int64) < id2.Value.(int64)
		})
		var (
			middleID      = expected[10].Values[0].Value.(int64)
			expectedCount int64
		)
		for _, row := range expected {
			idVal, _ := row.GetValue("id")
			if idVal.Value.(int64) > middleID {
				expectedCount += 1
			}
		}

		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "COUNT(*)"}},
			Conditions: OneOrMore{{FieldIsGreater(Field{Name: "id"}, OperandInteger, middleID)}},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, []Row{
			NewRowWithValues(
				[]Column{{Name: "COUNT(*)"}},
				[]OptionalValue{{Value: int64(expectedCount), Valid: true}},
			),
		}, collectRows(ctx, result))
	})
}

func TestCompileScanFilter(t *testing.T) {
	t.Parallel()

	t.Run("nil when no filters", func(t *testing.T) {
		f := compileScanFilter(testColumns, nil)
		assert.Nil(t, f)
	})

	t.Run("evaluates with precompiled column indexes", func(t *testing.T) {
		row := gen.Row()
		filters := OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: row.Values[0].Value.(int64)},
				},
			},
		}
		f := compileScanFilter(testColumns, filters)
		require.NotNil(t, f)

		ok, err := f(row)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestTable_Select_Overflow(t *testing.T) {
	table, txManager, _ := newTestTable(t, testOverflowColumns)
	var (
		ctx  = context.Background()
		rows = gen.OverflowRows(3, []uint32{
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
	for _, row := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, row.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, insertStmt)
		return err
	})
	require.NoError(t, err)

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testOverflowColumns...),
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// Set expected first overflow pages on rows
		overflow1, _ := rows[1].GetValue("profile")
		tp1 := overflow1.Value.(TextPointer)
		tp1.FirstPage = 1
		overflow1.Value = tp1
		rows[1], _ = rows[1].SetValue("profile", overflow1)

		overflow2, _ := rows[2].GetValue("profile")
		tp2 := overflow2.Value.(TextPointer)
		tp2.FirstPage = 2
		overflow2.Value = tp2
		rows[2], _ = rows[2].SetValue("profile", overflow2)

		// And now we can assert
		assert.Equal(t, rows, collectRows(ctx, result))
	})
}

func collectRows(ctx context.Context, r StatementResult) []Row {
	results := []Row{}
	for r.Rows.Next(ctx) {
		results = append(results, r.Rows.Row())
	}
	if err := r.Rows.Err(); err != nil {
		panic(err)
	}
	return results
}
