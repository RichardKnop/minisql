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
	rows[5].Values[2] = MakeNull()
	rows[21].Values[5] = MakeNull()
	rows[32].Values[2] = MakeNull()

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
			Limit:  MakeInt8(int64(10)),
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[0:10], collectRows(ctx, result))
	})

	t.Run("Select with OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Offset: MakeInt8(int64(10)),
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows[10:], collectRows(ctx, result))
	})

	t.Run("Select with LIMIT and OFFSET", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Limit:  MakeInt8(int64(5)),
			Offset: MakeInt8(int64(10)),
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
			return email1.AsTextPointer().String() < email2.AsTextPointer().String()
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
			return email1.AsTextPointer().String() > email2.AsTextPointer().String()
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
			return emailI.AsTextPointer().String() < emailJ.AsTextPointer().String()
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
			return emailI.AsTextPointer().String() > emailJ.AsTextPointer().String()
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
			return emailI.AsTextPointer().String() < emailJ.AsTextPointer().String()
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
				[]OptionalValue{MakeInt8(int64(len(rows)))},
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
			return id1.AsInt8() < id2.AsInt8()
		})
		var (
			middleID      = expected[10].Values[0].AsInt8()
			expectedCount int64
		)
		for _, row := range expected {
			idVal, _ := row.GetValue("id")
			if idVal.AsInt8() > middleID {
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
				[]OptionalValue{MakeInt8(int64(expectedCount))},
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
					Operand2: Operand{Type: OperandInteger, Value: row.Values[0].AsInt8()},
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

func TestCompileInvertedScanFilter(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Name: "payload", Kind: JSON, Size: MaxInlineVarchar},
		{Name: "kind", Kind: Varchar, Size: MaxInlineVarchar},
	}
	row := NewRowWithValues(columns, []OptionalValue{
		MakeVarchar(NewTextPointer([]byte(`{"type":"click","user":{"id":"u1"}}`))),
		MakeVarchar(NewTextPointer([]byte("event"))),
	})

	t.Run("predecodes json contains query", func(t *testing.T) {
		t.Parallel()

		filter := compileInvertedScanFilter(columns, OneOrMore{{jsonContainsCondition("payload", `{"user":{"id":"u1"}}`)}})
		require.NotNil(t, filter)

		ok, err := filter(row)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("applies remaining filters", func(t *testing.T) {
		t.Parallel()

		filters := OneOrMore{{
			jsonContainsCondition("payload", `{"type":"click"}`),
			FieldIsEqual(Field{Name: "kind"}, OperandQuotedString, NewTextPointer([]byte("audit"))),
		}}
		filter := compileInvertedScanFilter(columns, filters)
		require.NotNil(t, filter)

		ok, err := filter(row)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("rechecks object array queries", func(t *testing.T) {
		t.Parallel()

		arrayRow := NewRowWithValues(columns, []OptionalValue{
			MakeVarchar(NewTextPointer([]byte(`{"tags":[{"name":"mobile"}]}`))),
			MakeVarchar(NewTextPointer([]byte("event"))),
		})
		filter := compileInvertedScanFilter(columns, OneOrMore{{jsonContainsCondition("payload", `{"tags":[{"name":"web"}]}`)}})
		require.NotNil(t, filter)

		ok, err := filter(arrayRow)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("falls back for generic filters", func(t *testing.T) {
		t.Parallel()

		filters := OneOrMore{{FieldIsEqual(Field{Name: "kind"}, OperandQuotedString, NewTextPointer([]byte("event")))}}
		filter := compileInvertedScanFilter(columns, filters)
		require.NotNil(t, filter)

		ok, err := filter(row)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func jsonContainsCondition(columnName, query string) Condition {
	return Condition{
		Operand1: Operand{
			Type: OperandExpr,
			Value: &Expr{
				FuncName: "JSON_CONTAINS",
				Args: []*Expr{
					{Column: columnName},
					{Literal: NewTextPointer([]byte(query))},
				},
			},
		},
		Operator: Eq,
		Operand2: Operand{Type: OperandBoolean, Value: true},
	}
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
		tp1 := overflow1.AsTextPointer()
		tp1.FirstPage = 1
		overflow1 = MakeTextByColumnKind(Text, tp1)
		rows[1], _ = rows[1].SetValue("profile", overflow1)

		overflow2, _ := rows[2].GetValue("profile")
		tp2 := overflow2.AsTextPointer()
		tp2.FirstPage = 2
		overflow2 = MakeTextByColumnKind(Text, tp2)
		rows[2], _ = rows[2].SetValue("profile", overflow2)

		// And now we can assert
		assert.Equal(t, rows, collectRows(ctx, result))
	})
}

// TestTable_SelectGroupBy covers selectGroupBy via Table.Select.
// Uses testColumns: id(Int8), email(Varchar), age(Int4), verified(Boolean), score(Real), created(Timestamp).
// We insert rows with two distinct verified values (true/false) and assert group counts and sums.
func TestTable_SelectGroupBy(t *testing.T) {
	table, txManager, _ := newTestTable(t, testColumns)
	ctx := context.Background()

	// Insert 5 rows: 3 verified=true, 2 verified=false.  Age values: 10,20,30 / 40,50.
	insertStmt := Statement{
		Kind:   Insert,
		Fields: []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}, {Name: "score"}, {Name: "created"}},
	}
	for i, row := range [][]OptionalValue{
		{MakeInt8(int64(1)), MakeVarchar(NewTextPointer([]byte("a@e.com"))), MakeInt4(int32(10)), MakeBool(true), MakeReal(float32(1.0)), MakeNull()},
		{MakeInt8(int64(2)), MakeVarchar(NewTextPointer([]byte("b@e.com"))), MakeInt4(int32(20)), MakeBool(true), MakeReal(float32(2.0)), MakeNull()},
		{MakeInt8(int64(3)), MakeVarchar(NewTextPointer([]byte("c@e.com"))), MakeInt4(int32(30)), MakeBool(true), MakeReal(float32(3.0)), MakeNull()},
		{MakeInt8(int64(4)), MakeVarchar(NewTextPointer([]byte("d@e.com"))), MakeInt4(int32(40)), MakeBool(false), MakeReal(float32(4.0)), MakeNull()},
		{MakeInt8(int64(5)), MakeVarchar(NewTextPointer([]byte("e@e.com"))), MakeInt4(int32(50)), MakeBool(false), MakeReal(float32(5.0)), MakeNull()},
	} {
		_ = i
		ins := insertStmt
		ins.Inserts = [][]OptionalValue{row}
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := table.Insert(ctx, ins)
			return err
		})
		require.NoError(t, err)
	}

	t.Run("group_by_count", func(t *testing.T) {
		stmt := Statement{
			Kind: Select,
			Fields: []Field{
				{Name: "verified"},
				{Name: "count(*)"},
			},
			Aggregates: []AggregateExpr{
				{Kind: 0},
				{Kind: AggregateCount},
			},
			GroupBy: []Field{{Name: "verified"}},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		rows := collectRows(ctx, result)
		require.Len(t, rows, 2)

		counts := map[bool]int64{}
		for _, r := range rows {
			v, _ := r.GetValue("verified")
			c, _ := r.GetValue("count(*)")
			counts[v.AsBool()] = c.AsInt8()
		}
		assert.Equal(t, int64(3), counts[true])
		assert.Equal(t, int64(2), counts[false])
	})

	t.Run("group_by_sum_int", func(t *testing.T) {
		stmt := Statement{
			Kind: Select,
			Fields: []Field{
				{Name: "verified"},
				{Name: "sum(age)"},
			},
			Aggregates: []AggregateExpr{
				{Kind: 0},
				{Kind: AggregateSum, Column: "age"},
			},
			GroupBy: []Field{{Name: "verified"}},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		rows := collectRows(ctx, result)
		require.Len(t, rows, 2)

		sums := map[bool]int64{}
		for _, r := range rows {
			v, _ := r.GetValue("verified")
			s, _ := r.GetValue("sum(age)")
			sums[v.AsBool()] = s.AsInt8()
		}
		assert.Equal(t, int64(60), sums[true])  // 10+20+30
		assert.Equal(t, int64(90), sums[false]) // 40+50
	})

	t.Run("group_by_min_max", func(t *testing.T) {
		stmt := Statement{
			Kind: Select,
			Fields: []Field{
				{Name: "verified"},
				{Name: "min(age)"},
				{Name: "max(age)"},
			},
			Aggregates: []AggregateExpr{
				{Kind: 0},
				{Kind: AggregateMin, Column: "age"},
				{Kind: AggregateMax, Column: "age"},
			},
			GroupBy: []Field{{Name: "verified"}},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		rows := collectRows(ctx, result)
		require.Len(t, rows, 2)

		type minmax struct{ min, max int32 }
		groups := map[bool]minmax{}
		for _, r := range rows {
			v, _ := r.GetValue("verified")
			mn, _ := r.GetValue("min(age)")
			mx, _ := r.GetValue("max(age)")
			groups[v.AsBool()] = minmax{mn.AsInt4(), mx.AsInt4()}
		}
		assert.Equal(t, minmax{10, 30}, groups[true])
		assert.Equal(t, minmax{40, 50}, groups[false])
	})

	t.Run("group_by_avg", func(t *testing.T) {
		stmt := Statement{
			Kind: Select,
			Fields: []Field{
				{Name: "verified"},
				{Name: "avg(age)"},
			},
			Aggregates: []AggregateExpr{
				{Kind: 0},
				{Kind: AggregateAvg, Column: "age"},
			},
			GroupBy: []Field{{Name: "verified"}},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		rows := collectRows(ctx, result)
		require.Len(t, rows, 2)

		avgs := map[bool]float64{}
		for _, r := range rows {
			v, _ := r.GetValue("verified")
			a, _ := r.GetValue("avg(age)")
			avgs[v.AsBool()] = a.AsDouble()
		}
		assert.InDelta(t, 20.0, avgs[true], 0.001)  // (10+20+30)/3
		assert.InDelta(t, 45.0, avgs[false], 0.001) // (40+50)/2
	})

	t.Run("group_by_with_having", func(t *testing.T) {
		stmt := Statement{
			Kind: Select,
			Fields: []Field{
				{Name: "verified"},
				{Name: "count(*)"},
			},
			Aggregates: []AggregateExpr{
				{Kind: 0},
				{Kind: AggregateCount},
			},
			GroupBy: []Field{{Name: "verified"}},
			Having: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "count(*)"}},
					Operator: Gt,
					Operand2: Operand{Type: OperandInteger, Value: int64(2)},
				},
			}},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		rows := collectRows(ctx, result)
		// Only verified=true group has count=3 > 2.
		require.Len(t, rows, 1)
		v, _ := rows[0].GetValue("verified")
		assert.Equal(t, true, v.AsAny())
	})
}

// TestTable_SelectAggregate covers selectAggregate via Table.Select (no GROUP BY).
func TestTable_SelectAggregate(t *testing.T) {
	table, txManager, _ := newTestTable(t, testColumns)
	ctx := context.Background()

	insertStmt := Statement{
		Kind:   Insert,
		Fields: []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}, {Name: "score"}, {Name: "created"}},
	}
	for _, row := range [][]OptionalValue{
		{MakeInt8(int64(1)), MakeVarchar(NewTextPointer([]byte("a@e.com"))), MakeInt4(int32(10)), MakeBool(true), MakeReal(float32(1.0)), MakeNull()},
		{MakeInt8(int64(2)), MakeVarchar(NewTextPointer([]byte("b@e.com"))), MakeInt4(int32(20)), MakeBool(true), MakeReal(float32(2.0)), MakeNull()},
		{MakeInt8(int64(3)), MakeVarchar(NewTextPointer([]byte("c@e.com"))), MakeInt4(int32(30)), MakeBool(false), MakeReal(float32(3.0)), MakeNull()},
	} {
		ins := insertStmt
		ins.Inserts = [][]OptionalValue{row}
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := table.Insert(ctx, ins)
			return err
		})
		require.NoError(t, err)
	}

	t.Run("count_all", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "count(*)"}},
			Aggregates: []AggregateExpr{
				{Kind: AggregateCount},
			},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)
		rows := collectRows(ctx, result)
		require.Len(t, rows, 1)
		// countResult hardcodes "COUNT(*)" (uppercase) as the result column name.
		cnt, _ := rows[0].GetValue("COUNT(*)")
		assert.Equal(t, int64(3), cnt.AsAny())
	})

	t.Run("sum_int_column", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "sum(age)"}},
			Aggregates: []AggregateExpr{
				{Kind: AggregateSum, Column: "age"},
			},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)
		rows := collectRows(ctx, result)
		require.Len(t, rows, 1)
		s, _ := rows[0].GetValue("sum(age)")
		assert.Equal(t, int64(60), s.AsAny()) // 10+20+30
	})

	t.Run("avg_int_column", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "avg(age)"}},
			Aggregates: []AggregateExpr{
				{Kind: AggregateAvg, Column: "age"},
			},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)
		rows := collectRows(ctx, result)
		require.Len(t, rows, 1)
		a, _ := rows[0].GetValue("avg(age)")
		assert.InDelta(t, 20.0, a.AsDouble(), 0.001)
	})

	t.Run("min_max_int_column", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "min(age)"}, {Name: "max(age)"}},
			Aggregates: []AggregateExpr{
				{Kind: AggregateMin, Column: "age"},
				{Kind: AggregateMax, Column: "age"},
			},
		}
		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)
		rows := collectRows(ctx, result)
		require.Len(t, rows, 1)
		mn, _ := rows[0].GetValue("min(age)")
		mx, _ := rows[0].GetValue("max(age)")
		assert.Equal(t, int32(10), mn.AsAny())
		assert.Equal(t, int32(30), mx.AsAny())
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
