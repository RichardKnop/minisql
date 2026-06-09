package minisql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRow_OnlyFields_FastPathExactMatch(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	fields := fieldsFromColumns(row.Columns...)

	got := row.OnlyFields(fields...)
	require.NotEmpty(t, got.Columns)
	require.NotEmpty(t, row.Columns)
	assert.Equal(t, row, got)
	assert.Equal(t, &row.Columns[0], &got.Columns[0], "fast path should reuse column slice")
	assert.Equal(t, &row.Values[0], &got.Values[0], "fast path should reuse value slice")
}

func TestSelectedColumnsMask(t *testing.T) {
	t.Parallel()

	mask := selectedColumnsMask(testColumns, []Field{
		{Name: "id"},
		{Name: "verified"},
	})
	require.Len(t, mask, len(testColumns))
	assert.True(t, mask[0])  // id
	assert.False(t, mask[1]) // email
	assert.False(t, mask[2]) // age
	assert.True(t, mask[3])  // verified
	assert.False(t, mask[4]) // score
	assert.False(t, mask[5]) // created

	assert.Nil(t, selectedColumnsMask(testColumns, nil))
}

func TestRow_Marshal(t *testing.T) {
	t.Parallel()

	t.Run("unmarshal all values", func(t *testing.T) {
		row := gen.Row()

		// 8 for int8
		// 4+255 for varchar/text
		// 4 for int4
		// 1 for boolean
		// 4 for real
		// 8 for timestamp
		assert.Equal(t, uint64(8+(varcharLengthPrefixSize+MaxIndexKeySize)+4+1+4+8), row.Size())

		data, err := row.Marshal()
		require.NoError(t, err)

		view := NewRowView(testColumns, makeTestCell(0, row.NullBitmask(), data, row.Columns))
		actual, err := view.Materialize(selectedColumnsMask(testColumns, fieldsFromColumns(row.Columns...)))
		require.NoError(t, err)

		assert.Equal(t, row, actual)
	})

	t.Run("unmarshal partial values", func(t *testing.T) {
		row := gen.Row()

		data, err := row.Marshal()
		require.NoError(t, err)

		selectedFields := fieldsFromColumns(testColumns[0:2]...)

		view := NewRowView(testColumns, makeTestCell(0, row.NullBitmask(), data, row.Columns))
		partialRow, err := view.Materialize(selectedColumnsMask(testColumns, selectedFields))
		require.NoError(t, err)

		assert.Equal(t, row.Values[0], partialRow.Values[0])
		assert.Equal(t, row.Values[1], partialRow.Values[1])
		assert.False(t, partialRow.Values[2].Valid)
		assert.False(t, partialRow.Values[3].Valid)
		assert.False(t, partialRow.Values[4].Valid)
		assert.False(t, partialRow.Values[5].Valid)
	})
}

func TestRow_CheckOneOrMore(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	aRowWithNulls := gen.Row()
	aRowWithNulls.Values[4] = OptionalValue{} // 5th column to NULL

	var (
		idMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "id"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: row.Values[0].Value.(int64),
			},
		}
		idMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "id"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: row.Values[0].Value.(int64) + 1,
			},
		}
		emailMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "email"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: row.Values[1].Value,
			},
		}
		emailMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "email"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: NewTextPointer([]byte(row.Values[1].Value.(TextPointer).String() + "bogus")),
			},
		}
		ageMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "age"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(row.Values[2].Value.(int32)),
			},
		}
		ageMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "age"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(row.Values[2].Value.(int32) + 1),
			},
		}
		verifiedMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "verified"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: row.Values[3].Value.(bool),
			},
		}
		verifiedMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "verified"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: !row.Values[3].Value.(bool),
			},
		}
		timestampMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "created"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: row.Values[5].Value.(TimestampMicros),
			},
		}
		timestampMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "created"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: MustParseTimestampMicros("1000-01-01 00:00:00 BC"),
			},
		}
	)

	testCases := []struct {
		Name       string
		Row        Row
		Conditions OneOrMore
		Expected   bool
	}{
		{
			"row matches if conditions are empty",
			row,
			OneOrMore{},
			true,
		},
		{
			"row matches if condition comparing with integer is true",
			row,
			OneOrMore{
				{
					idMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with integer is false",
			row,
			OneOrMore{
				{
					idMismatch,
				},
			},
			false,
		},
		{
			"row matches if condition comparing with quoted string is true",
			row,
			OneOrMore{
				{
					emailMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with quoted string is false",
			row,
			OneOrMore{
				{
					emailMismatch,
				},
			},
			false,
		},
		{
			"row matches if condition comparing with boolean is true",
			row,
			OneOrMore{
				{
					verifiedMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with boolean is false",
			row,
			OneOrMore{
				{
					verifiedMismatch,
				},
			},
			false,
		},
		{
			"row matches if condition comparing with timestamp is true",
			row,
			OneOrMore{
				{
					timestampMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with timestamp is false",
			row,
			OneOrMore{
				{
					timestampMismatch,
				},
			},
			false,
		},
		{
			"row matches if all conditions are true",
			row,
			OneOrMore{
				{
					idMatch,
					emailMatch,
				},
			},
			true,
		},
		{
			"row does not match if not all conditions are true",
			row,
			OneOrMore{
				{
					idMatch,
					emailMismatch,
				},
			},
			false,
		},
		{
			"row matches if all condition groups are true",
			row,
			OneOrMore{
				{
					idMatch,
					emailMatch,
				},
				{
					ageMatch,
				},
			},
			true,
		},
		{
			"row matches if at least one of condition groups is true",
			row,
			OneOrMore{
				{
					idMatch,
					emailMismatch,
				},
				{
					ageMatch,
				},
			},
			true,
		},
		{
			"row does not match if all condition groups are false",
			row,
			OneOrMore{
				{
					idMatch,
					emailMismatch,
				},
				{
					ageMismatch,
				},
			},
			false,
		},
		{
			"row matches if != condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Ne,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(999),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if != condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Ne,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(row.Values[2].Value.(int32)),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if > condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(4),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if > condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(row.Values[2].Value.(int32)),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if < condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(999),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if < condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(4),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if >= condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(row.Values[2].Value.(int32)),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if >= condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(999),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if <= condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(row.Values[2].Value.(int32)),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if <= condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "age"},
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(row.Values[2].Value.(int32)) - 1,
						},
					},
				},
			},
			false,
		},
		{
			"row does not match if field value is NULL",
			aRowWithNulls,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "created"},
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: MustParseTimestampMicros("2000-01-01 00:00:00"),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if IN condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
						Operator: In,
						Operand2: Operand{
							Type:  OperandList,
							Value: []any{row.Values[0].Value.(int64) - 1, row.Values[0].Value.(int64), row.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if IN condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
						Operator: In,
						Operand2: Operand{
							Type:  OperandList,
							Value: []any{row.Values[0].Value.(int64) - 1, row.Values[0].Value.(int64) - 2, row.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			false,
		},
		{
			"row matches if NOT IN condition evaluates as true",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
						Operator: NotIn,
						Operand2: Operand{
							Type:  OperandList,
							Value: []any{row.Values[0].Value.(int64) - 1, row.Values[0].Value.(int64) - 2, row.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if NOT IN condition evaluates as false",
			row,
			OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
						Operator: NotIn,
						Operand2: Operand{
							Type:  OperandList,
							Value: []any{row.Values[0].Value.(int64) - 1, row.Values[0].Value.(int64), row.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			false,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			actual, err := aTestCase.Row.CheckOneOrMore(aTestCase.Conditions)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}

func TestRow_CheckOneOrMoreWithColumnIndexes(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	conditions := OneOrMore{
		{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandInteger, Value: row.Values[0].Value.(int64)},
			},
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Gte,
				Operand2: Operand{Type: OperandInteger, Value: int64(row.Values[2].Value.(int32))},
			},
		},
	}

	columnIndexes := map[string]int{
		"id":       0,
		"email":    1,
		"age":      2,
		"verified": 3,
		"score":    4,
		"created":  5,
	}

	got, err := row.CheckOneOrMoreWithColumnIndexes(conditions, columnIndexes)
	require.NoError(t, err)

	want, err := row.CheckOneOrMore(conditions)
	require.NoError(t, err)

	assert.Equal(t, want, got)
}

func TestRow_CompareFieldValueWithColumnIndexes_Errors(t *testing.T) {
	t.Parallel()

	row := NewRowWithValues(
		[]Column{
			{Name: "id", Kind: Int8, Size: 8},
			{Name: "age", Kind: Int4, Size: 4},
			{Name: "verified", Kind: Boolean, Size: 1},
		},
		[]OptionalValue{
			{Value: int64(10), Valid: true},
			{Value: int32(5), Valid: true},
			{Value: true, Valid: true},
		},
	)
	columnIndexes := map[string]int{
		"id":       0,
		"age":      1,
		"verified": 2,
	}

	t.Run("invalid field operand type", func(t *testing.T) {
		_, err := row.compareFieldValueWithColumnIndexes(
			Operand{Type: OperandInteger, Value: int64(1)},
			Operand{Type: OperandInteger, Value: int64(1)},
			Eq,
			columnIndexes,
		)
		assert.Error(t, err)
	})

	t.Run("value operand cannot be field", func(t *testing.T) {
		_, err := row.compareFieldValueWithColumnIndexes(
			Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operand{Type: OperandField, Value: Field{Name: "age"}},
			Eq,
			columnIndexes,
		)
		assert.Error(t, err)
	})

	t.Run("missing column index", func(t *testing.T) {
		_, err := row.compareFieldValueWithColumnIndexes(
			Operand{Type: OperandField, Value: Field{Name: "missing"}},
			Operand{Type: OperandInteger, Value: int64(1)},
			Eq,
			columnIndexes,
		)
		assert.Error(t, err)
	})

	t.Run("row values out of bounds", func(t *testing.T) {
		shortValuesRow := NewRowWithValues(row.Columns, []OptionalValue{{Value: int64(10), Valid: true}})
		_, err := shortValuesRow.compareFieldValueWithColumnIndexes(
			Operand{Type: OperandField, Value: Field{Name: "age"}},
			Operand{Type: OperandInteger, Value: int64(5)},
			Eq,
			columnIndexes,
		)
		assert.Error(t, err)
	})

	t.Run("null comparison with unsupported operator", func(t *testing.T) {
		_, err := row.compareFieldValueWithColumnIndexes(
			Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operand{Type: OperandNull},
			Gt,
			columnIndexes,
		)
		assert.Error(t, err)
	})

	t.Run("IN not supported for boolean", func(t *testing.T) {
		_, err := row.compareFieldValueWithColumnIndexes(
			Operand{Type: OperandField, Value: Field{Name: "verified"}},
			Operand{Type: OperandList, Value: []any{true, false}},
			In,
			columnIndexes,
		)
		assert.Error(t, err)
	})
}

func TestRow_CompareFieldsWithColumnIndexes_Errors(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "a", Kind: Int8, Size: 8},
		{Name: "b", Kind: Int8, Size: 8},
		{Name: "c", Kind: Int4, Size: 4},
	}
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: int64(2), Valid: true},
		{Value: int32(3), Valid: true},
	})
	columnIndexes := map[string]int{"a": 0, "b": 1, "c": 2}

	field := func(name string) Operand {
		return Operand{Type: OperandField, Value: Field{Name: name}}
	}

	t.Run("invalid field1 operand", func(t *testing.T) {
		_, err := row.compareFieldsWithColumnIndexes(Operand{Type: OperandInteger, Value: int64(1)}, field("b"), Eq, columnIndexes)
		assert.Error(t, err)
	})

	t.Run("invalid field2 operand", func(t *testing.T) {
		_, err := row.compareFieldsWithColumnIndexes(field("a"), Operand{Type: OperandInteger, Value: int64(1)}, Eq, columnIndexes)
		assert.Error(t, err)
	})

	t.Run("missing first column", func(t *testing.T) {
		_, err := row.compareFieldsWithColumnIndexes(field("missing"), field("b"), Eq, columnIndexes)
		assert.Error(t, err)
	})

	t.Run("missing second column", func(t *testing.T) {
		_, err := row.compareFieldsWithColumnIndexes(field("a"), field("missing"), Eq, columnIndexes)
		assert.Error(t, err)
	})

	t.Run("row values out of bounds", func(t *testing.T) {
		shortValuesRow := NewRowWithValues(cols, []OptionalValue{{Value: int64(1), Valid: true}})
		_, err := shortValuesRow.compareFieldsWithColumnIndexes(field("a"), field("b"), Eq, columnIndexes)
		assert.Error(t, err)
	})
}

func TestRow_GetValue(t *testing.T) {
	t.Parallel()

	row := NewRowWithValues(testColumns, []OptionalValue{
		{Value: int64(125478), Valid: true},
		{Value: "test@example.com", Valid: true},
	})

	t.Run("found", func(t *testing.T) {
		value, found := row.GetValue("email")
		assert.True(t, found)
		assert.Equal(t, OptionalValue{Value: "test@example.com", Valid: true}, value)
	})

	t.Run("not found", func(t *testing.T) {
		value, found := row.GetValue("bogus")
		assert.False(t, found)
		assert.Equal(t, OptionalValue{Value: nil, Valid: false}, value)
	})
}

func TestRow_SetValue(t *testing.T) {
	t.Parallel()

	row := NewRowWithValues(testColumns, []OptionalValue{
		{Value: int64(125478), Valid: true},
		{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
	})

	t.Run("changed", func(t *testing.T) {
		_, changed := row.SetValue("email", OptionalValue{Value: NewTextPointer([]byte("new@example.com")), Valid: true})
		assert.True(t, changed)
	})

	t.Run("not changed", func(t *testing.T) {
		_, changed := row.SetValue("id", OptionalValue{Value: int64(125478), Valid: true})
		assert.False(t, changed)
	})
}

func TestMarshalUnmarshalInt8(t *testing.T) {
	t.Parallel()

	cases := []int8{0, 1, -1, 127, -128}
	for _, v := range cases {
		buf := make([]byte, 1)
		marshalInt8(buf, v, 0)
		got := unmarshalInt8(buf, 0)
		assert.Equal(t, v, got)
	}
}

func TestMarshalUnmarshalFloat64(t *testing.T) {
	t.Parallel()

	cases := []float64{0.0, 1.5, -1.5, math.MaxFloat64, -math.MaxFloat64, math.Pi}
	for _, v := range cases {
		buf := make([]byte, 8)
		marshalFloat64(buf, v, 0)
		got := unmarshalFloat64(buf, 0)
		assert.InDelta(t, v, got, 1e-9)
	}
}

func TestRow_CompareFields(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Kind: Int8, Name: "x", Size: 8},
		{Kind: Int8, Name: "y", Size: 8},
		{Kind: Int4, Name: "z", Size: 4},
	}
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: int64(10), Valid: true},
		{Value: int64(10), Valid: true},
		{Value: int32(5), Valid: true},
	})

	field := func(name string) Operand {
		return Operand{Type: OperandField, Value: Field{Name: name}}
	}

	t.Run("equal fields match", func(t *testing.T) {
		ok, err := row.compareFields(field("x"), field("y"), Eq)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("different kind fields do not match", func(t *testing.T) {
		ok, err := row.compareFields(field("x"), field("z"), Eq)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("error on invalid field1 operand type", func(t *testing.T) {
		nonField := Operand{Type: OperandInteger, Value: int64(1)}
		_, err := row.compareFields(nonField, field("y"), Eq)
		assert.Error(t, err)
	})

	t.Run("error on invalid field2 operand type", func(t *testing.T) {
		nonField := Operand{Type: OperandInteger, Value: int64(1)}
		_, err := row.compareFields(field("x"), nonField, Eq)
		assert.Error(t, err)
	})

	t.Run("error when column not found", func(t *testing.T) {
		_, err := row.compareFields(field("missing"), field("y"), Eq)
		assert.Error(t, err)
	})
}

func TestRow_CheckCondition(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Kind: Int8, Name: "a", Size: 8},
		{Kind: Int8, Name: "b", Size: 8},
	}
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: int64(5), Valid: true},
		{Value: int64(5), Valid: true},
	})

	t.Run("both fields equal", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "a"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandField, Value: Field{Name: "b"}},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("both literals equal", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: int64(7)},
			Operator: Eq,
			Operand2: Operand{Type: OperandInteger, Value: int64(7)},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("both literals not equal", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: int64(7)},
			Operator: Eq,
			Operand2: Operand{Type: OperandInteger, Value: int64(8)},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("right-side field, left-side literal", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: int64(5)},
			Operator: Eq,
			Operand2: Operand{Type: OperandField, Value: Field{Name: "a"}},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestRow_CompareFieldValue_IsNull(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Kind: Int8, Name: "x", Size: 8, Nullable: true},
	}

	t.Run("IS NULL matches null field", func(t *testing.T) {
		row := NewRowWithValues(cols, []OptionalValue{{Valid: false}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "x"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandNull},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("IS NULL does not match non-null field", func(t *testing.T) {
		row := NewRowWithValues(cols, []OptionalValue{{Value: int64(1), Valid: true}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "x"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandNull},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("IS NOT NULL matches non-null field", func(t *testing.T) {
		row := NewRowWithValues(cols, []OptionalValue{{Value: int64(1), Valid: true}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "x"}},
			Operator: Ne,
			Operand2: Operand{Type: OperandNull},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestCompareScalarToOperand(t *testing.T) {
	t.Parallel()

	t.Run("nil val eq null operand", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(nil, Operand{Type: OperandNull}, Eq)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("nil val ne null operand is false", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(nil, Operand{Type: OperandNull}, Ne)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("nil val ne non-null operand", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(nil, Operand{Type: OperandInteger, Value: int64(1)}, Ne)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("nil val eq non-null operand is false", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(nil, Operand{Type: OperandInteger, Value: int64(1)}, Eq)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("nil val with non-eq/ne operator returns false", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(nil, Operand{Type: OperandInteger, Value: int64(1)}, Gt)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("non-null val eq null operand is false", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(int64(5), Operand{Type: OperandNull}, Eq)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("non-null val ne null operand is true", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(int64(5), Operand{Type: OperandNull}, Ne)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("int64 equality", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(int64(42), Operand{Type: OperandInteger, Value: int64(42)}, Eq)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("float64 comparison", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(float64(3.14), Operand{Type: OperandFloat, Value: float64(3.14)}, Eq)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("TextPointer comparison", func(t *testing.T) {
		t.Parallel()
		tp := NewTextPointer([]byte("hello"))
		ok, err := compareScalarToOperand(tp, Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("hello"))}, Eq)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("bool comparison", func(t *testing.T) {
		t.Parallel()
		ok, err := compareScalarToOperand(true, Operand{Type: OperandBoolean, Value: true}, Eq)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("unsupported type errors", func(t *testing.T) {
		t.Parallel()
		_, err := compareScalarToOperand([]byte("raw"), Operand{Type: OperandInteger, Value: int64(1)}, Eq)
		require.Error(t, err)
	})
}

func TestRow_CompareFieldValue_UUID(t *testing.T) {
	t.Parallel()

	const uuidStr1 = "550e8400-e29b-41d4-a716-446655440000"
	const uuidStr2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

	uv1, _ := ParseUUID(uuidStr1)
	uv2, _ := ParseUUID(uuidStr2)

	cols := []Column{{Name: "id", Kind: UUID, Size: 16}}

	t.Run("UUID equality match", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{{Value: uv1, Valid: true}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandQuotedString, Value: uv1},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("UUID equality no match", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{{Value: uv1, Valid: true}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandQuotedString, Value: uv2},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("UUID IN list found", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{{Value: uv1, Valid: true}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: In,
			Operand2: Operand{Type: OperandList, Value: []any{uv1, uv2}},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("UUID NOT IN list", func(t *testing.T) {
		t.Parallel()
		uv3, _ := ParseUUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
		row := NewRowWithValues(cols, []OptionalValue{{Value: uv3, Valid: true}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: NotIn,
			Operand2: Operand{Type: OperandList, Value: []any{uv1, uv2}},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("UUID NULL comparison", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{{Valid: false}})
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandNull},
		}
		ok, err := row.checkCondition(cond)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}
