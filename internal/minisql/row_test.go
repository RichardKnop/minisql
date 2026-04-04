package minisql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		assert.Equal(t, uint64(8+(varcharLengthPrefixSize+MaxInlineVarchar)+4+1+4+8), row.Size())

		data, err := row.Marshal()
		require.NoError(t, err)

		actual := NewRow(testColumns)
		actual, err = actual.Unmarshal(Cell{Value: data}, fieldsFromColumns(row.Columns...)...)
		require.NoError(t, err)

		assert.Equal(t, row, actual)
	})

	t.Run("unmarshal partial values", func(t *testing.T) {
		row := gen.Row()

		data, err := row.Marshal()
		require.NoError(t, err)

		selectedFields := fieldsFromColumns(testColumns[0:2]...)

		partialRow := NewRow(testColumns)
		partialRow, err = partialRow.Unmarshal(Cell{Value: data}, selectedFields...)
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
				Value: row.Values[5].Value.(Time),
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
				Value: MustParseTimestamp("1000-01-01 00:00:00 BC"),
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
							Value: MustParseTimestamp("2000-01-01 00:00:00"),
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
		assert.Equal(t, v, got)
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
