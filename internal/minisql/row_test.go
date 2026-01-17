package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRow_Marshal(t *testing.T) {
	t.Parallel()

	t.Run("unmarshal all values", func(t *testing.T) {
		aRow := gen.Row()

		// 8 for int8
		// 4+255 for varchar/text
		// 4 for int4
		// 1 for boolean
		// 4 for real
		// 8 for timestamp
		assert.Equal(t, uint64(8+(varcharLengthPrefixSize+MaxInlineVarchar)+4+1+4+8), aRow.Size())

		data, err := aRow.Marshal()
		require.NoError(t, err)

		actual := NewRow(testColumns)
		actual, err = actual.Unmarshal(Cell{Value: data}, fieldsFromColumns(aRow.Columns...)...)
		require.NoError(t, err)

		assert.Equal(t, aRow, actual)
	})

	t.Run("unmarshal partial values", func(t *testing.T) {
		aRow := gen.Row()

		data, err := aRow.Marshal()
		require.NoError(t, err)

		selectedFields := fieldsFromColumns(testColumns[0:2]...)

		partialRow := NewRow(testColumns)
		partialRow, err = partialRow.Unmarshal(Cell{Value: data}, selectedFields...)
		require.NoError(t, err)

		assert.Equal(t, aRow.Values[0], partialRow.Values[0])
		assert.Equal(t, aRow.Values[1], partialRow.Values[1])
		assert.False(t, partialRow.Values[2].Valid)
		assert.False(t, partialRow.Values[3].Valid)
		assert.False(t, partialRow.Values[4].Valid)
		assert.False(t, partialRow.Values[5].Valid)
	})
}

func TestRow_CheckOneOrMore(t *testing.T) {
	t.Parallel()

	aRow := gen.Row()
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
				Value: aRow.Values[0].Value.(int64),
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
				Value: aRow.Values[0].Value.(int64) + 1,
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
				Value: aRow.Values[1].Value,
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
				Value: NewTextPointer([]byte(aRow.Values[1].Value.(TextPointer).String() + "bogus")),
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
				Value: int64(aRow.Values[2].Value.(int32)),
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
				Value: int64(aRow.Values[2].Value.(int32) + 1),
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
				Value: aRow.Values[3].Value.(bool),
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
				Value: !aRow.Values[3].Value.(bool),
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
				Value: aRow.Values[5].Value.(Time),
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
			aRow,
			OneOrMore{},
			true,
		},
		{
			"row matches if condition comparing with integer is true",
			aRow,
			OneOrMore{
				{
					idMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with integer is false",
			aRow,
			OneOrMore{
				{
					idMismatch,
				},
			},
			false,
		},
		{
			"row matches if condition comparing with quoted string is true",
			aRow,
			OneOrMore{
				{
					emailMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with quoted string is false",
			aRow,
			OneOrMore{
				{
					emailMismatch,
				},
			},
			false,
		},
		{
			"row matches if condition comparing with boolean is true",
			aRow,
			OneOrMore{
				{
					verifiedMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with boolean is false",
			aRow,
			OneOrMore{
				{
					verifiedMismatch,
				},
			},
			false,
		},
		{
			"row matches if condition comparing with timestamp is true",
			aRow,
			OneOrMore{
				{
					timestampMatch,
				},
			},
			true,
		},
		{
			"row does not match if condition comparing with timestamp is false",
			aRow,
			OneOrMore{
				{
					timestampMismatch,
				},
			},
			false,
		},
		{
			"row matches if all conditions are true",
			aRow,
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
			aRow,
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
			aRow,
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
			aRow,
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
			aRow,
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
			aRow,
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
			aRow,
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
							Value: int64(aRow.Values[2].Value.(int32)),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if > condition evaluates as true",
			aRow,
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
			aRow,
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
							Value: int64(aRow.Values[2].Value.(int32)),
						},
					},
				},
			},
			false,
		},
		{
			"row matches if < condition evaluates as true",
			aRow,
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
			aRow,
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
			aRow,
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
							Value: int64(aRow.Values[2].Value.(int32)),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if >= condition evaluates as false",
			aRow,
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
			aRow,
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
							Value: int64(aRow.Values[2].Value.(int32)),
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if <= condition evaluates as false",
			aRow,
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
							Value: int64(aRow.Values[2].Value.(int32)) - 1,
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
			aRow,
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
							Value: []any{aRow.Values[0].Value.(int64) - 1, aRow.Values[0].Value.(int64), aRow.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if IN condition evaluates as false",
			aRow,
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
							Value: []any{aRow.Values[0].Value.(int64) - 1, aRow.Values[0].Value.(int64) - 2, aRow.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			false,
		},
		{
			"row matches if NOT IN condition evaluates as true",
			aRow,
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
							Value: []any{aRow.Values[0].Value.(int64) - 1, aRow.Values[0].Value.(int64) - 2, aRow.Values[0].Value.(int64) + 1},
						},
					},
				},
			},
			true,
		},
		{
			"row does not match if NOT IN condition evaluates as false",
			aRow,
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
							Value: []any{aRow.Values[0].Value.(int64) - 1, aRow.Values[0].Value.(int64), aRow.Values[0].Value.(int64) + 1},
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

	aRow := NewRowWithValues(testColumns, []OptionalValue{
		{Value: int64(125478), Valid: true},
		{Value: "test@example.com", Valid: true},
	})

	t.Run("found", func(t *testing.T) {
		value, found := aRow.GetValue("email")
		assert.True(t, found)
		assert.Equal(t, OptionalValue{Value: "test@example.com", Valid: true}, value)
	})

	t.Run("not found", func(t *testing.T) {
		value, found := aRow.GetValue("bogus")
		assert.False(t, found)
		assert.Equal(t, OptionalValue{Value: nil, Valid: false}, value)
	})
}

func TestRow_SetValue(t *testing.T) {
	t.Parallel()

	aRow := NewRowWithValues(testColumns, []OptionalValue{
		{Value: int64(125478), Valid: true},
		{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
	})

	t.Run("changed", func(t *testing.T) {
		_, changed := aRow.SetValue("email", OptionalValue{Value: NewTextPointer([]byte("new@example.com")), Valid: true})
		assert.True(t, changed)
	})

	t.Run("not changed", func(t *testing.T) {
		_, changed := aRow.SetValue("id", OptionalValue{Value: int64(125478), Valid: true})
		assert.False(t, changed)
	})
}
