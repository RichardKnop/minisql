package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRow_Marshal(t *testing.T) {
	t.Parallel()

	aRow := gen.Row()

	// 8 for int8, 255 for varchar, 4 for int4, 1 for boolean, 4 for real, 8 for double
	assert.Equal(t, uint64(8+255+4+1+4+8), aRow.Size())

	data, err := aRow.Marshal()
	require.NoError(t, err)

	actual := NewRow(testColumns)
	err = UnmarshalRow(Cell{Value: data}, &actual)
	require.NoError(t, err)

	assert.Equal(t, aRow, actual)
}

func TestRow_CheckOneOrMore(t *testing.T) {
	t.Parallel()

	var (
		aRow = Row{
			Columns: testColumns,
			Values: []OptionalValue{
				{Value: int64(125478), Valid: true},
				{Value: "john.doe@example.com", Valid: true},
				{Value: int32(25), Valid: true},
				{Value: true, Valid: true},
			},
		}
		idMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "id",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(125478),
			},
		}
		idMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "id",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(678),
			},
		}
		emailMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "email",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: "john.doe@example.com",
			},
		}
		emailMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "email",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: "jack.ipsum@example.com",
			},
		}
		ageMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "age",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(25),
			},
		}
		ageMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "age",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(42),
			},
		}
		verifiedMatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "verified",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: true,
			},
		}
		verifiedMismatch = Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "verified",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: false,
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
							Value: "age",
						},
						Operator: Ne,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(42),
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
							Value: "age",
						},
						Operator: Ne,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(25),
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
							Value: "age",
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(24),
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
							Value: "age",
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(25),
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
							Value: "age",
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(26),
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
							Value: "age",
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(25),
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
							Value: "age",
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(25),
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
							Value: "age",
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(26),
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
							Value: "age",
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(25),
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
							Value: "age",
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(24),
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

	aRow := Row{
		Columns: testColumns,
		Values: []OptionalValue{
			{Value: int64(125478), Valid: true},
			{Value: "test@example.com", Valid: true},
		},
	}

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

	aRow := Row{
		Columns: testColumns,
		Values: []OptionalValue{
			{Value: int64(125478), Valid: true},
			{Value: "test@example.com", Valid: true},
		},
	}

	t.Run("found and changed", func(t *testing.T) {
		found, changed := aRow.SetValue("email", OptionalValue{Value: "new@example.com", Valid: true})
		assert.True(t, found)
		assert.True(t, changed)
	})

	t.Run("found but not changed", func(t *testing.T) {
		found, changed := aRow.SetValue("id", OptionalValue{Value: int64(125478), Valid: true})
		assert.True(t, found)
		assert.False(t, changed)
	})

	t.Run("not found", func(t *testing.T) {
		found, changed := aRow.SetValue("bogus", OptionalValue{Value: "value", Valid: true})
		assert.False(t, found)
		assert.False(t, changed)
	})

}
