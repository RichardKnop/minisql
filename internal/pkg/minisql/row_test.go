package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRow_Marshal(t *testing.T) {
	t.Parallel()

	aRow := gen.Row()

	assert.Equal(t, uint64(8+255+4), aRow.Size())

	data, err := aRow.Marshal()
	require.NoError(t, err)

	actual := NewRow(testColumns)
	err = UnmarshalRow(data, &actual)
	require.NoError(t, err)

	assert.Equal(t, aRow, actual)
}

func TestRow_CheckOneOrMore(t *testing.T) {
	t.Parallel()

	var (
		aRow = Row{
			Columns: testColumns,
			Values: []any{
				int64(125478),
				"john.doe@example.com",
				int32(25),
			},
		}
		idMatch = Condition{
			Operand1: Operand{
				Type:  Field,
				Value: "id",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  Integer,
				Value: int64(125478),
			},
		}
		idMismatch = Condition{
			Operand1: Operand{
				Type:  Field,
				Value: "id",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  Integer,
				Value: int64(678),
			},
		}
		emailMatch = Condition{
			Operand1: Operand{
				Type:  Field,
				Value: "email",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  QuotedString,
				Value: "john.doe@example.com",
			},
		}
		emailMismatch = Condition{
			Operand1: Operand{
				Type:  Field,
				Value: "email",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  QuotedString,
				Value: "jack.ipsum@example.com",
			},
		}
		ageMatch = Condition{
			Operand1: Operand{
				Type:  Field,
				Value: "age",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  Integer,
				Value: int64(25),
			},
		}
		ageMismatch = Condition{
			Operand1: Operand{
				Type:  Field,
				Value: "age",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  Integer,
				Value: int64(42),
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
							Type:  Field,
							Value: "age",
						},
						Operator: Ne,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Ne,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  Integer,
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
							Type:  Field,
							Value: "age",
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  Integer,
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
