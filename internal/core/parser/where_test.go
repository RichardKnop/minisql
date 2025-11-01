package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

type whereTestCase struct {
	Name     string
	SQL      string
	Expected minisql.OneOrMore
	Err      error
}

func TestParse_Where(t *testing.T) {
	t.Parallel()

	testCases := []whereTestCase{
		{
			"WHERE with integer condition",
			"WHERE b = 1",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(1),
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with float condition",
			"WHERE b = 1.5",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandFloat,
							Value: float64(1.5),
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with quoted string",
			"WHERE b = '1'",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with quoted empty string",
			"WHERE b = ''",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with < works",
			"WHERE a < '1'",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Lt,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with <= works",
			"WHERE a <= '1'",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Lte,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with > works",
			"WHERE a > 25",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Gt,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(25),
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with >= works",
			"WHERE a >= 25",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Gte,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(25),
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with != works",
			"WHERE a != '1'",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Ne,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with != works (comparing field against another field)",
			"WHERE a != b",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Ne,
						Operand2: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with IS NULL",
			"WHERE b IS NULL",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type: minisql.OperandNull,
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with IS NOT NULL",
			"WHERE b IS NOT NULL",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Ne,
						Operand2: minisql.Operand{
							Type: minisql.OperandNull,
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions",
			"WHERE a = '1' AND b = 789",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(789),
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions using AND works",
			"WHERE a != '1' AND b = 2 and c = '3'",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Ne,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(2),
						},
					},
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "c",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "3",
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions using OR works",
			"WHERE a != '1' OR b = 2",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Ne,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
				},
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(2),
						},
					},
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions using both AND plus OR works",
			"WHERE a != '1' and b = 2 OR c= '3'",
			minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "a",
						},
						Operator: minisql.Ne,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "1",
						},
					},
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "b",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandInteger,
							Value: int64(2),
						},
					},
				},
				{
					{
						Operand1: minisql.Operand{
							Type:  minisql.OperandField,
							Value: "c",
						},
						Operator: minisql.Eq,
						Operand2: minisql.Operand{
							Type:  minisql.OperandQuotedString,
							Value: "3",
						},
					},
				},
			},
			nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := New().setSQL(aTestCase.SQL)
			aParser.step = stepWhere

			var err error
			for aParser.i < len(aParser.sql) {
				err = aParser.doParseWhere()
				if err != nil {
					break
				}
			}
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aParser.Conditions)
		})
	}
}
