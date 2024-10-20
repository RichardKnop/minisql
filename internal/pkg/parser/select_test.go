package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

func TestParse_Select(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "SELECT without FROM fails",
			SQL:      "SELECT",
			Expected: minisql.Statement{Kind: minisql.Select},
			Err:      errEmptyTableName,
		},
		{
			Name:     "SELECT without fields fails",
			SQL:      "SELECT FROM 'a'",
			Expected: minisql.Statement{Kind: minisql.Select},
			Err:      errSelectWithoutFields,
		},
		{
			Name: "SELECT with comma and empty field fails",
			SQL:  "SELECT b, FROM 'a'",
			Expected: minisql.Statement{
				Kind:   minisql.Select,
				Fields: []string{"b"},
			},
			Err: errSelectWithoutFields,
		},
		{
			Name: "SELECT works",
			SQL:  "SELECT a FROM 'b'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a"},
			},
			Err: nil,
		},
		{
			Name: "SELECT works with lowercase",
			SQL:  "select a fRoM 'b'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a"},
			},
			Err: nil,
		},
		{
			Name: "SELECT many fields works",
			SQL:  "SELECT a, c, d FROM 'b'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
			},
			Err: nil,
		},
		{
			Name: "SELECT with alias works",
			SQL:  "SELECT a as z, b as y, c FROM 'b'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "b", "c"},
				Aliases: map[string]string{
					"a": "z",
					"b": "y",
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT * works",
			SQL:  "SELECT * FROM 'b'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"*"},
			},
		},
		{
			Name: "SELECT a, * works",
			SQL:  "SELECT a, * FROM 'b'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "*"},
			},
		},
		{
			Name: "SELECT with empty WHERE fails",
			SQL:  "SELECT a, c, d FROM 'b' WHERE",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
			},
			Err: errEmptyWhereClause,
		},
		{
			Name: "SELECT with WHERE with only operand fails",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
						},
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "SELECT with WHERE with = and empty quoted string works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = ''",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with = works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with = and int value works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = 1",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.Integer,
								Value: int64(1),
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with < works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a < '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Lt,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with <= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a <= '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Lte,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with > works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a > '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Gt,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with >= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a >= '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Gte,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with != works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Ne,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with != works (comparing field against another field)",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != b",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Ne,
							Operand2: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
							},
						},
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with multiple conditions using AND works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' AND b = 2 and c = '3'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Ne,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.Integer,
								Value: int64(2),
							},
						},
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "c",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "3",
							},
						},
					},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with multiple conditions using OR works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' OR b = 2",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Ne,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
					},
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.Integer,
								Value: int64(2),
							},
						},
					},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with multiple conditions using both AND plus OR works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' and b = 2 OR c= '3'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "a",
							},
							Operator: minisql.Ne,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "1",
							},
						},
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.Integer,
								Value: int64(2),
							},
						},
					},
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "c",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.QuotedString,
								Value: "3",
							},
						},
					},
				},
			},
			Err: nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aStatement, err := New().Parse(context.Background(), aTestCase.SQL)
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}
