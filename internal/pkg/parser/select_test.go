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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "SELECT with WHERE with = works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = ''",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Eq,
						Operand2:        "",
						Operand2IsField: false,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Lt,
						Operand2:        "1",
						Operand2IsField: false,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Lte,
						Operand2:        "1",
						Operand2IsField: false,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Gt,
						Operand2:        "1",
						Operand2IsField: false,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Gte,
						Operand2:        "1",
						Operand2IsField: false,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Ne,
						Operand2:        "1",
						Operand2IsField: false,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Ne,
						Operand2:        "b",
						Operand2IsField: true,
					},
				},
			},
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
			Name: "SELECT with WHERE with two conditions using AND works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' AND b = '2'",
			Expected: minisql.Statement{
				Kind:      minisql.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Ne,
						Operand2:        "1",
						Operand2IsField: false,
					},
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        minisql.Eq,
						Operand2:        "2",
						Operand2IsField: false,
					},
				},
			},
			Err: nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := New(aTestCase.SQL)
			aStatement, err := aParser.Parse(context.Background())
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
