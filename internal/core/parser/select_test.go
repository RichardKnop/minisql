package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

func TestParse_Select(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"SELECT without FROM fails",
			"SELECT",
			nil,
			errEmptyTableName,
		},
		{
			"SELECT without fields fails",
			"SELECT FROM 'a'",
			nil,
			errSelectWithoutFields,
		},
		{
			"SELECT with comma and empty field fails",
			"SELECT b, FROM 'a'",
			nil,
			errSelectWithoutFields,
		},
		{
			"SELECT works",
			"SELECT a FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []string{"a"},
				},
			},
			nil,
		},
		{
			"SELECT works with lowercase",
			" select a fRoM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []string{"a"},
				},
			},
			nil,
		},
		{
			"SELECT many fields works",
			"SELECT a, c, d FROM b ;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []string{"a", "c", "d"},
				},
			},
			nil,
		},
		{
			"SELECT with alias works",
			"SELECT a as z, b as y, c FROM b ; ",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []string{"a", "b", "c"},
					Aliases: map[string]string{
						"a": "z",
						"b": "y",
					},
				},
			},
			nil,
		},
		{
			"SELECT * works",
			"SELECT * FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []string{"*"},
				},
			},
			nil,
		},
		{
			"SELECT a, * works",
			"SELECT a, * FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []string{"a", "*"},
				},
			},
			nil,
		},
		{
			"SELECT with empty WHERE fails",
			"SELECT a, c, d FROM b WHERE",
			nil,
			errEmptyWhereClause,
		},
		{
			"SELECT with WHERE with only operand fails",
			"SELECT a, c, d FROM b WHERE a",
			nil,
			errWhereWithoutOperator,
		},
		{
			"SELECT with WHERE with multiple conditions using AND works",
			`SELECT a, c, d FROM "b" WHERE a != '1' AND b = 2 and c = '3';`,
			[]minisql.Statement{
				{
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
			},
			nil,
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
