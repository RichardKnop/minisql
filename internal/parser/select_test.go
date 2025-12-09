package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
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
					Fields:    []minisql.Field{{Name: "a"}},
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
					Fields:    []minisql.Field{{Name: "a"}},
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
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
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
					Fields:    []minisql.Field{{Name: "a"}, {Name: "b"}, {Name: "c"}},
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
					Fields:    []minisql.Field{{Name: "*"}},
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
					Fields:    []minisql.Field{{Name: "a"}, {Name: "*"}},
				},
			},
			nil,
		},
		{
			"SELECT with LIMIT works",
			"SELECT * FROM b LIMIT 10;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					Limit:     minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with OFFSET works",
			"SELECT * FROM b OFFSET 10;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					Offset:    minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with LIMIT and OFFSET works",
			"SELECT * FROM b LIMIT 10 OFFSET 20;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					Limit:     minisql.OptionalValue{Value: int64(10), Valid: true},
					Offset:    minisql.OptionalValue{Value: int64(20), Valid: true},
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
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
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
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and LIMIT works",
			`SELECT a, c, d FROM "b" WHERE a = 2 LIMIT 10;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							{
								Operand1: minisql.Operand{
									Type:  minisql.OperandField,
									Value: "a",
								},
								Operator: minisql.Eq,
								Operand2: minisql.Operand{
									Type:  minisql.OperandInteger,
									Value: int64(2),
								},
							},
						},
					},
					Limit: minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and OFFSET works",
			`SELECT a, c, d FROM "b" WHERE a = 2 OFFSET 10;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							{
								Operand1: minisql.Operand{
									Type:  minisql.OperandField,
									Value: "a",
								},
								Operator: minisql.Eq,
								Operand2: minisql.Operand{
									Type:  minisql.OperandInteger,
									Value: int64(2),
								},
							},
						},
					},
					Offset: minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and LIMIT and OFFSET works",
			`SELECT a, c, d FROM "b" WHERE a = 2 LIMIT 10 OFFSET 20;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							{
								Operand1: minisql.Operand{
									Type:  minisql.OperandField,
									Value: "a",
								},
								Operator: minisql.Eq,
								Operand2: minisql.Operand{
									Type:  minisql.OperandInteger,
									Value: int64(2),
								},
							},
						},
					},
					Limit:  minisql.OptionalValue{Value: int64(10), Valid: true},
					Offset: minisql.OptionalValue{Value: int64(20), Valid: true},
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
