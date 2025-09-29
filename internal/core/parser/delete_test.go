package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

func TestParse_Delete(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty DELETE fails",
			SQL:      "DELETE FROM",
			Expected: minisql.Statement{Kind: minisql.Delete},
			Err:      errEmptyTableName,
		},
		{
			Name: "DELETE without WHERE fails",
			SQL:  "DELETE FROM 'a'",
			Expected: minisql.Statement{
				Kind:      minisql.Delete,
				TableName: "a",
			},
			Err: errWhereRequiredForUpdateDelete,
		},
		{
			Name: "DELETE with empty WHERE fails",
			SQL:  "DELETE FROM 'a' WHERE",
			Expected: minisql.Statement{
				Kind:      minisql.Delete,
				TableName: "a",
			},
			Err: errEmptyWhereClause,
		},
		{
			Name: "DELETE with WHERE with field but no operator fails",
			SQL:  "DELETE FROM 'a' WHERE b",
			Expected: minisql.Statement{
				Kind:      minisql.Delete,
				TableName: "a",
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
							},
						},
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "DELETE with WHERE works",
			SQL:  "DELETE FROM 'a' WHERE b = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Delete,
				TableName: "a",
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
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
			Name: "DELETE with multiple conditions works",
			SQL:  "DELETE FROM 'a' WHERE a = '1' AND b = 789",
			Expected: minisql.Statement{
				Kind:      minisql.Delete,
				TableName: "a",
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
						{
							Operand1: minisql.Operand{
								Type:  minisql.Field,
								Value: "b",
							},
							Operator: minisql.Eq,
							Operand2: minisql.Operand{
								Type:  minisql.Integer,
								Value: int64(789),
							},
						},
					},
				},
			},
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
