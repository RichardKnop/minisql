package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/core/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_Delete(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"Empty DELETE fails",
			"DELETE FROM",
			nil,
			errEmptyTableName,
		},
		{
			"DELETE without WHERE works",
			"DELETE FROM 'a';",
			[]minisql.Statement{
				{
					Kind:      minisql.Delete,
					TableName: "a",
				},
			},
			nil,
		},
		{
			"DELETE with empty WHERE fails",
			"DELETE FROM 'a' WHERE",
			nil,
			errEmptyWhereClause,
		},
		{
			"DELETE with WHERE with field but no operator fails",
			"DELETE FROM 'a' WHERE b",
			nil,
			errWhereWithoutOperator,
		},
		{
			"DELETE with multiple conditions works",
			"DELETE FROM 'a' WHERE a = '1' AND b = 789;",
			[]minisql.Statement{
				{
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
