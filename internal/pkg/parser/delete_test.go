package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "b",
						Operand1IsField: true,
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
				Conditions: []minisql.Condition{
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        minisql.Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "DELETE with multiple conditions works",
			SQL:  "DELETE FROM 'a' WHERE a = '1' AND b = '789'",
			Expected: minisql.Statement{
				Kind:      minisql.Delete,
				TableName: "a",
				Conditions: []minisql.Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        minisql.Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        minisql.Eq,
						Operand2:        "789",
						Operand2IsField: false,
					},
				},
			},
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
