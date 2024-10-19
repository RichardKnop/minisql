package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

func TestParse_Update(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty UPDATE fails",
			SQL:      "UPDATE",
			Expected: minisql.Statement{Kind: minisql.Update},
			Err:      errEmptyTableName,
		},
		{
			Name: "Incomplete UPDATE with table name fails",
			SQL:  "UPDATE 'a'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
			},
			Err: errWhereRequiredForUpdateDelete,
		},
		{
			Name: "Incomplete UPDATE with table name and SET fails",
			SQL:  "UPDATE 'a' SET",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
			},
			Err: errWhereRequiredForUpdateDelete,
		},
		{
			Name: "Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			SQL:  "UPDATE 'a' SET b WHERE",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
			},
			Err: errUpdateExpectedEquals,
		},
		{
			Name: "Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			SQL:  "UPDATE 'a' SET b = WHERE",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
			},
			Err: errUpdateExpectedQuotedValue,
		},
		{
			Name: "Incomplete UPDATE due to no WHERE clause fails",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]any{
					"b": "hello",
				},
			},
			Err: errEmptyWhereClause,
		},
		{
			Name: "Incomplete UPDATE due incomplete WHERE clause fails",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]any{
					"b": "hello",
				},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1:        "a",
							Operand1IsField: true,
						},
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "UPDATE works",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]any{
					"b": "hello",
				},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1:        "a",
							Operand1IsField: true,
							Operator:        minisql.Eq,
							Operand2:        "1",
							Operand2IsField: false,
						},
					},
				},
			},
		},
		{
			Name: "UPDATE works with simple quote inside",
			SQL:  "UPDATE 'a' SET b = 'hello\\'world' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]any{
					"b": "hello\\'world",
				},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1:        "a",
							Operand1IsField: true,
							Operator:        minisql.Eq,
							Operand2:        "1",
							Operand2IsField: false,
						},
					},
				},
			},
		},
		{
			Name: "UPDATE with multiple SETs works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]any{
					"b": "hello",
					"c": "bye",
				},
				Conditions: minisql.OneOrMore{
					{
						{
							Operand1:        "a",
							Operand1IsField: true,
							Operator:        minisql.Eq,
							Operand2:        "1",
							Operand2IsField: false,
						},
					},
				},
			},
		},
		{
			Name: "UPDATE with multiple SETs and multiple conditions works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = '789'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]any{
					"b": "hello",
					"c": "bye",
				},
				Conditions: minisql.OneOrMore{
					{
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
