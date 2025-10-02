package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/core/minisql"
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
			Err: errUpdateExpectedQuotedValueOrInt,
		},
		{
			Name: "Incomplete UPDATE due to no WHERE clause fails",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: "hello", Valid: true},
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
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: "hello", Valid: true},
				},
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
			Name: "UPDATE works",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: "hello", Valid: true},
				},
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
			Name: "UPDATE works with int value being set",
			SQL:  "UPDATE 'a' SET b = 25 WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: int64(25), Valid: true},
				},
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
			Name: "UPDATE works with NULL",
			SQL:  "UPDATE 'a' SET b = NULL WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Valid: false},
				},
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
			Name: "UPDATE works with simple quote inside",
			SQL:  "UPDATE 'a' SET b = 'hello\\'world' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: "hello\\'world", Valid: true},
				},
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
			Name: "UPDATE with multiple SETs works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1'",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: "hello", Valid: true},
					"c": {Value: "bye", Valid: true},
				},
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
			Name: "UPDATE with multiple SETs and multiple conditions works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = 789",
			Expected: minisql.Statement{
				Kind:      minisql.Update,
				TableName: "a",
				Updates: map[string]minisql.OptionalValue{
					"b": {Value: "hello", Valid: true},
					"c": {Value: "bye", Valid: true},
				},
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
