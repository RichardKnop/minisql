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
			"Empty UPDATE fails",
			"UPDATE",
			nil,
			errEmptyTableName,
		},
		{
			"Incomplete UPDATE with just table name fails",
			"UPDATE 'a'",
			nil,
			errNoFieldsToUpdate,
		},
		{
			"Incomplete UPDATE with table name and SET without field fails",
			"UPDATE 'a' SET ",
			nil,
			errNoFieldsToUpdate,
		},
		{
			"Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			"UPDATE 'a' SET b WHERE",
			nil,
			errUpdateExpectedEquals,
		},
		{
			"Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			"UPDATE 'a' SET b = WHERE",
			nil,
			errUpdateExpectedQuotedValueOrInt,
		},
		{
			"Incomplete UPDATE due to no WHERE clause fails",
			"UPDATE 'a' SET b = 'hello' WHERE",
			nil,
			errEmptyWhereClause,
		},
		{
			"Incomplete UPDATE due incomplete WHERE clause fails",
			"UPDATE 'a' SET b = 'hello' WHERE a",
			nil,
			errWhereWithoutOperator,
		},
		{
			"UPDATE works",
			"UPDATE 'a' SET b = 'hello' WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
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
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE works with int value being set",
			"UPDATE 'a' SET b = 25 WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: int64(25), Valid: true},
					},
					Conditions: minisql.OneOrMore{
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
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE works with float value being set",
			"UPDATE 'a' SET b = 3.75 WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: float64(3.75), Valid: true},
					},
					Conditions: minisql.OneOrMore{
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
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE works with NULL",
			"UPDATE 'a' SET b = NULL WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Valid: false},
					},
					Conditions: minisql.OneOrMore{
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
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE works with simple quote inside",
			"UPDATE 'a' SET b = 'hello\\'world' WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello\\'world")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
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
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE with multiple SETs works",
			"UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello")), Valid: true},
						"c": {Value: minisql.NewTextPointer([]byte("bye")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
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
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE with multiple SETs and multiple conditions works",
			"UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = 789 ; ",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello")), Valid: true},
						"c": {Value: minisql.NewTextPointer([]byte("bye")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
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
