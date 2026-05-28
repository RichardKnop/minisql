package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
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
			errWhereExpectedField,
		},
		{
			"Incomplete UPDATE due incomplete WHERE clause fails",
			"UPDATE 'a' SET b = 'hello' WHERE a",
			nil,
			errWhereUnknownOperator,
		},
		{
			"UPDATE works",
			"UPDATE 'a' SET b = 'hello' WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE works with boolean value being set",
			"UPDATE 'a' SET b = false;",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: false, Valid: true},
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
					Fields:    []minisql.Field{{Name: "b"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: int64(25), Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
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
					Fields:    []minisql.Field{{Name: "b"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: float64(3.75), Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
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
					Fields:    []minisql.Field{{Name: "b"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Valid: false},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
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
					Fields:    []minisql.Field{{Name: "b"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello\\'world")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE with multiple SETs works",
			"UPDATE 'a' SET b = 'hello', c = NOW() WHERE a = '1';",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello")), Valid: true},
						"c": {Value: minisql.FunctionNow, Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
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
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("hello")), Valid: true},
						"c": {Value: minisql.NewTextPointer([]byte("bye")), Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
							minisql.FieldIsEqual(minisql.Field{Name: "b"}, minisql.OperandInteger, int64(789)),
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE with placeholders works",
			"UPDATE 'a' SET b = 'foo', c = ? WHERE a = ? AND b = 789 ; ",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						"c": {Value: minisql.Placeholder{}, Valid: true},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandPlaceholder, nil),
							minisql.FieldIsEqual(minisql.Field{Name: "b"}, minisql.OperandInteger, int64(789)),
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
				require.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}

func TestParse_UpdateArithmetic(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"UPDATE SET col = col + 1 (column plus literal)",
			"UPDATE 'products' SET count = count + 1;",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "products",
					Fields:    []minisql.Field{{Name: "count"}},
					Updates: map[string]minisql.OptionalValue{
						"count": {
							Value: &minisql.Expr{
								Left:  &minisql.Expr{Column: "count"},
								Right: &minisql.Expr{Literal: int64(1)},
								Op:    minisql.ArithAdd,
							},
							Valid: true,
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE SET col = col * 1.1 (column times float literal)",
			"UPDATE 'products' SET price = price * 1.1;",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "products",
					Fields:    []minisql.Field{{Name: "price"}},
					Updates: map[string]minisql.OptionalValue{
						"price": {
							Value: &minisql.Expr{
								Left:  &minisql.Expr{Column: "price"},
								Right: &minisql.Expr{Literal: float64(1.1)},
								Op:    minisql.ArithMul,
							},
							Valid: true,
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE SET col = col - col2 (column minus column)",
			"UPDATE 'orders' SET diff = total - discount;",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "orders",
					Fields:    []minisql.Field{{Name: "diff"}},
					Updates: map[string]minisql.OptionalValue{
						"diff": {
							Value: &minisql.Expr{
								Left:  &minisql.Expr{Column: "total"},
								Right: &minisql.Expr{Column: "discount"},
								Op:    minisql.ArithSub,
							},
							Valid: true,
						},
					},
				},
			},
			nil,
		},
		{
			"UPDATE SET col = a + b * c (operator precedence: * before +)",
			"UPDATE 't' SET x = a + b * c;",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "t",
					Fields:    []minisql.Field{{Name: "x"}},
					Updates: map[string]minisql.OptionalValue{
						"x": {
							Value: &minisql.Expr{
								Left: &minisql.Expr{Column: "a"},
								Right: &minisql.Expr{
									Left:  &minisql.Expr{Column: "b"},
									Right: &minisql.Expr{Column: "c"},
									Op:    minisql.ArithMul,
								},
								Op: minisql.ArithAdd,
							},
							Valid: true,
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
				require.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}
