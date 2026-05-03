package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_CheckConstraint(t *testing.T) {
	t.Parallel()

	leaf := func(cond minisql.Condition) *minisql.ConditionNode {
		return &minisql.ConditionNode{Leaf: &cond}
	}
	fieldCond := func(fieldName string, op minisql.Operator, operandType minisql.OperandType, value any) minisql.Condition {
		return minisql.Condition{
			Operand1: minisql.Operand{Type: minisql.OperandField, Value: minisql.Field{Name: fieldName}},
			Operator: op,
			Operand2: minisql.Operand{Type: operandType, Value: value},
		}
	}

	testCases := []testCase{
		{
			"CHECK (price > 0) sets Check and CheckCond on column",
			"CREATE TABLE products (price int8 not null check (price > 0));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "products",
					Columns: []minisql.Column{
						{
							Name:      "price",
							Kind:      minisql.Int8,
							Size:      8,
							Nullable:  false,
							Check:     "price > 0",
							CheckCond: leaf(fieldCond("price", minisql.Gt, minisql.OperandInteger, int64(0))),
						},
					},
				},
			},
			nil,
		},
		{
			"CHECK (qty >= 0) parses Gte operator",
			"CREATE TABLE stock (qty int8 check (qty >= 0));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "stock",
					Columns: []minisql.Column{
						{
							Name:      "qty",
							Kind:      minisql.Int8,
							Size:      8,
							Nullable:  true,
							Check:     "qty >= 0",
							CheckCond: leaf(fieldCond("qty", minisql.Gte, minisql.OperandInteger, int64(0))),
						},
					},
				},
			},
			nil,
		},
		{
			"column without CHECK leaves Check empty and CheckCond nil",
			"CREATE TABLE t (n int8 not null);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "t",
					Columns: []minisql.Column{
						{
							Name:     "n",
							Kind:     minisql.Int8,
							Size:     8,
							Nullable: false,
						},
					},
				},
			},
			nil,
		},
		{
			"CHECK after DEFAULT is supported",
			"CREATE TABLE t (n int8 not null default 1 check (n > 0));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "t",
					Columns: []minisql.Column{
						{
							Name:     "n",
							Kind:     minisql.Int8,
							Size:     8,
							Nullable: false,
							DefaultValue: minisql.OptionalValue{
								Value: int64(1),
								Valid: true,
							},
							Check:     "n > 0",
							CheckCond: leaf(fieldCond("n", minisql.Gt, minisql.OperandInteger, int64(0))),
						},
					},
				},
			},
			nil,
		},
		{
			"multiple columns each with independent CHECK constraints",
			"CREATE TABLE dims (w int8 check (w > 0), h int8 check (h > 0));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "dims",
					Columns: []minisql.Column{
						{
							Name:      "w",
							Kind:      minisql.Int8,
							Size:      8,
							Nullable:  true,
							Check:     "w > 0",
							CheckCond: leaf(fieldCond("w", minisql.Gt, minisql.OperandInteger, int64(0))),
						},
						{
							Name:      "h",
							Kind:      minisql.Int8,
							Size:      8,
							Nullable:  true,
							Check:     "h > 0",
							CheckCond: leaf(fieldCond("h", minisql.Gt, minisql.OperandInteger, int64(0))),
						},
					},
				},
			},
			nil,
		},
		{
			"CHECK with AND expression",
			"CREATE TABLE t (n int8 check (n > 0 AND n < 100));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "t",
					Columns: []minisql.Column{
						{
							Name:     "n",
							Kind:     minisql.Int8,
							Size:     8,
							Nullable: true,
							Check:    "n > 0 AND n < 100",
							CheckCond: &minisql.ConditionNode{
								Left:  leaf(fieldCond("n", minisql.Gt, minisql.OperandInteger, int64(0))),
								Op:    minisql.LogicOpAnd,
								Right: leaf(fieldCond("n", minisql.Lt, minisql.OperandInteger, int64(100))),
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
			t.Parallel()
			got, err := New().Parse(context.Background(), aTestCase.SQL)
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, got)
		})
	}
}
