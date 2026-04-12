package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_CastExpression(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"CAST column to INT8 in SELECT",
			`SELECT CAST(price AS INT8) FROM "items"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "items",
					Fields: []minisql.Field{
						{
							Name: "CAST(price AS int8)",
							Expr: &minisql.Expr{
								CastExpr:       &minisql.Expr{Column: "price"},
								CastTargetType: minisql.Int8,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"CAST column to DOUBLE in SELECT",
			`SELECT CAST(amount AS DOUBLE) FROM "ledger"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "ledger",
					Fields: []minisql.Field{
						{
							Name: "CAST(amount AS double)",
							Expr: &minisql.Expr{
								CastExpr:       &minisql.Expr{Column: "amount"},
								CastTargetType: minisql.Double,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"CAST column to TEXT in SELECT",
			`SELECT CAST(code AS TEXT) FROM "products"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "products",
					Fields: []minisql.Field{
						{
							Name: "CAST(code AS text)",
							Expr: &minisql.Expr{
								CastExpr:       &minisql.Expr{Column: "code"},
								CastTargetType: minisql.Text,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"CAST literal to INT8",
			`SELECT CAST(3.7 AS INT8) FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "CAST(3.7 AS int8)",
							Expr: &minisql.Expr{
								CastExpr:       &minisql.Expr{Literal: float64(3.7)},
								CastTargetType: minisql.Int8,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"CAST with AS alias",
			`SELECT CAST(price AS DOUBLE) AS discounted FROM "items"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "items",
					Fields: []minisql.Field{
						{
							Name:  "CAST(price AS double)",
							Alias: "discounted",
							Expr: &minisql.Expr{
								CastExpr:       &minisql.Expr{Column: "price"},
								CastTargetType: minisql.Double,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"CAST with VARCHAR type",
			`SELECT CAST(id AS VARCHAR(20)) FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "CAST(id AS varchar)",
							Expr: &minisql.Expr{
								CastExpr:       &minisql.Expr{Column: "id"},
								CastTargetType: minisql.Varchar,
							},
						},
					},
				},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			statements, err := New().Parse(context.Background(), tc.SQL)
			if tc.Err != nil {
				require.Error(t, err)
				assert.Equal(t, tc.Err, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Expected, statements)
		})
	}
}
