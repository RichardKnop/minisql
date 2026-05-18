package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_Explain(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"EXPLAIN SELECT",
			"EXPLAIN SELECT * FROM users WHERE id = 1;",
			[]minisql.Statement{
				{
					Kind: minisql.Explain,
					ExplainStatement: &minisql.Statement{
						Kind:      minisql.Select,
						TableName: "users",
						Fields:    []minisql.Field{{Name: "*"}},
						Conditions: minisql.OneOrMore{
							{
								minisql.FieldIsEqual(
									minisql.Field{Name: "id"},
									minisql.OperandInteger,
									int64(1),
								),
							},
						},
					},
				},
			},
			nil,
		},
		{
			"EXPLAIN ANALYZE SELECT",
			"EXPLAIN ANALYZE SELECT id FROM users;",
			[]minisql.Statement{
				{
					Kind:           minisql.Explain,
					ExplainAnalyze: true,
					ExplainStatement: &minisql.Statement{
						Kind:      minisql.Select,
						TableName: "users",
						Fields:    []minisql.Field{{Name: "id"}},
					},
				},
			},
			nil,
		},
		{
			"EXPLAIN unsupported statement still parses",
			"EXPLAIN INSERT INTO users (id) VALUES (1);",
			[]minisql.Statement{
				{
					Kind: minisql.Explain,
					ExplainStatement: &minisql.Statement{
						Kind:      minisql.Insert,
						TableName: "users",
						Fields:    []minisql.Field{{Name: "id"}},
						Inserts: [][]minisql.OptionalValue{
							{minisql.MakeInt8(int64(1))},
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
				assert.ErrorIs(t, err, tc.Err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Expected, statements)
		})
	}
}
