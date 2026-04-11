package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_Union(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"UNION ALL two tables",
			`SELECT id, name FROM "employees" UNION ALL SELECT id, name FROM "contractors"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "employees",
					Fields:    []minisql.Field{{Name: "id"}, {Name: "name"}},
					Unions: []minisql.UnionClause{
						{
							All: true,
							Stmt: minisql.Statement{
								Kind:      minisql.Select,
								TableName: "contractors",
								Fields:    []minisql.Field{{Name: "id"}, {Name: "name"}},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"UNION (dedup) two tables",
			`SELECT id FROM "a" UNION SELECT id FROM "b"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "id"}},
					Unions: []minisql.UnionClause{
						{
							All: false,
							Stmt: minisql.Statement{
								Kind:      minisql.Select,
								TableName: "b",
								Fields:    []minisql.Field{{Name: "id"}},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"UNION ALL three tables (chained)",
			`SELECT id FROM "a" UNION ALL SELECT id FROM "b" UNION ALL SELECT id FROM "c"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "id"}},
					Unions: []minisql.UnionClause{
						{
							All: true,
							Stmt: minisql.Statement{
								Kind:      minisql.Select,
								TableName: "b",
								Fields:    []minisql.Field{{Name: "id"}},
								Unions: []minisql.UnionClause{
									{
										All: true,
										Stmt: minisql.Statement{
											Kind:      minisql.Select,
											TableName: "c",
											Fields:    []minisql.Field{{Name: "id"}},
										},
									},
								},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"UNION ALL with WHERE clause on first branch",
			`SELECT id FROM "employees" WHERE active = TRUE UNION ALL SELECT id FROM "contractors"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "employees",
					Fields:    []minisql.Field{{Name: "id"}},
					Conditions: minisql.OneOrMore{
						{
							{
								Operand1: minisql.Operand{Type: minisql.OperandField, Value: minisql.Field{Name: "active"}},
								Operator: minisql.Eq,
								Operand2: minisql.Operand{Type: minisql.OperandBoolean, Value: true},
							},
						},
					},
					Unions: []minisql.UnionClause{
						{
							All: true,
							Stmt: minisql.Statement{
								Kind:      minisql.Select,
								TableName: "contractors",
								Fields:    []minisql.Field{{Name: "id"}},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"UNION ALL with trailing semicolon",
			`SELECT id FROM "a" UNION ALL SELECT id FROM "b";`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "id"}},
					Unions: []minisql.UnionClause{
						{
							All: true,
							Stmt: minisql.Statement{
								Kind:      minisql.Select,
								TableName: "b",
								Fields:    []minisql.Field{{Name: "id"}},
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
