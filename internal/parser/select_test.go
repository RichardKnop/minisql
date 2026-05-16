package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_Select(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"SELECT DISTINCT works",
			"SELECT DISTINCT a FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Distinct:  true,
					Fields:    []minisql.Field{{Name: "a"}},
				},
			},
			nil,
		},
		{
			"SELECT DISTINCT multiple fields works",
			"SELECT DISTINCT a, c FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Distinct:  true,
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}},
				},
			},
			nil,
		},
		{
			"SELECT DISTINCT * works",
			"SELECT DISTINCT * FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Distinct:  true,
					Fields:    []minisql.Field{{Name: "*"}},
				},
			},
			nil,
		},
		{
			"SELECT DISTINCT with ORDER BY works",
			"SELECT DISTINCT a FROM b ORDER BY a;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Distinct:  true,
					Fields:    []minisql.Field{{Name: "a"}},
					OrderBy: []minisql.OrderBy{
						{
							Field:     minisql.Field{Name: "a"},
							Direction: minisql.Asc,
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT without FROM fails",
			"SELECT",
			nil,
			errEmptyTableName,
		},
		{
			"SELECT without fields fails",
			"SELECT FROM 'a'",
			nil,
			errSelectWithoutFields,
		},
		{
			"SELECT with comma and empty field fails",
			"SELECT b, FROM 'a'",
			nil,
			errSelectWithoutFields,
		},
		{
			"SELECT a, * works fails",
			"SELECT a, * FROM b;",
			nil,
			errCannotCombineAsterisk,
		},
		{
			"SELECT *, a works fails",
			"SELECT *, a FROM b;",
			nil,
			errExpectedFrom,
		},
		{
			// COUNT(*) combined with other fields is allowed by the parser (for GROUP BY queries).
			// Semantic validation (non-aggregate column must appear in GROUP BY) happens at execution time.
			"SELECT a, COUNT(*) parses successfully",
			"SELECT a, COUNT(*) FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields: []minisql.Field{
						{Name: "a"},
						{Name: "COUNT(*)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateCount},
					},
				},
			},
			nil,
		},
		{
			"SELECT COUNT(*), a works fails",
			"SELECT COUNT(*), a FROM b;",
			nil,
			errExpectedFrom,
		},
		{
			"SELECT * works",
			"SELECT * FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
				},
			},
			nil,
		},
		{
			"SELECT with table alias works",
			"SELECT * FROM b as b_alias;",
			[]minisql.Statement{
				{
					Kind:       minisql.Select,
					TableName:  "b",
					TableAlias: "b_alias",
					Fields:     []minisql.Field{{Name: "*"}},
				},
			},
			nil,
		},
		{
			"SELECT COUNT(*) works",
			"SELECT COUNT(*) FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "COUNT(*)"}},
				},
			},
			nil,
		},
		{
			"SELECT works",
			"SELECT a FROM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}},
				},
			},
			nil,
		},
		{
			"SELECT works with lowercase",
			" select a fRoM b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}},
				},
			},
			nil,
		},
		{
			"SELECT many fields works",
			"SELECT a, c, d FROM b ;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
				},
			},
			nil,
		},
		{
			"SELECT with alias works",
			"SELECT a as z, b as y, c FROM b ; ",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a", Alias: "z"}, {Name: "b", Alias: "y"}, {Name: "c"}},
					Aliases: map[string]string{
						"a": "z",
						"b": "y",
					},
				},
			},
			nil,
		},
		{
			"SELECT with ORDER BY works",
			"SELECT * FROM b ORDER BY a;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					OrderBy: []minisql.OrderBy{
						{
							Field:     minisql.Field{Name: "a"},
							Direction: minisql.Asc,
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with ORDER BY ASC works",
			"SELECT * FROM b ORDER BY a ASC;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					OrderBy: []minisql.OrderBy{
						{
							Field:     minisql.Field{Name: "a"},
							Direction: minisql.Asc,
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with ORDER BY DESC works",
			"SELECT * FROM b ORDER BY a DESC;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					OrderBy: []minisql.OrderBy{
						{
							Field:     minisql.Field{Name: "a"},
							Direction: minisql.Desc,
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with ORDER BY multiple fields works",
			"SELECT * FROM b ORDER BY a DESC, c ASC;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					OrderBy: []minisql.OrderBy{
						{
							Field:     minisql.Field{Name: "a"},
							Direction: minisql.Desc,
						},
						{
							Field:     minisql.Field{Name: "c"},
							Direction: minisql.Asc,
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with LIMIT works",
			"SELECT * FROM b LIMIT 10;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					Limit:     minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with OFFSET works",
			"SELECT * FROM b OFFSET 10;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					Offset:    minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with LIMIT and OFFSET works",
			"SELECT * FROM b LIMIT 10 OFFSET 20;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "*"}},
					Limit:     minisql.OptionalValue{Value: int64(10), Valid: true},
					Offset:    minisql.OptionalValue{Value: int64(20), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with empty WHERE fails",
			"SELECT a, c, d FROM b WHERE",
			nil,
			errWhereExpectedField,
		},
		{
			"SELECT with WHERE with only operand fails",
			"SELECT a, c, d FROM b WHERE a",
			nil,
			errWhereUnknownOperator,
		},
		{
			"SELECT with WHERE with multiple conditions using AND works",
			`SELECT a, c, d FROM "b" WHERE a != '1' AND b = 2 and c = 'Foo Bar';`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsNotEqual(minisql.Field{Name: "a"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
							minisql.FieldIsEqual(minisql.Field{Name: "b"}, minisql.OperandInteger, int64(2)),
							minisql.FieldIsEqual(minisql.Field{Name: "c"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("Foo Bar"))),
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and LIMIT works",
			`SELECT a, c, d FROM "b" WHERE a = 2 LIMIT 10;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandInteger, int64(2)),
						},
					},
					Limit: minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and OFFSET works",
			`SELECT a, c, d FROM "b" WHERE a = 2 OFFSET 10;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandInteger, int64(2)),
						},
					},
					Offset: minisql.OptionalValue{Value: int64(10), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and LIMIT and OFFSET works",
			`SELECT a, c, d FROM "b" WHERE a = 2 LIMIT 10 OFFSET 20;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandInteger, int64(2)),
						},
					},
					Limit:  minisql.OptionalValue{Value: int64(10), Valid: true},
					Offset: minisql.OptionalValue{Value: int64(20), Valid: true},
				},
			},
			nil,
		},
		{
			"SELECT with WHERE and ORDER B works",
			`SELECT a, c, d FROM "b" WHERE a = 2 ORDER BY c ASC, d DESC;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "a"}, minisql.OperandInteger, int64(2)),
						},
					},
					OrderBy: []minisql.OrderBy{
						{
							Field:     minisql.Field{Name: "c"},
							Direction: minisql.Asc,
						},
						{
							Field:     minisql.Field{Name: "d"},
							Direction: minisql.Desc,
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT ending with IS NOT NULL works",
			`select * from a where b = 'c' and d is not null;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "*"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "b"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("c"))),
							minisql.FieldIsNotNull(minisql.Field{Name: "d"}),
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT ending with IS NULL works",
			`select * from a where b is null;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "*"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsNull(minisql.Field{Name: "b"}),
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with placeholders in WHERE works",
			`SELECT a, c, d FROM "b" WHERE a != ? and b > ?;`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "b",
					Fields:    []minisql.Field{{Name: "a"}, {Name: "c"}, {Name: "d"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsNotEqual(minisql.Field{Name: "a"}, minisql.OperandPlaceholder, nil),
							minisql.FieldIsGreater(minisql.Field{Name: "b"}, minisql.OperandPlaceholder, nil),
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with INNER JOIN",
			"SELECT u.id, p.name FROM users AS u INNER JOIN profiles AS p ON u.id = p.user_id;",
			[]minisql.Statement{
				{
					Kind:       minisql.Select,
					TableName:  "users",
					TableAlias: "u",
					Fields: []minisql.Field{
						{AliasPrefix: "u", Name: "id"},
						{AliasPrefix: "p", Name: "name"},
					},
					Joins: []minisql.Join{
						{
							Type:       minisql.Inner,
							TableName:  "profiles",
							TableAlias: "p",
							Conditions: minisql.Conditions{
								minisql.FieldIsEqual(
									minisql.Field{
										AliasPrefix: "u",
										Name:        "id",
									},
									minisql.OperandField,
									minisql.Field{
										AliasPrefix: "p",
										Name:        "user_id",
									},
								),
							},
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with INNER JOIN and WHERE clause",
			`SELECT u.id, p.name FROM users AS u
			INNER JOIN profiles AS p ON u.id = p.user_id
			WHERE u.dob > '1999-01-01 00:00:00';`,
			[]minisql.Statement{
				{
					Kind:       minisql.Select,
					TableName:  "users",
					TableAlias: "u",
					Fields: []minisql.Field{
						{AliasPrefix: "u", Name: "id"},
						{AliasPrefix: "p", Name: "name"},
					},
					Joins: []minisql.Join{
						{
							Type:       minisql.Inner,
							TableName:  "profiles",
							TableAlias: "p",
							Conditions: minisql.Conditions{
								minisql.FieldIsEqual(
									minisql.Field{
										AliasPrefix: "u",
										Name:        "id",
									},
									minisql.OperandField,
									minisql.Field{
										AliasPrefix: "p",
										Name:        "user_id",
									},
								),
							},
						},
					},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsGreater(minisql.Field{
								AliasPrefix: "u",
								Name:        "dob",
							}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1999-01-01 00:00:00"))),
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with FULL OUTER JOIN",
			"SELECT u.id, o.id FROM users AS u FULL OUTER JOIN orders AS o ON u.id = o.user_id;",
			[]minisql.Statement{
				{
					Kind:       minisql.Select,
					TableName:  "users",
					TableAlias: "u",
					Fields: []minisql.Field{
						{AliasPrefix: "u", Name: "id"},
						{AliasPrefix: "o", Name: "id"},
					},
					Joins: []minisql.Join{
						{
							Type:       minisql.FullOuter,
							TableName:  "orders",
							TableAlias: "o",
							Conditions: minisql.Conditions{
								minisql.FieldIsEqual(
									minisql.Field{AliasPrefix: "u", Name: "id"},
									minisql.OperandField,
									minisql.Field{AliasPrefix: "o", Name: "user_id"},
								),
							},
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with FULL JOIN (short form)",
			"SELECT u.id, o.id FROM users AS u FULL JOIN orders AS o ON u.id = o.user_id;",
			[]minisql.Statement{
				{
					Kind:       minisql.Select,
					TableName:  "users",
					TableAlias: "u",
					Fields: []minisql.Field{
						{AliasPrefix: "u", Name: "id"},
						{AliasPrefix: "o", Name: "id"},
					},
					Joins: []minisql.Join{
						{
							Type:       minisql.FullOuter,
							TableName:  "orders",
							TableAlias: "o",
							Conditions: minisql.Conditions{
								minisql.FieldIsEqual(
									minisql.Field{AliasPrefix: "u", Name: "id"},
									minisql.OperandField,
									minisql.Field{AliasPrefix: "o", Name: "user_id"},
								),
							},
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT with nested INNER JOIN",
			`SELECT u.id, p.name FROM users AS u
			INNER JOIN profiles AS p ON u.id = p.user_id
			LEFT JOIN avatars As a ON a.profile_id = p.id;`,
			[]minisql.Statement{
				{
					Kind:       minisql.Select,
					TableName:  "users",
					TableAlias: "u",
					Fields: []minisql.Field{
						{AliasPrefix: "u", Name: "id"},
						{AliasPrefix: "p", Name: "name"},
					},
					Joins: []minisql.Join{
						{
							Type:       minisql.Inner,
							TableName:  "profiles",
							TableAlias: "p",
							Conditions: minisql.Conditions{
								minisql.FieldIsEqual(
									minisql.Field{
										AliasPrefix: "u",
										Name:        "id",
									},
									minisql.OperandField,
									minisql.Field{
										AliasPrefix: "p",
										Name:        "user_id",
									},
								),
							},
							Joins: []minisql.Join{
								{
									Type:       minisql.Left,
									TableName:  "avatars",
									TableAlias: "a",
									Conditions: minisql.Conditions{
										minisql.FieldIsEqual(
											minisql.Field{
												AliasPrefix: "a",
												Name:        "profile_id",
											},
											minisql.OperandField,
											minisql.Field{
												AliasPrefix: "p",
												Name:        "id",
											},
										),
									},
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

func TestParse_SelectGroupBy(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"GROUP BY single column",
			"SELECT user_id, SUM(total) FROM orders GROUP BY user_id;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "SUM(total)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateSum, Column: "total"},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
				},
			},
			nil,
		},
		{
			"GROUP BY multiple columns",
			"SELECT a, b, COUNT(*) FROM t GROUP BY a, b;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{Name: "a"},
						{Name: "b"},
						{Name: "COUNT(*)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{},
						{Kind: minisql.AggregateCount},
					},
					GroupBy: []minisql.Field{{Name: "a"}, {Name: "b"}},
				},
			},
			nil,
		},
		{
			"GROUP BY with ORDER BY",
			"SELECT user_id, SUM(total) FROM orders GROUP BY user_id ORDER BY user_id;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "SUM(total)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateSum, Column: "total"},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					OrderBy: []minisql.OrderBy{
						{Field: minisql.Field{Name: "user_id"}, Direction: minisql.Asc},
					},
				},
			},
			nil,
		},
		{
			"GROUP BY with LIMIT",
			"SELECT user_id, COUNT(*) FROM orders GROUP BY user_id LIMIT 5;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "COUNT(*)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateCount},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					Limit:   minisql.OptionalValue{Valid: true, Value: int64(5)},
				},
			},
			nil,
		},
		{
			"SELECT without GROUP BY still works (aggregate-only)",
			"SELECT SUM(price) FROM items;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "items",
					Fields:    []minisql.Field{{Name: "SUM(price)"}},
					Aggregates: []minisql.AggregateExpr{
						{Kind: minisql.AggregateSum, Column: "price"},
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

func TestParse_SelectHaving(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"HAVING with SUM aggregate",
			"SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 100;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "SUM(total)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateSum, Column: "total"},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					Having: minisql.OneOrMore{
						{minisql.FieldIsGreater(minisql.Field{Name: "SUM(total)"}, minisql.OperandInteger, int64(100))},
					},
				},
			},
			nil,
		},
		{
			"HAVING with COUNT(*)",
			"SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) >= 2;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "COUNT(*)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateCount},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					Having: minisql.OneOrMore{
						{minisql.FieldIsGreaterOrEqual(minisql.Field{Name: "COUNT(*)"}, minisql.OperandInteger, int64(2))},
					},
				},
			},
			nil,
		},
		{
			"HAVING with GROUP BY column",
			"SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING user_id > 1;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "SUM(total)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateSum, Column: "total"},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					Having: minisql.OneOrMore{
						{minisql.FieldIsGreater(minisql.Field{Name: "user_id"}, minisql.OperandInteger, int64(1))},
					},
				},
			},
			nil,
		},
		{
			"HAVING with ORDER BY",
			"SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 50 ORDER BY user_id;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "SUM(total)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateSum, Column: "total"},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					Having: minisql.OneOrMore{
						{minisql.FieldIsGreater(minisql.Field{Name: "SUM(total)"}, minisql.OperandInteger, int64(50))},
					},
					OrderBy: []minisql.OrderBy{
						{Field: minisql.Field{Name: "user_id"}, Direction: minisql.Asc},
					},
				},
			},
			nil,
		},
		{
			"WHERE and HAVING together",
			"SELECT user_id, SUM(total) FROM orders WHERE total > 0 GROUP BY user_id HAVING SUM(total) > 50;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "orders",
					Fields: []minisql.Field{
						{Name: "user_id"},
						{Name: "SUM(total)"},
					},
					Aggregates: []minisql.AggregateExpr{
						{},
						{Kind: minisql.AggregateSum, Column: "total"},
					},
					GroupBy: []minisql.Field{{Name: "user_id"}},
					Conditions: minisql.OneOrMore{
						{minisql.FieldIsGreater(minisql.Field{Name: "total"}, minisql.OperandInteger, int64(0))},
					},
					Having: minisql.OneOrMore{
						{minisql.FieldIsGreater(minisql.Field{Name: "SUM(total)"}, minisql.OperandInteger, int64(50))},
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

func TestParse_SelectArithmetic(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"SELECT col * literal produces computed field",
			"SELECT price * 1.1 FROM products;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "products",
					Fields: []minisql.Field{
						{
							Name: "price * 1.1",
							Expr: &minisql.Expr{
								Left:  &minisql.Expr{Column: "price"},
								Right: &minisql.Expr{Literal: float64(1.1)},
								Op:    minisql.ArithMul,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT col * literal AS alias sets alias on field",
			"SELECT price * 1.1 AS discounted FROM products;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "products",
					Fields: []minisql.Field{
						{
							Name:  "price * 1.1",
							Alias: "discounted",
							Expr: &minisql.Expr{
								Left:  &minisql.Expr{Column: "price"},
								Right: &minisql.Expr{Literal: float64(1.1)},
								Op:    minisql.ArithMul,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT a + b mixed with plain field",
			"SELECT id, a + b FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{Name: "id"},
						{
							Name: "a + b",
							Expr: &minisql.Expr{
								Left:  &minisql.Expr{Column: "a"},
								Right: &minisql.Expr{Column: "b"},
								Op:    minisql.ArithAdd,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"SELECT operator precedence: a + b * c",
			"SELECT a + b * c FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "a + (b * c)",
							Expr: &minisql.Expr{
								Left: &minisql.Expr{Column: "a"},
								Right: &minisql.Expr{
									Left:  &minisql.Expr{Column: "b"},
									Right: &minisql.Expr{Column: "c"},
									Op:    minisql.ArithMul,
								},
								Op: minisql.ArithAdd,
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

func TestParse_SelectScalarFunctions(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"COALESCE with two column args",
			"SELECT COALESCE(a, b) FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "COALESCE(a, b)",
							Expr: &minisql.Expr{
								FuncName: "COALESCE",
								Args: []*minisql.Expr{
									{Column: "a"},
									{Column: "b"},
								},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"COALESCE with literal fallback",
			"SELECT COALESCE(score, 0) FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "COALESCE(score, 0)",
							Expr: &minisql.Expr{
								FuncName: "COALESCE",
								Args: []*minisql.Expr{
									{Column: "score"},
									{Literal: int64(0)},
								},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"COALESCE with NULL literal",
			"SELECT COALESCE(a, NULL, b) FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "COALESCE(a, NULL, b)",
							Expr: &minisql.Expr{
								FuncName: "COALESCE",
								Args: []*minisql.Expr{
									{Column: "a"},
									{IsNull: true},
									{Column: "b"},
								},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"COALESCE with alias",
			"SELECT COALESCE(name, 'unknown') AS display_name FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name:  "COALESCE(name, unknown)",
							Alias: "display_name",
							Expr: &minisql.Expr{
								FuncName: "COALESCE",
								Args: []*minisql.Expr{
									{Column: "name"},
									{Literal: minisql.NewTextPointer([]byte("unknown"))},
								},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"NULLIF with column and literal",
			"SELECT NULLIF(status, 0) FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "NULLIF(status, 0)",
							Expr: &minisql.Expr{
								FuncName: "NULLIF",
								Args: []*minisql.Expr{
									{Column: "status"},
									{Literal: int64(0)},
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
			t.Parallel()
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

func TestParse_SelectCaseWhen(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"searched CASE with ELSE",
			"SELECT CASE WHEN score >= 90 THEN 1 ELSE 0 END FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "CASE WHEN score >= 90 THEN 1 ELSE 0 END",
							Expr: &minisql.Expr{
								CaseClauses: []minisql.CaseWhen{
									{
										Cond: &minisql.ConditionNode{
											Leaf: &minisql.Condition{
												Operand1: minisql.Operand{
													Type:  minisql.OperandField,
													Value: minisql.Field{Name: "score"},
												},
												Operator: minisql.Gte,
												Operand2: minisql.Operand{
													Type:  minisql.OperandInteger,
													Value: int64(90),
												},
											},
										},
										Then: &minisql.Expr{Literal: int64(1)},
									},
								},
								CaseElse: &minisql.Expr{Literal: int64(0)},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"searched CASE without ELSE",
			"SELECT CASE WHEN active = TRUE THEN 1 END FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "CASE WHEN active = true THEN 1 END",
							Expr: &minisql.Expr{
								CaseClauses: []minisql.CaseWhen{
									{
										Cond: &minisql.ConditionNode{
											Leaf: &minisql.Condition{
												Operand1: minisql.Operand{
													Type:  minisql.OperandField,
													Value: minisql.Field{Name: "active"},
												},
												Operator: minisql.Eq,
												Operand2: minisql.Operand{
													Type:  minisql.OperandBoolean,
													Value: true,
												},
											},
										},
										Then: &minisql.Expr{Literal: int64(1)},
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
			"searched CASE with AS alias",
			"SELECT CASE WHEN score >= 90 THEN 1 ELSE 0 END AS grade FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name:  "CASE WHEN score >= 90 THEN 1 ELSE 0 END",
							Alias: "grade",
							Expr: &minisql.Expr{
								CaseClauses: []minisql.CaseWhen{
									{
										Cond: &minisql.ConditionNode{
											Leaf: &minisql.Condition{
												Operand1: minisql.Operand{
													Type:  minisql.OperandField,
													Value: minisql.Field{Name: "score"},
												},
												Operator: minisql.Gte,
												Operand2: minisql.Operand{
													Type:  minisql.OperandInteger,
													Value: int64(90),
												},
											},
										},
										Then: &minisql.Expr{Literal: int64(1)},
									},
								},
								CaseElse: &minisql.Expr{Literal: int64(0)},
							},
						},
					},
				},
			},
			nil,
		},
		{
			"simple CASE",
			"SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'pending' ELSE 'other' END FROM t;",
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "CASE status WHEN 1 THEN active WHEN 2 THEN pending ELSE other END",
							Expr: &minisql.Expr{
								CaseInput: &minisql.Expr{Column: "status"},
								CaseClauses: []minisql.CaseWhen{
									{
										When: &minisql.Expr{Literal: int64(1)},
										Then: &minisql.Expr{Literal: minisql.NewTextPointer([]byte("active"))},
									},
									{
										When: &minisql.Expr{Literal: int64(2)},
										Then: &minisql.Expr{Literal: minisql.NewTextPointer([]byte("pending"))},
									},
								},
								CaseElse: &minisql.Expr{Literal: minisql.NewTextPointer([]byte("other"))},
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
