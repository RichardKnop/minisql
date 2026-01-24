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
			"SELECT a, COUNT(*) works fails",
			"SELECT a, COUNT(*) FROM b;",
			nil,
			errCannotCombineCountAsterisk,
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
					Fields:    []minisql.Field{{Name: "a"}, {Name: "b"}, {Name: "c"}},
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
			errEmptyWhereClause,
		},
		{
			"SELECT with WHERE with only operand fails",
			"SELECT a, c, d FROM b WHERE a",
			nil,
			errWhereWithoutOperator,
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
