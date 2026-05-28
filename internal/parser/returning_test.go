package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_Returning(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"INSERT RETURNING single column",
			"INSERT INTO 'users' (id, name) VALUES (1, 'Alice') RETURNING id",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "users",
					Fields:    []minisql.Field{{Name: "id"}, {Name: "name"}},
					Inserts: [][]minisql.OptionalValue{
						{
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("Alice")), Valid: true},
						},
					},
					ReturningFields: []minisql.Field{{Name: "id"}},
				},
			},
			nil,
		},
		{
			"INSERT RETURNING multiple columns",
			"INSERT INTO 'users' (name, score) VALUES ('Bob', 10) RETURNING id, name, score",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "users",
					Fields:    []minisql.Field{{Name: "name"}, {Name: "score"}},
					Inserts: [][]minisql.OptionalValue{
						{
							{Value: minisql.NewTextPointer([]byte("Bob")), Valid: true},
							{Value: int64(10), Valid: true},
						},
					},
					ReturningFields: []minisql.Field{{Name: "id"}, {Name: "name"}, {Name: "score"}},
				},
			},
			nil,
		},
		{
			"INSERT RETURNING star",
			"INSERT INTO 'users' (name) VALUES ('Carol') RETURNING *",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "users",
					Fields:    []minisql.Field{{Name: "name"}},
					Inserts: [][]minisql.OptionalValue{
						{
							{Value: minisql.NewTextPointer([]byte("Carol")), Valid: true},
						},
					},
					ReturningFields: []minisql.Field{{Name: "*"}},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO NOTHING RETURNING id",
			"INSERT INTO 'users' (id, name) VALUES (1, 'Dave') ON CONFLICT DO NOTHING RETURNING id",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "users",
					ConflictAction: minisql.ConflictActionDoNothing,
					Fields:         []minisql.Field{{Name: "id"}, {Name: "name"}},
					Inserts: [][]minisql.OptionalValue{
						{
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("Dave")), Valid: true},
						},
					},
					ReturningFields: []minisql.Field{{Name: "id"}},
				},
			},
			nil,
		},
		{
			"UPDATE RETURNING single column",
			"UPDATE 'users' SET score = 99 WHERE id = '1' RETURNING score",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "users",
					Updates: map[string]minisql.OptionalValue{
						"score": {Value: int64(99), Valid: true},
					},
					Fields: []minisql.Field{{Name: "score"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "id"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
						},
					},
					ReturningFields: []minisql.Field{{Name: "score"}},
				},
			},
			nil,
		},
		{
			"UPDATE RETURNING multiple columns",
			"UPDATE 'users' SET score = 99 WHERE id = '1' RETURNING id, score",
			[]minisql.Statement{
				{
					Kind:      minisql.Update,
					TableName: "users",
					Updates: map[string]minisql.OptionalValue{
						"score": {Value: int64(99), Valid: true},
					},
					Fields: []minisql.Field{{Name: "score"}},
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "id"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
						},
					},
					ReturningFields: []minisql.Field{{Name: "id"}, {Name: "score"}},
				},
			},
			nil,
		},
		{
			"DELETE RETURNING columns",
			"DELETE FROM 'sessions' WHERE id = '42' RETURNING id, name",
			[]minisql.Statement{
				{
					Kind:      minisql.Delete,
					TableName: "sessions",
					Conditions: minisql.OneOrMore{
						{
							minisql.FieldIsEqual(minisql.Field{Name: "id"}, minisql.OperandQuotedString, minisql.NewTextPointer([]byte("42"))),
						},
					},
					ReturningFields: []minisql.Field{{Name: "id"}, {Name: "name"}},
				},
			},
			nil,
		},
		{
			"INSERT RETURNING with semicolon terminator",
			"INSERT INTO 'users' (id) VALUES (1) RETURNING id;",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "users",
					Fields:    []minisql.Field{{Name: "id"}},
					Inserts: [][]minisql.OptionalValue{
						{
							{Value: int64(1), Valid: true},
						},
					},
					ReturningFields: []minisql.Field{{Name: "id"}},
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
