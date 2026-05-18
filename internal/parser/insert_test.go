package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_Insert(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"Empty INSERT fails",
			"INSERT INTO",
			nil,
			errEmptyTableName,
		},
		{
			"INSERT with no rows to insert fails",
			"INSERT INTO 'a'",
			nil,
			errNoRowsToInsert,
		},
		{
			"INSERT with incomplete value section fails",
			"INSERT INTO 'a' (",
			nil,
			errNoRowsToInsert,
		},
		{
			"INSERT with incomplete value section fails #2",
			"INSERT INTO 'a' (b",
			nil,
			errNoRowsToInsert,
		},
		{
			"INSERT with incomplete value section fails #3",
			"INSERT INTO 'a' (b)",
			nil,
			errNoRowsToInsert,
		},
		{
			"INSERT with incomplete value section fails #4",
			"INSERT INTO 'a' (b) VALUES",
			nil,
			errNoRowsToInsert,
		},
		{
			"INSERT with incomplete row fails",
			"INSERT INTO 'a' (b) VALUES (",
			nil,
			errInsertFieldValueCountMismatch,
		},
		{
			"INSERT works",
			"INSERT INTO 'a' (b) VALUES ('1') ;",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}},
					Inserts:   [][]minisql.OptionalValue{{minisql.MakeVarchar(minisql.NewTextPointer([]byte("1")))}},
				},
			},
			nil,
		},
		{
			"INSERT * fails",
			"INSERT INTO 'a' (*) VALUES ('1')",
			nil,
			errInsertNoFields,
		},
		{
			"INSERT with multiple fields works",
			"INSERT INTO 'a' (b,c,    d) VALUES ('1',2 ,  3.75 );",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "d"}},
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("1"))),
							minisql.MakeInt8(int64(2)),
							minisql.MakeDouble(float64(3.75)),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT with multiple fields and multiple values works",
			"INSERT INTO 'a' (b,c,    d) VALUES ('1',2 ,  3.75 ),('4','5' ,'6' );",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "d"}},
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("1"))),
							minisql.MakeInt8(int64(2)),
							minisql.MakeDouble(float64(3.75)),
						},
						{
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("4"))),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("5"))),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("6"))),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT with multiple fields of different types works",
			"INSERT INTO 'a' (b, c, d, e, f) VALUES (25, 'foo', true, 42.69, NULL);",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "d"}, {Name: "e"}, {Name: "f"}},
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(25)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
							minisql.MakeBool(true),
							minisql.MakeDouble(float64(42.69)),
							minisql.MakeNull(),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT with NOW() function works",
			"INSERT INTO 'a' (b, c, d) VALUES (25, NOW(), 'foo');",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "d"}},
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(25)),
							minisql.MakeFunction(minisql.FunctionNow),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT with placeholders works",
			"INSERT INTO 'a' (b, c, d) VALUES (?, ?, ?);",
			[]minisql.Statement{
				{
					Kind:      minisql.Insert,
					TableName: "a",
					Fields:    []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "d"}},
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakePlaceholder(),
							minisql.MakePlaceholder(),
							minisql.MakePlaceholder(),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO NOTHING with semicolon works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO NOTHING;",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoNothing,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO NOTHING without semicolon works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO NOTHING",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoNothing,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET single column with semicolon works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET c = 'bar';",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": minisql.MakeVarchar(minisql.NewTextPointer([]byte("bar"))),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET single column without semicolon works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET c = 'bar'",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": minisql.MakeVarchar(minisql.NewTextPointer([]byte("bar"))),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET multiple columns works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET b = 2, c = NULL;",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "b"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"b": minisql.MakeInt8(int64(2)),
						"c": minisql.MakeNull(),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET with placeholder works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET c = ?;",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": minisql.MakePlaceholder(),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET with NOW() works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET c = NOW();",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": minisql.MakeFunction(minisql.FunctionNow),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET EXCLUDED.col works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET c = EXCLUDED.c;",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": minisql.MakeExcludedRef(minisql.ExcludedRef{Column: "c"}),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET multiple EXCLUDED.col works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET b = EXCLUDED.b, c = EXCLUDED.c;",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "b"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"b": minisql.MakeExcludedRef(minisql.ExcludedRef{Column: "b"}),
						"c": minisql.MakeExcludedRef(minisql.ExcludedRef{Column: "c"}),
					},
				},
			},
			nil,
		},
		{
			"INSERT ON CONFLICT DO UPDATE SET mixed EXCLUDED.col and literal works",
			"INSERT INTO 'a' (b, c) VALUES (1, 'foo') ON CONFLICT DO UPDATE SET b = EXCLUDED.b, c = 'override';",
			[]minisql.Statement{
				{
					Kind:           minisql.Insert,
					TableName:      "a",
					Fields:         []minisql.Field{{Name: "b"}, {Name: "c"}, {Name: "b"}, {Name: "c"}},
					ConflictAction: minisql.ConflictActionDoUpdate,
					Inserts: [][]minisql.OptionalValue{
						{
							minisql.MakeInt8(int64(1)),
							minisql.MakeVarchar(minisql.NewTextPointer([]byte("foo"))),
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"b": minisql.MakeExcludedRef(minisql.ExcludedRef{Column: "b"}),
						"c": minisql.MakeVarchar(minisql.NewTextPointer([]byte("override"))),
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
