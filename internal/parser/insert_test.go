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
					Inserts:   [][]minisql.OptionalValue{{{Value: minisql.NewTextPointer([]byte("1")), Valid: true}}},
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
							{Value: minisql.NewTextPointer([]byte("1")), Valid: true},
							{Value: int64(2), Valid: true},
							{Value: float64(3.75), Valid: true},
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
							{Value: minisql.NewTextPointer([]byte("1")), Valid: true},
							{Value: int64(2), Valid: true},
							{Value: float64(3.75), Valid: true},
						},
						{
							{Value: minisql.NewTextPointer([]byte("4")), Valid: true},
							{Value: minisql.NewTextPointer([]byte("5")), Valid: true},
							{Value: minisql.NewTextPointer([]byte("6")), Valid: true},
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
							{Value: int64(25), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
							{Value: true, Valid: true},
							{Value: float64(42.69), Valid: true},
							{Valid: false},
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
							{Value: int64(25), Valid: true},
							{Value: minisql.FunctionNow, Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
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
							{Value: minisql.Placeholder{}, Valid: true},
							{Value: minisql.Placeholder{}, Valid: true},
							{Value: minisql.Placeholder{}, Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": {Value: minisql.NewTextPointer([]byte("bar")), Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": {Value: minisql.NewTextPointer([]byte("bar")), Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: int64(2), Valid: true},
						"c": {Valid: false},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": {Value: minisql.Placeholder{}, Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": {Value: minisql.FunctionNow, Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"c": {Value: minisql.ExcludedRef{Column: "c"}, Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.ExcludedRef{Column: "b"}, Valid: true},
						"c": {Value: minisql.ExcludedRef{Column: "c"}, Valid: true},
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
							{Value: int64(1), Valid: true},
							{Value: minisql.NewTextPointer([]byte("foo")), Valid: true},
						},
					},
					Updates: map[string]minisql.OptionalValue{
						"b": {Value: minisql.ExcludedRef{Column: "b"}, Valid: true},
						"c": {Value: minisql.NewTextPointer([]byte("override")), Valid: true},
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
