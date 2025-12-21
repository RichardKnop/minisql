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
