package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

func TestParse_Insert(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty INSERT fails",
			SQL:      "INSERT INTO",
			Expected: minisql.Statement{Kind: minisql.Insert},
			Err:      errEmptyTableName,
		},
		{
			Name: "INSERT with no rows to insert fails",
			SQL:  "INSERT INTO 'a'",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails",
			SQL:  "INSERT INTO 'a' (",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails #2",
			SQL:  "INSERT INTO 'a' (b",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b"},
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails #3",
			SQL:  "INSERT INTO 'a' (b)",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b"},
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails #4",
			SQL:  "INSERT INTO 'a' (b) VALUES",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b"},
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete row fails",
			SQL:  "INSERT INTO 'a' (b) VALUES (",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]any{{}},
			},
			Err: errInsertFieldValueCountMismatch,
		},
		{
			Name: "INSERT works",
			SQL:  "INSERT INTO 'a' (b) VALUES ('1')",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]any{{"1"}},
			},
		},
		{
			Name: "INSERT * fails",
			SQL:  "INSERT INTO 'a' (*) VALUES ('1')",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
			},
			Err: errInsertNoFields,
		},
		{
			Name: "INSERT with multiple fields works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' )",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]any{{"1", "2", "3"}},
			},
		},
		{
			Name: "INSERT with multiple fields and multiple values works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' ),('4','5' ,'6' )",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]any{{"1", "2", "3"}, {"4", "5", "6"}},
			},
		},
		{
			Name: "INSERT with multiple fields of different types works",
			SQL:  "INSERT INTO 'a' (b, c, d, e, f) VALUES (25, 'foo', 7, 'bar', 1500000)",
			Expected: minisql.Statement{
				Kind:      minisql.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d", "e", "f"},
				Inserts:   [][]any{{int64(25), "foo", int64(7), "bar", int64(1500000)}},
			},
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
