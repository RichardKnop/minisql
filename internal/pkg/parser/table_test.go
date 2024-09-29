package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

func TestParse_CreateTable(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty CREATE TABLE fails",
			SQL:      "CREATE TABLE",
			Expected: minisql.Statement{Kind: minisql.CreateTable},
			Err:      errEmptyTableName,
		},
		{
			Name: "CREATE TABLE with no opening parens fails",
			SQL:  "CREATE TABLE foo",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
			},
			Err: errCreateTableNoColumns,
		},
		{
			Name: "CREATE TABLE with no schema fails",
			SQL:  "CREATE TABLE foo ()",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
			},
			Err: errCreateTableNoColumns,
		},
		{
			Name: "CREATE TABLE with invalid column schema fails",
			SQL:  "CREATE TABLE foo (bar INVALID",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
				Columns: []minisql.Column{
					{
						Name: "bar",
					},
				},
			},
			Err: errCreateTableInvalidColumDef,
		},
		{
			Name: "CREATE TABLE with single int4 column works",
			SQL:  "CREATE TABLE foo (bar int4)",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
				Columns: []minisql.Column{
					{
						Name: "bar",
						Kind: minisql.Int4,
						Size: 4,
					},
				},
			},
		},
		{
			Name: "CREATE TABLE with single int8 column works",
			SQL:  "CREATE TABLE foo (bar int8)",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
				Columns: []minisql.Column{
					{
						Name: "bar",
						Kind: minisql.Int8,
						Size: 8,
					},
				},
			},
		},
		{
			Name: "CREATE TABLE with single varchar column works",
			SQL:  "CREATE TABLE foo (bar varchar(255))",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
				Columns: []minisql.Column{
					{
						Name: "bar",
						Kind: minisql.Varchar,
						Size: 255,
					},
				},
			},
		},
		{
			Name: "CREATE TABLE with multiple columns works",
			SQL:  "CREATE TABLE foo (bar int4, lorem int8, ipsum varchar(255))",
			Expected: minisql.Statement{
				Kind:      minisql.CreateTable,
				TableName: "foo",
				Columns: []minisql.Column{
					{
						Name: "bar",
						Kind: minisql.Int4,
						Size: 4,
					},
					{
						Name: "lorem",
						Kind: minisql.Int8,
						Size: 8,
					},
					{
						Name: "ipsum",
						Kind: minisql.Varchar,
						Size: 255,
					},
				},
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

func TestParse_DropTable(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty DROP TABLE fails",
			SQL:      "DROP TABLE",
			Expected: minisql.Statement{Kind: minisql.DropTable},
			Err:      errEmptyTableName,
		},
		{
			Name: "DROP TABLE works",
			SQL:  "DROP TABLE foo",
			Expected: minisql.Statement{
				Kind:      minisql.DropTable,
				TableName: "foo",
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
