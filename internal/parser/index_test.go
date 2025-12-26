package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_CreateIndex(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"Empty CREATE INDEX fails",
			"CREATE INDEX",
			nil,
			errEmptyIndexName,
		},
		{
			"CREATE INDEX with no opening parens fails",
			"CREATE INDEX foo",
			nil,
			errEmptyTableName,
		},
		{
			"CREATE INDEX with no column fails",
			"CREATE INDEX foo ON bar",
			nil,
			errCreateIndexNoColumns,
		},
		{
			"CREATE UNIQUE INDEX not supported",
			"CREATE UNIQUE INDEX foo ON bar (qux);",
			nil,
			errInvalidStatementKind,
		},
		{
			"CREATE INDEX with single column works",
			"CREATE INDEX foo ON bar (qux);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateIndex,
					IndexName: "foo",
					TableName: "bar",
					Columns: []minisql.Column{
						{
							Name: "qux",
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE INDEX with IF NOT EXISTS works",
			"CREATE INDEX IF NOT EXISTS foo ON bar (qux);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateIndex,
					IndexName: "foo",
					TableName: "bar",
					Columns: []minisql.Column{
						{
							Name: "qux",
						},
					},
					IfNotExists: true,
				},
			},
			nil,
		},
		{
			"CREATE INDEX with multiple columns works",
			"CREATE INDEX foo ON bar (qux, baz);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateIndex,
					IndexName: "foo",
					TableName: "bar",
					Columns: []minisql.Column{
						{
							Name: "qux",
						},
						{
							Name: "baz",
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

func TestParse_DropIndex(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty DROP INDEX fails",
			SQL:      "DROP INDEX",
			Expected: nil,
			Err:      errEmptyIndexName,
		},
		{
			Name: "DROP INDEX works",
			SQL:  "DROP INDEX foo;",
			Expected: []minisql.Statement{
				{
					Kind:      minisql.DropIndex,
					IndexName: "foo",
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
