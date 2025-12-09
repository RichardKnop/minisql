package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_CreateTable(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"Empty CREATE TABLE fails",
			"CREATE TABLE",
			nil,
			errEmptyTableName,
		},
		{
			"CREATE TABLE with no opening parens fails",
			"CREATE TABLE foo",
			nil,
			errCreateTableNoColumns,
		},
		{
			"CREATE TABLE with no schema fails",
			"CREATE TABLE foo ()",
			nil,
			errCreateTableNoColumns,
		},
		{
			"CREATE TABLE with invalid column schema fails",
			"CREATE TABLE foo (bar INVALID",
			nil,
			errCreateTableInvalidColumDef,
		},
		{
			"CREATE TABLE with single boolean column works",
			"CREATE TABLE foo (bar boolean);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Boolean,
							Size:     1,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with IF NOT EXISTS works",
			"CREATE TABLE IF NOT EXISTS foo (bar boolean);",
			[]minisql.Statement{
				{
					Kind:        minisql.CreateTable,
					TableName:   "foo",
					IfNotExists: true,
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Boolean,
							Size:     1,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single int4 column works",
			"CREATE TABLE foo (bar int4);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Int4,
							Size:     4,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single int8 column works",
			"CREATE TABLE foo (bar int8);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Int8,
							Size:     8,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single real column works",
			"CREATE TABLE foo (bar real);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Real,
							Size:     4,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single double column works",
			"CREATE TABLE foo (bar double);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Double,
							Size:     8,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single text column works",
			"CREATE TABLE foo (bar text);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Text,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single varchar column works",
			"CREATE TABLE foo (bar varchar(255));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Varchar,
							Size:     minisql.MaxInlineVarchar,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with single not null column works",
			"CREATE TABLE foo (bar int4 not null);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Int4,
							Size:     4,
							Nullable: false,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with quoted table name identifier works",
			`CREATE TABLE "foo" (bar int4);`,
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Int4,
							Size:     4,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with multiple columns works",
			`CREATE TABLE foo (
				bar boolean not null, 
				baz int4, 
				qux int8 not null, 
				lorem real null, 
				ipsum double, 
				sit varchar(255)
			);`,
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:     "bar",
							Kind:     minisql.Boolean,
							Size:     1,
							Nullable: false,
						},
						{
							Name:     "baz",
							Kind:     minisql.Int4,
							Size:     4,
							Nullable: true,
						},
						{
							Name:     "qux",
							Kind:     minisql.Int8,
							Size:     8,
							Nullable: false,
						},
						{
							Name:     "lorem",
							Kind:     minisql.Real,
							Size:     4,
							Nullable: true,
						},
						{
							Name:     "ipsum",
							Kind:     minisql.Double,
							Size:     8,
							Nullable: true,
						},
						{
							Name:     "sit",
							Kind:     minisql.Varchar,
							Size:     minisql.MaxInlineVarchar,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with multiple primary key columns fails",
			`CREATE TABLE foo (
				id int8 primary key, 
				bar varchar(255) primary key
			);`,
			nil,
			errCreateTableMultiplePrimaryKeys,
		},
		{
			"CREATE TABLE with VARCHER primary key with size > 255 fails",
			`CREATE TABLE foo (
				id varchar(300) primary key, 
				bar varchar(255) primary key
			);`,
			nil,
			errCreateTablePrimaryKeyVarcharTooLarge,
		},
		{
			"CREATE TABLE with TEXT primary key fails",
			`CREATE TABLE foo (
				id text primary key, 
				bar varchar(255)
			);`,
			nil,
			errCreateTablePrimaryKeyTextNotAllowed,
		},
		{
			"CREATE TABLE with primary key",
			`CREATE TABLE foo (
				id int8 primary key, 
				bar varchar(255)
			);`,
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:       "id",
							Kind:       minisql.Int8,
							Size:       8,
							PrimaryKey: true,
							Nullable:   false,
						},
						{
							Name:     "bar",
							Kind:     minisql.Varchar,
							Size:     minisql.MaxInlineVarchar,
							Nullable: true,
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE with autoincrementing primary key",
			`CREATE TABLE foo (
				id int8 primary key autoincrement, 
				bar varchar(255)
			);`,
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "foo",
					Columns: []minisql.Column{
						{
							Name:          "id",
							Kind:          minisql.Int8,
							Size:          8,
							PrimaryKey:    true,
							Autoincrement: true,
							Nullable:      false,
						},
						{
							Name:     "bar",
							Kind:     minisql.Varchar,
							Size:     minisql.MaxInlineVarchar,
							Nullable: true,
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

func TestParse_DropTable(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty DROP TABLE fails",
			SQL:      "DROP TABLE",
			Expected: nil,
			Err:      errEmptyTableName,
		},
		{
			Name: "DROP TABLE works",
			SQL:  "DROP TABLE foo;",
			Expected: []minisql.Statement{
				{
					Kind:      minisql.DropTable,
					TableName: "foo",
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
