package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_AlterTable(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "ALTER TABLE missing table name fails",
			SQL:      "ALTER TABLE",
			Expected: nil,
			Err:      errEmptyTableName,
		},
		{
			Name:     "ALTER TABLE with unknown action fails",
			SQL:      "ALTER TABLE foo FROBNICATE;",
			Expected: nil,
			Err:      errAlterTableExpectedAction,
		},
		{
			Name: "ADD COLUMN int8",
			SQL:  "ALTER TABLE users ADD COLUMN age int8;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableAddColumn,
					Columns: []minisql.Column{
						{Name: "age", Kind: minisql.Int8, Size: 8, Nullable: true},
					},
				},
			},
		},
		{
			Name: "ADD COLUMN varchar with size",
			SQL:  "ALTER TABLE users ADD COLUMN nickname varchar(64);",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableAddColumn,
					Columns: []minisql.Column{
						{Name: "nickname", Kind: minisql.Varchar, Size: 64, Nullable: true},
					},
				},
			},
		},
		{
			Name: "ADD COLUMN with NOT NULL",
			SQL:  "ALTER TABLE users ADD COLUMN active boolean NOT NULL;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableAddColumn,
					Columns: []minisql.Column{
						{Name: "active", Kind: minisql.Boolean, Size: 1, Nullable: false},
					},
				},
			},
		},
		{
			Name: "ADD COLUMN with DEFAULT integer",
			SQL:  "ALTER TABLE users ADD COLUMN score int4 DEFAULT 0;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableAddColumn,
					Columns: []minisql.Column{
						{
							Name:     "score",
							Kind:     minisql.Int4,
							Size:     4,
							Nullable: true,
							DefaultValue: minisql.OptionalValue{
								Value: int64(0),
								Valid: true,
							},
						},
					},
				},
			},
		},
		{
			Name: "ADD COLUMN with NOT NULL and DEFAULT",
			SQL:  "ALTER TABLE users ADD COLUMN score int4 NOT NULL DEFAULT 0;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableAddColumn,
					Columns: []minisql.Column{
						{
							Name:     "score",
							Kind:     minisql.Int4,
							Size:     4,
							Nullable: false,
							DefaultValue: minisql.OptionalValue{
								Value: int64(0),
								Valid: true,
							},
						},
					},
				},
			},
		},
		{
			Name: "DROP COLUMN",
			SQL:  "ALTER TABLE users DROP COLUMN email;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableDropColumn,
					AlterColumnName:  "email",
				},
			},
		},
		{
			Name: "RENAME COLUMN",
			SQL:  "ALTER TABLE users RENAME COLUMN email TO email_address;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableRenameColumn,
					AlterColumnName:  "email",
					NewColumnName:    "email_address",
				},
			},
		},
		{
			Name: "RENAME TO",
			SQL:  "ALTER TABLE users RENAME TO members;",
			Expected: []minisql.Statement{
				{
					Kind:             minisql.AlterTable,
					TableName:        "users",
					AlterTableAction: minisql.AlterTableRenameTo,
					NewTableName:     "members",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			stmts, err := New().Parse(context.Background(), tc.SQL)
			if tc.Err != nil {
				require.ErrorIs(t, err, tc.Err)
				return
			}
			if tc.Expected == nil {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Expected, stmts)
		})
	}
}
