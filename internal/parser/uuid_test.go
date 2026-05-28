package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_UUIDColumn(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"CREATE TABLE with uuid column",
			"CREATE TABLE users (id uuid, name varchar(100));",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "users",
					Columns: []minisql.Column{
						{Name: "id", Kind: minisql.UUID, Size: 16, Nullable: true},
						{Name: "name", Kind: minisql.Varchar, Size: 100, Nullable: true},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE uuid not null",
			"CREATE TABLE sessions (id uuid not null);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "sessions",
					Columns: []minisql.Column{
						{Name: "id", Kind: minisql.UUID, Size: 16, Nullable: false},
					},
				},
			},
			nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			t.Parallel()
			got, err := New().Parse(context.Background(), aTestCase.SQL)
			if aTestCase.Err != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, got)
		})
	}
}

func TestParse_CastAsUUID(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(), `select cast(raw_id as uuid) from t;`)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Len(t, stmts[0].Fields, 1)
	require.NotNil(t, stmts[0].Fields[0].Expr)
	assert.Equal(t, minisql.UUID, stmts[0].Fields[0].Expr.CastTargetType)
}
