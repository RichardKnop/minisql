package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/core/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_Transaction(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"BEGIN",
			`BEGIN;`,
			[]minisql.Statement{
				{
					Kind: minisql.BeginTransaction,
				},
			},
			nil,
		},
		{
			"COMMIT",
			`COMMIT;`,
			[]minisql.Statement{
				{
					Kind: minisql.CommitTransaction,
				},
			},
			nil,
		},
		{
			"ROLLBACK",
			`ROLLBACK;`,
			[]minisql.Statement{
				{
					Kind: minisql.RollbackTransaction,
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
