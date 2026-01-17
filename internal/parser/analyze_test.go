package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_Analyze(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"ANALYZE without specific target",
			"ANALYZE;",
			[]minisql.Statement{
				{
					Kind: minisql.Analyze,
				},
			},
			nil,
		},
		{
			"ANALYZE with specific target",
			"ANALYZE foo_bar;",
			[]minisql.Statement{
				{
					Kind:   minisql.Analyze,
					Target: "foo_bar",
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
