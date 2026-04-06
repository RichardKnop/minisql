package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_Pragma(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name: "PRAGMA quick_check",
			SQL:  "PRAGMA quick_check;",
			Expected: []minisql.Statement{{
				Kind:       minisql.Pragma,
				PragmaName: "quick_check",
			}},
		},
		{
			Name: "PRAGMA integrity_check without semicolon",
			SQL:  "pragma integrity_check",
			Expected: []minisql.Statement{{
				Kind:       minisql.Pragma,
				PragmaName: "integrity_check",
			}},
		},
		{
			Name: "PRAGMA requires a name",
			SQL:  "PRAGMA;",
			Err:  errEmptyPragmaName,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			statements, err := New().Parse(context.Background(), testCase.SQL)
			if testCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, testCase.Err)
				assert.Empty(t, statements)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.Expected, statements)
		})
	}
}
