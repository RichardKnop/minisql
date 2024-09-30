package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

type testCase struct {
	Name     string
	SQL      string
	Expected minisql.Statement
	Err      error
}

func TestParse_Empty(t *testing.T) {
	t.Parallel()

	aStatement, err := New().Parse(context.Background(), "")
	require.Error(t, err)
	assert.Equal(t, minisql.Statement{}, aStatement)
	assert.Equal(t, errEmptyStatementKind, err)
}

func TestPeekQuotedStringWithLength(t *testing.T) {
	t.Parallel()

	aParser := New()
	aParser.setSQL(" 'Hello, 世界' ")

	quotedValue, ln := aParser.peekQuotedStringWithLength()
	assert.Equal(t, "Hello, 世界", quotedValue)
	assert.Equal(t, 15, ln)
}

func TestPeepIntWithLength(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name           string
		SQL            string
		ExpectedValue  int64
		ExpectedLength int
	}{
		{
			"Invalid integer",
			"foo",
			0,
			0,
		},
		{
			"Valid integer",
			"150",
			150,
			3,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := New()
			aParser.setSQL(aTestCase.SQL)
			intValue, ln := aParser.peepIntWithLength()
			assert.Equal(t, aTestCase.ExpectedValue, intValue)
			assert.Equal(t, aTestCase.ExpectedLength, ln)
		})
	}
}
