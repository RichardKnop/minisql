package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

type testCase struct {
	Name     string
	SQL      string
	Expected []minisql.Statement
	Err      error
}

func TestParse_Empty(t *testing.T) {
	t.Parallel()

	statements, err := New().Parse(context.Background(), "")
	require.Error(t, err)
	assert.Empty(t, statements)
	assert.Equal(t, errEmptyStatementKind, err)
}

func TestPeekQuotedStringWithLength(t *testing.T) {
	t.Parallel()

	p := &parserItem{
		sql: "'Hello, 世界'",
	}

	quotedValue, ln := p.peekQuotedStringWithLength()
	assert.Equal(t, "Hello, 世界", quotedValue)
	assert.Equal(t, 15, ln)
}

func TestPeekIntWithLength(t *testing.T) {
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
			p := &parserItem{
				sql: aTestCase.SQL,
			}
			intValue, ln := p.peekIntWithLength()
			assert.Equal(t, aTestCase.ExpectedValue, intValue)
			assert.Equal(t, aTestCase.ExpectedLength, ln)
		})
	}
}

// TestParse_FuzzerRegressions covers inputs that previously caused infinite
// loops in the parser. Each case must return within a reasonable time and
// must not panic — an error return is acceptable.
func TestParse_FuzzerRegressions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
	}{
		{
			// \x15 (NAK) is a C0 control character that strings.Fields does not
			// treat as whitespace. The tokenizer had no rule for it and returned
			// ("", 0) from peekWithLength, so pop() never advanced p.i.
			name: "control character mid-SQL causes infinite loop",
			sql:  "SELECT * FROM users\x15ORDER BY id ASC;",
		},
		{
			// \xe7 is an invalid UTF-8 lead byte; strings.Map re-encodes it as
			// U+FFFD (\xef\xbf\xbd). The tokenizer also returned ("", 0) for
			// U+FFFD, and stepStatementEnd looped forever when peek() returned ""
			// but p.i was still less than len(sql).
			name: "non-ASCII byte mid-SQL causes infinite loop",
			sql:  "SELECT * FROM users\xe7\xe7\xe7\xe7\xe7E id IN (1, 2, 3);",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The test must complete (no hang); a parse error is fine.
			_, _ = New().Parse(context.Background(), tc.sql)
		})
	}
}

func TestPeekIdentifierWithLength(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name           string
		SQL            string
		ExpectedValue  string
		ExpectedLength int
	}{
		{
			"Invalid identifier",
			"'foo'",
			"",
			0,
		},
		{
			"Valid identifier",
			"foobar",
			"foobar",
			6,
		},
		{
			"Valid identifier with underscore and digits",
			"foo_bar123",
			"foo_bar123",
			10,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			p := &parserItem{
				sql: aTestCase.SQL,
			}
			identifier, ln := p.peekIdentifierWithLength()
			assert.Equal(t, aTestCase.ExpectedValue, identifier)
			assert.Equal(t, aTestCase.ExpectedLength, ln)
		})
	}
}
