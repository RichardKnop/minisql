package parser

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseError_Error tests the formatted error string for ParseError.
func TestParseError_Error(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		Err      *ParseError
		Expected string
	}{
		{
			Name: "with Near text includes quoted snippet and position",
			Err: &ParseError{
				Pos:  7,
				Near: "foo bar",
				Msg:  "at SELECT: expected FROM",
			},
			Expected: `at SELECT: expected FROM (near "foo bar", position 7)`,
		},
		{
			Name: "without Near text omits snippet",
			Err: &ParseError{
				Pos: 0,
				Msg: "statement kind cannot be empty",
			},
			Expected: "statement kind cannot be empty (position 0)",
		},
		{
			Name: "position zero with Near",
			Err: &ParseError{
				Pos:  0,
				Near: "SELECT",
				Msg:  "at STATEMENT: expected semicolon",
			},
			Expected: `at STATEMENT: expected semicolon (near "SELECT", position 0)`,
		},
		{
			Name: "Near containing special characters is quoted correctly",
			Err: &ParseError{
				Pos:  3,
				Near: `"quoted" text`,
				Msg:  "at CREATE TABLE: expected table name",
			},
			Expected: "at CREATE TABLE: expected table name (near \"\\\"quoted\\\" text\", position 3)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Expected, tc.Err.Error())
		})
	}
}

// TestParseError_Unwrap tests that errors.Is works through the wrapped sentinel.
func TestParseError_Unwrap(t *testing.T) {
	t.Parallel()

	sentinel := fmt.Errorf("some sentinel error")
	pe := &ParseError{
		Pos:  5,
		Near: "token",
		Msg:  sentinel.Error(),
		err:  sentinel,
	}

	t.Run("errors.Is matches wrapped sentinel", func(t *testing.T) {
		assert.True(t, errors.Is(pe, sentinel))
	})

	t.Run("errors.Is does not match unrelated error", func(t *testing.T) {
		other := fmt.Errorf("other error")
		assert.False(t, errors.Is(pe, other))
	})

	t.Run("Unwrap returns sentinel directly", func(t *testing.T) {
		assert.Equal(t, sentinel, pe.Unwrap())
	})

	t.Run("ParseError with nil err unwraps to nil", func(t *testing.T) {
		inline := &ParseError{Pos: 0, Near: "foo", Msg: "some message"}
		assert.Nil(t, inline.Unwrap())
	})
}

// TestParserItem_near tests the near() helper on parserItem.
func TestParserItem_near(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		SQL      string
		Pos      int
		Expected string
	}{
		{
			Name:     "returns up to 20 chars from position",
			SQL:      "SELECT foo FROM bar WHERE id = 1",
			Pos:      0,
			Expected: "SELECT foo FROM bar ",
		},
		{
			Name:     "returns fewer than 20 chars when near end of string",
			SQL:      "SELECT foo",
			Pos:      7,
			Expected: "foo",
		},
		{
			Name:     "returns exactly 20 chars when available",
			SQL:      "abcdefghijklmnopqrstuvwxyz",
			Pos:      0,
			Expected: "abcdefghijklmnopqrst",
		},
		{
			Name:     "returns empty string when position is at end",
			SQL:      "SELECT",
			Pos:      6,
			Expected: "",
		},
		{
			Name:     "returns empty string when position is past end",
			SQL:      "SELECT",
			Pos:      100,
			Expected: "",
		},
		{
			Name:     "returns snippet from mid-string position",
			SQL:      "SELECT foo FROM bar",
			Pos:      11,
			Expected: "FROM bar",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			p := &parserItem{sql: tc.SQL, i: tc.Pos}
			assert.Equal(t, tc.Expected, p.near())
		})
	}
}

// TestParserItem_errorf tests that errorf produces a ParseError with the
// correct position, Near snippet, and formatted message.
func TestParserItem_errorf(t *testing.T) {
	t.Parallel()

	p := &parserItem{
		sql: "SELECT foo FROM bar",
		i:   7,
	}

	err := p.errorf("at SELECT: unexpected token %q", "foo")

	require.Error(t, err)

	var pe *ParseError
	require.True(t, errors.As(err, &pe))

	assert.Equal(t, 7, pe.Pos)
	assert.Equal(t, "foo FROM bar", pe.Near)
	assert.Equal(t, `at SELECT: unexpected token "foo"`, pe.Msg)
	assert.Nil(t, pe.Unwrap(), "errorf should not wrap a sentinel")
	assert.Equal(t, `at SELECT: unexpected token "foo" (near "foo FROM bar", position 7)`, err.Error())
}

// TestParserItem_wrapErr tests that wrapErr produces a ParseError that wraps
// the sentinel and preserves errors.Is compatibility.
func TestParserItem_wrapErr(t *testing.T) {
	t.Parallel()

	sentinel := fmt.Errorf("at WHERE: expected field")
	p := &parserItem{
		sql: "WHERE 123 = foo",
		i:   6,
	}

	err := p.wrapErr(sentinel)

	require.Error(t, err)

	var pe *ParseError
	require.True(t, errors.As(err, &pe))

	assert.Equal(t, 6, pe.Pos)
	assert.Equal(t, "123 = foo", pe.Near)
	assert.Equal(t, "at WHERE: expected field", pe.Msg)
	assert.True(t, errors.Is(err, sentinel), "wrapErr must preserve errors.Is for the sentinel")
	assert.Equal(t, `at WHERE: expected field (near "123 = foo", position 6)`, err.Error())
}

// TestParserItem_wrapErr_atEndOfInput tests wrapErr when the parser is at
// the end of the SQL string — Near should be empty.
func TestParserItem_wrapErr_atEndOfInput(t *testing.T) {
	t.Parallel()

	sentinel := errEmptyStatementKind
	p := &parserItem{sql: "", i: 0}

	err := p.wrapErr(sentinel)

	var pe *ParseError
	require.True(t, errors.As(err, &pe))

	assert.Equal(t, 0, pe.Pos)
	assert.Empty(t, pe.Near)
	assert.Equal(t, "statement kind cannot be empty (position 0)", err.Error())
	assert.True(t, errors.Is(err, sentinel))
}
