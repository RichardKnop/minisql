package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLikeMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		str     string
		want    bool
	}{
		// Exact match (no wildcards)
		{"exact match", "hello", "hello", true},
		{"exact mismatch", "hello", "world", false},
		{"empty pattern empty string", "", "", true},
		{"empty pattern non-empty string", "", "hello", false},
		{"non-empty pattern empty string", "hello", "", false},

		// Percent wildcard
		{"percent matches empty suffix", "hello%", "hello", true},
		{"percent matches any suffix", "hello%", "hello world", true},
		{"percent prefix match", "%world", "hello world", true},
		{"percent prefix no match", "%world", "hello earth", false},
		{"percent substring", "%ello%", "hello world", true},
		{"percent substring no match", "%xyz%", "hello world", false},
		{"percent only matches everything", "%", "anything at all", true},
		{"percent only matches empty", "%", "", true},
		{"double percent", "%%", "hello", true},
		{"percent between literals", "a%b", "ab", true},
		{"percent between literals with content", "a%b", "a123b", true},
		{"percent between literals no match", "a%b", "a123c", false},

		// Underscore wildcard
		{"underscore matches one char", "h_llo", "hello", true},
		{"underscore no match empty", "h_llo", "hllo", false},
		{"underscore no match extra", "h_llo", "heello", false},
		{"underscore at start", "_ello", "hello", true},
		{"underscore at end", "hell_", "hello", true},
		{"underscore at end too short", "hell_", "hell", false},
		{"multiple underscores", "h__lo", "hello", true},
		{"multiple underscores no match", "h__lo", "helo", false},

		// Mixed wildcards
		{"percent and underscore", "%_end", "end", false},
		{"percent and underscore match", "%_end", "amend", true},
		{"underscore then percent", "h_%", "hi", true},
		{"underscore then percent needs two chars", "h_%", "h", false},

		// Case sensitivity
		{"case sensitive — no match", "Hello", "hello", false},
		{"case sensitive — match", "Hello", "Hello", true},

		// Patterns longer than string
		{"pattern longer than string", "toolong", "too", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := likeMatch(tt.pattern, tt.str)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareText_Like(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		pattern  string
		operator Operator
		want     bool
		wantErr  bool
	}{
		{"LIKE prefix match", "foobar", "foo%", Like, true, false},
		{"LIKE prefix no match", "bazbar", "foo%", Like, false, false},
		{"LIKE suffix match", "foobar", "%bar", Like, true, false},
		{"LIKE substring match", "foobar", "%oob%", Like, true, false},
		{"LIKE exact match", "foobar", "foobar", Like, true, false},
		{"LIKE underscore match", "foobar", "foo__r", Like, true, false},
		{"NOT LIKE prefix no match", "foobar", "baz%", NotLike, true, false},
		{"NOT LIKE prefix match returns false", "foobar", "foo%", NotLike, false, false},
		{"unknown operator errors", "foobar", "foo%", Operator(999), false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareText(
				NewTextPointer([]byte(tt.value)),
				NewTextPointer([]byte(tt.pattern)),
				tt.operator,
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
