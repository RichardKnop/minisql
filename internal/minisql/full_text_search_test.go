package minisql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextSearchTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "lowercases and splits on punctuation",
			input: "MiniSQL, Database-pages!",
			want:  []string{"minisql", "database", "pages"},
		},
		{
			name:  "keeps digits inside tokens",
			input: "SQLite FTS5 and JSON2",
			want:  []string{"sqlite", "fts5", "json2"},
		},
		{
			name:  "drops stop words",
			input: "the database and the index",
			want:  []string{"database", "index"},
		},
		{
			name:  "unicode letters are preserved",
			input: "Café DATABASE",
			want:  []string{"café", "database"},
		},
		{
			name:  "empty and stop-word-only input returns no tokens",
			input: "the and of",
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, textSearchTokens(tt.input))
		})
	}
}

func TestUniqueTextSearchTokens(t *testing.T) {
	t.Parallel()

	got := uniqueTextSearchTokens("MiniSQL minisql database MiniSQL")
	assert.Equal(t, []string{"minisql", "database"}, got)

	overlong := strings.Repeat("x", MaxIndexKeySize+1)
	got = uniqueTextSearchTokens("database " + overlong)
	assert.Equal(t, []string{"database"}, got)
}

func TestTextSearchTokenPositions(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []textSearchTokenPosition{
		{Term: "mini", Position: 0},
		{Term: "embedded", Position: 1},
		{Term: "database", Position: 2},
	}, textSearchTokenPositions("The mini embedded database"))
}

func TestParseTextSearchQuery(t *testing.T) {
	t.Parallel()

	query, ok := parseTextSearchQuery(`mini "database pages"`)
	assert.True(t, ok)
	assert.Equal(t, []string{"mini"}, query.Terms)
	assert.Equal(t, [][]string{{"database", "pages"}}, query.Phrases)
	assert.Equal(t, []string{"mini", "database", "pages"}, query.allUniqueTokens())
	assert.False(t, query.hasOverlongToken())

	query, ok = parseTextSearchQuery("database " + strings.Repeat("x", MaxIndexKeySize+1))
	assert.True(t, ok)
	assert.True(t, query.hasOverlongToken())

	_, ok = parseTextSearchQuery(`"unterminated phrase`)
	assert.False(t, ok)
}

func TestTextSearchMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		document string
		query    string
		want     bool
	}{
		{
			name:     "matches when all unique query terms are present",
			document: "MiniSQL is an embedded database with database pages",
			query:    "minisql database",
			want:     true,
		},
		{
			name:     "query stop words are ignored",
			document: "MiniSQL stores pages",
			query:    "the minisql",
			want:     true,
		},
		{
			name:     "missing term fails implicit AND",
			document: "MiniSQL stores pages",
			query:    "minisql database",
			want:     false,
		},
		{
			name:     "stop-word-only query does not match",
			document: "MiniSQL stores pages",
			query:    "the and of",
			want:     false,
		},
		{
			name:     "empty document does not match",
			document: "",
			query:    "minisql",
			want:     false,
		},
		{
			name:     "quoted phrase requires adjacent tokens",
			document: "MiniSQL stores database pages together",
			query:    `"database pages"`,
			want:     true,
		},
		{
			name:     "quoted phrase rejects scattered tokens",
			document: "MiniSQL stores database values across many pages",
			query:    `"database pages"`,
			want:     false,
		},
		{
			name:     "plain terms and phrase combine with AND",
			document: "MiniSQL stores database pages",
			query:    `minisql "database pages"`,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, textSearchMatch(tt.document, tt.query))
		})
	}
}

func TestTextSearchRank(t *testing.T) {
	t.Parallel()

	t.Run("scores term frequency with saturation", func(t *testing.T) {
		t.Parallel()

		oneHit := textSearchRank("MiniSQL database", "minisql database")
		twoHits := textSearchRank("MiniSQL database database", "minisql database")
		manyHits := textSearchRank("MiniSQL database "+strings.Repeat("database ", 20), "minisql database")

		assert.Greater(t, twoHits, oneHit)
		assert.Less(t, manyHits-twoHits, twoHits-oneHit)
	})

	t.Run("missing query term lowers rank through coverage", func(t *testing.T) {
		t.Parallel()

		fullMatch := textSearchRank("MiniSQL database", "minisql database")
		partialMatch := textSearchRank("MiniSQL database", "minisql postgres")

		assert.Greater(t, fullMatch, partialMatch)
		assert.Greater(t, partialMatch, float64(0))
	})

	t.Run("duplicate query tokens are counted once", func(t *testing.T) {
		t.Parallel()

		assert.InDelta(t,
			textSearchRank("MiniSQL MiniSQL database", "minisql database"),
			textSearchRank("MiniSQL MiniSQL database", "minisql minisql database"),
			0.0000000001,
		)
	})

	t.Run("phrase match outranks separated terms", func(t *testing.T) {
		t.Parallel()

		phrase := textSearchRank("MiniSQL stores database pages together", `"database pages"`)
		separated := textSearchRank("MiniSQL stores database values across many pages", `"database pages"`)

		assert.Greater(t, phrase, separated)
	})

	t.Run("dense cluster outranks scattered terms", func(t *testing.T) {
		t.Parallel()

		dense := textSearchRank("mini database pages are nearby", "mini database pages")
		scattered := textSearchRank("mini "+strings.Repeat("noise ", 20)+"database "+strings.Repeat("noise ", 20)+"pages", "mini database pages")

		assert.Greater(t, dense, scattered)
	})

	t.Run("short focused document outranks long noisy document", func(t *testing.T) {
		t.Parallel()

		short := textSearchRank("mini database", "mini database")
		long := textSearchRank("mini database "+strings.Repeat("noise ", 80), "mini database")

		assert.Greater(t, short, long)
	})

	t.Run("empty inputs and stop-word-only queries have zero rank", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, float64(0), textSearchRank("MiniSQL database", "the and of"))
		assert.Equal(t, float64(0), textSearchRank("", "minisql"))
		assert.Equal(t, float64(0), textSearchRank("MiniSQL database", `"unterminated phrase`))
	})
}
