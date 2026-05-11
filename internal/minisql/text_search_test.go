package minisql

import (
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

	tests := []struct {
		name     string
		document string
		query    string
		want     float64
	}{
		{
			name:     "log-scaled average term frequency",
			document: "MiniSQL database database",
			query:    "minisql database",
			want:     0.8958797346140275,
		},
		{
			name:     "missing query term contributes zero",
			document: "MiniSQL database",
			query:    "minisql postgres",
			want:     0.34657359027997264,
		},
		{
			name:     "duplicate query tokens are counted once",
			document: "MiniSQL MiniSQL database",
			query:    "minisql minisql database",
			want:     0.8958797346140275,
		},
		{
			name:     "stop-word-only query has zero rank",
			document: "MiniSQL database",
			query:    "the and of",
			want:     0,
		},
		{
			name:     "empty document has zero rank",
			document: "",
			query:    "minisql",
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.InDelta(t, tt.want, textSearchRank(tt.document, tt.query), 0.0000000001)
		})
	}
}
