package minisql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONInvertedTermsForDocument(t *testing.T) {
	t.Parallel()

	terms, err := jsonInvertedTermsForDocument(`{
		"type": "click",
		"user": {"id": "u1"},
		"tags": ["web", "mobile"],
		"active": true,
		"count": 1
	}`)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"k:active",
		"k:count",
		"k:tags",
		"k:type",
		"k:user",
		"k:user.id",
		"kv:active:b:true",
		"kv:count:n:1",
		"kv:tags[]:s:\"mobile\"",
		"kv:tags[]:s:\"web\"",
		"kv:type:s:\"click\"",
		"kv:user.id:s:\"u1\"",
	}, terms)
}

func TestJSONInvertedTermsSkipOverlongGeneratedTerms(t *testing.T) {
	t.Parallel()

	terms, err := jsonInvertedTermsForDocument(`{"short":"ok","long":"` + strings.Repeat("x", MaxIndexKeySize+1) + `"}`)
	require.NoError(t, err)

	assert.Contains(t, terms, "k:long")
	assert.Contains(t, terms, `kv:short:s:"ok"`)
	assert.NotContains(t, terms, `kv:long:s:"`+strings.Repeat("x", MaxIndexKeySize+1)+`"`)
}

func TestJSONContains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		doc   string
		query string
		want  bool
	}{
		{
			name:  "object subset",
			doc:   `{"type":"click","user":{"id":"u1","role":"admin"}}`,
			query: `{"user":{"id":"u1"}}`,
			want:  true,
		},
		{
			name:  "array membership",
			doc:   `{"tags":["web","mobile"]}`,
			query: `{"tags":["mobile"]}`,
			want:  true,
		},
		{
			name:  "number equivalence",
			doc:   `{"count":1}`,
			query: `{"count":1.0}`,
			want:  true,
		},
		{
			name:  "missing nested value",
			doc:   `{"user":{"id":"u1"}}`,
			query: `{"user":{"id":"u2"}}`,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := jsonContains(tt.doc, tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJSONInvertedQueryTermsAreExact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "scalar root", query: `"click"`, want: true},
		{name: "non-empty object scalar tree", query: `{"type":"click","user":{"id":"u1"}}`, want: true},
		{name: "unique scalar array query", query: `{"tags":["web","mobile"]}`, want: true},
		{name: "duplicate scalar array query needs recheck", query: `{"tags":["web","web"]}`, want: false},
		{name: "object array query needs recheck", query: `{"tags":[{"name":"web"}]}`, want: false},
		{name: "empty object needs recheck", query: `{"user":{}}`, want: false},
		{name: "empty array needs recheck", query: `{"tags":[]}`, want: false},
		{name: "overlong scalar term needs recheck", query: `{"long":"` + strings.Repeat("x", MaxIndexKeySize+1) + `"}`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			value, err := decodeJSONForInvertedIndex(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, jsonInvertedQueryTermsAreExact(value))
		})
	}
}

func TestJSONInvertedIndexHelpers(t *testing.T) {
	t.Parallel()

	payloadColumn := Column{Name: "payload", Kind: JSON}
	storageColumns := secondaryIndexStorageColumns(SecondaryIndex{
		IndexInfo: IndexInfo{
			Method:  IndexMethodInverted,
			Columns: []Column{payloadColumn},
		},
	})
	require.Len(t, storageColumns, 1)
	assert.Equal(t, "__json_term__", storageColumns[0].Name)
	assert.Equal(t, Varchar, storageColumns[0].Kind)

	row := Row{
		Key:     42,
		Columns: []Column{payloadColumn},
		Values:  []OptionalValue{{Valid: true, Value: NewTextPointer([]byte(`{"type":"click","tags":["web"]}`))}},
	}
	terms, err := jsonInvertedTermsForRow(SecondaryIndex{
		IndexInfo: IndexInfo{Name: "idx_payload", Columns: []Column{payloadColumn}},
	}, row)
	require.NoError(t, err)
	assert.Contains(t, terms, `kv:type:s:"click"`)
	assert.Contains(t, terms, `kv:tags[]:s:"web"`)

	index := &fakeFullTextInvertedIndex{
		mode:     invertedPostingModeRowIDs,
		postings: make(map[string][]invertedPosting),
	}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    "idx_payload_inv",
			Method:  IndexMethodInverted,
			Columns: []Column{payloadColumn},
		},
		InvertedIndex: index,
	}
	table := NewTable(testLogger, nil, nil, "events", []Column{payloadColumn}, 0, nil)
	ctx := context.Background()

	require.NoError(t, table.insertInvertedIndexKeys(ctx, secondaryIndex, row.Key, row))
	assert.Contains(t, index.inserted, `kv:type:s:"click"`)

	oldRow := row
	newRow := Row{
		Key:     42,
		Columns: []Column{payloadColumn},
		Values:  []OptionalValue{{Valid: true, Value: NewTextPointer([]byte(`{"type":"view"}`))}},
	}
	require.NoError(t, table.updateInvertedIndexKeys(ctx, secondaryIndex, oldRow, newRow))
	assert.Contains(t, index.deleted, `kv:type:s:"click"`)
	assert.Contains(t, index.inserted, `kv:type:s:"view"`)
}

func TestJSONInvertedCountExactIndexScan(t *testing.T) {
	t.Parallel()

	payloadColumn := Column{Name: "payload", Kind: JSON}
	index := &fakeFullTextInvertedIndex{
		mode: invertedPostingModeRowIDs,
		postings: map[string][]invertedPosting{
			`k:type`:            {{RowID: 1}, {RowID: 2}, {RowID: 3}},
			`kv:type:s:"click"`: {{RowID: 1}, {RowID: 3}},
		},
	}
	table := NewTable(testLogger, nil, nil, "events", []Column{payloadColumn}, 0, nil, WithSecondaryIndex(SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    "idx_payload_inv",
			Method:  IndexMethodInverted,
			Columns: []Column{payloadColumn},
		},
		InvertedIndex: index,
	}))

	result, ok, err := table.tryCountFromExactInvertedIndex(context.Background(), QueryPlan{Scans: []Scan{{
		TableName: "events",
		Type:      ScanTypeInverted,
		IndexName: "idx_payload_inv",
		IndexKeys: []any{`k:type`, `kv:type:s:"click"`},
		Filters:   OneOrMore{{jsonContainsCondition("payload", `{"type":"click"}`)}},
	}}})
	require.NoError(t, err)
	require.True(t, ok)

	require.True(t, result.Rows.Next(context.Background()))
	countValue, ok := result.Rows.Row().GetValue("COUNT(*)")
	require.True(t, ok)
	assert.Equal(t, int64(2), countValue.Value)
}

func TestJSONInvertedCountSkipsNonExactIndexScan(t *testing.T) {
	t.Parallel()

	payloadColumn := Column{Name: "payload", Kind: JSON}
	table := NewTable(testLogger, nil, nil, "events", []Column{payloadColumn}, 0, nil, WithSecondaryIndex(SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    "idx_payload_inv",
			Method:  IndexMethodInverted,
			Columns: []Column{payloadColumn},
		},
		InvertedIndex: &fakeFullTextInvertedIndex{mode: invertedPostingModeRowIDs, postings: make(map[string][]invertedPosting)},
	}))

	_, ok, err := table.tryCountFromExactInvertedIndex(context.Background(), QueryPlan{Scans: []Scan{{
		TableName: "events",
		Type:      ScanTypeInverted,
		IndexName: "idx_payload_inv",
		IndexKeys: []any{`k:tags`},
		Filters:   OneOrMore{{jsonContainsCondition("payload", `{"tags":[{"name":"web"}]}`)}},
	}}})
	require.NoError(t, err)
	assert.False(t, ok)
}
