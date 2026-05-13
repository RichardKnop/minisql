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

	index := &fakeFullTextIndex{rowIDs: make(map[any][]RowID)}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    "idx_payload_inv",
			Method:  IndexMethodInverted,
			Columns: []Column{payloadColumn},
		},
		Index: index,
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
