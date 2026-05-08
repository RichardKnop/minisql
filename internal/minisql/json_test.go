package minisql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormaliseJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{
			name:  "object with whitespace",
			input: `{ "a" : 1 , "b" : 2 }`,
			want:  `{"a":1,"b":2}`,
		},
		{
			name:  "array with whitespace",
			input: `[ 1 , 2 , 3 ]`,
			want:  `[1,2,3]`,
		},
		{
			name:  "nested",
			input: `{"x": {"y": [1, 2]}}`,
			want:  `{"x":{"y":[1,2]}}`,
		},
		{
			name:  "already compact",
			input: `{"a":1}`,
			want:  `{"a":1}`,
		},
		{
			name:  "preserves key order",
			input: `{"z":1,"a":2,"m":3}`,
			want:  `{"z":1,"a":2,"m":3}`,
		},
		{
			name:  "string value",
			input: `"hello"`,
			want:  `"hello"`,
		},
		{
			name:  "null literal",
			input: `null`,
			want:  `null`,
		},
		{
			name:  "boolean true",
			input: `true`,
			want:  `true`,
		},
		{
			name:  "boolean false",
			input: `false`,
			want:  `false`,
		},
		{
			name:  "integer",
			input: `42`,
			want:  `42`,
		},
		{
			name:    "invalid JSON",
			input:   `{bad}`,
			wantErr: "invalid JSON",
		},
		{
			name:    "empty string",
			input:   ``,
			wantErr: "invalid JSON",
		},
		{
			name:    "invalid UTF-8",
			input:   string([]byte{0xff, 0xfe}),
			wantErr: "invalid UTF-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := normaliseJSON(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.wantErr),
					"expected error containing %q, got %q", tt.wantErr, err.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvalJSONPath(t *testing.T) {
	t.Parallel()

	doc := map[string]any{
		"name": "alice",
		"age":  float64(30),
		"tags": []any{"go", "sql"},
		"addr": map[string]any{"city": "London"},
	}

	tests := []struct {
		name      string
		root      any
		path      string
		wantVal   any
		wantFound bool
		wantErr   bool
	}{
		{
			name:      "root",
			root:      doc,
			path:      "$",
			wantVal:   doc,
			wantFound: true,
		},
		{
			name:      "empty path treated as root",
			root:      doc,
			path:      "",
			wantVal:   doc,
			wantFound: true,
		},
		{
			name:      "simple key",
			root:      doc,
			path:      "$.name",
			wantVal:   "alice",
			wantFound: true,
		},
		{
			name:      "numeric key",
			root:      doc,
			path:      "$.age",
			wantVal:   float64(30),
			wantFound: true,
		},
		{
			name:      "nested key",
			root:      doc,
			path:      "$.addr.city",
			wantVal:   "London",
			wantFound: true,
		},
		{
			name:      "array index 0",
			root:      doc,
			path:      "$.tags[0]",
			wantVal:   "go",
			wantFound: true,
		},
		{
			name:      "array index 1",
			root:      doc,
			path:      "$.tags[1]",
			wantVal:   "sql",
			wantFound: true,
		},
		{
			name:      "bracket key syntax",
			root:      doc,
			path:      "$['name']",
			wantVal:   "alice",
			wantFound: true,
		},
		{
			name:      "missing key",
			root:      doc,
			path:      "$.missing",
			wantFound: false,
		},
		{
			name:      "array out of bounds",
			root:      doc,
			path:      "$.tags[99]",
			wantFound: false,
		},
		{
			name:      "key on non-object",
			root:      doc,
			path:      "$.name.sub",
			wantFound: false,
		},
		{
			name:    "no leading dollar",
			root:    doc,
			path:    "name",
			wantErr: true,
		},
		{
			name:    "unclosed bracket",
			root:    doc,
			path:    "$.tags[0",
			wantErr: true,
		},
		{
			name:    "invalid array index",
			root:    doc,
			path:    "$.tags[x]",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, found, err := evalJSONPath(tt.root, tt.path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				assert.Equal(t, tt.wantVal, val)
			}
		})
	}
}

func TestJSONToScalar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
		want  any
	}{
		{"nil", nil, nil},
		{"string", "hello", NewTextPointer([]byte("hello"))},
		{"integral float", float64(42), int64(42)},
		{"real float", float64(3.14), float64(3.14)},
		{"bool true", true, int64(1)},
		{"bool false", false, int64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := jsonToScalar(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJSONToScalar_ObjectAndArray(t *testing.T) {
	t.Parallel()

	obj := map[string]any{"k": "v"}
	got := jsonToScalar(obj)
	tp, ok := got.(TextPointer)
	require.True(t, ok, "expected TextPointer for object, got %T", got)
	assert.Equal(t, `{"k":"v"}`, tp.String())

	arr := []any{float64(1), float64(2)}
	got = jsonToScalar(arr)
	tp, ok = got.(TextPointer)
	require.True(t, ok, "expected TextPointer for array, got %T", got)
	assert.Equal(t, `[1,2]`, tp.String())
}

func TestJSONTypeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		val  any
		want string
	}{
		{nil, "null"},
		{map[string]any{}, "object"},
		{[]any{}, "array"},
		{"hello", "text"},
		{float64(42), "integer"},
		{float64(3.14), "real"},
		{true, "true"},
		{false, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, jsonTypeName(tt.val))
		})
	}
}

func TestJSONExtract(t *testing.T) {
	t.Parallel()

	doc := `{"name":"alice","scores":[10,20],"meta":{"active":true}}`

	tests := []struct {
		name       string
		path       string
		wantFrag   string
		wantScalar any
		wantFound  bool
	}{
		{
			name:       "string field",
			path:       "$.name",
			wantFrag:   `"alice"`,
			wantScalar: NewTextPointer([]byte("alice")),
			wantFound:  true,
		},
		{
			name:       "array element",
			path:       "$.scores[0]",
			wantFrag:   `10`,
			wantScalar: int64(10),
			wantFound:  true,
		},
		{
			name:       "nested bool",
			path:       "$.meta.active",
			wantFrag:   `true`,
			wantScalar: int64(1),
			wantFound:  true,
		},
		{
			name:      "missing key",
			path:      "$.missing",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			frag, scalar, found, err := jsonExtract(doc, tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				assert.Equal(t, tt.wantFrag, frag)
				assert.Equal(t, tt.wantScalar, scalar)
			}
		})
	}
}
