package minisql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ErrInvalidJSON is returned when a value fails JSON validation for a JSON column.
type ErrInvalidJSON struct {
	Cause error
}

func (e ErrInvalidJSON) Error() string {
	return fmt.Sprintf("invalid JSON: %v", e.Cause)
}

func (e ErrInvalidJSON) Unwrap() error {
	return e.Cause
}

// normaliseJSON validates s as a JSON document, then re-encodes it in compact
// form (no extra whitespace, preserving key insertion order).
// Rejects invalid UTF-8 and null bytes.
func normaliseJSON(s string) (string, error) {
	if !utf8.ValidString(s) {
		return "", ErrInvalidJSON{Cause: fmt.Errorf("invalid UTF-8")}
	}
	var compacted bytes.Buffer
	compacted.Grow(len(s))
	if err := json.Compact(&compacted, []byte(s)); err != nil {
		return "", ErrInvalidJSON{Cause: err}
	}
	compactedBytes := compacted.Bytes()
	if bytes.IndexByte(compactedBytes, 0) >= 0 {
		return "", ErrInvalidJSON{Cause: fmt.Errorf("null byte not allowed in JSON")}
	}
	if bytes.ContainsAny(compactedBytes, "<>&") ||
		bytes.Contains(compactedBytes, []byte{0xe2, 0x80, 0xa8}) ||
		bytes.Contains(compactedBytes, []byte{0xe2, 0x80, 0xa9}) {
		var escaped bytes.Buffer
		escaped.Grow(compacted.Len())
		json.HTMLEscape(&escaped, compactedBytes)
		return escaped.String(), nil
	}
	return compacted.String(), nil
}

// jsonExtract extracts the value at path from doc and returns both forms:
//   - jsonFrag: the value re-serialised as a JSON fragment (used by ->)
//   - scalar:   the value as a Go SQL scalar (used by ->> and JSON_EXTRACT)
//   - found:    false if the path does not exist in the document
func jsonExtract(doc, path string) (jsonFrag string, scalar any, found bool, err error) {
	var root any
	if err = json.Unmarshal([]byte(doc), &root); err != nil {
		return "", nil, false, fmt.Errorf("invalid JSON document: %w", err)
	}
	val, found, err := evalJSONPath(root, path)
	if err != nil || !found {
		return "", nil, found, err
	}
	b, err := json.Marshal(val)
	if err != nil {
		return "", nil, false, err
	}
	return string(b), jsonToScalar(val), true, nil
}

// jsonToScalar converts a Go value decoded from JSON into the most natural SQL
// scalar type: string -> TextPointer, integral float64 -> int64, other float64 ->
// float64, bool -> int64 (1/0), object/array -> JSON text as TextPointer, nil -> nil.
func jsonToScalar(val any) any {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case string:
		return NewTextPointer([]byte(v))
	case float64:
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v
	case bool:
		if v {
			return int64(1)
		}
		return int64(0)
	default:
		// object or array: re-serialise as JSON text
		b, _ := json.Marshal(v)
		return NewTextPointer(b)
	}
}

// evalJSONPath navigates the parsed JSON tree along a simple path expression.
// Supported syntax: $ (root), $.key, $['key'], $[n] (array index), and any
// combination thereof, e.g. $.a.b[0].c.
// Returns (value, found, error); found is false when the key/index is absent.
func evalJSONPath(root any, path string) (any, bool, error) {
	if path == "$" || path == "" {
		return root, true, nil
	}
	if !strings.HasPrefix(path, "$") {
		return nil, false, fmt.Errorf("invalid JSON path %q: must start with $", path)
	}

	cur := root
	rest := path[1:] // strip leading $

	for rest != "" {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			end := nextJSONStepIdx(rest)
			key := rest[:end]
			rest = rest[end:]
			if key == "" {
				return nil, false, fmt.Errorf("invalid JSON path: empty key after '.'")
			}
			obj, ok := cur.(map[string]any)
			if !ok {
				return nil, false, nil
			}
			cur, ok = obj[key]
			if !ok {
				return nil, false, nil
			}
		case '[':
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, false, fmt.Errorf("invalid JSON path %q: unclosed '['", path)
			}
			inner := rest[1:end]
			rest = rest[end+1:]
			// $['key'] syntax
			if len(inner) >= 2 && inner[0] == '\'' && inner[len(inner)-1] == '\'' {
				key := inner[1 : len(inner)-1]
				obj, ok := cur.(map[string]any)
				if !ok {
					return nil, false, nil
				}
				cur, ok = obj[key]
				if !ok {
					return nil, false, nil
				}
				continue
			}
			// $[n] array index
			idx, err := strconv.Atoi(inner)
			if err != nil {
				return nil, false, fmt.Errorf("invalid JSON path array index %q", inner)
			}
			arr, ok := cur.([]any)
			if !ok {
				return nil, false, nil
			}
			if idx < 0 || idx >= len(arr) {
				return nil, false, nil
			}
			cur = arr[idx]
		default:
			return nil, false, fmt.Errorf("invalid JSON path %q: expected '.' or '['", path)
		}
	}
	return cur, true, nil
}

// nextJSONStepIdx returns the index of the next '.' or '[' in s, or len(s) if
// neither is found.
func nextJSONStepIdx(s string) int {
	for i, c := range s {
		if c == '.' || c == '[' {
			return i
		}
	}
	return len(s)
}

// jsonTypeName returns the JSON type name for val as used by JSON_TYPE().
// Follows SQLite naming: "object", "array", "text", "integer", "real",
// "true", "false", "null".
func jsonTypeName(val any) string {
	if val == nil {
		return "null"
	}
	switch v := val.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "text"
	case float64:
		if v == float64(int64(v)) {
			return "integer"
		}
		return "real"
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return "null"
	}
}
