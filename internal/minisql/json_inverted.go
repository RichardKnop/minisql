package minisql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strings"
)

func jsonInvertedTermColumn() Column {
	return Column{Name: "__json_term__", Kind: Varchar, Size: MaxIndexKeySize}
}

var jsonInvertedPathReplacer = strings.NewReplacer(
	`\`, `\\`,
	`.`, `\.`,
	`[`, `\[`,
	`]`, `\]`,
)

// jsonContains reports whether doc contains query using JSON subset semantics.
// Objects must contain every queried key recursively, arrays must contain every
// queried element, and scalar values must match by JSON type and value.
func jsonContains(doc, query string) (bool, error) {
	queryValue, err := decodeJSONForInvertedIndex(query)
	if err != nil {
		return false, fmt.Errorf("invalid JSON query: %w", err)
	}
	return jsonContainsDecodedQuery(doc, queryValue)
}

// jsonContainsDecodedQuery checks containment when the query side has already
// been decoded, avoiding repeated parsing for indexed scan rechecks.
func jsonContainsDecodedQuery(doc string, queryValue any) (bool, error) {
	docValue, err := decodeJSONForInvertedIndex(doc)
	if err != nil {
		return false, fmt.Errorf("invalid JSON document: %w", err)
	}
	return jsonContainsValue(docValue, queryValue), nil
}

// jsonInvertedTermsForDocument extracts every key-existence and scalar
// key/value term that should be stored for one JSON document.
func jsonInvertedTermsForDocument(doc string) ([]string, error) {
	return jsonInvertedTermsForDocumentInto(doc, nil)
}

func jsonInvertedTermsForDocumentInto(doc string, terms []string) ([]string, error) {
	value, err := decodeJSONForInvertedIndex(doc)
	if err != nil {
		return nil, err
	}
	return jsonInvertedTermsInto(value, terms), nil
}

// jsonInvertedTermsForQuery extracts the terms that must be present before a
// JSON_CONTAINS predicate can match. The executor still rechecks the predicate
// against the full row because terms are only a lossless prefilter for v1.
func jsonInvertedTermsForQuery(query string) ([]string, error) {
	value, err := decodeJSONForInvertedIndex(query)
	if err != nil {
		return nil, err
	}
	return jsonInvertedTerms(value), nil
}

// jsonInvertedQueryTermsAreExact reports whether the generated terms fully
// prove JSON containment without reparsing the candidate document. Array and
// empty-container queries still need rechecking unless the array contains only
// unique scalar values whose term presence is enough to prove membership.
func jsonInvertedQueryTermsAreExact(value any) bool {
	return jsonInvertedQueryTermsAreExactAt("", value)
}

func jsonInvertedQueryTermsAreExactAt(path string, value any) bool {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 {
			return false
		}
		for key, child := range v {
			childPath := joinJSONInvertedPath(path, key)
			if len([]byte("k:"+childPath)) > MaxIndexKeySize {
				return false
			}
			if !jsonInvertedQueryTermsAreExactAt(childPath, child) {
				return false
			}
		}
		return true
	case []any:
		return jsonInvertedScalarArrayTermsAreExactAt(path+"[]", v)
	default:
		if path == "" {
			path = "$"
		}
		return len([]byte("kv:"+path+":"+jsonInvertedScalarTerm(v))) <= MaxIndexKeySize
	}
}

func jsonInvertedScalarArrayTermsAreExactAt(path string, values []any) bool {
	if len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		switch value.(type) {
		case map[string]any, []any:
			return false
		}
		term := "kv:" + path + ":" + jsonInvertedScalarTerm(value)
		if len([]byte(term)) > MaxIndexKeySize {
			return false
		}
		if _, ok := seen[term]; ok {
			return false
		}
		seen[term] = struct{}{}
	}
	return true
}

// decodeJSONForInvertedIndex parses JSON with numbers preserved as json.Number
// so term generation and containment can canonicalize numeric values explicitly.
func decodeJSONForInvertedIndex(input string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(input))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

// jsonInvertedTerms returns a stable, deduplicated list of all index terms
// generated from a decoded JSON value.
func jsonInvertedTerms(value any) []string {
	return jsonInvertedTermsInto(value, nil)
}

func jsonInvertedTermsInto(value any, terms []string) []string {
	collectJSONInvertedTerms(&terms, "", value)
	slices.Sort(terms)
	return slices.Compact(terms)
}

// collectJSONInvertedTerms walks the decoded JSON tree and emits key-existence
// terms for objects plus scalar key/value terms for leaves and array members.
func collectJSONInvertedTerms(terms *[]string, path string, value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			childPath := joinJSONInvertedPath(path, key)
			appendJSONInvertedTerm(terms, "k:"+childPath)
			collectJSONInvertedTerms(terms, childPath, child)
		}
	case []any:
		arrayPath := path + "[]"
		for _, child := range v {
			collectJSONInvertedTerms(terms, arrayPath, child)
		}
	default:
		if path == "" {
			path = "$"
		}
		appendJSONInvertedTerm(terms, "kv:"+path+":"+jsonInvertedScalarTerm(v))
	}
}

// appendJSONInvertedTerm adds a term only when it fits in the B+ tree key size
// used by the v1 inverted-index storage.
func appendJSONInvertedTerm(terms *[]string, term string) {
	if len([]byte(term)) > MaxIndexKeySize {
		return
	}
	*terms = append(*terms, term)
}

// joinJSONInvertedPath appends an escaped object key to the current dotted JSON
// path used in generated terms.
func joinJSONInvertedPath(parent, key string) string {
	escaped := jsonInvertedPathSegment(key)
	if parent == "" {
		return escaped
	}
	return parent + "." + escaped
}

// jsonInvertedPathSegment escapes path separator characters inside object keys
// so similarly named paths generate different term strings.
func jsonInvertedPathSegment(segment string) string {
	return jsonInvertedPathReplacer.Replace(segment)
}

// jsonInvertedScalarTerm encodes a scalar JSON value with a type prefix, keeping
// strings, booleans, numbers, and null distinct in the term space.
func jsonInvertedScalarTerm(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		encoded, _ := json.Marshal(v)
		return "s:" + string(encoded)
	case bool:
		if v {
			return "b:true"
		}
		return "b:false"
	case json.Number:
		return "n:" + canonicalJSONNumber(v)
	default:
		encoded, _ := json.Marshal(v)
		return "j:" + string(encoded)
	}
}

// jsonContainsValue implements recursive JSON subset containment over decoded
// values; it is used as the authoritative predicate after index prefiltering.
func jsonContainsValue(doc, query any) bool {
	switch q := query.(type) {
	case map[string]any:
		d, ok := doc.(map[string]any)
		if !ok {
			return false
		}
		for key, queryChild := range q {
			docChild, ok := d[key]
			if !ok || !jsonContainsValue(docChild, queryChild) {
				return false
			}
		}
		return true
	case []any:
		d, ok := doc.([]any)
		if !ok {
			return false
		}
		used := make([]bool, len(d))
		for _, queryChild := range q {
			found := false
			for i, docChild := range d {
				if used[i] {
					continue
				}
				if jsonContainsValue(docChild, queryChild) {
					used[i] = true
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return jsonScalarEqual(doc, query)
	}
}

// jsonScalarEqual compares scalar JSON values by type, with numeric values
// compared in canonical rational form so 1 and 1.0 are equal.
func jsonScalarEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case json.Number:
		bv, ok := b.(json.Number)
		if !ok {
			return false
		}
		return jsonNumberEqual(av, bv)
	default:
		ab, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		return bytes.Equal(ab, bb)
	}
}

// jsonNumberEqual compares JSON numbers after canonicalization.
func jsonNumberEqual(a, b json.Number) bool {
	return canonicalJSONNumber(a) == canonicalJSONNumber(b)
}

// canonicalJSONNumber converts a JSON number to a stable representation used by
// both term generation and scalar containment comparison.
func canonicalJSONNumber(n json.Number) string {
	r, ok := new(big.Rat).SetString(n.String())
	if !ok {
		return n.String()
	}
	if r.IsInt() {
		return r.Num().String()
	}
	return r.String()
}
