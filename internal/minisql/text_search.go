package minisql

import (
	"math"
	"unicode"
)

var textSearchStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "by": {},
	"for": {}, "from": {}, "in": {}, "is": {}, "it": {}, "of": {}, "on": {}, "or": {},
	"that": {}, "the": {}, "to": {}, "was": {}, "with": {},
}

func textSearchTokens(input string) []string {
	tokens := make([]string, 0)
	current := make([]rune, 0, 16)

	flush := func() {
		if len(current) == 0 {
			return
		}
		token := string(current)
		current = current[:0]
		if _, stop := textSearchStopWords[token]; stop {
			return
		}
		tokens = append(tokens, token)
	}

	for _, r := range input {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current = append(current, unicode.ToLower(r))
			continue
		}
		flush()
	}
	flush()

	return tokens
}

func uniqueTextSearchTokens(input string) []string {
	tokens := textSearchTokens(input)
	seen := make(map[string]struct{}, len(tokens))
	unique := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if len([]byte(token)) > MaxIndexKeySize {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		unique = append(unique, token)
	}
	return unique
}

func textSearchMatch(document, query string) bool {
	queryTokens := uniqueTextSearchTokens(query)
	if len(queryTokens) == 0 {
		return false
	}

	docTokens := textSearchTokens(document)
	if len(docTokens) == 0 {
		return false
	}

	docSet := make(map[string]struct{}, len(docTokens))
	for _, token := range docTokens {
		docSet[token] = struct{}{}
	}
	for _, token := range queryTokens {
		if _, ok := docSet[token]; !ok {
			return false
		}
	}
	return true
}

func textSearchRank(document, query string) float64 {
	queryTokens := uniqueTextSearchTokens(query)
	if len(queryTokens) == 0 {
		return 0
	}

	docTokens := textSearchTokens(document)
	if len(docTokens) == 0 {
		return 0
	}

	frequencies := make(map[string]int, len(docTokens))
	for _, token := range docTokens {
		frequencies[token] += 1
	}

	var score float64
	for _, token := range queryTokens {
		score += math.Log1p(float64(frequencies[token]))
	}
	return score / float64(len(queryTokens))
}
