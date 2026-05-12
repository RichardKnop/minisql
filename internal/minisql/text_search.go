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

type textSearchTokenPosition struct {
	Term     string
	Position uint32
}

type textSearchQuery struct {
	Terms   []string
	Phrases [][]string
}

// textSearchTokens returns the normalized token terms from input, without
// positions. It is used by ranking and callers that only need term presence.
func textSearchTokens(input string) []string {
	positions := textSearchTokenPositions(input)
	tokens := make([]string, len(positions))
	for i, positioned := range positions {
		tokens[i] = positioned.Term
	}
	return tokens
}

// textSearchTokenPositions lowercases input, splits on non-letter/non-digit
// boundaries, removes stop words, and assigns dense positions to emitted tokens.
func textSearchTokenPositions(input string) []textSearchTokenPosition {
	tokens := make([]textSearchTokenPosition, 0)
	current := make([]rune, 0, 16)
	var position uint64

	flush := func() {
		if len(current) == 0 {
			return
		}
		token := string(current)
		current = current[:0]
		if _, stop := textSearchStopWords[token]; stop {
			return
		}
		if position > maxFullTextPostingComponent {
			return
		}
		tokens = append(tokens, textSearchTokenPosition{Term: token, Position: uint32(position)})
		position += 1
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

// uniqueTextSearchTokens returns de-duplicated indexable tokens in first-seen
// order. Tokens too large for the current B+ tree key format are skipped.
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

// parseTextSearchQuery splits a MATCH query into plain terms and double-quoted
// phrases. Plain terms and phrases are combined with implicit AND semantics.
func parseTextSearchQuery(input string) (textSearchQuery, bool) {
	var (
		query       textSearchQuery
		outside     []rune
		phrase      []rune
		insideQuote bool
	)

	appendTerms := func(input string) {
		query.Terms = appendUniqueTextSearchTerms(query.Terms, textSearchTokens(input)...)
	}
	appendPhrase := func(input string) {
		tokens := textSearchTokens(input)
		if len(tokens) > 0 {
			query.Phrases = append(query.Phrases, tokens)
		}
	}

	for _, r := range input {
		if r != '"' {
			if insideQuote {
				phrase = append(phrase, r)
			} else {
				outside = append(outside, r)
			}
			continue
		}

		if insideQuote {
			appendPhrase(string(phrase))
			phrase = phrase[:0]
			insideQuote = false
			continue
		}

		appendTerms(string(outside))
		outside = outside[:0]
		insideQuote = true
	}

	if insideQuote {
		return textSearchQuery{}, false
	}
	appendTerms(string(outside))

	if len(query.Terms) == 0 && len(query.Phrases) == 0 {
		return textSearchQuery{}, false
	}
	return query, true
}

// appendUniqueTextSearchTerms appends terms that are not already present,
// preserving the order of their first occurrence.
func appendUniqueTextSearchTerms(existing []string, terms ...string) []string {
	seen := make(map[string]struct{}, len(existing)+len(terms))
	for _, term := range existing {
		seen[term] = struct{}{}
	}
	for _, term := range terms {
		if len([]byte(term)) > MaxIndexKeySize {
			continue
		}
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		existing = append(existing, term)
	}
	return existing
}

// allUniqueTokens returns every distinct token needed to evaluate the query,
// including tokens that came from phrases.
func (q textSearchQuery) allUniqueTokens() []string {
	tokens := appendUniqueTextSearchTerms(nil, q.Terms...)
	for _, phrase := range q.Phrases {
		tokens = appendUniqueTextSearchTerms(tokens, phrase...)
	}
	return tokens
}

// textSearchMatch evaluates MATCH semantics in memory. Plain query terms must
// all be present, and every quoted phrase must appear at adjacent token positions.
func textSearchMatch(document, query string) bool {
	parsedQuery, ok := parseTextSearchQuery(query)
	if !ok {
		return false
	}

	docTokens := textSearchTokenPositions(document)
	if len(docTokens) == 0 {
		return false
	}

	positions := make(map[string][]uint32, len(docTokens))
	for _, token := range docTokens {
		positions[token.Term] = append(positions[token.Term], token.Position)
	}
	for _, token := range parsedQuery.Terms {
		if len(positions[token]) == 0 {
			return false
		}
	}
	for _, phrase := range parsedQuery.Phrases {
		if !textSearchPhraseMatches(positions, phrase) {
			return false
		}
	}
	return true
}

// textSearchRank returns a simple log-scaled term-frequency score for the query
// tokens. Phrase proximity does not affect ranking yet.
func textSearchRank(document, query string) float64 {
	parsedQuery, ok := parseTextSearchQuery(query)
	if !ok {
		return 0
	}
	queryTokens := parsedQuery.allUniqueTokens()
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

// textSearchPhraseMatches reports whether all tokens in phrase appear at
// consecutive positions in the already-tokenized document position map.
func textSearchPhraseMatches(positions map[string][]uint32, phrase []string) bool {
	if len(phrase) == 0 {
		return false
	}
	starts := positions[phrase[0]]
	if len(starts) == 0 {
		return false
	}
	positionSets := make([]map[uint32]struct{}, len(phrase))
	for i, term := range phrase {
		if len(positions[term]) == 0 {
			return false
		}
		positionSets[i] = make(map[uint32]struct{}, len(positions[term]))
		for _, position := range positions[term] {
			positionSets[i][position] = struct{}{}
		}
	}
	for _, start := range starts {
		matches := true
		for offset := 1; offset < len(phrase); offset++ {
			if _, ok := positionSets[offset][start+uint32(offset)]; !ok {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}
