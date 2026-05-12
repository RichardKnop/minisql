package minisql

import (
	"fmt"
	"math"
	"unicode"
)

var textSearchStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "by": {},
	"for": {}, "from": {}, "in": {}, "is": {}, "it": {}, "of": {}, "on": {}, "or": {},
	"that": {}, "the": {}, "to": {}, "was": {}, "with": {},
}

const (
	fullTextPostingPositionBits = 32
	maxFullTextPostingComponent = uint64(^uint32(0))
)

type textSearchTokenPosition struct {
	Term     string
	Position uint32
}

type textSearchQuery struct {
	Terms   []string
	Phrases [][]string
}

func textSearchTokens(input string) []string {
	positions := textSearchTokenPositions(input)
	tokens := make([]string, len(positions))
	for i, positioned := range positions {
		tokens[i] = positioned.Term
	}
	return tokens
}

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

func (q textSearchQuery) allUniqueTokens() []string {
	tokens := appendUniqueTextSearchTerms(nil, q.Terms...)
	for _, phrase := range q.Phrases {
		tokens = appendUniqueTextSearchTerms(tokens, phrase...)
	}
	return tokens
}

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

func encodeFullTextPosting(rowID RowID, position uint32) (RowID, error) {
	if uint64(rowID) > maxFullTextPostingComponent {
		return 0, fmt.Errorf("full-text row id %d exceeds positional posting limit", rowID)
	}
	return RowID(uint64(rowID)<<fullTextPostingPositionBits | uint64(position)), nil
}

func decodeFullTextPosting(posting RowID) (RowID, uint32) {
	return RowID(uint64(posting) >> fullTextPostingPositionBits), uint32(posting)
}
