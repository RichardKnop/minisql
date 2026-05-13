package minisql

import (
	"math"
	"slices"
	"unicode"
)

func fullTextTokenColumn() Column {
	return Column{Name: "__fts_token__", Kind: Varchar, Size: MaxIndexKeySize}
}

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
// preserving the order of their first occurrence. It intentionally does not
// enforce index key limits so sequential MATCH semantics can still handle long
// terms; index planning checks key sizes separately.
func appendUniqueTextSearchTerms(existing []string, terms ...string) []string {
	seen := make(map[string]struct{}, len(existing)+len(terms))
	for _, term := range existing {
		seen[term] = struct{}{}
	}
	for _, term := range terms {
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

// hasOverlongToken reports whether any parsed query token cannot be represented
// as a key in the current full-text B+ tree index.
func (q textSearchQuery) hasOverlongToken() bool {
	for _, term := range q.Terms {
		if len([]byte(term)) > MaxIndexKeySize {
			return true
		}
	}
	for _, phrase := range q.Phrases {
		for _, term := range phrase {
			if len([]byte(term)) > MaxIndexKeySize {
				return true
			}
		}
	}
	return false
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

// textSearchRank returns a relevance score for the query tokens. It combines
// saturated term frequency, query coverage, mild document-length normalization,
// phrase boosts, and proximity boosts from token positions.
func textSearchRank(document, query string) float64 {
	parsedQuery, ok := parseTextSearchQuery(query)
	if !ok {
		return 0
	}
	queryTokens := parsedQuery.allUniqueTokens()
	if len(queryTokens) == 0 {
		return 0
	}

	docTokens := textSearchTokenPositions(document)
	if len(docTokens) == 0 {
		return 0
	}
	return textSearchRankPositions(docTokens, parsedQuery)
}

// textSearchRankPositions scores an already-tokenized document against a parsed
// query. It is the shared core behind TS_RANK and keeps ranking independent from
// whether MATCH used an index or a sequential scan.
func textSearchRankPositions(docTokens []textSearchTokenPosition, query textSearchQuery) float64 {
	queryTokens := query.allUniqueTokens()
	if len(queryTokens) == 0 || len(docTokens) == 0 {
		return 0
	}

	positions := make(map[string][]uint32, len(docTokens))
	frequencies := make(map[string]int, len(docTokens))
	for _, token := range docTokens {
		frequencies[token.Term] += 1
		positions[token.Term] = append(positions[token.Term], token.Position)
	}

	var (
		frequencyScore float64
		matchedTerms   int
	)
	for _, token := range queryTokens {
		frequency := frequencies[token]
		if frequency == 0 {
			continue
		}
		matchedTerms += 1
		frequencyScore += saturatedTermFrequency(frequency)
	}

	if matchedTerms == 0 {
		return 0
	}

	coverage := float64(matchedTerms) / float64(len(queryTokens))
	base := (frequencyScore / float64(len(queryTokens))) * (0.5 + 0.5*coverage)
	base *= textSearchLengthNormalization(len(docTokens), len(queryTokens))

	return base + textSearchPhraseBoost(positions, query, len(queryTokens)) + textSearchProximityBoost(docTokens, queryTokens, coverage)
}

// saturatedTermFrequency gives repeated term hits diminishing returns so a
// document cannot win purely by repeating one query token many times.
func saturatedTermFrequency(frequency int) float64 {
	if frequency <= 0 {
		return 0
	}
	raw := math.Log1p(float64(frequency))
	return raw / (1 + raw)
}

// textSearchLengthNormalization mildly penalizes documents with many extra
// tokens beyond the query length, favoring focused matches over noisy text.
func textSearchLengthNormalization(documentTokens, queryTokens int) float64 {
	if documentTokens <= 0 || queryTokens <= 0 {
		return 0
	}
	extraTokens := max(documentTokens-queryTokens, 0)
	return 1 / math.Sqrt(1+float64(extraTokens)/20)
}

// textSearchPhraseBoost rewards quoted phrases that appear as adjacent tokens
// in the document.
func textSearchPhraseBoost(positions map[string][]uint32, query textSearchQuery, queryTokenCount int) float64 {
	if queryTokenCount == 0 {
		return 0
	}
	var boost float64
	for _, phrase := range query.Phrases {
		if textSearchPhraseMatches(positions, phrase) {
			boost += 0.25 * (float64(len(phrase)) / float64(queryTokenCount))
		}
	}
	return boost
}

// textSearchProximityBoost rewards documents where all query tokens appear in a
// compact span, making clustered matches rank above scattered matches.
func textSearchProximityBoost(docTokens []textSearchTokenPosition, queryTokens []string, coverage float64) float64 {
	if len(queryTokens) < 2 || coverage == 0 {
		return 0
	}
	span, ok := textSearchMinCoverSpan(docTokens, queryTokens)
	if !ok || span == 0 {
		return 0
	}
	density := float64(len(queryTokens)) / float64(span)
	return 0.20 * density * coverage
}

// textSearchMinCoverSpan finds the shortest token-position window containing at
// least one occurrence of every unique query token.
func textSearchMinCoverSpan(docTokens []textSearchTokenPosition, queryTokens []string) (uint32, bool) {
	querySet := make(map[string]struct{}, len(queryTokens))
	for _, token := range queryTokens {
		querySet[token] = struct{}{}
	}
	hits := make([]textSearchTokenPosition, 0, len(docTokens))
	for _, token := range docTokens {
		if _, ok := querySet[token.Term]; ok {
			hits = append(hits, token)
		}
	}
	if len(hits) == 0 {
		return 0, false
	}
	slices.SortFunc(hits, func(a, b textSearchTokenPosition) int {
		return int(a.Position) - int(b.Position)
	})

	counts := make(map[string]int, len(queryTokens))
	var (
		have     int
		left     int
		bestSpan uint32
		found    bool
	)
	for right, hit := range hits {
		if counts[hit.Term] == 0 {
			have += 1
		}
		counts[hit.Term] += 1

		for have == len(queryTokens) {
			span := hits[right].Position - hits[left].Position + 1
			if !found || span < bestSpan {
				bestSpan = span
				found = true
			}
			leftTerm := hits[left].Term
			counts[leftTerm] -= 1
			if counts[leftTerm] == 0 {
				have -= 1
			}
			left += 1
		}
	}

	return bestSpan, found
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
