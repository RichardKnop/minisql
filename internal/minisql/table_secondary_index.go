package minisql

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/zap"
)

// SecondaryIndex associates a non-unique B+ tree index with its metadata.
// It supports plain column indexes, composite indexes, partial indexes
// (with a WHERE predicate), and expression indexes.
type SecondaryIndex struct {
	Index         BTreeIndex
	InvertedIndex invertedIndex
	IndexInfo
}

func secondaryIndexStorageColumns(secondaryIndex SecondaryIndex) []Column {
	if secondaryIndex.Method == IndexMethodFullText {
		return []Column{fullTextTokenColumn()}
	}
	if secondaryIndex.Method == IndexMethodInverted {
		return []Column{jsonInvertedTermColumn()}
	}
	return secondaryIndex.Columns
}

func secondaryIndexUsesDedicatedInvertedStorage(method IndexMethod) bool {
	return method == IndexMethodFullText || method == IndexMethodInverted
}

func invertedIndexPostingModeForIndexMethod(method IndexMethod) invertedIndexPostingMode {
	if method == IndexMethodFullText {
		return invertedIndexPostingModePositions
	}
	return invertedIndexPostingModeRowIDs
}

// rowSatisfiesWhereCond returns true when the row satisfies the partial index predicate,
// or always true for a full (non-partial) index.
func (si SecondaryIndex) rowSatisfiesWhereCond(row Row) (bool, error) {
	if len(si.WhereCond) == 0 {
		return true, nil
	}
	return row.CheckOneOrMore(si.WhereCond)
}

func (t *Table) insertSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, keys []OptionalValue, rowID RowID, row Row) error {
	if secondaryIndex.Method == IndexMethodFullText {
		return t.insertFullTextIndexKeys(ctx, secondaryIndex, rowID, row)
	}
	if secondaryIndex.Method == IndexMethodInverted {
		return t.insertInvertedIndexKeys(ctx, secondaryIndex, rowID, row)
	}
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	// Partial index: skip rows that don't satisfy the WHERE predicate.
	if ok, err := secondaryIndex.rowSatisfiesWhereCond(row); err != nil {
		return fmt.Errorf("partial index %s where check: %w", secondaryIndex.Name, err)
	} else if !ok {
		return nil
	}

	// Expression index: evaluate the expression against the row.
	if secondaryIndex.Expression != nil {
		key, ok, err := evalExprIndexKey(secondaryIndex.Expression, secondaryIndex.Columns[0], row)
		if err != nil {
			return fmt.Errorf("expression index %s eval: %w", secondaryIndex.Name, err)
		}
		if !ok {
			return nil // NULL result — don't index
		}
		if err := secondaryIndex.Index.Insert(ctx, key, rowID); err != nil {
			return fmt.Errorf("failed to insert key for expression index %s: %w", secondaryIndex.Name, err)
		}
		return nil
	}

	if len(keys) == 0 {
		return fmt.Errorf("no keys provided for secondary index %s", secondaryIndex.Name)
	}

	if len(keys) > 1 {
		// Composite secondary index: all key columns must be non-NULL
		keyValues := make([]any, 0, len(keys))
		for i, key := range keys {
			if !key.Valid {
				return nil // skip if any column is NULL
			}
			castedKey, err := castKeyValue(secondaryIndex.Columns[i], key.Value)
			if err != nil {
				return fmt.Errorf("failed to cast key value for secondary index %s: %w", secondaryIndex.Name, err)
			}
			keyValues = append(keyValues, castedKey)
		}
		ck := NewCompositeKey(secondaryIndex.Columns, keyValues...)
		t.logger.Debug("inserting secondary index key",
			zap.String("index", secondaryIndex.Name),
			zap.Any("key", ck),
		)
		if err := secondaryIndex.Index.Insert(ctx, ck, rowID); err != nil {
			return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
		}
		return nil
	}

	key := keys[0]

	// We only need to insert into the index if the key is not NULL
	if !key.Valid {
		return nil
	}

	castedKey, err := castKeyValue(secondaryIndex.Columns[0], key.Value)
	if err != nil {
		return fmt.Errorf("failed to cast key value for secondary index  %s: %w", secondaryIndex.Name, err)
	}

	t.logger.Debug("inserting secondary index key",
		zap.String("index", secondaryIndex.Name),
		zap.Any("key", castedKey),
	)

	if err := secondaryIndex.Index.Insert(ctx, castedKey, rowID); err != nil {
		return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
	}

	return nil
}

func (t *Table) updateSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, oldKeyParts []OptionalValue, oldRow, row Row) error {
	if secondaryIndex.Method == IndexMethodFullText {
		return t.updateFullTextIndexKeys(ctx, secondaryIndex, oldRow, row)
	}
	if secondaryIndex.Method == IndexMethodInverted {
		return t.updateInvertedIndexKeys(ctx, secondaryIndex, oldRow, row)
	}
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	// Expression index: evaluate expression against old and new rows.
	if secondaryIndex.Expression != nil {
		syntheticCol := secondaryIndex.Columns[0]
		rowID := row.Key

		newInIndex, err := secondaryIndex.rowSatisfiesWhereCond(row)
		if err != nil {
			return fmt.Errorf("partial index %s where check (new row): %w", secondaryIndex.Name, err)
		}
		oldInIndex, err := secondaryIndex.rowSatisfiesWhereCond(oldRow)
		if err != nil {
			return fmt.Errorf("partial index %s where check (old row): %w", secondaryIndex.Name, err)
		}

		if newInIndex {
			newKey, ok, err := evalExprIndexKey(secondaryIndex.Expression, syntheticCol, row)
			if err != nil {
				return fmt.Errorf("expression index %s eval (new row): %w", secondaryIndex.Name, err)
			}
			if ok {
				if err := secondaryIndex.Index.Insert(ctx, newKey, rowID); err != nil {
					return fmt.Errorf("failed to insert key for expression index %s: %w", secondaryIndex.Name, err)
				}
			}
		}
		if oldInIndex {
			oldKey, ok, err := evalExprIndexKey(secondaryIndex.Expression, syntheticCol, oldRow)
			if err != nil {
				return fmt.Errorf("expression index %s eval (old row): %w", secondaryIndex.Name, err)
			}
			if ok {
				if err := secondaryIndex.Index.Delete(ctx, oldKey, rowID); err != nil {
					return fmt.Errorf("failed to delete key for expression index %s: %w", secondaryIndex.Name, err)
				}
			}
		}
		return nil
	}

	if len(oldKeyParts) == 0 {
		return fmt.Errorf("no old keys provided for secondary index %s", secondaryIndex.Name)
	}

	if len(oldKeyParts) > 1 {
		return t.updateCompositeSecondaryIndexKey(ctx, secondaryIndex, oldKeyParts, oldRow, row)
	}

	oldKey := oldKeyParts[0]

	newKey, ok := row.GetValue(secondaryIndex.Columns[0].Name)
	if !ok {
		return nil
	}
	rowID := row.Key

	// Partial index: new row in index only if it satisfies the WHERE predicate.
	newInIndex, err := secondaryIndex.rowSatisfiesWhereCond(row)
	if err != nil {
		return fmt.Errorf("partial index %s where check (new row): %w", secondaryIndex.Name, err)
	}

	// We only need to insert into the index if the key is not NULL and row satisfies WHERE.
	if newKey.Valid && newInIndex {
		castedKey, err := castKeyValue(secondaryIndex.Columns[0], newKey.Value)
		if err != nil {
			return fmt.Errorf("failed to cast secondary index key for %s: %w", secondaryIndex.Name, err)
		}
		// We try to insert new index key first to avoid leaving table in inconsistent state
		// If the new index key is already taken, we return an error without modifying the existing row
		if err := secondaryIndex.Index.Insert(ctx, castedKey, rowID); err != nil {
			return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	// Partial index: only delete old key if old row was in the index and key was non-NULL.
	oldInIndex, err := secondaryIndex.rowSatisfiesWhereCond(oldRow)
	if err != nil {
		return fmt.Errorf("partial index %s where check (old row): %w", secondaryIndex.Name, err)
	}
	if oldInIndex && oldKey.Valid {
		castedOldKey, err := castKeyValue(secondaryIndex.Columns[0], oldKey.Value)
		if err != nil {
			return fmt.Errorf("failed to cast old secondary index value for %s: %w", secondaryIndex.Name, err)
		}
		if err := secondaryIndex.Index.Delete(ctx, castedOldKey, rowID); err != nil {
			return fmt.Errorf("failed to delete key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	return nil
}

func (t *Table) insertFullTextIndexKeys(ctx context.Context, secondaryIndex SecondaryIndex, rowID RowID, row Row) error {
	if secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("table %s has full-text index %s but no inverted index instance", t.Name, secondaryIndex.Name)
	}
	if ok, err := secondaryIndex.rowSatisfiesWhereCond(row); err != nil {
		return fmt.Errorf("partial full-text index %s where check: %w", secondaryIndex.Name, err)
	} else if !ok {
		return nil
	}

	tokens, err := fullTextTokenPositionsForRow(secondaryIndex, row)
	if err != nil {
		return err
	}
	postings := fullTextPostingsByTerm(rowID, tokens)
	batch := newInvertedIndexMutationBatch(secondaryIndex.InvertedIndex.Mode())
	for term, posting := range postings {
		batch.Insert(term, posting)
	}
	if err := batch.Apply(ctx, secondaryIndex.InvertedIndex); err != nil {
		return fmt.Errorf("failed to insert tokens for full-text index %s: %w", secondaryIndex.Name, err)
	}
	return nil
}

func (t *Table) updateFullTextIndexKeys(ctx context.Context, secondaryIndex SecondaryIndex, oldRow, row Row) error {
	if secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("table %s has full-text index %s but no inverted index instance", t.Name, secondaryIndex.Name)
	}
	rowID := row.Key
	oldInIndex, err := secondaryIndex.rowSatisfiesWhereCond(oldRow)
	if err != nil {
		return fmt.Errorf("partial full-text index %s where check (old row): %w", secondaryIndex.Name, err)
	}
	newInIndex, err := secondaryIndex.rowSatisfiesWhereCond(row)
	if err != nil {
		return fmt.Errorf("partial full-text index %s where check (new row): %w", secondaryIndex.Name, err)
	}
	if oldInIndex && !newInIndex {
		return t.deleteFullTextIndexKeys(ctx, secondaryIndex, rowID, oldRow)
	}
	if !oldInIndex && newInIndex {
		return t.insertFullTextIndexKeys(ctx, secondaryIndex, rowID, row)
	}
	if !oldInIndex && !newInIndex {
		return nil
	}

	oldPostings, err := fullTextPostingsByTermForRow(secondaryIndex, rowID, oldRow)
	if err != nil {
		return err
	}
	newPostings, err := fullTextPostingsByTermForRow(secondaryIndex, rowID, row)
	if err != nil {
		return err
	}
	batch := newInvertedIndexMutationBatch(secondaryIndex.InvertedIndex.Mode())
	for term, oldPosting := range oldPostings {
		newPosting, ok := newPostings[term]
		if !ok {
			batch.Delete(term, oldPosting)
			continue
		}
		if !slices.Equal(oldPosting.Positions, newPosting.Positions) {
			batch.Delete(term, oldPosting)
			batch.Insert(term, newPosting)
		}
		delete(newPostings, term)
	}
	for term, posting := range newPostings {
		batch.Insert(term, posting)
	}
	if err := batch.Apply(ctx, secondaryIndex.InvertedIndex); err != nil {
		return fmt.Errorf("failed to update tokens for full-text index %s: %w", secondaryIndex.Name, err)
	}
	return nil
}

func (t *Table) deleteFullTextIndexKeys(ctx context.Context, secondaryIndex SecondaryIndex, rowID RowID, row Row) error {
	if secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("table %s has full-text index %s but no inverted index instance", t.Name, secondaryIndex.Name)
	}
	tokens, err := fullTextTokenPositionsForRow(secondaryIndex, row)
	if err != nil {
		return err
	}
	postings := fullTextPostingsByTerm(rowID, tokens)
	batch := newInvertedIndexMutationBatch(secondaryIndex.InvertedIndex.Mode())
	for term, posting := range postings {
		batch.Delete(term, posting)
	}
	if err := batch.Apply(ctx, secondaryIndex.InvertedIndex); err != nil {
		return fmt.Errorf("failed to delete tokens for full-text index %s: %w", secondaryIndex.Name, err)
	}
	return nil
}

func fullTextPostingsByTermForRow(secondaryIndex SecondaryIndex, rowID RowID, row Row) (map[string]invertedPosting, error) {
	tokens, err := fullTextTokenPositionsForRow(secondaryIndex, row)
	if err != nil {
		return nil, err
	}
	return fullTextPostingsByTerm(rowID, tokens), nil
}

func fullTextPostingsByTerm(rowID RowID, tokens []textSearchTokenPosition) map[string]invertedPosting {
	return fullTextPostingsByTermInto(rowID, tokens, make(map[string]invertedPosting, len(tokens)))
}

func fullTextPostingsByTermInto(
	rowID RowID,
	tokens []textSearchTokenPosition,
	postings map[string]invertedPosting,
) map[string]invertedPosting {
	clear(postings)
	for _, token := range tokens {
		posting := postings[token.Term]
		posting.RowID = rowID
		posting.Positions = append(posting.Positions, token.Position)
		postings[token.Term] = posting
	}
	for term, posting := range postings {
		slices.Sort(posting.Positions)
		posting.Positions = slices.Compact(posting.Positions)
		postings[term] = posting
	}
	return postings
}

func (t *Table) insertInvertedIndexKeys(ctx context.Context, secondaryIndex SecondaryIndex, rowID RowID, row Row) error {
	if secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("table %s has inverted index %s but no inverted index instance", t.Name, secondaryIndex.Name)
	}
	if ok, err := secondaryIndex.rowSatisfiesWhereCond(row); err != nil {
		return fmt.Errorf("partial inverted index %s where check: %w", secondaryIndex.Name, err)
	} else if !ok {
		return nil
	}

	terms, err := jsonInvertedTermsForRow(secondaryIndex, row)
	if err != nil {
		return err
	}
	batch := newInvertedIndexMutationBatch(secondaryIndex.InvertedIndex.Mode())
	for _, term := range terms {
		batch.Insert(term, invertedPosting{RowID: rowID})
	}
	if err := batch.Apply(ctx, secondaryIndex.InvertedIndex); err != nil {
		return fmt.Errorf("failed to insert JSON terms for inverted index %s: %w", secondaryIndex.Name, err)
	}
	return nil
}

func (t *Table) updateInvertedIndexKeys(ctx context.Context, secondaryIndex SecondaryIndex, oldRow, row Row) error {
	if secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("table %s has inverted index %s but no inverted index instance", t.Name, secondaryIndex.Name)
	}
	rowID := row.Key
	oldInIndex, err := secondaryIndex.rowSatisfiesWhereCond(oldRow)
	if err != nil {
		return fmt.Errorf("partial inverted index %s where check (old row): %w", secondaryIndex.Name, err)
	}
	newInIndex, err := secondaryIndex.rowSatisfiesWhereCond(row)
	if err != nil {
		return fmt.Errorf("partial inverted index %s where check (new row): %w", secondaryIndex.Name, err)
	}
	if oldInIndex && !newInIndex {
		return t.deleteInvertedIndexKeys(ctx, secondaryIndex, rowID, oldRow)
	}
	if !oldInIndex && newInIndex {
		return t.insertInvertedIndexKeys(ctx, secondaryIndex, rowID, row)
	}
	if !oldInIndex && !newInIndex {
		return nil
	}

	oldTerms, err := jsonInvertedTermSetForRow(secondaryIndex, oldRow)
	if err != nil {
		return err
	}
	newTerms, err := jsonInvertedTermSetForRow(secondaryIndex, row)
	if err != nil {
		return err
	}
	batch := newInvertedIndexMutationBatch(secondaryIndex.InvertedIndex.Mode())
	for term := range oldTerms {
		if _, ok := newTerms[term]; ok {
			delete(newTerms, term)
			continue
		}
		batch.Delete(term, invertedPosting{RowID: rowID})
	}
	for term := range newTerms {
		batch.Insert(term, invertedPosting{RowID: rowID})
	}
	if err := batch.Apply(ctx, secondaryIndex.InvertedIndex); err != nil {
		return fmt.Errorf("failed to update JSON terms for inverted index %s: %w", secondaryIndex.Name, err)
	}
	return nil
}

func (t *Table) deleteInvertedIndexKeys(ctx context.Context, secondaryIndex SecondaryIndex, rowID RowID, row Row) error {
	if secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("table %s has inverted index %s but no inverted index instance", t.Name, secondaryIndex.Name)
	}
	terms, err := jsonInvertedTermsForRow(secondaryIndex, row)
	if err != nil {
		return err
	}
	batch := newInvertedIndexMutationBatch(secondaryIndex.InvertedIndex.Mode())
	for _, term := range terms {
		batch.Delete(term, invertedPosting{RowID: rowID})
	}
	if err := batch.Apply(ctx, secondaryIndex.InvertedIndex); err != nil {
		return fmt.Errorf("failed to delete JSON terms for inverted index %s: %w", secondaryIndex.Name, err)
	}
	return nil
}

func jsonInvertedTermsForRow(secondaryIndex SecondaryIndex, row Row) ([]string, error) {
	return jsonInvertedTermsForRowInto(secondaryIndex, row, nil)
}

func jsonInvertedTermsForRowInto(secondaryIndex SecondaryIndex, row Row, terms []string) ([]string, error) {
	if len(secondaryIndex.Columns) != 1 {
		return nil, fmt.Errorf("inverted index %s requires exactly one source column", secondaryIndex.Name)
	}
	value, ok := row.GetValue(secondaryIndex.Columns[0].Name)
	if !ok || !value.Valid {
		return nil, nil
	}
	doc, ok := toStringVal(value.Value)
	if !ok {
		return nil, fmt.Errorf("inverted index %s column %q must be JSON text", secondaryIndex.Name, secondaryIndex.Columns[0].Name)
	}
	return jsonInvertedTermsForDocumentInto(doc, terms)
}

func jsonInvertedTermSetForRow(secondaryIndex SecondaryIndex, row Row) (map[string]struct{}, error) {
	terms, err := jsonInvertedTermsForRow(secondaryIndex, row)
	if err != nil {
		return nil, err
	}
	termSet := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		termSet[term] = struct{}{}
	}
	return termSet, nil
}

func fullTextTokensForRow(secondaryIndex SecondaryIndex, row Row) ([]string, error) {
	positions, err := fullTextTokenPositionsForRow(secondaryIndex, row)
	if err != nil {
		return nil, err
	}
	tokens := make([]string, 0, len(positions))
	for _, token := range positions {
		tokens = appendUniqueTextSearchTerms(tokens, token.Term)
	}
	return tokens, nil
}

func fullTextTokenPositionsForRow(secondaryIndex SecondaryIndex, row Row) ([]textSearchTokenPosition, error) {
	positions, _, err := fullTextTokenPositionsForRowInto(secondaryIndex, row, nil, nil)
	return positions, err
}

func fullTextTokenPositionsForRowInto(
	secondaryIndex SecondaryIndex,
	row Row,
	positions []textSearchTokenPosition,
	current []rune,
) ([]textSearchTokenPosition, []rune, error) {
	if len(secondaryIndex.Columns) != 1 {
		return nil, current, fmt.Errorf("full-text index %s requires exactly one source column", secondaryIndex.Name)
	}
	value, ok := row.GetValue(secondaryIndex.Columns[0].Name)
	if !ok || !value.Valid {
		return nil, current, nil
	}
	doc, ok := toStringVal(value.Value)
	if !ok {
		return nil, current, fmt.Errorf("full-text index %s column %q must be text", secondaryIndex.Name, secondaryIndex.Columns[0].Name)
	}
	positions, current = textSearchTokenPositionsInto(doc, positions, current)
	writeIdx := 0
	for readIdx := range positions {
		token := positions[readIdx]
		if len([]byte(token.Term)) > MaxIndexKeySize {
			continue
		}
		positions[writeIdx] = token
		writeIdx++
	}
	return positions[:writeIdx], current, nil
}

func (t *Table) updateCompositeSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, oldKeyParts []OptionalValue, oldRow, row Row) error {
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	// Check if old key should have been in the index (all columns non-NULL, satisfies WHERE).
	// Note: minisql doesn't index NULL values even for secondary indexes.
	oldNullsOK := true
	oldKeyValues := make([]any, 0, len(oldKeyParts))
	for i, key := range oldKeyParts {
		if !key.Valid {
			oldNullsOK = false
			break
		}
		castedKey, err := castKeyValue(secondaryIndex.Columns[i], key.Value)
		if err != nil {
			return fmt.Errorf("failed to cast old secondary index value for %s: %w", secondaryIndex.Name, err)
		}
		oldKeyValues = append(oldKeyValues, castedKey)
	}
	oldWhereOK, err := secondaryIndex.rowSatisfiesWhereCond(oldRow)
	if err != nil {
		return fmt.Errorf("partial index %s where check (old row): %w", secondaryIndex.Name, err)
	}
	oldKeyInIndex := oldNullsOK && oldWhereOK

	// Check if new key should be in the index (all columns non-NULL, satisfies WHERE).
	newNullsOK := true
	newKeyValues := make([]any, 0, len(oldKeyParts))
	for _, col := range secondaryIndex.Columns {
		keyValue, ok := row.GetValue(col.Name)
		if !ok {
			return fmt.Errorf("failed to get value for new secondary index %s", secondaryIndex.Name)
		}
		if !keyValue.Valid {
			newNullsOK = false
			break
		}
		castedKey, err := castKeyValue(col, keyValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast new secondary index value for %s: %w", secondaryIndex.Name, err)
		}
		newKeyValues = append(newKeyValues, castedKey)
	}
	newWhereOK, err := secondaryIndex.rowSatisfiesWhereCond(row)
	if err != nil {
		return fmt.Errorf("partial index %s where check (new row): %w", secondaryIndex.Name, err)
	}
	newKeyInIndex := newNullsOK && newWhereOK

	rowID := row.Key

	// Insert new key if all columns are non-NULL and row satisfies WHERE.
	if newKeyInIndex {
		ck := NewCompositeKey(secondaryIndex.Columns, newKeyValues...)
		if err := secondaryIndex.Index.Insert(ctx, ck, rowID); err != nil {
			return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	// Delete old key if it was in the index.
	if oldKeyInIndex {
		oldCK := NewCompositeKey(secondaryIndex.Columns, oldKeyValues...)
		if err := secondaryIndex.Index.Delete(ctx, oldCK, rowID); err != nil {
			return fmt.Errorf("failed to delete key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	return nil
}
