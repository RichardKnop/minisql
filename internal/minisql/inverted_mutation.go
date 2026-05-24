package minisql

import (
	"context"
	"fmt"
	"sort"
)

type invertedBatchApplier interface {
	ApplyBatch(context.Context, invertedIndexMutationBatch) error
}

type invertedIndexMutationBatch struct {
	mode    invertedIndexPostingMode
	inserts map[string][]invertedPosting
	deletes map[string][]invertedPosting
}

func newInvertedIndexMutationBatch(mode invertedIndexPostingMode) invertedIndexMutationBatch {
	return invertedIndexMutationBatch{
		mode:    mode,
		inserts: make(map[string][]invertedPosting),
		deletes: make(map[string][]invertedPosting),
	}
}

// Insert records an inserted posting for term.
func (b *invertedIndexMutationBatch) Insert(term string, posting invertedPosting) {
	b.inserts[term] = append(b.inserts[term], posting)
}

// Delete records a deleted posting for term.
func (b *invertedIndexMutationBatch) Delete(term string, posting invertedPosting) {
	b.deletes[term] = append(b.deletes[term], posting)
}

// Empty reports whether the batch has no mutations.
func (b invertedIndexMutationBatch) Empty() bool {
	return len(b.inserts) == 0 && len(b.deletes) == 0
}

// Apply applies the batch to index.
func (b invertedIndexMutationBatch) Apply(ctx context.Context, index invertedIndex) error {
	if b.Empty() {
		return nil
	}
	if applier, ok := index.(invertedBatchApplier); ok {
		return applier.ApplyBatch(ctx, b)
	}
	return applyInvertedIndexMutationBatchLegacy(ctx, index, b)
}

func applyInvertedIndexMutationBatchLegacy(ctx context.Context, index invertedIndex, batch invertedIndexMutationBatch) error {
	deleteTerms := sortedInvertedMutationTerms(batch.deletes)
	for _, term := range deleteTerms {
		for _, posting := range batch.deletes[term] {
			if err := index.Delete(ctx, term, posting); err != nil {
				return fmt.Errorf("delete inverted posting for term %q: %w", term, err)
			}
		}
	}

	insertTerms := sortedInvertedMutationTerms(batch.inserts)
	for _, term := range insertTerms {
		postings := batch.inserts[term]
		if len(postings) == 1 {
			if err := index.Insert(ctx, term, postings[0]); err != nil {
				return fmt.Errorf("insert inverted posting for term %q: %w", term, err)
			}
			continue
		}
		if err := index.InsertMany(ctx, term, postings); err != nil {
			return fmt.Errorf("insert inverted postings for term %q: %w", term, err)
		}
	}
	return nil
}

func sortedInvertedMutationTerms(postings map[string][]invertedPosting) []string {
	terms := make([]string, 0, len(postings))
	for term := range postings {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}
