package minisql

import (
	"context"
	"fmt"
	"sort"
)

type invertedBatchApplier interface {
	ApplyBatch(context.Context, invertedIndexMutationBatch) error
}

type invertedRowIDBatchApplier interface {
	ApplyRowIDBatch(context.Context, invertedRowIDMutationBatch) error
}

type invertedIndexMutationBatch struct {
	mode    invertedIndexPostingMode
	inserts map[string][]invertedPosting
	deletes map[string][]invertedPosting
}

type invertedRowIDMutationBatch struct {
	inserts map[string][]RowID
	deletes map[string][]RowID
}

func newInvertedRowIDMutationBatchWithCapacity(insertTerms, deleteTerms int) invertedRowIDMutationBatch {
	return invertedRowIDMutationBatch{
		inserts: make(map[string][]RowID, insertTerms),
		deletes: make(map[string][]RowID, deleteTerms),
	}
}

// Insert records an inserted row ID for term.
func (b *invertedRowIDMutationBatch) Insert(term string, rowID RowID) {
	b.inserts[term] = append(b.inserts[term], rowID)
}

// Delete records a deleted row ID for term.
func (b *invertedRowIDMutationBatch) Delete(term string, rowID RowID) {
	b.deletes[term] = append(b.deletes[term], rowID)
}

func newInvertedIndexMutationBatch(mode invertedIndexPostingMode) invertedIndexMutationBatch {
	return newInvertedIndexMutationBatchWithCapacity(mode, 0, 0)
}

func newInvertedIndexMutationBatchWithCapacity(
	mode invertedIndexPostingMode,
	insertTerms, deleteTerms int,
) invertedIndexMutationBatch {
	return invertedIndexMutationBatch{
		mode:    mode,
		inserts: make(map[string][]invertedPosting, insertTerms),
		deletes: make(map[string][]invertedPosting, deleteTerms),
	}
}

// Insert records an inserted posting for term.
func (b *invertedIndexMutationBatch) Insert(term string, posting invertedPosting) {
	b.inserts[term] = append(b.inserts[term], posting)
}

// InsertMany records multiple inserted postings for term.
func (b *invertedIndexMutationBatch) InsertMany(term string, postings []invertedPosting) {
	if len(postings) == 0 {
		return
	}
	b.inserts[term] = postings
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

// Apply applies a compact row-ID-only batch to index.
func (b invertedRowIDMutationBatch) Apply(ctx context.Context, index invertedIndex) error {
	if len(b.inserts) == 0 && len(b.deletes) == 0 {
		return nil
	}
	if applier, ok := index.(invertedRowIDBatchApplier); ok {
		return applier.ApplyRowIDBatch(ctx, b)
	}

	batch := newInvertedIndexMutationBatchWithCapacity(invertedIndexPostingModeRowIDs, len(b.inserts), len(b.deletes))
	for term, rowIDs := range b.inserts {
		for _, rowID := range rowIDs {
			batch.Insert(term, invertedPosting{RowID: rowID})
		}
	}
	for term, rowIDs := range b.deletes {
		for _, rowID := range rowIDs {
			batch.Delete(term, invertedPosting{RowID: rowID})
		}
	}
	return batch.Apply(ctx, index)
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
