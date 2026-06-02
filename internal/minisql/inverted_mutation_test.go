package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvertedIndexMutationBatch_ApplyUsesBatchApplier(t *testing.T) {
	t.Parallel()

	index := &recordingBatchInvertedIndex{}
	batch := newInvertedIndexMutationBatch(invertedIndexPostingModeRowIDs)
	batch.Insert("status:open", invertedPosting{RowID: 10})
	batch.Delete("status:closed", invertedPosting{RowID: 9})

	require.NoError(t, batch.Apply(context.Background(), index))
	require.Len(t, index.batches, 1)
	assert.Equal(t, []invertedPosting{{RowID: 10}}, index.batches[0].inserts["status:open"])
	assert.Equal(t, []invertedPosting{{RowID: 9}}, index.batches[0].deletes["status:closed"])
	assert.False(t, index.legacyCalled)
}

func TestInvertedIndexMutationBatch_ApplyLegacyInDeterministicOrder(t *testing.T) {
	t.Parallel()

	index := &recordingLegacyInvertedIndex{mode: invertedIndexPostingModeRowIDs}
	batch := newInvertedIndexMutationBatch(invertedIndexPostingModeRowIDs)
	batch.Insert("z", invertedPosting{RowID: 3})
	batch.Insert("a", invertedPosting{RowID: 1})
	batch.Insert("a", invertedPosting{RowID: 2})
	batch.Delete("m", invertedPosting{RowID: 5})
	batch.Delete("b", invertedPosting{RowID: 4})

	require.NoError(t, batch.Apply(context.Background(), index))
	assert.Equal(t, []string{
		"delete:b:4",
		"delete:m:5",
		"insertMany:a:1,2",
		"insert:z:3",
	}, index.calls)
}

func TestInvertedRowIDMutationBatch_ApplyUsesRowIDBatchApplier(t *testing.T) {
	t.Parallel()

	index := &recordingRowIDBatchInvertedIndex{}
	batch := invertedRowIDMutationBatch{
		inserts: map[string][]RowID{"status:open": {10}},
		deletes: map[string][]RowID{"status:closed": {9}},
	}

	require.NoError(t, batch.Apply(context.Background(), index))
	require.Len(t, index.batches, 1)
	assert.Equal(t, []RowID{10}, index.batches[0].inserts["status:open"])
	assert.Equal(t, []RowID{9}, index.batches[0].deletes["status:closed"])
	assert.False(t, index.legacyCalled)
}

func TestInvertedRowIDMutationBatch_ApplyFallsBackToLegacyBatch(t *testing.T) {
	t.Parallel()

	index := &recordingLegacyInvertedIndex{mode: invertedIndexPostingModeRowIDs}
	batch := invertedRowIDMutationBatch{
		inserts: map[string][]RowID{"a": {1, 2}},
		deletes: map[string][]RowID{"b": {4}},
	}

	require.NoError(t, batch.Apply(context.Background(), index))
	assert.Equal(t, []string{
		"delete:b:4",
		"insertMany:a:1,2",
	}, index.calls)
}

func TestInvertedRowIDMutationBatch_LazilyInitializesMaps(t *testing.T) {
	t.Parallel()

	var batch invertedRowIDMutationBatch
	batch.Insert("status:open", 10)
	batch.Delete("status:closed", 9)

	assert.Equal(t, []RowID{10}, batch.inserts["status:open"])
	assert.Equal(t, []RowID{9}, batch.deletes["status:closed"])
}

type recordingBatchInvertedIndex struct {
	recordingLegacyInvertedIndex
	batches []invertedIndexMutationBatch
}

func (r *recordingBatchInvertedIndex) ApplyBatch(_ context.Context, batch invertedIndexMutationBatch) error {
	r.batches = append(r.batches, batch)
	return nil
}

type recordingRowIDBatchInvertedIndex struct {
	recordingLegacyInvertedIndex
	batches []invertedRowIDMutationBatch
}

func (r *recordingRowIDBatchInvertedIndex) ApplyRowIDBatch(_ context.Context, batch invertedRowIDMutationBatch) error {
	r.batches = append(r.batches, batch)
	return nil
}

type recordingLegacyInvertedIndex struct {
	mode         invertedIndexPostingMode
	calls        []string
	legacyCalled bool
}

func (r *recordingLegacyInvertedIndex) GetRootPageIdx() PageIndex {
	return 0
}

func (r *recordingLegacyInvertedIndex) Mode() invertedIndexPostingMode {
	return r.mode
}

func (r *recordingLegacyInvertedIndex) Insert(_ context.Context, term string, posting invertedPosting) error {
	r.legacyCalled = true
	r.calls = append(r.calls, "insert:"+term+":"+rowIDListString([]invertedPosting{posting}))
	return nil
}

func (r *recordingLegacyInvertedIndex) InsertMany(_ context.Context, term string, postings []invertedPosting) error {
	r.legacyCalled = true
	r.calls = append(r.calls, "insertMany:"+term+":"+rowIDListString(postings))
	return nil
}

func (r *recordingLegacyInvertedIndex) Replace(_ context.Context, term string, oldPosting, newPosting invertedPosting) error {
	r.legacyCalled = true
	r.calls = append(r.calls, "replace:"+term+":"+rowIDListString([]invertedPosting{oldPosting, newPosting}))
	return nil
}

func (r *recordingLegacyInvertedIndex) Delete(_ context.Context, term string, posting invertedPosting) error {
	r.legacyCalled = true
	r.calls = append(r.calls, "delete:"+term+":"+rowIDListString([]invertedPosting{posting}))
	return nil
}

func (r *recordingLegacyInvertedIndex) Lookup(_ context.Context, _ string) (invertedPostingIterator, error) {
	return &singleBlockInvertedPostingIterator{}, nil
}

func (r *recordingLegacyInvertedIndex) Stats(_ context.Context, _ string) (invertedPostingStats, error) {
	return invertedPostingStats{}, nil
}

func rowIDListString(postings []invertedPosting) string {
	out := make([]byte, 0, len(postings)*3)
	for i, posting := range postings {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, byte('0'+posting.RowID))
	}
	return string(out)
}
