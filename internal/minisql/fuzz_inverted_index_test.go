package minisql

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

// FuzzInvertedIndex verifies that the log-structured inverted index never panics
// on arbitrary term strings and row IDs.  The fuzzer exercises Insert, Lookup,
// and ForEachRowID — the three paths most likely to panic on malformed input.
// Delete and batch operations are included to stress the merge path.
//
// The only invariant enforced is no-panic: errors are expected and fine.
//
// Run for a fixed duration during development:
//
//	go test -fuzz=FuzzInvertedIndex -fuzztime=60s ./internal/minisql/
//
// Seeds are replayed as ordinary unit tests on every `go test` invocation.
func FuzzInvertedIndex(f *testing.F) {
	// Seeds: (term, rowID)
	// Cover common full-text and JSON inverted index term shapes.
	seeds := [][2]any{
		{"hello", int64(1)},
		{"world", int64(2)},
		{"", int64(0)},
		{"a", int64(1)},
		// JSON inverted index key shapes
		{`kv:type:s:"click"`, int64(7)},
		{`kv:active:b:true`, int64(3)},
		{`kv:count:i:42`, int64(5)},
		// Very long term
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", int64(99)},
		// Special characters
		{"\x00\x01\x02", int64(10)},
		{"term with spaces", int64(11)},
		{"term\twith\ttabs", int64(12)},
		{"term\nwith\nnewlines", int64(13)},
		// Unicode
		{"日本語テスト", int64(20)},
		{"émoji 🚀", int64(21)},
		// Boundary row IDs
		{"boundary", int64(-1)},
		{"boundary", int64(0)},
	}
	for _, seed := range seeds {
		f.Add(seed[0].(string), seed[1].(int64))
	}

	f.Fuzz(func(t *testing.T, term string, rowID int64) {
		ctx := context.Background()
		pager, tempFile := initTest(t)
		basePager := pager.ForInvertedIndex()
		txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(basePager), pager, nil)
		txPager := NewTransactionalPager(basePager, txManager, "fuzz_table", "fuzz_idx")

		var metaRoot PageIndex
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			metaPage, err := txPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			basePage, err := txPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			metaRoot = metaPage.Index
			index, err := NewLogStructuredInvertedIndex(
				ctx, "fuzz_idx", invertedIndexPostingModeRowIDs, txPager, metaPage.Index, basePage.Index,
			)
			if err != nil {
				return err
			}

			// Insert the fuzz term.
			_ = index.Insert(ctx, term, invertedPosting{RowID: RowID(rowID)})

			// Insert a second term to stress the merge path.
			_ = index.Insert(ctx, term+"_2", invertedPosting{RowID: RowID(rowID + 1)})

			// Delete must not panic even if the term was never inserted.
			_ = index.Delete(ctx, term, invertedPosting{RowID: RowID(rowID)})

			return nil
		})
		if err != nil {
			return
		}

		// Lookup and iterate must not panic on any term.
		_ = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			opened, err := OpenInvertedIndex(ctx, "fuzz_idx", invertedIndexPostingModeRowIDs, txPager, metaRoot)
			if err != nil {
				return nil
			}

			iter, err := opened.Lookup(ctx, term)
			if err != nil {
				return nil
			}
			// Drain the iterator.
			for {
				_, ok, err := iter.NextBlock(ctx)
				if err != nil || !ok {
					break
				}
			}

			// ForEachRowID must not panic.
			if scanner, ok := opened.(invertedRowIDScanner); ok {
				_ = scanner.ForEachRowID(ctx, term, func(_ RowID) error {
					return nil
				})
			}

			return nil
		})
	})
}

// FuzzInvertedIndexBatch stresses the ApplyBatch path with sequences of
// inserts and deletes for a small set of terms, exercising segment merges.
func FuzzInvertedIndexBatch(f *testing.F) {
	// Seed: two terms and an operation byte (0=insert, 1=delete)
	f.Add("foo", "bar", []byte{0, 0, 1, 0, 1, 1})
	f.Add("hello world", "go fuzz", []byte{0, 1, 0, 0, 1})
	f.Add("", "x", []byte{0})
	f.Add("a", "b", []byte{0, 0, 0, 1, 1, 1, 0, 0})

	f.Fuzz(func(t *testing.T, term1, term2 string, ops []byte) {
		if len(ops) == 0 || len(ops) > 64 {
			return
		}

		ctx := context.Background()
		pager, tempFile := initTest(t)
		basePager := pager.ForInvertedIndex()
		txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(basePager), pager, nil)
		txPager := NewTransactionalPager(basePager, txManager, "fuzz_table", "fuzz_batch_idx")

		terms := []string{term1, term2}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			metaPage, err := txPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			basePage, err := txPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			index, err := NewLogStructuredInvertedIndex(
				ctx, "fuzz_batch_idx", invertedIndexPostingModeRowIDs, txPager, metaPage.Index, basePage.Index,
			)
			if err != nil {
				return err
			}

			// Apply a batch derived from ops bytes: each byte picks a term (bit 1)
			// and an operation (bit 0: insert vs delete).
			batch := newInvertedIndexMutationBatch(invertedIndexPostingModeRowIDs)
			for i, op := range ops {
				term := terms[int(op>>1)%len(terms)]
				rowID := RowID(i + 1)
				if op&1 == 0 {
					batch.Insert(term, invertedPosting{RowID: rowID})
				} else {
					batch.Delete(term, invertedPosting{RowID: rowID})
				}
			}
			_ = index.ApplyBatch(ctx, batch)
			return nil
		})
		_ = err
	})
}
