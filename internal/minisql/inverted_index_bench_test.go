package minisql

import (
	"context"
	"fmt"
	"os"
	"testing"

	"go.uber.org/zap"
)

func BenchmarkDedicatedInvertedIndex_PositionalPostingTreeMutation(b *testing.B) {
	benchmarks := []struct {
		name string
		run  func(context.Context, *dedicatedInvertedIndex, RowID, []uint32) error
	}{
		{
			name: "replace_hot",
			run: func(ctx context.Context, index *dedicatedInvertedIndex, rowID RowID, positions []uint32) error {
				next := []uint32{positions[0] + 1}
				if err := index.Replace(ctx, "common", invertedPosting{RowID: rowID, Positions: positions}, invertedPosting{RowID: rowID, Positions: next}); err != nil {
					return err
				}
				positions[0] = next[0]
				return nil
			},
		},
		{
			name: "delete_insert_hot",
			run: func(ctx context.Context, index *dedicatedInvertedIndex, rowID RowID, positions []uint32) error {
				if err := index.Delete(ctx, "common", invertedPosting{RowID: rowID, Positions: positions}); err != nil {
					return err
				}
				next := []uint32{positions[0] + 1}
				if err := index.Insert(ctx, "common", invertedPosting{RowID: rowID, Positions: next}); err != nil {
					return err
				}
				positions[0] = next[0]
				return nil
			},
		},
		{
			name: "delete_rare_insert_rare",
			run: func(ctx context.Context, index *dedicatedInvertedIndex, rowID RowID, positions []uint32) error {
				oldTerm := fmt.Sprintf("rare-%04d-%d", rowID, positions[0])
				newTerm := fmt.Sprintf("rare-%04d-%d", rowID, positions[0]+1)
				if err := index.Delete(ctx, oldTerm, invertedPosting{RowID: rowID, Positions: positions}); err != nil {
					return err
				}
				next := []uint32{positions[0] + 1}
				if err := index.Insert(ctx, newTerm, invertedPosting{RowID: rowID, Positions: next}); err != nil {
					return err
				}
				positions[0] = next[0]
				return nil
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			index, txManager := newBenchmarkDedicatedInvertedIndex(b, "idx_body", invertedIndexPostingModePositions)
			ctx := context.Background()
			positionsByRow := make(map[RowID][]uint32, 1000)
			requireNoErrorB(b, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
				for rowID := RowID(1); rowID <= 1000; rowID++ {
					positions := []uint32{1}
					positionsByRow[rowID] = positions
					if err := index.Insert(ctx, "common", invertedPosting{RowID: rowID, Positions: positions}); err != nil {
						return err
					}
					if err := index.Insert(ctx, fmt.Sprintf("rare-%04d-1", rowID), invertedPosting{RowID: rowID, Positions: positions}); err != nil {
						return err
					}
				}
				return nil
			}))

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				rowID := RowID(i%1000 + 1)
				positions := positionsByRow[rowID]
				requireNoErrorB(b, txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
					return bm.run(ctx, index, rowID, positions)
				}))
			}
		})
	}
}

func requireNoErrorB(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}

func newBenchmarkDedicatedInvertedIndex(b *testing.B, name string, mode invertedIndexPostingMode) (*dedicatedInvertedIndex, *TransactionManager) {
	b.Helper()

	tempFile, err := os.CreateTemp("", testDBName)
	requireNoErrorB(b, err)
	b.Cleanup(func() { _ = os.Remove(tempFile.Name()) })

	pager, err := NewPager(tempFile, PageSize, 1000)
	requireNoErrorB(b, err)
	basePager := pager.ForInvertedIndex()
	txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(basePager), pager, nil)
	txPager := NewTransactionalPager(basePager, txManager, "test_table", name)
	index, err := NewDedicatedInvertedIndex(name, mode, txPager, 0)
	requireNoErrorB(b, err)
	requireNoErrorB(b, txManager.ExecuteInTransaction(context.Background(), index.InitRootPage))
	return index, txManager
}
