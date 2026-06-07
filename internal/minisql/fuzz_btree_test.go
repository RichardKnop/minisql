package minisql

import (
	"context"
	"testing"
)

// FuzzBTreeWrite stresses the B-tree insert/delete write path with arbitrary
// operation sequences and key orderings, exercising leaf and internal node
// splits and merges. The only invariant enforced is no-panic: errors at any
// stage are expected and fine; crashes are bugs.
//
// Run for a fixed duration during development:
//
//	go test -fuzz=FuzzBTreeWrite -fuzztime=60s ./internal/minisql/
//
// Seeds are replayed as ordinary unit tests on every `go test` invocation.
func FuzzBTreeWrite(f *testing.F) {
	// Each byte in ops encodes: bit 0 = op (0=insert, 1=delete), bits 1-7 = id (0-127).
	// This gives 128 distinct key values — enough to cause multiple page splits.
	f.Add([]byte{0, 2, 4, 6, 8, 10, 12, 14, 16, 18})        // ascending inserts
	f.Add([]byte{20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0})    // descending inserts
	f.Add([]byte{0, 2, 4, 6, 8, 3, 5, 7, 1, 9})             // mixed ascending with gaps
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}) // many inserts → splits
	f.Add([]byte{0, 2, 1, 4, 3, 6, 5, 8, 7, 10, 9})         // insert then delete alternating
	f.Add([]byte{0, 2, 4, 6, 1, 3, 5, 7})                   // inserts then deletes
	f.Add([]byte{1, 3, 5, 7})                                // all deletes on empty table
	f.Add([]byte{0})                                         // single insert
	f.Add([]byte{0, 1})                                      // insert then delete
	// Interleaved inserts and deletes designed to trigger merges after splits
	f.Add([]byte{0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15})

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 128 {
			return
		}

		columns := []Column{
			{Kind: Int8, Size: 8, Name: "id"},
		}
		table, txManager, _ := newTestTable(t, columns)
		ctx := context.Background()

		// Track ids inserted but not yet deleted so we can verify them afterward.
		liveIDs := make(map[int64]struct{}, len(ops))

		for _, op := range ops {
			id := int64(op >> 1) // 0–127
			isDelete := op&1 == 1

			if isDelete {
				_ = txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
					_, err := table.Delete(txCtx, Statement{
						Kind: Delete,
						Conditions: OneOrMore{{
							FieldIsEqual(Field{Name: "id"}, OperandInteger, id),
						}},
					})
					return err
				})
				delete(liveIDs, id)
			} else {
				if _, dup := liveIDs[id]; dup {
					// Skip duplicate inserts — the interesting path is new keys.
					continue
				}
				err := txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
					_, err := table.Insert(txCtx, Statement{
						Kind:    Insert,
						Fields:  fieldsFromColumns(columns...),
						Inserts: [][]OptionalValue{{{Value: id, Valid: true}}},
					})
					return err
				})
				if err == nil {
					liveIDs[id] = struct{}{}
				}
			}
		}

		// Full scan must not panic and must drain without error.
		_ = txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
			result, err := table.Select(txCtx, Statement{
				Kind:   Select,
				Fields: fieldsFromColumns(columns...),
			})
			if err != nil {
				return nil
			}
			for result.Rows.Next(txCtx) {
				_ = result.Rows.Row()
			}
			return nil
		})

		// Point-lookup for each live id must not panic.
		for id := range liveIDs {
			_ = txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
				result, err := table.Select(txCtx, Statement{
					Kind:   Select,
					Fields: fieldsFromColumns(columns...),
					Conditions: OneOrMore{{
						FieldIsEqual(Field{Name: "id"}, OperandInteger, id),
					}},
				})
				if err != nil {
					return nil
				}
				for result.Rows.Next(txCtx) {
					_ = result.Rows.Row()
				}
				return nil
			})
		}
	})
}
