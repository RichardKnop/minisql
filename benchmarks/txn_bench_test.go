//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkTxn_NInserts measures the cost of committing a transaction that
// contains exactly txnSize INSERT statements.  This isolates commit + WAL-flush
// overhead from the raw insert throughput measured by BenchmarkInsert_Batch.
func BenchmarkTxn_NInserts(b *testing.B) {
	const txnSize = 50
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			b.ResetTimer()
			for i := range b.N {
				tx, err := db.Begin()
				if err != nil {
					b.Fatalf("begin: %v", err)
				}
				stmt, err := tx.Prepare(d.insertRowNoID)
				if err != nil {
					_ = tx.Rollback()
					b.Fatalf("prepare: %v", err)
				}
				for j := range txnSize {
					name := fmt.Sprintf("txn-%d-%d", i, j)
					email := fmt.Sprintf("txn%d_%d@example.com", i, j)
					if _, err := stmt.Exec(name, j%100, email); err != nil {
						stmt.Close()
						_ = tx.Rollback()
						b.Fatalf("exec: %v", err)
					}
				}
				stmt.Close()
				if err := tx.Commit(); err != nil {
					b.Fatalf("commit: %v", err)
				}
			}
			b.ReportMetric(float64(txnSize), "rows/op")
		})
	}
}
