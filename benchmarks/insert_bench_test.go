//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkInsert_SingleRow measures the cost of inserting one row per
// transaction (the common OLTP write pattern).
func BenchmarkInsert_SingleRow(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			stmt, err := db.Prepare(d.insertRowNoID)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				name := fmt.Sprintf("user-%d", i)
				email := fmt.Sprintf("user%d@example.com", i)
				if _, err := stmt.Exec(name, i%100, email); err != nil {
					b.Fatalf("exec: %v", err)
				}
			}
		})
	}
}

// BenchmarkInsert_Batch measures inserting 100 rows inside a single explicit
// transaction.  This amortises commit overhead across many writes.
func BenchmarkInsert_Batch(b *testing.B) {
	const batchSize = 100
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
				for j := range batchSize {
					name := fmt.Sprintf("user-%d-%d", i, j)
					email := fmt.Sprintf("user%d_%d@example.com", i, j)
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
			b.ReportMetric(float64(batchSize), "rows/op")
		})
	}
}
