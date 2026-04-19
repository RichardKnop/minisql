//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkDelete_ByPK measures deleting a single row by primary key.
// Because MiniSQL does not implement LastInsertId, we use a fixed seed table
// and re-insert the deleted row each iteration to keep the row count stable.
func BenchmarkDelete_ByPK(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var (
				delQuery string
				insQuery string
			)
			switch d.name {
			case "minisql":
				delQuery = `delete from "bench_rows" where id = ?`
				insQuery = `insert into "bench_rows" (name, age, email) values (?, ?, ?)`
			default:
				delQuery = `DELETE FROM bench_rows WHERE id = ?`
				insQuery = `INSERT INTO bench_rows (name, age, email) VALUES (?, ?, ?)`
			}

			delStmt, err := db.Prepare(delQuery)
			if err != nil {
				b.Fatalf("prepare delete: %v", err)
			}
			defer delStmt.Close()

			insStmt, err := db.Prepare(insQuery)
			if err != nil {
				b.Fatalf("prepare insert: %v", err)
			}
			defer insStmt.Close()

			b.ResetTimer()
			for i := range b.N {
				// Cycle through IDs 1..seedN to always hit an existing row.
				id := (i % seedN) + 1
				if _, err := delStmt.Exec(id); err != nil {
					b.Fatalf("delete: %v", err)
				}
				// Re-insert to keep the table full for the next cycle.
				name := fmt.Sprintf("user-%06d", i)
				email := fmt.Sprintf("user%06d@example.com", i)
				if _, err := insStmt.Exec(name, i%100, email); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}
