//go:build bench

package benchmarks

import (
	"testing"
)

const vacuumSeedN = 1_000

// BenchmarkVacuum_Small measures VACUUM performance on a small (~1K row) table
// with approximately 50% fragmentation.  Each iteration recreates the
// fragmentation so the measurement reflects an actual compaction pass.
func BenchmarkVacuum_Small(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			var (
				truncateT  string
				deletHalfT string
			)
			switch d.name {
			case "minisql":
				// minisql requires a WHERE clause for DELETE; id > 0 matches all rows
				// since autoincrement IDs start at 1.
				truncateT  = `delete from "bench_rows" where id > 0`
				deletHalfT = `delete from "bench_rows" where age < 50`
			default:
				truncateT  = `DELETE FROM bench_rows WHERE id > 0`
				deletHalfT = `DELETE FROM bench_rows WHERE age < 50`
			}

			for range b.N {
				b.StopTimer()
				// Truncate, re-seed, and delete half to create fragmentation.
				mustExec(b, db, truncateT)
				seedRows(b, db, d, vacuumSeedN)
				mustExec(b, db, deletHalfT)
				b.StartTimer()

				mustExec(b, db, "VACUUM")
			}
		})
	}
}

// BenchmarkWAL_Checkpoint measures the time to flush unflushed WAL frames back
// into the main database file.  Each iteration seeds 100 rows (generating WAL
// frames) before triggering the checkpoint, so the measurement reflects a
// non-trivial checkpoint rather than a no-op.
func BenchmarkWAL_Checkpoint(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			for range b.N {
				b.StopTimer()
				seedRows(b, db, d, 100)
				b.StartTimer()

				rows, err := db.Query("PRAGMA wal_checkpoint")
				if err != nil {
					b.Fatalf("wal_checkpoint: %v", err)
				}
				for rows.Next() {
				}
				if err := rows.Close(); err != nil {
					b.Fatalf("rows close: %v", err)
				}
			}
		})
	}
}

// BenchmarkExplain measures the overhead of EXPLAIN — plan derivation without
// executing the query.  A correctly-implemented EXPLAIN should add negligible
// latency beyond a normal plan call.
func BenchmarkExplain(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			// Create an index so the planner has a real decision to make.
			var (
				createIdx string
				query     string
			)
			switch d.name {
			case "minisql":
				createIdx = `create index "idx_bench_explain_age" on "bench_rows" (age)`
				query     = `explain select id, name from "bench_rows" where age = ?`
			default:
				createIdx = `CREATE INDEX IF NOT EXISTS idx_bench_explain_age ON bench_rows (age)`
				query     = `EXPLAIN QUERY PLAN SELECT id, name FROM bench_rows WHERE age = ?`
			}
			mustExec(b, db, createIdx)

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				rows, err := stmt.Query(i % 100)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				for rows.Next() {
				}
				if err := rows.Close(); err != nil {
					b.Fatalf("rows close: %v", err)
				}
			}
		})
	}
}
