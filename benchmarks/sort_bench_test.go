//go:build bench

package benchmarks

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/RichardKnop/minisql"
)

const sortBenchN = 10_000

// BenchmarkOrderBy_Sort profiles the ORDER BY sort pipeline with and without
// disk spill, using the same 10 000-row table in both cases.
//
// Two sub-benchmarks are run:
//   - no-spill: sort_mem_limit=0 — rows are sorted entirely in memory (the
//     baseline showing pure sort cost with zero I/O).
//   - spill-64k: sort_mem_limit=65536 — the 64 KB threshold forces the 10 000
//     rows to be flushed across ~8 sorted run files that are then N-way merged.
//     This exercises the full disk-spill pipeline: runWriter, runReader, and
//     externalSortMerge.
//
// Run with -cpuprofile / -memprofile to identify bottlenecks in the spill code:
//
//	go test -tags bench ./benchmarks/ -bench=BenchmarkOrderBy_Sort \
//	  -benchtime=10s -cpuprofile=cpu.out && go tool pprof cpu.out
func BenchmarkOrderBy_Sort(b *testing.B) {
	cases := []struct {
		name         string
		sortMemLimit int
	}{
		{"no-spill", 0},
		{"spill-64k", 65536},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			f, err := os.CreateTemp("", "bench_sort_spill_")
			if err != nil {
				b.Fatalf("create temp file: %v", err)
			}
			path := f.Name()
			f.Close()
			b.Cleanup(func() {
				os.Remove(path)
				os.Remove(path + "-wal")
			})

			dsn := fmt.Sprintf("%s?sort_mem_limit=%d", path, tc.sortMemLimit)
			db, err := sql.Open("minisql", dsn)
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			b.Cleanup(func() { db.Close() })

			if _, err := db.Exec(`create table "sort_bench" (
				id    int8 primary key autoincrement,
				email varchar(255)
			)`); err != nil {
				b.Fatalf("create table: %v", err)
			}

			// Insert in descending email order so a naïve scan always gives
			// wrong results — the sort is never a no-op.
			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			ins, err := tx.Prepare(`insert into "sort_bench" (email) values (?)`)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert: %v", err)
			}
			for i := sortBenchN; i >= 1; i-- {
				if _, err := ins.Exec(fmt.Sprintf("user%06d@example.com", i)); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert row %d: %v", i, err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit seed: %v", err)
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(`select id, email from "sort_bench" order by email asc`)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var (
						id    int64
						email string
					)
					if err := rows.Scan(&id, &email); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n++
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
				b.ReportMetric(float64(n), "rows/op")
			}
		})
	}
}
