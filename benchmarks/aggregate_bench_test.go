//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

const aggregateSeedN = 10_000

// BenchmarkGroupBy_Aggregate measures GROUP BY with COUNT(*) and SUM over the
// seeded bench_rows table (100 distinct age values, ~100 rows per group).
func BenchmarkGroupBy_Aggregate(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, aggregateSeedN)

			var query string
			switch d.name {
			case "minisql":
				query = `select age, count(*), sum(age) from "bench_rows" group by age`
			default:
				query = `SELECT age, count(*), sum(age) FROM bench_rows GROUP BY age`
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var (
						age   int
						cnt   int64
						total int64
					)
					if err := rows.Scan(&age, &cnt, &total); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n += 1
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
				b.ReportMetric(float64(n), "groups/op")
			}
		})
	}
}

// BenchmarkHaving_Filter measures GROUP BY + HAVING selectivity — only groups
// whose count exceeds a threshold are returned.
func BenchmarkHaving_Filter(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, aggregateSeedN)

			// threshold = 90: with 10K rows and 100 age values (0-99), each age
			// appears ~100 times.  All groups exceed 90 so we measure the full
			// pipeline cost, not early termination.  Inline literal avoids the
			// need for HAVING placeholder support (not universally implemented).
			var query string
			switch d.name {
			case "minisql":
				// No alias on count(*): minisql HAVING resolves aggregate columns by
				// their raw expression name (count(*)), not by SELECT alias.
				query = `select age, count(*) from "bench_rows" group by age having count(*) > 90`
			default:
				query = `SELECT age, count(*) AS cnt FROM bench_rows GROUP BY age HAVING count(*) > 90`
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var (
						age int
						cnt int64
					)
					if err := rows.Scan(&age, &cnt); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n += 1
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
				b.ReportMetric(float64(n), "groups/op")
			}
		})
	}
}

// BenchmarkDistinct_HighCardinality measures DISTINCT on the email column
// which is unique per row (maximum cardinality = aggregateSeedN distinct values).
// Exercises the sort + dedup streaming path.
func BenchmarkDistinct_HighCardinality(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			// Seed a dedicated table with unique emails to avoid cross-test pollution.
			var (
				createT string
				insertT string
				query   string
			)
			switch d.name {
			case "minisql":
				createT = `create table "bench_distinct" (id int8 primary key autoincrement, email varchar(255))`
				insertT = `insert into "bench_distinct" (email) values (?)`
				query = `select distinct email from "bench_distinct"`
			default:
				createT = `CREATE TABLE bench_distinct (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT)`
				insertT = `INSERT INTO bench_distinct (email) VALUES (?)`
				query = `SELECT DISTINCT email FROM bench_distinct`
			}

			mustExec(b, db, createT)
			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			ins, err := tx.Prepare(insertT)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert: %v", err)
			}
			for i := range aggregateSeedN {
				if _, err := ins.Exec(fmt.Sprintf("user%06d@example.com", i)); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert %d: %v", i, err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit: %v", err)
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var email string
					if err := rows.Scan(&email); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n += 1
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
