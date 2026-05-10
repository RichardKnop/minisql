//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

const seedN = 10_000

// BenchmarkSelect_PointScan measures a single-row lookup by primary key.
func BenchmarkSelect_PointScan(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var query string
			switch d.name {
			case "minisql":
				query = `select id, name, age, email from "bench_rows" where id = ?`
			default:
				query = `SELECT id, name, age, email FROM bench_rows WHERE id = ?`
			}

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				id := (i % seedN) + 1
				rows, err := stmt.Query(id)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				for rows.Next() {
					var (
						rowID int64
						name  string
						age   int
						email string
					)
					if err := rows.Scan(&rowID, &name, &age, &email); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
			}
		})
	}
}

// BenchmarkSelect_Limit measures a sequential scan with LIMIT — exercises the
// early-termination streaming path that stops scanning once the limit is reached.
func BenchmarkSelect_Limit(b *testing.B) {
	const limit = 10
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var query string
			switch d.name {
			case "minisql":
				query = `select id, name, age, email from "bench_rows" limit 10`
			default:
				query = `SELECT id, name, age, email FROM bench_rows LIMIT 10`
			}

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for range b.N {
				rows, err := stmt.Query()
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var (
						rowID int64
						name  string
						age   int
						email string
					)
					if err := rows.Scan(&rowID, &name, &age, &email); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n += 1
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
				if n != limit {
					b.Fatalf("expected %d rows, got %d", limit, n)
				}
			}
		})
	}
}

// BenchmarkSelect_FullScan measures a sequential full-table scan with no WHERE
// clause.
func BenchmarkSelect_FullScan(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var query string
			switch d.name {
			case "minisql":
				query = `select id, name, age, email from "bench_rows"`
			default:
				query = `SELECT id, name, age, email FROM bench_rows`
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
						rowID int64
						name  string
						age   int
						email string
					)
					if err := rows.Scan(&rowID, &name, &age, &email); err != nil {
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

// BenchmarkSelect_CountStar measures COUNT(*) aggregation over the full table.
func BenchmarkSelect_CountStar(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var query string
			switch d.name {
			case "minisql":
				query = `select count(*) from "bench_rows"`
			default:
				query = `SELECT count(*) FROM bench_rows`
			}

			b.ResetTimer()
			for range b.N {
				var count int64
				if err := db.QueryRow(query).Scan(&count); err != nil {
					b.Fatalf("scan: %v", err)
				}
			}
		})
	}
}

// BenchmarkSelect_IndexRangeScan measures a range query on the age column with
// a secondary index present — exercises the index range scan path.
func BenchmarkSelect_IndexRangeScan(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			// Create secondary index on age after seeding so the planner can use it.
			var createIdx string
			switch d.name {
			case "minisql":
				createIdx = `create index "idx_bench_rows_age" on "bench_rows" (age)`
			default:
				createIdx = `CREATE INDEX IF NOT EXISTS idx_bench_rows_age ON bench_rows (age)`
			}
			mustExec(b, db, createIdx)

			var query string
			switch d.name {
			case "minisql":
				query = `select id, name, age from "bench_rows" where age >= ? and age <= ?`
			default:
				query = `SELECT id, name, age FROM bench_rows WHERE age >= ? AND age <= ?`
			}

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				lo := i % 50
				hi := lo + 10
				rows, err := stmt.Query(lo, hi)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				for rows.Next() {
					var id int64
					var name string
					var age int
					if err := rows.Scan(&id, &name, &age); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
			}
		})
	}
}

// BenchmarkSelect_SecondaryIndex_LowSelectivity measures an equality point scan
// on a secondary non-unique index where the indexed column has very few distinct
// values (~5 000 rows per key for seedN = 10 000).  At this scale each key's row
// IDs span multiple overflow pages, so the benchmark exercises the VisitRowIDs
// lazy-iterator path and reports all matching rows.
func BenchmarkSelect_SecondaryIndex_LowSelectivity(b *testing.B) {
	const statuses = 2 // 'active' / 'inactive' → ~seedN/2 rows per value
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			var (
				createStatus string
				createIdx    string
				insertStatus string
				query        string
			)
			switch d.name {
			case "minisql":
				createStatus = `create table "bench_status" (
					id     int8 primary key autoincrement,
					name   varchar(255),
					status varchar(16)
				)`
				createIdx = `create index "idx_bench_status" on "bench_status" (status)`
				insertStatus = `insert into "bench_status" (name, status) values (?, ?)`
				query = `select id, name from "bench_status" where status = ?`
			default:
				createStatus = `CREATE TABLE IF NOT EXISTS bench_status (
					id     INTEGER PRIMARY KEY AUTOINCREMENT,
					name   TEXT,
					status TEXT
				)`
				createIdx = `CREATE INDEX IF NOT EXISTS idx_bench_status ON bench_status (status)`
				insertStatus = `INSERT INTO bench_status (name, status) VALUES (?, ?)`
				query = `SELECT id, name FROM bench_status WHERE status = ?`
			}

			mustExec(b, db, createStatus)

			// Seed rows; alternate status values so each has ~seedN/statuses matches.
			statusValues := []string{"active", "inactive"}
			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			ins, err := tx.Prepare(insertStatus)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert: %v", err)
			}
			for i := range seedN {
				status := statusValues[i%statuses]
				name := fmt.Sprintf("user-%06d", i)
				if _, err := ins.Exec(name, status); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert row %d: %v", i, err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit seed: %v", err)
			}

			mustExec(b, db, createIdx)

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for range b.N {
				rows, err := stmt.Query("active")
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var id int64
					var name string
					if err := rows.Scan(&id, &name); err != nil {
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

// BenchmarkSelect_SecondaryIndex_LowSelectivityLimit measures the same
// low-selectivity secondary index scan as above but with LIMIT 10.  This
// exercises early termination: VisitRowIDs stops reading overflow pages as
// soon as the limit is satisfied rather than loading all row IDs up front.
func BenchmarkSelect_SecondaryIndex_LowSelectivityLimit(b *testing.B) {
	const (
		statuses = 2
		limit    = 10
	)
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			var (
				createStatus string
				createIdx    string
				insertStatus string
				query        string
			)
			switch d.name {
			case "minisql":
				createStatus = `create table "bench_status" (
					id     int8 primary key autoincrement,
					name   varchar(255),
					status varchar(16)
				)`
				createIdx = `create index "idx_bench_status" on "bench_status" (status)`
				insertStatus = `insert into "bench_status" (name, status) values (?, ?)`
				query = `select id, name from "bench_status" where status = ? limit 10`
			default:
				createStatus = `CREATE TABLE IF NOT EXISTS bench_status (
					id     INTEGER PRIMARY KEY AUTOINCREMENT,
					name   TEXT,
					status TEXT
				)`
				createIdx = `CREATE INDEX IF NOT EXISTS idx_bench_status ON bench_status (status)`
				insertStatus = `INSERT INTO bench_status (name, status) VALUES (?, ?)`
				query = `SELECT id, name FROM bench_status WHERE status = ? LIMIT 10`
			}

			mustExec(b, db, createStatus)

			statusValues := []string{"active", "inactive"}
			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			ins, err := tx.Prepare(insertStatus)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert: %v", err)
			}
			for i := range seedN {
				status := statusValues[i%statuses]
				name := fmt.Sprintf("user-%06d", i)
				if _, err := ins.Exec(name, status); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert row %d: %v", i, err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit seed: %v", err)
			}

			mustExec(b, db, createIdx)

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for range b.N {
				rows, err := stmt.Query("active")
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var id int64
					var name string
					if err := rows.Scan(&id, &name); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n += 1
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
				if n != limit {
					b.Fatalf("expected %d rows, got %d", limit, n)
				}
			}
		})
	}
}

// BenchmarkSelect_RangeScan measures a range query on the age column (no
// secondary index — exercises a full-table scan with a WHERE filter).
func BenchmarkSelect_RangeScan(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var query string
			switch d.name {
			case "minisql":
				query = `select id, name, age from "bench_rows" where age >= ? and age <= ?`
			default:
				query = `SELECT id, name, age FROM bench_rows WHERE age >= ? AND age <= ?`
			}

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				lo := i % 50
				hi := lo + 10
				rows, err := stmt.Query(lo, hi)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				for rows.Next() {
					var id int64
					var name string
					var age int
					if err := rows.Scan(&id, &name, &age); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
			}
		})
	}
}
