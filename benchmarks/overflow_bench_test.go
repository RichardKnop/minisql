//go:build bench

package benchmarks

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
)

// overflowSizes lists the blob sizes exercised by overflow benchmarks.
// The names are chosen to make the page-level behaviour obvious:
//
//   - inline  (512 B)  – fits within MaxInlineVarchar, no overflow pages at all
//   - 1pg     (~4 KB)  – spills onto exactly one overflow page
//   - 4pg     (~16 KB) – four overflow pages
//   - 16pg    (~64 KB) – sixteen overflow pages (near the current hard limit)
//
// "inline" acts as a control: if it is slower than expected the overhead is
// in the write/read path itself, not in overflow page management.
var overflowSizes = []struct {
	name string
	size int
}{
	{"inline", 512},
	{"1pg", 4096},
	{"4pg", 16384},
	{"16pg", 65280},
}

// makeBlob returns a deterministic byte slice of the given length.
// Using a non-zero, non-repeating pattern avoids compression skewing results.
func makeBlob(size int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = alphabet[i%len(alphabet)]
	}
	return string(b)
}

func overflowCreateTable(d dbDriver) string {
	if d.name == "sqlite" {
		return `CREATE TABLE IF NOT EXISTS overflow_rows (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			body TEXT NOT NULL
		)`
	}
	return `create table "overflow_rows" (
		id   int8 primary key autoincrement,
		body text not null
	)`
}

func overflowInsertSQL(d dbDriver) string {
	if d.name == "sqlite" {
		return `INSERT INTO overflow_rows (body) VALUES (?)`
	}
	return `insert into "overflow_rows" (body) values (?)`
}

func overflowSelectSQL(d dbDriver) string {
	if d.name == "sqlite" {
		return `SELECT body FROM overflow_rows WHERE id = ?`
	}
	return `select body from "overflow_rows" where id = ?`
}

func overflowSelectAllSQL(d dbDriver) string {
	if d.name == "sqlite" {
		return `SELECT id, body FROM overflow_rows`
	}
	return `select id, body from "overflow_rows"`
}

func overflowUpdateSQL(d dbDriver) string {
	if d.name == "sqlite" {
		return `UPDATE overflow_rows SET body = ? WHERE id = ?`
	}
	return `update "overflow_rows" set body = ? where id = ?`
}

// openOverflowDB opens a temporary database for the given driver and creates
// the overflow_rows table used by all overflow benchmarks.
func openOverflowDB(b *testing.B, d dbDriver) (*sql.DB, func()) {
	b.Helper()

	f, err := os.CreateTemp("", "bench_overflow_"+d.name+"_")
	if err != nil {
		b.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		b.Fatalf("close temp file: %v", err)
	}

	db, err := sql.Open(d.driverName, d.dsn(path))
	if err != nil {
		b.Fatalf("open db (%s): %v", d.name, err)
	}
	db.SetMaxOpenConns(1)

	if d.afterOpen != nil {
		if err := d.afterOpen(db); err != nil {
			b.Fatalf("afterOpen (%s): %v", d.name, err)
		}
	}

	mustExec(b, db, overflowCreateTable(d))

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(path + "-wal")
		_ = os.Remove(path + "-shm")
	}
	return db, cleanup
}

// BenchmarkOverflow_Insert measures the cost of inserting a single row with a
// TEXT body that spans zero or more overflow pages. Each b.N iteration is one
// INSERT + implicit commit (auto-commit mode).
//
// This isolates the write path: GetFreePage calls, OverflowPage.Marshal, and
// the WAL append for each overflow page.
func BenchmarkOverflow_Insert(b *testing.B) {
	for _, sz := range overflowSizes {
		blob := makeBlob(sz.size)
		for _, d := range drivers {
			b.Run(fmt.Sprintf("%s/%s", sz.name, d.name), func(b *testing.B) {
				db, cleanup := openOverflowDB(b, d)
				defer cleanup()

				stmt, err := db.Prepare(overflowInsertSQL(d))
				if err != nil {
					b.Fatalf("prepare: %v", err)
				}
				defer stmt.Close()

				b.SetBytes(int64(sz.size))
				b.ResetTimer()
				for range b.N {
					if _, err := stmt.Exec(blob); err != nil {
						b.Fatalf("exec: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkOverflow_PointRead measures the cost of reading a single large-body
// row by primary key. The table is seeded once before the timer starts; each
// b.N iteration performs one SELECT + overflow page chain traversal.
//
// This isolates the read path: ReadPage calls per overflow page, the
// OverflowPage.Unmarshal zero-copy sub-slice, and the append loop.
func BenchmarkOverflow_PointRead(b *testing.B) {
	const seedRows = 100

	for _, sz := range overflowSizes {
		blob := makeBlob(sz.size)
		for _, d := range drivers {
			b.Run(fmt.Sprintf("%s/%s", sz.name, d.name), func(b *testing.B) {
				db, cleanup := openOverflowDB(b, d)
				defer cleanup()

				// Seed rows once; record the row IDs so lookups are deterministic.
				ids := make([]int64, seedRows)
				ins, err := db.Prepare(overflowInsertSQL(d))
				if err != nil {
					b.Fatalf("prepare insert: %v", err)
				}
				for i := range seedRows {
					res, err := ins.Exec(blob)
					if err != nil {
						b.Fatalf("seed insert %d: %v", i, err)
					}
					ids[i], _ = res.LastInsertId()
				}
				ins.Close()

				sel, err := db.Prepare(overflowSelectSQL(d))
				if err != nil {
					b.Fatalf("prepare select: %v", err)
				}
				defer sel.Close()

				b.SetBytes(int64(sz.size))
				b.ResetTimer()
				for i := range b.N {
					id := ids[i%seedRows]
					var body string
					if err := sel.QueryRow(id).Scan(&body); err != nil {
						b.Fatalf("scan: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkOverflow_FullScan measures the cost of a sequential full-table scan
// over rows with large TEXT bodies. The table is seeded with 50 rows before the
// timer starts; each b.N iteration scans all 50 rows, reading every overflow
// page chain.
//
// This is the worst-case read pattern: the LRU page cache cannot help because
// the working set (50 × numPages overflow pages) likely exceeds the cache window
// for large blobs.
func BenchmarkOverflow_FullScan(b *testing.B) {
	const seedRows = 50

	for _, sz := range overflowSizes {
		blob := makeBlob(sz.size)
		for _, d := range drivers {
			b.Run(fmt.Sprintf("%s/%s", sz.name, d.name), func(b *testing.B) {
				db, cleanup := openOverflowDB(b, d)
				defer cleanup()

				ins, err := db.Prepare(overflowInsertSQL(d))
				if err != nil {
					b.Fatalf("prepare insert: %v", err)
				}
				for i := range seedRows {
					if _, err := ins.Exec(blob); err != nil {
						b.Fatalf("seed insert %d: %v", i, err)
					}
				}
				ins.Close()

				sel, err := db.Prepare(overflowSelectAllSQL(d))
				if err != nil {
					b.Fatalf("prepare select all: %v", err)
				}
				defer sel.Close()

				totalBytes := int64(seedRows) * int64(sz.size)
				b.SetBytes(totalBytes)
				b.ResetTimer()
				for range b.N {
					rows, err := sel.Query()
					if err != nil {
						b.Fatalf("query: %v", err)
					}
					var (
						id   int64
						body string
					)
					for rows.Next() {
						if err := rows.Scan(&id, &body); err != nil {
							rows.Close()
							b.Fatalf("scan: %v", err)
						}
					}
					rows.Close()
				}
				b.ReportMetric(float64(seedRows), "rows/op")
			})
		}
	}
}

// BenchmarkOverflow_Update measures the cost of replacing a large TEXT body
// in-place. Each b.N iteration runs one UPDATE that writes a new overflow page
// chain and (in MiniSQL's copy-on-write WAL model) also writes the old pages to
// the WAL as modified.
//
// This benchmark surfaces the total write amplification for blob updates.
func BenchmarkOverflow_Update(b *testing.B) {
	for _, sz := range overflowSizes {
		blob := makeBlob(sz.size)
		newBlob := strings.ToUpper(makeBlob(sz.size)) // different content, same size

		for _, d := range drivers {
			b.Run(fmt.Sprintf("%s/%s", sz.name, d.name), func(b *testing.B) {
				db, cleanup := openOverflowDB(b, d)
				defer cleanup()

				// Insert one row to update repeatedly.
				res, err := db.Exec(overflowInsertSQL(d), blob)
				if err != nil {
					b.Fatalf("seed insert: %v", err)
				}
				rowID, _ := res.LastInsertId()

				upd, err := db.Prepare(overflowUpdateSQL(d))
				if err != nil {
					b.Fatalf("prepare update: %v", err)
				}
				defer upd.Close()

				b.SetBytes(int64(sz.size))
				b.ResetTimer()
				for i := range b.N {
					// Alternate between blob and newBlob to avoid any
					// short-circuit optimisation that detects identical content.
					payload := blob
					if i%2 == 1 {
						payload = newBlob
					}
					if _, err := upd.Exec(payload, rowID); err != nil {
						b.Fatalf("exec update: %v", err)
					}
				}
			})
		}
	}
}
