//go:build bench

package benchmarks

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	_ "github.com/RichardKnop/minisql"
)

// Corpus sizes and vector dimensions used across vector benchmarks.
const (
	vecSmallN  = 1_000
	vecMediumN = 10_000
)

var vecDims = []int{3, 128, 768}

// openVecDB opens a fresh minisql database with a vector table for the given
// number of dimensions. Returns the db and a cleanup function.
func openVecDB(b testing.TB, dims int) (*sql.DB, func()) {
	b.Helper()

	f, err := os.CreateTemp("", "bench_vec_")
	if err != nil {
		b.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		b.Fatalf("close temp file: %v", err)
	}

	db, err := sql.Open("minisql", path)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)

	mustExec(b, db, fmt.Sprintf(`create table "vecs" (
		id  int8 primary key autoincrement,
		v   vector(%d) not null
	)`, dims))

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(path + "-wal")
	}
	return db, cleanup
}

// randVec generates a random unit-ish float32 slice of length dims.
func randVec(r *rand.Rand, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = r.Float32()*2 - 1 // uniform in [-1, 1]
	}
	return v
}

// vecLiteral formats a float32 slice as the SQL literal "[v0, v1, ...]".
func vecLiteral(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%g", f)
	}
	b.WriteByte(']')
	return b.String()
}

// seedVectors inserts n random vectors into the "vecs" table in batches of 500.
func seedVectors(b testing.TB, db *sql.DB, r *rand.Rand, dims, n int) {
	b.Helper()

	const batchSize = 500
	stmt, err := db.Prepare(`insert into vecs (v) values (?)`)
	if err != nil {
		b.Fatalf("prepare insert: %v", err)
	}
	defer stmt.Close()

	for start := 0; start < n; start += batchSize {
		end := min(start+batchSize, n)
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		txStmt := tx.Stmt(stmt)
		for range end - start {
			if _, err := txStmt.Exec(randVec(r, dims)); err != nil {
				_ = tx.Rollback()
				txStmt.Close()
				b.Fatalf("insert vector: %v", err)
			}
		}
		txStmt.Close()
		if err := tx.Commit(); err != nil {
			b.Fatalf("commit batch: %v", err)
		}
	}
}

// createHNSWIndex builds the HNSW index on the "vecs" table.
func createHNSWIndex(b testing.TB, db *sql.DB) {
	b.Helper()
	mustExec(b, db, `CREATE HNSW INDEX "idx_v" ON "vecs" (v);`)
}

// dropHNSWIndex drops the HNSW index on the "vecs" table.
func dropHNSWIndex(b testing.TB, db *sql.DB) {
	b.Helper()
	mustExec(b, db, `DROP INDEX "idx_v";`)
}

// ---- Build benchmarks ----

// BenchmarkHNSW_BuildIndex measures the time to CREATE HNSW INDEX over an
// already-populated table. The seed phase is excluded from the timer.
func BenchmarkHNSW_BuildIndex(b *testing.B) {
	for _, dims := range vecDims {
		for _, n := range []int{vecSmallN, vecMediumN} {
			b.Run(fmt.Sprintf("dims%d/n%d", dims, n), func(b *testing.B) {
				r := rand.New(rand.NewPCG(42, uint64(dims)))
				db, cleanup := openVecDB(b, dims)
				defer cleanup()
				seedVectors(b, db, r, dims, n)
				createHNSWIndex(b, db) // initial build so the index exists to drop

				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					dropHNSWIndex(b, db)
					b.StartTimer()
					createHNSWIndex(b, db)
					b.StopTimer()
				}
				b.ReportMetric(float64(n), "rows/op")
			})
		}
	}
}

// ---- Search benchmarks ----

// BenchmarkHNSW_ANNSearch measures approximate nearest-neighbor top-k search
// routed through the HNSW index for different corpus sizes, dimensions, and k.
func BenchmarkHNSW_ANNSearch(b *testing.B) {
	for _, dims := range vecDims {
		for _, n := range []int{vecSmallN, vecMediumN} {
			for _, k := range []int{1, 10} {
				b.Run(fmt.Sprintf("dims%d/n%d/top%d", dims, n, k), func(b *testing.B) {
					r := rand.New(rand.NewPCG(42, uint64(dims)))
					db, cleanup := openVecDB(b, dims)
					defer cleanup()
					seedVectors(b, db, r, dims, n)
					createHNSWIndex(b, db)

					// Pre-generate a pool of query literals to avoid formatting
					// allocations in the measured loop.
					const queryPoolSize = 256
					queries := make([]string, queryPoolSize)
					for i := range queries {
						queries[i] = vecLiteral(randVec(r, dims))
					}

					b.ResetTimer()
					for i := range b.N {
						q := queries[i%queryPoolSize]
						sql := fmt.Sprintf(
							`SELECT id, VEC_L2(v, '%s') AS dist FROM vecs ORDER BY dist LIMIT %d;`,
							q, k,
						)
						rows, err := db.Query(sql)
						if err != nil {
							b.Fatalf("query: %v", err)
						}
						var id int64
						var dist float64
						for rows.Next() {
							if err := rows.Scan(&id, &dist); err != nil {
								rows.Close()
								b.Fatalf("scan: %v", err)
							}
						}
						if err := rows.Err(); err != nil {
							rows.Close()
							b.Fatalf("rows err: %v", err)
						}
						rows.Close()
					}
				})
			}
		}
	}
}

// BenchmarkHNSW_SeqScan measures the brute-force sequential-scan cost for
// nearest-neighbor search without an HNSW index. Compare with
// BenchmarkHNSW_ANNSearch to quantify the index speedup.
func BenchmarkHNSW_SeqScan(b *testing.B) {
	for _, dims := range vecDims {
		for _, n := range []int{vecSmallN} {
			b.Run(fmt.Sprintf("dims%d/n%d/top1", dims, n), func(b *testing.B) {
				r := rand.New(rand.NewPCG(42, uint64(dims)))
				db, cleanup := openVecDB(b, dims)
				defer cleanup()
				seedVectors(b, db, r, dims, n)
				// No HNSW index — forces a sequential scan.

				const queryPoolSize = 256
				queries := make([]string, queryPoolSize)
				for i := range queries {
					queries[i] = vecLiteral(randVec(r, dims))
				}

				b.ResetTimer()
				for i := range b.N {
					q := queries[i%queryPoolSize]
					sql := fmt.Sprintf(
						`SELECT id, VEC_L2(v, '%s') AS dist FROM vecs ORDER BY dist LIMIT 1;`,
						q,
					)
					rows, err := db.Query(sql)
					if err != nil {
						b.Fatalf("query: %v", err)
					}
					var id int64
					var dist float64
					for rows.Next() {
						if err := rows.Scan(&id, &dist); err != nil {
							rows.Close()
							b.Fatalf("scan: %v", err)
						}
					}
					rows.Close()
				}
			})
		}
	}
}

// ---- Online DML benchmarks ----

// BenchmarkHNSW_Insert_WithIndex measures the per-row INSERT cost when an HNSW
// index exists. The overhead over BenchmarkHNSW_Insert_NoIndex is the graph
// maintenance cost.
func BenchmarkHNSW_Insert_WithIndex(b *testing.B) {
	for _, dims := range vecDims {
		b.Run(fmt.Sprintf("dims%d", dims), func(b *testing.B) {
			r := rand.New(rand.NewPCG(42, uint64(dims)))
			db, cleanup := openVecDB(b, dims)
			defer cleanup()
			seedVectors(b, db, r, dims, vecSmallN)
			createHNSWIndex(b, db)

			stmt, err := db.Prepare(`insert into vecs (v) values (?)`)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for range b.N {
				if _, err := stmt.Exec(randVec(r, dims)); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}

// BenchmarkHNSW_Insert_NoIndex measures the per-row INSERT cost without an
// HNSW index. Use this as the baseline to isolate online maintenance overhead.
func BenchmarkHNSW_Insert_NoIndex(b *testing.B) {
	for _, dims := range vecDims {
		b.Run(fmt.Sprintf("dims%d", dims), func(b *testing.B) {
			r := rand.New(rand.NewPCG(42, uint64(dims)))
			db, cleanup := openVecDB(b, dims)
			defer cleanup()
			seedVectors(b, db, r, dims, vecSmallN)

			stmt, err := db.Prepare(`insert into vecs (v) values (?)`)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for range b.N {
				if _, err := stmt.Exec(randVec(r, dims)); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}
