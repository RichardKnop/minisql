# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `go test -tags bench ./benchmarks/ -run='^$' -bench='.' -benchmem -count=3 -timeout=600s` (median of 3 runs). All rows refreshed 2026-06-17.  
**GOMAXPROCS:** 10

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime. Single-iteration benchmarks (HNSW build at large N) carry higher variance than multi-iteration ones.

2026-06-22: INSERT arg-buffer reuse — two per-exec heap allocations eliminated for prepared single-row INSERT statements by pre-allocating reuse buffers on `Conn`. (1) `insertArgsBuf []any`: replaces the `make([]any, N)` in `toInternalArgs`; the buffer is grown-or-reused before each exec and filled in place. (2) `insertsOuterBuf [][]OptionalValue`: replaces the `make([][]OptionalValue, N)` in the `BindArguments` delayed-bind fast path; a new `BindArgumentsReusing(args []any, outerBuf [][]OptionalValue)` engine method copies the template row pointers into the reuse buffer and stores it as `stmt.Inserts` without allocating. Both buffers are safe to reuse because `database/sql` guarantees each `Conn` is used by at most one goroutine at a time, and `prepareInsert` fully consumes both buffers before `ExecContext` returns. Multi-row prepared INSERT (`INSERT … VALUES (…),(…)`) also benefits from `insertArgsBuf` reuse. Net effect on batch/prepared-batch INSERT (100 rows/tx): allocs **1,954 → 1,755 (−10.2%)**, memory **115 KiB → 108 KiB (−6%)**.

2026-06-19: INSERT autoincrement cache + single-row `rowValues` elimination — two targeted allocator removals. (1) `lastAutoincrementKey atomic.Int64` on `Table` (initialised to −1) caches the last written PK value; `insertAutoincrementedPrimaryKey` uses the cache directly and falls back to `SeekLastKey` only on cold start, eliminating the per-insert B-tree traversal and its `int64→any` boxing. Explicit PK inserts update the cache via a CAS high-water-mark loop so gaps from explicit keys are handled correctly. (2) `Table.Insert` no longer allocates a `rowValues` buffer for single-row inserts that arrive in column order (the common prepared-stmt path): when `len(stmt.Inserts)==1` and `len(values)==len(t.Columns)` we alias `rowValues = values` directly, skipping one `make([]OptionalValue, nCols)` per exec. Net effect on batch insert (100 rows/tx): allocs **2,143 → 1,954 (−8.8%)**, memory **123 KiB → 115 KiB (−6.5%)**; multi-values INSERT allocs **1,457 → 1,363 (−6.5%)** (single-row optimisation does not apply to multi-row statements; only the SeekLastKey saving contributes there).

2026-06-17: VACUUM direct row-copy path (`vacuumCopier`) — replaces the per-row `Statement{...}` + `[][]OptionalValue` + `rowValues` allocations in the VACUUM copy loop with a `vacuumCopier` struct that pre-computes column-index maps once per table and calls `insertPrimaryKey` / `insertUniqueIndexKey` / `insertSecondaryIndexKey` / `LeafNodeInsert` directly. Skips field-name linear scans (`InsertValuesForColumns`), `rowValues` allocation, JSON normalisation, check-constraint validation, FK checks, conflict checks, and RETURNING overhead — all unnecessary when copying pre-validated data from the live database. VACUUM small: **1.39 ms → 1.22 ms (−12%)**; allocs: 5,668 → 4,667 (−18%); B/op: 745 KiB → 687 KiB (−8%).

2026-06-17: VACUUM deferred-write optimization — `pagerImpl.SetNoIntermediateSync(true)` on the temp DB eliminates per-commit `pwrite`+`fsync` calls during the copy phase. All cached pages are written to disk in a single sequential `WriteAt` per consecutive run at `Close()` time, followed by one `fsync`. Reduces I/O syscall count from O(commits × pages) to O(1 fsync + ~3 writes). VACUUM small: **1.51 ms → 1.39 ms (−8%)**; allocs: 5,722 → 5,668 (−1%); B/op: +17 KiB from the contiguous flush buffer (traded N pool round-trips for one heap allocation).

2026-06-17: Full-text build — flat-buffer sort-at-flush — replaced the per-term `map[string][]invertedPosting` accumulation map in `populateFullTextIndex` with a flat `[]flatPosting` slice sorted at flush time. A single contiguous `postingPool` replaces per-term `append` growth events. Allocations reduced **−24%** (12,298 → 9,373). The 1000-doc benchmark shows a memory increase (1.06 MiB → 1.87 MiB) due to a 16-byte per-entry string header overhead and buffer growth copies in a cold-start workload that never reaches the 64K-posting flush threshold; in production with large tables and multi-flush cycles the flat buffer is reused after the first flush, giving ~68% per-flush byte reduction and ~99.9% alloc-event reduction.

2026-06-16: VACUUM correctness and I/O reduction (Priority 4) — three changes targeting VACUUM: (1) `Database.closeForDiscard()` closes the live DB without checkpointing or syncing (the live DB is discarded immediately after close, so the WAL checkpoint is wasted work); (2) `WAL.CloseNoSync()` / `pagerImpl.CloseNoSync()` close file handles without fdatasync for the discard path; (3) after VACUUM the WAL is now correctly restored — previously the WAL was lost after every VACUUM and all subsequent writes used the slower `commitDirect` path instead of WAL. The microbenchmark (1000-row table, thousands of VACUUMs/second) shows a ~20% regression because WAL is now properly maintained across VACUUM iterations (each seeding pass now goes through WAL, which is correct but adds per-iteration overhead vs the pre-existing WAL-loss bug). Real-world VACUUM on large tables with significant WAL backlogs benefits from skipping the checkpoint.

2026-06-16: Inverted index build allocation reduction (second pass) — two changes targeting `mutationSegmentCells` and `rowIDMutationSegmentCells`: (1) pre-size the cells slice to avoid growth: `len(terms)` was too small when multi-block terms (e.g., "common" with 1000 docs spans 5 × 1 KB blocks) pushed total cells past capacity — compute an extra-block estimate from posting counts and pre-allocate exactly; (2) pool the `[]string` terms-sort buffer via `sync.Pool` so the ~30 KB backing array is reused across build iterations rather than allocated fresh each time. Full-text build: **1.19 MiB → 1.06 MiB (−11%)**.

2026-06-16: Inverted index build allocation reduction — skip `append(deleteCells, insertCells...)` copy in `appendSegmentCells` for the insert-only path (build, online INSERT). Both callers discard their slice arguments after the call, so aliasing `cells = insertCells` and sorting in-place is safe. Full-text build: **1.42 MiB → 1.19 MiB (−16%)**, 3.44 ms → 3.08 ms (−10%); JSON inverted build: **1.29 MiB → 1.17 MiB (−9%)**, 2.11 ms → 2.00 ms (−5%).

2026-06-16: INSERT allocation reduction (Priority 3) — four micro-optimisations targeting the INSERT hot path: (1) `Seek`/`SeekWithPrefix`/`SeekLastKey` converted to iterative loops, eliminating goroutine-stack growth that appeared as heap allocations in pprof; (2) `SeekNextRowID` now returns `Cursor` by value instead of `*Cursor`, removing one heap allocation per row; (3) `WithTransaction` context wrap cached on `Conn` for explicit-transaction batches, eliminating one `context.WithValue` allocation per statement; (4) `typeCodes []byte` slice cached on `Table` at construction time and reused in `saveToCell`, removing one `make([]byte, nCols)` per INSERT. Net effect on `BenchmarkInsert_Batch` (100 rows/tx): **2637 → 2153 allocs/op (−18.4%)**, 134 KiB → 124 KiB (−7.5%).

2026-06-16: Vector overflow page reuse on UPDATE — `updateOverflowVectors` replaces the `freeOverflowPages` + `storeOverflowVectors` pair with in-place reuse of existing overflow pages. `VECTOR(n)` dimensions are fixed at column-definition time, so old and new chains always have the same page count; every UPDATE is a pure page-reuse with zero `AddFreePage` + `GetFreePage` calls. Dimension validation in `coerceColumnValue` (stmt.go) guarantees the invariant holds before any data reaches `updateOverflow`.

2026-06-14: Full-text and JSON inverted index delete and insert improved significantly after the log-structured inverted index refactor (PR #236): full-text delete 92.6 µs → 48.2 µs (−48%), 81.4 KiB → 17.2 KiB (−79%); JSON inverted insert 73.8 µs → 48.9 µs (−34%), delete 296 µs → 114 µs (−62%).

2026-06-14: Overflow page reuse on UPDATE — `updateOverflowTexts` replaces the free-then-reallocate cycle (`freeOverflowPages` + `storeOverflowTexts`) with in-place reuse of existing overflow pages. Also fixes a latent bug where unchanged text-overflow columns were re-stored on UPDATE (creating orphaned duplicate chains). Net effect on same-size overflow UPDATE: −26% time at inline, −43% at 1-page, −39% at 4-page, −36% at 16-page; allocs halved at 16-page (131→60).

2026-06-11: Tier-2 point-scan optimizations: `conditionsCanSkipFolding`, `buildColumnNames` precomputed once, `RuntimeIndexKeys` decouples per-execution index key injection, read-only transaction object reused via single-slot cache. Net effect on point scan: −20% heap, −7% allocs, −2% time.

---

## 2026-06-17 Baseline

The results are grouped by benchmark family. In comparison tables, a time ratio below `1.0×` means MiniSQL is faster than SQLite; above `1.0×` means slower.

### SELECT

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Point scan | 4.2 µs | 3.4 µs | 1.24× | 2.0 KiB | 679 B | 38 / 26 |
| Limit | 6.7 µs | 7.9 µs | 0.85× | 2.8 KiB | 1.7 KiB | 92 / 104 |
| Full scan | 3.63 ms | 5.19 ms | 0.70× | 1.23 MiB | 1.29 MiB | 79,820 / 99,758 |
| Count star | 6.9 µs | 10.1 µs | 0.68× | 2.3 KiB | 400 B | 26 / 13 |
| Index range scan | 714 µs | 773 µs | 0.92× | 82.9 KiB | 85.9 KiB | 5,534 / 6,581 |
| Secondary index, low selectivity | 1.75 ms | 2.75 ms | 0.64× | 314 KiB | 313 KiB | 24,913 / 29,886 |
| Secondary index, low selectivity limit | 7.7 µs | 8.3 µs | 0.93× | 3.2 KiB | 1.1 KiB | 82 / 64 |
| Range scan | 777 µs | 852 µs | 0.91× | 79.7 KiB | 85.9 KiB | 5,504 / 6,581 |

### INSERT, UPDATE, DELETE, and Constraints

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Insert single row | 11.0 µs | 56.7 µs | 0.19× | 2.0 KiB | 311 B | 27 / 12 |
| Insert batch | 322 µs | 242 µs | 1.33× | 109 KiB | 31.8 KiB | 1,756 / 1,310 |
| Insert prepared batch | 333 µs | 242 µs | 1.38× | 108 KiB | 31.8 KiB | 1,755 / 1,309 |
| Insert multi-values | 184 µs | 180 µs | 1.02× | 102 KiB | 25.9 KiB | 1,362 / 623 |
| Update by PK | 8.3 µs | 39.1 µs | 0.21× | 3.6 KiB | 263 B | 38 / 10 |
| Delete by PK | 15.6 µs | 86.1 µs | 0.18× | 3.1 KiB | 447 B | 49 / 19 |
| ON CONFLICT DO UPDATE | 7.7 µs | 40.9 µs | 0.19× | 1.6 KiB | 259 B | 29 / 10 |
| Foreign key insert | 10.4 µs | 45.0 µs | 0.23× | 1.9 KiB | 192 B | 24 / 8 |
| Foreign key delete cascade | 25.1 µs | 53.0 µs | 0.47× | 7.1 KiB | 128 B | 111 / 5 |

### Aggregates, DISTINCT, CTE, and Subquery

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| GROUP BY aggregate | 889 µs | 2.07 ms | 0.43× | 33.4 KiB | 3.5 KiB | 457 / 309 |
| HAVING filter | 726 µs | 1.93 ms | 0.38× | 25.4 KiB | 1.9 KiB | 262 / 111 |
| DISTINCT high cardinality | 2.78 ms | 5.73 ms | 0.49× | 1.26 MiB | 586 KiB | 40,092 / 40,010 |
| DISTINCT + ORDER BY high cardinality | 3.21 ms | 5.07 ms | 0.63× | 4.54 MiB | 586 KiB | 90,100 / 40,010 |
| DISTINCT + ORDER BY indexed | 2.85 ms | 3.28 ms | 0.87× | 4.38 MiB | 586 KiB | 60,081 / 40,010 |
| CTE materialise | 354 µs | 448 µs | 0.79× | 6.3 KiB | 400 B | 86 / 13 |
| Subquery IN list | 3.13 ms | 3.74 ms | 0.84× | 559 KiB | 235 KiB | 15,197 / 20,010 |

### Joins

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Inner join, small-large | 5.79 ms | 4.79 ms | 1.21× | 1.00 MiB | 1.07 MiB | 79,855 / 99,757 |
| Inner join, low selectivity | 111 µs | 742 µs | 0.15× | 19.5 KiB | 11.3 KiB | 1,101 / 1,009 |
| Left join, unmatched rows | 3.65 ms | 4.16 ms | 0.88× | 869 KiB | 708 KiB | 79,643 / 70,157 |

### ORDER BY Disk Spill

MiniSQL-only sub-benchmarks on a 10 000-row table sorted by a `varchar(255)` email column.
`no-spill` uses `sort_mem_limit=0` (pure in-memory sort); `spill-64k` uses `sort_mem_limit=65536`,
which flushes the rows across ~8 sorted run files that are then N-way merged.

| Sub-benchmark | Time | Rows/op | Notes |
|---|---|---|---|
| no-spill | 3.63 ms | 10 000 | pure in-memory sort, baseline |
| spill-64k | 9.81 ms | 10 000 | after buffered I/O (64 KiB); was 55.9 ms unbuffered (~4.3× improvement) |

### Full-Text Inverted Index

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Build index | 3.75 ms | 2.10 ms | 1.78× | 1.87 MiB | 392 B | 9,373 / 20 |
| Insert with index | 34.7 µs | 90.8 µs | 0.38× | 12.0 KiB | 271 B | 126 / 10 |
| Search single term, rare | 4.6 µs | 6.5 µs | 0.71× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, medium | 4.1 µs | 7.5 µs | 0.55× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, common | 4.3 µs | 61.0 µs | 0.07× | 2.1 KiB | 424 B | 41 / 15 |
| Search multi-term AND | 15.3 µs | 33.7 µs | 0.46× | 11.1 KiB | 408 B | 60 / 13 |
| Search phrase | 16.2 µs | 24.4 µs | 0.67× | 26.0 KiB | 416 B | 276 / 14 |
| Update with index | 35.4 µs | 98.2 µs | 0.36× | 17.7 KiB | 291 B | 183 / 12 |
| Delete with index | 48.9 µs | 140 µs | 0.35× | 17.1 KiB | 135 B | 171 / 6 |

### Full-Text MiniSQL-Only Checks

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Search after deletes | 81.9 µs | 10.9 KiB | 47 |

### JSON Inverted Index DML

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Build index | 2.00 ms | 1.19 MiB | 26,679 |
| Insert with index | 49.0 µs | 58.9 KiB | 140 |
| Contains after deletes | 60.2 µs | 19.0 KiB | 75 |
| Update with index | 5.98 µs | 4.1 KiB | 44 |
| Delete with index | 116 µs | 26.3 KiB | 147 |

### JSON Contains Comparisons

| Predicate | MiniSQL indexed | MiniSQL sequential | SQLite JSON scan | SQLite expression index |
|---|---|---|---|---|
| Key/value | 15.7 µs / 7.5 KiB | 1.93 ms / 1.94 MiB | 700 µs / 424 B | 26.4 µs / 424 B |
| Object subset | 26.4 µs / 8.6 KiB | 1.97 ms / 1.94 MiB | 740 µs / 424 B | 124 µs / 424 B |

### Maintenance and Explain

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| VACUUM small | 1.22 ms | 288 µs | 4.2× | 687 KiB | 88 B | 4,667 / 4 |
| WAL checkpoint | 203 µs | 106 µs | 1.9× | 3.3 KiB | 440 B | 37 / 12 |
| EXPLAIN | 5.2 µs | 1.2 µs | 4.2× | 5.5 KiB | 680 B | 51 / 18 |

### HNSW Build Index

`vecMediumN` reduced from 10,000 → 3,000 so each sub-benchmark completes in 2–6 s instead of 9–38 s. Small-N cases (n=1000) run multiple iterations; large-N cases (n=3000) may be single-iteration — expect ±15% variance from thermal effects.

| Dims | Rows | Time | Memory | Allocs |
|---|---|---|---|---|
| 3 | 1000 | 653 ms | 5.0 MiB | 25,476 |
| 3 | 3000 | 2.87 s | 46.3 MiB | 105,485 |
| 128 | 1000 | 775 ms | 6.5 MiB | 27,090 |
| 128 | 3000 | 4.83 s | 44.7 MiB | 108,198 |
| 768 | 1000 | 1.15 s | 13.8 MiB | 26,932 |
| 768 | 3000 | 5.26 s | 66.9 MiB | 108,777 |

### HNSW ANN Search

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 29.6 µs | 13.2 KiB | 55 |
| 3 | 1000 | 10 | 35.2 µs | 16.9 KiB | 123 |
| 3 | 3000 | 1 | 45.8 µs | 22.5 KiB | 57 |
| 3 | 3000 | 10 | 51.6 µs | 26.2 KiB | 129 |
| 128 | 1000 | 1 | 177 µs | 41.6 KiB | 60 |
| 128 | 1000 | 10 | 184 µs | 54.1 KiB | 141 |
| 128 | 3000 | 1 | 291 µs | 77.6 KiB | 65 |
| 128 | 3000 | 10 | 302 µs | 90.2 KiB | 146 |
| 768 | 1000 | 1 | 679 µs | 46.5 KiB | 60 |
| 768 | 1000 | 10 | 707 µs | 104.1 KiB | 136 |
| 768 | 3000 | 1 | 1.04 ms | 82.6 KiB | 65 |
| 768 | 3000 | 10 | 1.09 ms | 140.2 KiB | 145 |

### HNSW Sequential Scan

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 661 µs | 664 KiB | 10,821 |
| 128 | 1000 | 1 | 8.48 ms | 5.93 MiB | 11,826 |
| 768 | 1000 | 1 | 46.4 ms | 31.4 MiB | 11,850 |

### HNSW Insert Overhead

| Dims | With index | No index | Time ratio |
|---|---|---|---|
| 3 | 1.13 ms / 222 KiB | 20.7 µs / 6.9 KiB | 54.3× |
| 128 | 3.24 ms / 236 KiB | 21.2 µs / 7.4 KiB | 153× |
| 768 | 11.5 ms / 310 KiB | 21.7 µs / 9.8 KiB | 529× |

### Memory Outliers

| Area | Benchmark | MiniSQL memory |
|---|---|---|
| HNSW | Build index, dims768, 3k rows | 66.9 MiB |
| HNSW | Build index, dims128, 3k rows | 44.7 MiB |
| HNSW | Build index, dims3, 3k rows | 46.3 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY | 4.54 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY indexed | 4.38 MiB |
| DISTINCT | High-cardinality distinct (no ORDER BY) | 1.26 MiB |
| Full-text | Build index (cold-start benchmark, 1000 docs, no flush) | 1.87 MiB |
| JSON inverted | Build index | 1.19 MiB |
| SELECT | Full scan | 1.23 MiB |
| Join | Inner join, small-large | 1.00 MiB |
| Join | Left join, unmatched rows | 869 KiB |
| Maintenance | VACUUM small | 687 KiB (WAL overhead included since Priority 4 WAL-restore fix) |
| Subquery | IN list | 559 KiB |

### Overflow Page INSERT

One INSERT per b.N iteration, auto-commit. "inline" (512 B) fits within `MaxInlineVarchar` and uses no overflow pages; it is the control group. `1pg`/`4pg`/`16pg` spill to 1, 4, and 16 overflow pages respectively.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 12.8 µs | 47.2 µs | 0.27× | 4.1 KiB | 144 B | 24 / 7 |
| 1pg (~4 KB) | 31.6 µs | 61.9 µs | 0.51× | 19.6 KiB | 144 B | 42 / 7 |
| 4pg (~16 KB) | 75.3 µs | 106 µs | 0.71× | 57.8 KiB | 144 B | 69 / 7 |
| 16pg (~64 KB) | 189 µs | 238 µs | 0.80× | 212 KiB | 144 B | 174 / 7 |

### Overflow Page Point Read

100 rows seeded once; each b.N iteration does one `SELECT … WHERE id = ?` and traverses the overflow chain.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 4.1 µs | 3.1 µs | 1.32× | 2.4 KiB | 1.5 KiB | 40 / 18 |
| 1pg (~4 KB) | 5.7 µs | 4.7 µs | 1.21× | 9.9 KiB | 8.5 KiB | 41 / 18 |
| 4pg (~16 KB) | 8.1 µs | 14.5 µs | 0.56× | 33.9 KiB | 32.5 KiB | 41 / 18 |
| 16pg (~64 KB) | 15.8 µs | 41.9 µs | 0.38× | 130 KiB | 128 KiB | 41 / 18 |

### Overflow Page Full Scan

50 rows seeded once; each b.N iteration scans all 50 rows reading every overflow chain.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 17.2 µs | 25.9 µs | 0.66× | 27.1 KiB | 51.4 KiB | 123 / 214 |
| 1pg (~4 KB) | 77.1 µs | 90.4 µs | 0.85× | 402 KiB | 401 KiB | 173 / 214 |
| 4pg (~16 KB) | 227 µs | 528 µs | 0.43× | 1.56 MiB | 1.56 MiB | 173 / 214 |
| 16pg (~64 KB) | 657 µs | 2.16 ms | 0.30× | 6.25 MiB | 6.25 MiB | 173 / 214 |

### Overflow Page UPDATE (in-place reuse, text + vector)

One row seeded; each b.N iteration updates the blob body. Old overflow chain is reused in-place — no free+alloc per page for same-size updates (text since 2026-06-14; vector since 2026-06-16).

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 7.5 µs | 43.3 µs | 0.17× | 4.6 KiB | 176 B | 33 / 7 |
| 1pg (~4 KB) | 15.7 µs | 47.0 µs | 0.33× | 15.9 KiB | 176 B | 40 / 7 |
| 4pg (~16 KB) | 32.2 µs | 69.2 µs | 0.47× | 52.2 KiB | 176 B | 44 / 7 |
| 16pg (~64 KB) | 97.6 µs | 130 µs | 0.75× | 201 KiB | 176 B | 60 / 7 |

### Good Next Optimisation Targets

- HNSW build allocation counts are stable; build memory remains the largest broad-suite outlier at 3k rows (66.9 MiB for dims768). The 3k corpus was chosen so each sub-benchmark completes in 2–6 s (was 9–38 s at 10k). Single-iteration benchmarks carry high variance — compare only against a fresh count=3 run.
- Full-text build allocates ~1.87 MiB per operation in the benchmark cold-start case (1000 docs, buffer never flushes). In multi-flush production workloads the flat-buffer optimization delivers ~68% per-flush byte reduction. JSON inverted build ~1.19 MiB remains a target.
- GROUP BY / HAVING memory gap vs SQLite (9.5× / 13.4×) is largely structural: Go's runtime map has higher per-entry overhead than SQLite's C hash table. Further reduction requires a custom open-address hash table — low ROI at this stage.
- DISTINCT high cardinality without ORDER BY (1.26 MiB vs SQLite 586 KiB, 2.2×): streaming hash-set path; the gap is structural — SQLite sorts/hashes on the C side and delivers rows lazily, avoiding upfront Go heap allocation.
- DISTINCT + ORDER BY high cardinality (4.54 MiB vs SQLite 586 KiB): ORDER BY requires upfront materialization of all rows before sorting. The sort-then-adjacent-dedup optimization removes hash-set overhead from deduplication, but the dominant cost is O(N) row materialization — unavoidable without disk-backed sort (see ROADMAP).
- VACUUM (1.39 ms vs SQLite 265 µs, 5.2×): deferred-write optimization eliminated per-commit fsyncs and now batches all page writes into one sequential I/O at close. Remaining gap vs SQLite is structural — MiniSQL rebuilds the B-tree row-by-row through normal INSERT paths (parse, marshal, split), while SQLite bulk-copies pages directly. A page-level bulk-copy path for the temp DB would close most of the gap.
