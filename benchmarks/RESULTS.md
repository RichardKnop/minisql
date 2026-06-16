# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `go test -tags bench ./benchmarks/ -run='^$' -bench='.' -benchmem -count=1` (single pass). All rows refreshed 2026-06-16 (Priority 3).  
**GOMAXPROCS:** 10

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime. Single-iteration benchmarks (HNSW build at large N) carry higher variance than multi-iteration ones.

2026-06-16: INSERT allocation reduction (Priority 3) — four micro-optimisations targeting the INSERT hot path: (1) `Seek`/`SeekWithPrefix`/`SeekLastKey` converted to iterative loops, eliminating goroutine-stack growth that appeared as heap allocations in pprof; (2) `SeekNextRowID` now returns `Cursor` by value instead of `*Cursor`, removing one heap allocation per row; (3) `WithTransaction` context wrap cached on `Conn` for explicit-transaction batches, eliminating one `context.WithValue` allocation per statement; (4) `typeCodes []byte` slice cached on `Table` at construction time and reused in `saveToCell`, removing one `make([]byte, nCols)` per INSERT. Net effect on `BenchmarkInsert_Batch` (100 rows/tx): **2637 → 2153 allocs/op (−18.4%)**, 134 KiB → 124 KiB (−7.5%).

2026-06-16: Vector overflow page reuse on UPDATE — `updateOverflowVectors` replaces the `freeOverflowPages` + `storeOverflowVectors` pair with in-place reuse of existing overflow pages. `VECTOR(n)` dimensions are fixed at column-definition time, so old and new chains always have the same page count; every UPDATE is a pure page-reuse with zero `AddFreePage` + `GetFreePage` calls. Dimension validation in `coerceColumnValue` (stmt.go) guarantees the invariant holds before any data reaches `updateOverflow`.

2026-06-14: Full-text and JSON inverted index delete and insert improved significantly after the log-structured inverted index refactor (PR #236): full-text delete 92.6 µs → 48.2 µs (−48%), 81.4 KiB → 17.2 KiB (−79%); JSON inverted insert 73.8 µs → 48.9 µs (−34%), delete 296 µs → 114 µs (−62%).

2026-06-14: Overflow page reuse on UPDATE — `updateOverflowTexts` replaces the free-then-reallocate cycle (`freeOverflowPages` + `storeOverflowTexts`) with in-place reuse of existing overflow pages. Also fixes a latent bug where unchanged text-overflow columns were re-stored on UPDATE (creating orphaned duplicate chains). Net effect on same-size overflow UPDATE: −26% time at inline, −43% at 1-page, −39% at 4-page, −36% at 16-page; allocs halved at 16-page (131→60).

2026-06-11: Tier-2 point-scan optimizations: `conditionsCanSkipFolding`, `buildColumnNames` precomputed once, `RuntimeIndexKeys` decouples per-execution index key injection, read-only transaction object reused via single-slot cache. Net effect on point scan: −20% heap, −7% allocs, −2% time.

---

## 2026-06-16 Baseline

The results are grouped by benchmark family. In comparison tables, a time ratio below `1.0×` means MiniSQL is faster than SQLite; above `1.0×` means slower.

### SELECT

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Point scan | 5.0 µs | 4.0 µs | 1.25× | 2.0 KiB | 679 B | 38 / 26 |
| Limit | 8.0 µs | 9.2 µs | 0.87× | 2.8 KiB | 1.7 KiB | 92 / 104 |
| Full scan | 4.0 ms | 6.0 ms | 0.67× | 1.26 MiB | 1.33 MiB | 79,820 / 99,758 |
| Count star | 7.1 µs | 10.6 µs | 0.67× | 2.3 KiB | 400 B | 26 / 13 |
| Index range scan | 1.14 ms | 839 µs | 1.36× | 82.9 KiB | 86.0 KiB | 5,534 / 6,581 |
| Secondary index, low selectivity | 1.88 ms | 3.10 ms | 0.61× | 314 KiB | 313 KiB | 24,913 / 29,886 |
| Secondary index, low selectivity limit | 8.9 µs | 9.3 µs | 0.96× | 3.2 KiB | 1.1 KiB | 82 / 64 |
| Range scan | 857 µs | 953 µs | 0.90× | 79.7 KiB | 86.0 KiB | 5,504 / 6,581 |

### INSERT, UPDATE, DELETE, and Constraints

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Insert single row | 11.9 µs | 53.2 µs | 0.22× | 2.0 KiB | 311 B | 27 / 12 |
| Insert batch | 366 µs | 240 µs | 1.52× | 124 KiB | 31.1 KiB | 2,153 / 1,308 |
| Insert prepared batch | 353 µs | 237 µs | 1.49× | 123 KiB | 31.1 KiB | 2,152 / 1,307 |
| Insert multi-values | 204 µs | 188 µs | 1.08× | 107 KiB | 25.2 KiB | 1,462 / 622 |
| Update by PK | 9.1 µs | 44.9 µs | 0.20× | 3.5 KiB | 263 B | 38 / 10 |
| Delete by PK | 15.5 µs | 68.9 µs | 0.22× | 3.0 KiB | 447 B | 47 / 19 |
| ON CONFLICT DO UPDATE | 8.5 µs | 58.6 µs | 0.15× | 1.6 KiB | 260 B | 29 / 10 |
| Foreign key insert | 12.1 µs | 53.6 µs | 0.23× | 1.8 KiB | 192 B | 24 / 8 |
| Foreign key delete cascade | 47.6 µs | 91.0 µs | 0.52× | 7.1 KiB | 128 B | 111 / 5 |

### Aggregates, DISTINCT, CTE, and Subquery

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| GROUP BY aggregate | 1.05 ms | 2.27 ms | 0.46× | 33.4 KiB | 3.5 KiB | 457 / 309 |
| HAVING filter | 724 µs | 1.92 ms | 0.38× | 25.3 KiB | 1.9 KiB | 262 / 111 |
| DISTINCT high cardinality | 2.71 ms | 5.59 ms | 0.48× | 1.26 MiB | 587 KiB | 40,092 / 40,010 |
| DISTINCT + ORDER BY high cardinality | 3.20 ms | 5.22 ms | 0.61× | 4.54 MiB | 587 KiB | 90,100 / 40,010 |
| DISTINCT + ORDER BY indexed | 2.83 ms | 3.43 ms | 0.82× | 4.38 MiB | 587 KiB | 60,080 / 40,010 |
| CTE materialise | 377 µs | 474 µs | 0.79× | 6.3 KiB | 400 B | 86 / 13 |
| Subquery IN list | 3.56 ms | 4.01 ms | 0.89× | 559 KiB | 235 KiB | 15,197 / 20,010 |

### Joins

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Inner join, small-large | 5.89 ms | 4.96 ms | 1.19× | 1.00 MiB | 1.07 MiB | 79,854 / 99,757 |
| Inner join, low selectivity | 113 µs | 873 µs | 0.13× | 19.5 KiB | 11.3 KiB | 1,101 / 1,009 |
| Left join, unmatched rows | 4.46 ms | 4.93 ms | 0.90× | 869 KiB | 708 KiB | 79,643 / 70,157 |

### ORDER BY Disk Spill

MiniSQL-only sub-benchmarks on a 10 000-row table sorted by a `varchar(255)` email column.
`no-spill` uses `sort_mem_limit=0` (pure in-memory sort); `spill-64k` uses `sort_mem_limit=65536`,
which flushes the rows across ~8 sorted run files that are then N-way merged.

| Sub-benchmark | Time | Rows/op | Notes |
|---|---|---|---|
| no-spill | 4.9 ms | 10 000 | pure in-memory sort, baseline |
| spill-64k | 13.0 ms | 10 000 | after buffered I/O (64 KiB); was 55.9 ms unbuffered (~4.3× improvement) |

### Full-Text Inverted Index

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Build index | 3.44 ms | 2.27 ms | 1.52× | 1.42 MiB | 392 B | 12,289 / 20 |
| Insert with index | 40.2 µs | 100.5 µs | 0.40× | 14.4 KiB | 271 B | 135 / 10 |
| Search single term, rare | 5.4 µs | 7.0 µs | 0.77× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, medium | 5.3 µs | 8.3 µs | 0.64× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, common | 4.7 µs | 70.6 µs | 0.07× | 2.1 KiB | 424 B | 41 / 15 |
| Search multi-term AND | 18.7 µs | 38.7 µs | 0.48× | 11.1 KiB | 408 B | 60 / 13 |
| Search phrase | 23.6 µs | 25.9 µs | 0.91× | 26.0 KiB | 416 B | 276 / 14 |
| Update with index | 39.8 µs | 114.8 µs | 0.35× | 17.9 KiB | 292 B | 191 / 12 |
| Delete with index | 97.2 µs | 154.0 µs | 0.63× | 81.7 KiB | 135 B | 909 / 6 |

### Full-Text MiniSQL-Only Checks

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Search after deletes | 87.5 µs | 10.9 KiB | 47 |

### JSON Inverted Index DML

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Build index | 2.11 ms | 1.29 MiB | 26,670 |
| Insert with index | 71.2 µs | 163.5 KiB | 150 |
| Contains after deletes | 61.4 µs | 19.0 KiB | 75 |
| Update with index | 5.9 µs | 4.0 KiB | 43 |
| Delete with index | 283 µs | 64.5 KiB | 152 |

### JSON Contains Comparisons

| Predicate | MiniSQL indexed | MiniSQL sequential | SQLite JSON scan | SQLite expression index |
|---|---|---|---|---|
| Key/value | 15.7 µs / 7.5 KiB | 1.97 ms / 1.99 MiB | 730 µs / 424 B | 28.9 µs / 424 B |
| Object subset | 32.4 µs / 8.6 KiB | 3.02 ms / 1.99 MiB | 876 µs / 424 B | 136 µs / 424 B |

### Maintenance and Explain

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| VACUUM small | 1.38 ms | 313 µs | 4.4× | 729 KiB | 88 B | 5,678 / 4 |
| WAL checkpoint | 493 µs | 110 µs | 4.5× | 3.2 KiB | 440 B | 37 / 12 |
| EXPLAIN | 5.4 µs | 1.3 µs | 4.2× | 5.5 KiB | 680 B | 51 / 18 |

### HNSW Build Index

`vecMediumN` reduced from 10,000 → 3,000 so each sub-benchmark completes in 2–6 s instead of 9–38 s. Small-N cases (n=1000) run multiple iterations; large-N cases (n=3000) may be single-iteration — expect ±15% variance from thermal effects.

| Dims | Rows | Time | Memory | Allocs |
|---|---|---|---|---|
| 3 | 1000 | 660 ms | 5.6 MiB | 26,352 |
| 3 | 3000 | 2.55 s | 42.9 MiB | 104,849 |
| 128 | 1000 | 794 ms | 6.7 MiB | 26,820 |
| 128 | 3000 | 3.97 s | 41.1 MiB | 104,451 |
| 768 | 1000 | 1.19 s | 13.7 MiB | 26,595 |
| 768 | 3000 | 5.56 s | 72.9 MiB | 108,838 |

### HNSW ANN Search

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 33.4 µs | 13.2 KiB | 55 |
| 3 | 1000 | 10 | 39.6 µs | 16.9 KiB | 123 |
| 3 | 3000 | 1 | 45.7 µs | 22.5 KiB | 57 |
| 3 | 3000 | 10 | 53.1 µs | 26.2 KiB | 129 |
| 128 | 1000 | 1 | 181 µs | 41.6 KiB | 60 |
| 128 | 1000 | 10 | 190 µs | 54.1 KiB | 141 |
| 128 | 3000 | 1 | 302 µs | 77.6 KiB | 65 |
| 128 | 3000 | 10 | 301 µs | 90.2 KiB | 146 |
| 768 | 1000 | 1 | 691 µs | 46.5 KiB | 60 |
| 768 | 1000 | 10 | 726 µs | 104.1 KiB | 136 |
| 768 | 3000 | 1 | 1.09 ms | 82.6 KiB | 65 |
| 768 | 3000 | 10 | 1.16 ms | 140.1 KiB | 145 |

### HNSW Sequential Scan

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 674 µs | 664 KiB | 10,821 |
| 128 | 1000 | 1 | 8.61 ms | 6.07 MiB | 11,825 |
| 768 | 1000 | 1 | 47.4 ms | 31.4 MiB | 11,850 |

### HNSW Insert Overhead

| Dims | With index | No index | Time ratio |
|---|---|---|---|
| 3 | 1.19 ms / 223 KiB | 22.6 µs / 6.9 KiB | 52.7× |
| 128 | 3.35 ms / 220 KiB | 20.4 µs / 7.4 KiB | 164.2× |
| 768 | 14.0 ms / 246 KiB | 21.6 µs / 9.8 KiB | 647.7× |

### Memory Outliers

| Area | Benchmark | MiniSQL memory |
|---|---|---|
| HNSW | Build index, dims768, 3k rows | 72.9 MiB |
| HNSW | Build index, dims128, 3k rows | 41.1 MiB |
| HNSW | Build index, dims3, 3k rows | 42.9 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY | 4.54 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY indexed | 4.38 MiB |
| DISTINCT | High-cardinality distinct (no ORDER BY) | 1.26 MiB |
| Full-text | Build index | 1.42 MiB |
| JSON inverted | Build index | 1.29 MiB |
| SELECT | Full scan | 1.26 MiB |
| Join | Inner join, small-large | 1.00 MiB |
| Join | Left join, unmatched rows | 869 KiB |
| Maintenance | VACUUM small | 729 KiB |
| Subquery | IN list | 559 KiB |

### Overflow Page INSERT

One INSERT per b.N iteration, auto-commit. "inline" (512 B) fits within `MaxInlineVarchar` and uses no overflow pages; it is the control group. `1pg`/`4pg`/`16pg` spill to 1, 4, and 16 overflow pages respectively.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 13.9 µs | 47.0 µs | 0.30× | 4.1 KiB | 144 B | 24 / 7 |
| 1pg (~4 KB) | 32.7 µs | 72.0 µs | 0.45× | 19.7 KiB | 144 B | 44 / 7 |
| 4pg (~16 KB) | 61.3 µs | 107.7 µs | 0.57× | 58.2 KiB | 144 B | 71 / 7 |
| 16pg (~64 KB) | 174.2 µs | 364.1 µs | 0.48× | 211 KiB | 144 B | 174 / 7 |

### Overflow Page Point Read

100 rows seeded once; each b.N iteration does one `SELECT … WHERE id = ?` and traverses the overflow chain.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 7.5 µs | 4.4 µs | 1.70× | 2.4 KiB | 1.5 KiB | 40 / 18 |
| 1pg (~4 KB) | 8.7 µs | 8.7 µs | 1.00× | 9.8 KiB | 8.5 KiB | 41 / 18 |
| 4pg (~16 KB) | 16.7 µs | 24.2 µs | 0.69× | 33.8 KiB | 32.5 KiB | 41 / 18 |
| 16pg (~64 KB) | 39.0 µs | 79.1 µs | 0.49× | 130 KiB | 128 KiB | 41 / 18 |

### Overflow Page Full Scan

50 rows seeded once; each b.N iteration scans all 50 rows reading every overflow chain.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 28.3 µs | 44.9 µs | 0.63× | 27.1 KiB | 51.4 KiB | 123 / 214 |
| 1pg (~4 KB) | 157.3 µs | 171.1 µs | 0.92× | 402 KiB | 401 KiB | 173 / 214 |
| 4pg (~16 KB) | 462.6 µs | 1.01 ms | 0.46× | 1.60 MiB | 1.60 MiB | 173 / 214 |
| 16pg (~64 KB) | 1.54 ms | 3.85 ms | 0.40× | 6.41 MiB | 6.41 MiB | 173 / 214 |

### Overflow Page UPDATE (in-place reuse, text + vector)

One row seeded; each b.N iteration updates the blob body. Old overflow chain is reused in-place — no free+alloc per page for same-size updates (text since 2026-06-14; vector since 2026-06-16).

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 9.4 µs | 52.1 µs | 0.18× | 4.6 KiB | 176 B | 33 / 7 |
| 1pg (~4 KB) | 21.1 µs | 71.6 µs | 0.29× | 15.9 KiB | 176 B | 40 / 7 |
| 4pg (~16 KB) | 49.7 µs | 80.5 µs | 0.62× | 52.2 KiB | 176 B | 44 / 7 |
| 16pg (~64 KB) | 144.9 µs | 147.2 µs | 0.98× | 201 KiB | 176 B | 60 / 7 |

### Good Next Optimisation Targets

- HNSW build allocation counts are stable; build memory remains the largest broad-suite outlier at 3k rows (72.9 MiB for dims768). The 3k corpus was chosen so each sub-benchmark completes in 2–6 s (was 9–38 s at 10k). Single-iteration benchmarks carry high variance — compare only against a fresh count=3 run.
- Full-text and JSON build paths still allocate ~1.3–1.4 MiB per operation and remain the most relevant inverted-index targets.
- GROUP BY / HAVING memory gap vs SQLite (9.6× / 13.3×) is largely structural: Go's runtime map has higher per-entry overhead than SQLite's C hash table. Further reduction requires a custom open-address hash table — low ROI at this stage.
- DISTINCT high cardinality without ORDER BY (1.26 MiB vs SQLite 586 KiB, 2.2×): streaming hash-set path; the gap is structural — SQLite sorts/hashes on the C side and delivers rows lazily, avoiding upfront Go heap allocation.
- DISTINCT + ORDER BY high cardinality (4.53 MiB vs SQLite 586 KiB): ORDER BY requires upfront materialization of all rows before sorting. The sort-then-adjacent-dedup optimization removes hash-set overhead from deduplication, but the dominant cost is O(N) row materialization — unavoidable without disk-backed sort (see ROADMAP).
- VACUUM is much improved after streaming row copy, though it still allocates far more than SQLite because it rebuilds the compacted MiniSQL database through normal table/index write paths.
