# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `go test -tags bench ./benchmarks/ -run='^$' -bench='.' -benchmem -count=1` (single pass). All rows refreshed 2026-06-16.  
**GOMAXPROCS:** 10

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime. Single-iteration benchmarks (HNSW build at large N) carry higher variance than multi-iteration ones.

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
| Point scan | 4.43 µs | 3.48 µs | 1.3× | 2.0 KiB | 679 B | 39 / 26 |
| Limit | 6.99 µs | 8.11 µs | 0.86× | 2.8 KiB | 1.7 KiB | 92 / 104 |
| Full scan | 3.93 ms | 5.01 ms | 0.78× | 1.23 MiB | 1.30 MiB | 79,820 / 99,758 |
| Count star | 6.35 µs | 10.04 µs | 0.63× | 2.3 KiB | 400 B | 26 / 13 |
| Index range scan | 692 µs | 821 µs | 0.84× | 82.9 KiB | 85.9 KiB | 5,534 / 6,581 |
| Secondary index, low selectivity | 1.72 ms | 2.67 ms | 0.64× | 314 KiB | 313 KiB | 24,913 / 29,886 |
| Secondary index, low selectivity limit | 7.48 µs | 8.12 µs | 0.92× | 3.2 KiB | 1.1 KiB | 82 / 64 |
| Range scan | 784 µs | 858 µs | 0.91× | 79.7 KiB | 85.9 KiB | 5,504 / 6,581 |

### INSERT, UPDATE, DELETE, and Constraints

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Insert single row | 10.8 µs | 49.0 µs | 0.22× | 2.1 KiB | 311 B | 31 / 12 |
| Insert batch | 356 µs | 242 µs | 1.47× | 134 KiB | 31.0 KiB | 2,637 / 1,300 |
| Insert prepared batch | 387 µs | 261 µs | 1.48× | 133 KiB | 31.0 KiB | 2,635 / 1,299 |
| Insert multi-values | 216 µs | 174 µs | 1.24× | 110 KiB | 25.2 KiB | 1,756 / 616 |
| Update by PK | 8.12 µs | 36.7 µs | 0.22× | 3.6 KiB | 263 B | 39 / 10 |
| Delete by PK | 18.7 µs | 274 µs | 0.07× | 3.2 KiB | 446 B | 54 / 19 |
| ON CONFLICT DO UPDATE | 7.16 µs | 35.0 µs | 0.20× | 1.6 KiB | 260 B | 31 / 10 |
| Foreign key insert | 11.8 µs | 46.7 µs | 0.25× | 1.9 KiB | 192 B | 28 / 8 |
| Foreign key delete cascade | 23.8 µs | 54.7 µs | 0.43× | 7.1 KiB | 128 B | 111 / 5 |

### Aggregates, DISTINCT, CTE, and Subquery

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| GROUP BY aggregate | 891 µs | 2.14 ms | 0.42× | 33.6 KiB | 3.5 KiB | 457 / 309 |
| HAVING filter | 729 µs | 1.89 ms | 0.39× | 25.4 KiB | 1.9 KiB | 262 / 111 |
| DISTINCT high cardinality | 2.73 ms | 5.67 ms | 0.48× | 1.26 MiB | 586.3 KiB | 40,092 / 40,010 |
| DISTINCT + ORDER BY high cardinality | 3.32 ms | 5.16 ms | 0.64× | 4.53 MiB | 586.3 KiB | 90,100 / 40,010 |
| DISTINCT + ORDER BY indexed | 2.92 ms | 3.35 ms | 0.87× | 4.38 MiB | 586.3 KiB | 60,081 / 40,010 |
| CTE materialise | 343 µs | 438 µs | 0.78× | 6.3 KiB | 400 B | 86 / 13 |
| Subquery IN list | 2.40 ms | 3.54 ms | 0.68× | 559 KiB | 234.7 KiB | 15,197 / 20,010 |

### Joins

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Inner join, small-large | 5.65 ms | 4.76 ms | 1.19× | 1.00 MiB | 1.07 MiB | 79,855 / 99,757 |
| Inner join, low selectivity | 108 µs | 730 µs | 0.15× | 21.0 KiB | 11.3 KiB | 1,198 / 1,009 |
| Left join, unmatched rows | 3.63 ms | 4.10 ms | 0.89× | 869 KiB | 708 KiB | 79,643 / 70,157 |

### ORDER BY Disk Spill

MiniSQL-only sub-benchmarks on a 10 000-row table sorted by a `varchar(255)` email column.
`no-spill` uses `sort_mem_limit=0` (pure in-memory sort); `spill-64k` uses `sort_mem_limit=65536`,
which flushes the rows across ~8 sorted run files that are then N-way merged.

| Sub-benchmark | Time | Rows/op | Notes |
|---|---|---|---|
| no-spill | 3.62 ms | 10 000 | pure in-memory sort, baseline |
| spill-64k | 9.49 ms | 10 000 | after buffered I/O (64 KiB); was 55.9 ms unbuffered (~5.9× improvement) |

### Full-Text Inverted Index

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Build index | 2.88 ms | 1.98 ms | 1.46× | 1.39 MiB | 392 B | 12,313 / 20 |
| Insert with index | 34.3 µs | 82.7 µs | 0.41× | 12.7 KiB | 271 B | 131 / 10 |
| Search single term, rare | 4.45 µs | 6.49 µs | 0.69× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, medium | 4.00 µs | 7.47 µs | 0.54× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, common | 4.15 µs | 59.9 µs | 0.07× | 2.1 KiB | 424 B | 41 / 15 |
| Search multi-term AND | 14.9 µs | 33.0 µs | 0.45× | 11.1 KiB | 408 B | 60 / 13 |
| Search phrase | 15.7 µs | 24.1 µs | 0.65× | 26.0 KiB | 416 B | 276 / 14 |
| Update with index | 38.0 µs | 107.6 µs | 0.35× | 17.8 KiB | 291 B | 185 / 12 |
| Delete with index | 48.2 µs | 144.3 µs | 0.33× | 17.2 KiB | 135 B | 173 / 6 |

### Full-Text MiniSQL-Only Checks

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Search after deletes | 81.0 µs | 10.9 KiB | 47 |

### JSON Inverted Index DML

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Build index | 1.92 ms | 1.26 MiB | 26,671 |
| Insert with index | 48.9 µs | 61.9 KiB | 146 |
| Contains after deletes | 58.3 µs | 19.0 KiB | 75 |
| Update with index | 6.1 µs | 4.1 KiB | 45 |
| Delete with index | 114 µs | 26.5 KiB | 149 |

### JSON Contains Comparisons

| Predicate | MiniSQL indexed | MiniSQL sequential | SQLite JSON scan | SQLite expression index |
|---|---|---|---|---|
| Key/value | 15.0 µs / 7.5 KiB | 1.89 ms / 1.94 MiB | 682 µs / 424 B | 26.3 µs / 424 B |
| Object subset | 25.7 µs / 8.6 KiB | 1.92 ms / 1.94 MiB | 717 µs / 424 B | 121 µs / 424 B |

### Maintenance and Explain

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| VACUUM small | 1.49 ms | 308 µs | 4.9× | 754 KiB | 89 B | 6,979 / 4 |
| WAL checkpoint | 208 µs | 115 µs | 1.81× | 3.4 KiB | 440 B | 37 / 12 |
| EXPLAIN | 5.92 µs | 1.22 µs | 4.9× | 5.5 KiB | 680 B | 51 / 18 |

### HNSW Build Index

Single-iteration benchmarks at large N — expect ±15% variance between runs due to thermal effects.

| Dims | Rows | Time | Memory | Allocs |
|---|---|---|---|---|
| 3 | 1000 | 710.8 ms | 6.1 MiB | 26,845 |
| 3 | 10000 | 9.32 s | 121 MiB | 327,583 |
| 128 | 1000 | 762.9 ms | 6.3 MiB | 26,246 |
| 128 | 10000 | 23.1 s | 155 MiB | 345,994 |
| 768 | 1000 | 1.27 s | 13.7 MiB | 27,555 |
| 768 | 10000 | 38.5 s | 229 MiB | 348,048 |

### HNSW ANN Search

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 31.0 µs | 13.2 KiB | 55 |
| 3 | 1000 | 10 | 37.0 µs | 16.9 KiB | 123 |
| 3 | 10000 | 1 | 43.9 µs | 22.5 KiB | 57 |
| 3 | 10000 | 10 | 52.1 µs | 26.2 KiB | 129 |
| 128 | 1000 | 1 | 175.5 µs | 41.5 KiB | 60 |
| 128 | 1000 | 10 | 187.2 µs | 54.1 KiB | 141 |
| 128 | 10000 | 1 | 370.9 µs | 77.6 KiB | 65 |
| 128 | 10000 | 10 | 375.5 µs | 90.1 KiB | 146 |
| 768 | 1000 | 1 | 707.9 µs | 46.6 KiB | 60 |
| 768 | 1000 | 10 | 745.8 µs | 104.1 KiB | 136 |
| 768 | 10000 | 1 | 1.47 ms | 82.6 KiB | 65 |
| 768 | 10000 | 10 | 1.51 ms | 140.2 KiB | 146 |

### HNSW Sequential Scan

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 660.8 µs | 664.3 KiB | 10,821 |
| 128 | 1000 | 1 | 8.42 ms | 5.93 MiB | 11,826 |
| 768 | 1000 | 1 | 45.6 ms | 31.4 MiB | 11,850 |

### HNSW Insert Overhead

| Dims | With index | No index | Time ratio |
|---|---|---|---|
| 3 | 1.18 ms / 224 KiB | 21.6 µs / 6.9 KiB | 54.6× |
| 128 | 3.23 ms / 231 KiB | 20.8 µs / 7.4 KiB | 155.4× |
| 768 | 12.2 ms / 320 KiB | 21.5 µs / 9.8 KiB | 566.1× |

### Memory Outliers

| Area | Benchmark | MiniSQL memory |
|---|---|---|
| HNSW | Build index, dims768, 10k rows | 229 MiB |
| HNSW | Build index, dims128, 10k rows | 155 MiB |
| HNSW | Build index, dims3, 10k rows | 121 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY | 4.53 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY indexed | 4.38 MiB |
| DISTINCT | High-cardinality distinct (no ORDER BY) | 1.26 MiB |
| Full-text | Build index | 1.39 MiB |
| JSON inverted | Build index | 1.26 MiB |
| SELECT | Full scan | 1.23 MiB |
| Join | Inner join, small-large | 1.00 MiB |
| Join | Left join, unmatched rows | 869 KiB |
| Maintenance | VACUUM small | 754 KiB |
| Subquery | IN list | 559 KiB |

### Overflow Page INSERT

One INSERT per b.N iteration, auto-commit. "inline" (512 B) fits within `MaxInlineVarchar` and uses no overflow pages; it is the control group. `1pg`/`4pg`/`16pg` spill to 1, 4, and 16 overflow pages respectively.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 13.5 µs | 45.8 µs | 0.29× | 4.2 KiB | 144 B | 28 / 7 |
| 1pg (~4 KB) | 35.5 µs | 64.2 µs | 0.55× | 19.6 KiB | 144 B | 45 / 7 |
| 4pg (~16 KB) | 68.2 µs | 105.4 µs | 0.65× | 57.8 KiB | 144 B | 72 / 7 |
| 16pg (~64 KB) | 194.3 µs | 257.1 µs | 0.76× | 212 KiB | 144 B | 177 / 7 |

### Overflow Page Point Read

100 rows seeded once; each b.N iteration does one `SELECT … WHERE id = ?` and traverses the overflow chain.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 4.32 µs | 3.15 µs | 1.37× | 2.4 KiB | 1.5 KiB | 40 / 18 |
| 1pg (~4 KB) | 5.98 µs | 4.96 µs | 1.21× | 9.9 KiB | 8.5 KiB | 41 / 18 |
| 4pg (~16 KB) | 8.70 µs | 15.4 µs | 0.57× | 33.9 KiB | 32.5 KiB | 41 / 18 |
| 16pg (~64 KB) | 16.8 µs | 41.7 µs | 0.40× | 130 KiB | 128 KiB | 41 / 18 |

### Overflow Page Full Scan

50 rows seeded once; each b.N iteration scans all 50 rows reading every overflow chain.

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 17.4 µs | 26.9 µs | 0.65× | 27.1 KiB | 51.4 KiB | 123 / 214 |
| 1pg (~4 KB) | 74.9 µs | 86.4 µs | 0.87× | 402 KiB | 401 KiB | 173 / 214 |
| 4pg (~16 KB) | 222.9 µs | 518.1 µs | 0.43× | 1.56 MiB | 1.56 MiB | 173 / 214 |
| 16pg (~64 KB) | 599.4 µs | 2.07 ms | 0.29× | 6.25 MiB | 6.25 MiB | 173 / 214 |

### Overflow Page UPDATE (in-place reuse, text + vector)

One row seeded; each b.N iteration updates the blob body. Old overflow chain is reused in-place — no free+alloc per page for same-size updates (text since 2026-06-14; vector since 2026-06-16).

| Blob size | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs (MiniSQL / SQLite) |
|---|---|---|---|---|---|---|
| inline (512 B) | 7.21 µs | 40.5 µs | 0.18× | 4.6 KiB | 176 B | 33 / 7 |
| 1pg (~4 KB) | 15.7 µs | 44.4 µs | 0.35× | 15.9 KiB | 176 B | 40 / 7 |
| 4pg (~16 KB) | 33.7 µs | 72.3 µs | 0.47× | 52.2 KiB | 176 B | 44 / 7 |
| 16pg (~64 KB) | 95.4 µs | 137.4 µs | 0.69× | 201 KiB | 176 B | 60 / 7 |

### Good Next Optimisation Targets

- HNSW build allocation counts are stable; build memory remains the largest broad-suite outlier at 10k rows (229 MiB for dims768). Single-iteration benchmarks carry high variance — compare only against a fresh count=3 run.
- Full-text and JSON build paths still allocate ~1.3–1.4 MiB per operation and remain the most relevant inverted-index targets.
- GROUP BY / HAVING memory gap vs SQLite (9.6× / 13.3×) is largely structural: Go's runtime map has higher per-entry overhead than SQLite's C hash table. Further reduction requires a custom open-address hash table — low ROI at this stage.
- DISTINCT high cardinality without ORDER BY (1.26 MiB vs SQLite 586 KiB, 2.2×): streaming hash-set path; the gap is structural — SQLite sorts/hashes on the C side and delivers rows lazily, avoiding upfront Go heap allocation.
- DISTINCT + ORDER BY high cardinality (4.53 MiB vs SQLite 586 KiB): ORDER BY requires upfront materialization of all rows before sorting. The sort-then-adjacent-dedup optimization removes hash-set overhead from deduplication, but the dominant cost is O(N) row materialization — unavoidable without disk-backed sort (see ROADMAP).
- VACUUM is much improved after streaming row copy, though it still allocates far more than SQLite because it rebuilds the compacted MiniSQL database through normal table/index write paths.
