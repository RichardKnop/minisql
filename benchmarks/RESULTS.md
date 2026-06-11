# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `go test -tags bench ./benchmarks/ -run='^$' -bench='...' -benchmem -count=3` for each family; HNSW rows from a previous run (unchanged — HNSW build takes 50+ s per sub-benchmark). All other rows refreshed 2026-06-11.  
**GOMAXPROCS:** 10

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`) median of 3 runs; memory figures are heap allocations reported by the Go runtime.

This file was refreshed after Tier-1 point-scan optimizations (2026-06-11): `Statement.Clone()` now shares the Fields slice instead of deep-copying it when SELECT field expressions contain no placeholders; `cachedSelectedFields` is precomputed once at `PrepareStatement` time; and `QueryPlan.CachedFieldIndexes`/`CachedResultColumns` cache the `rowViewProjectionPlan` result for prepared statements. Combined effect: point scan −31% time, −33% heap (47 → 42 allocs/op, 3.7 KiB → 2.5 KiB/op, ratio vs SQLite 1.5× → 1.3×).

Prior milestone (2026-06-07): GROUP BY + JOIN support addition, `groupOrder` slice removal from `groupByAccumulator`, `computeGroupValues` refactor. GROUP BY / HAVING memory improved (was 35.5 / 27.4 KiB, then 33.6 / 25.6 KiB) after eliminating the redundant per-group string copy. Added `DISTINCT + ORDER BY indexed` row to capture the streaming adjacent-compare dedup path.

---

## 2026-06-11 — Tier-1 Optimization

The results are grouped by benchmark family so each table can be read without horizontal scrolling. In comparison tables, a time ratio below `1.0×` means MiniSQL is faster than SQLite; above `1.0×` means slower.

### SELECT

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Point scan | 4.40 µs | 3.40 µs | 1.3× | 2.5 KiB | 679 B | 42 / 26 |
| Limit | 8.77 µs | 9.66 µs | 0.9× | 3.8 KiB | 1.7 KiB | 97 / 104 |
| Full scan | 3.60 ms | 5.00 ms | 0.7× | 1.29 MiB | 1.36 MiB | 79,822 / 99,758 |
| Count star | 7.65 µs | 10.93 µs | 0.7× | 2.6 KiB | 400 B | 27 / 13 |
| Index range scan | 1.11 ms | 879.6 µs | 1.3× | 83.8 KiB | 85.9 KiB | 5,539 / 6,581 |
| Secondary index, low selectivity | 2.01 ms | 3.14 ms | 0.6× | 315.0 KiB | 313.0 KiB | 24,920 / 29,886 |
| Secondary index, low selectivity limit | 9.41 µs | 9.30 µs | 1.0× | 4.1 KiB | 1.1 KiB | 89 / 64 |
| Range scan | 879.6 µs | 1.01 ms | 0.9× | 80.8 KiB | 85.9 KiB | 5,511 / 6,581 |

### INSERT, UPDATE, DELETE, and Constraints

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Insert single row | 15.0 µs | 66.0 µs | 0.2× | 2.1 KiB | 311 B | 31 / 12 |
| Insert batch | 430 µs | 291 µs | 1.5× | 133.6 KiB | 31.0 KiB | 2,632 / 1,299 |
| Insert prepared batch | 420 µs | 284 µs | 1.5× | 132.4 KiB | 31.0 KiB | 2,633 / 1,299 |
| Insert multi-values | 226 µs | 210 µs | 1.1× | 109.5 KiB | 25.2 KiB | 1,756 / 616 |
| Update by PK | 9.81 µs | 47.98 µs | 0.2× | 3.8 KiB | 263 B | 40 / 10 |
| Delete by PK | 20.5 µs | 129 µs | 0.2× | 3.3 KiB | 447 B | 55 / 19 |
| ON CONFLICT DO UPDATE | 9.08 µs | 43.20 µs | 0.2× | 1.6 KiB | 259 B | 31 / 10 |
| Foreign key insert | 12.28 µs | 57.78 µs | 0.2× | 1.9 KiB | 191 B | 28 / 8 |
| Foreign key delete cascade | 51.50 µs | 83.25 µs | 0.6× | 7.2 KiB | 128 B | 111 / 5 |

### Aggregates, DISTINCT, CTE, and Subquery

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| GROUP BY aggregate | 979 µs | 2.50 ms | 0.4× | 34.4 KiB | 3.6 KiB | 458 / 309 |
| HAVING filter | 808 µs | 2.17 ms | 0.4× | 26.2 KiB | 2.0 KiB | 263 / 111 |
| DISTINCT high cardinality | 3.40 ms | 6.70 ms | 0.5× | 1.27 MiB | 586.3 KiB | 40,093 / 40,010 |
| DISTINCT + ORDER BY high cardinality | 4.50 ms | 6.10 ms | 0.7× | 4.53 MiB | 586.3 KiB | 90,097 / 40,010 |
| DISTINCT + ORDER BY indexed | 4.00 ms | 4.10 ms | 1.0× | 4.59 MiB | 586.3 KiB | 60,079 / 40,010 |
| CTE materialise | 380.4 µs | 502.0 µs | 0.8× | 6.6 KiB | 400 B | 89 / 13 |
| Subquery IN list | 2.90 ms | 4.10 ms | 0.7× | 559.0 KiB | 234.7 KiB | 15,197 / 20,010 |

### Joins

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Inner join, small-large | 6.60 ms | 5.80 ms | 1.1× | 1.03 MiB | 1.07 MiB | 79,855 / 99,757 |
| Inner join, low selectivity | 128 µs | 808 µs | 0.16× | 21.5 KiB | 11.6 KiB | 1,198 / 1,009 |
| Left join, unmatched rows | 4.30 ms | 4.90 ms | 0.9× | 890 KiB | 708.2 KiB | 79,643 / 70,157 |

### ORDER BY Disk Spill

MiniSQL-only sub-benchmarks on a 10 000-row table sorted by a `varchar(255)` email column.
`no-spill` uses `sort_mem_limit=0` (pure in-memory sort); `spill-64k` uses `sort_mem_limit=65536`,
which flushes the rows across ~8 sorted run files that are then N-way merged.
The ~11× overhead is dominated by temp-file I/O in `runWriter`/`runReader`/`externalSortMerge`
and is the primary target for future optimisation (e.g. buffered I/O, larger default batch size).

| Sub-benchmark | Time | Rows/op | Notes |
|---|---|---|---|
| no-spill | 5.2 ms | 10 000 | pure in-memory sort, baseline |
| spill-64k | 12.5 ms | 10 000 | after buffered I/O (64 KiB); was 55.9 ms unbuffered (~4.5× improvement) |

### Full-Text Inverted Index

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Build index | 4.34 ms | 2.71 ms | 1.6× | 1.43 MiB | 392 B | 12,359 / 20 |
| Insert with index | 41.71 µs | 112.5 µs | 0.4× | 13.0 KiB | 271 B | 133 / 10 |
| Search single term, rare | 6.64 µs | 7.65 µs | 0.9× | 2.4 KiB | 408 B | 41 / 13 |
| Search single term, medium | 6.85 µs | 8.73 µs | 0.8× | 2.4 KiB | 408 B | 41 / 13 |
| Search single term, common | 6.13 µs | 70.71 µs | 0.1× | 2.4 KiB | 424 B | 43 / 15 |
| Search multi-term AND | 20.59 µs | 39.08 µs | 0.5× | 11.4 KiB | 408 B | 62 / 13 |
| Search phrase | 27.43 µs | 26.53 µs | 1.0× | 26.3 KiB | 416 B | 278 / 14 |
| Update with index | 44.66 µs | 133.9 µs | 0.3× | 18.4 KiB | 290 B | 190 / 12 |
| Delete with index | 57.07 µs | 175.3 µs | 0.3× | 17.1 KiB | 135 B | 173 / 6 |

### Full-Text MiniSQL-Only Checks

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Search after deletes | 93.63 µs | 11.1 KiB | 49 |

### JSON Inverted Index DML

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Build index | 2.90 ms | 1.31 MiB | 26,670 |
| Insert with index | 62 µs | 47 KiB | 144 |
| Contains after deletes | 73 µs | 19.6 KiB | 76 |
| Update with index | 7.90 µs | 4.3 KiB | 46 |
| Delete with index | 139 µs | 27.3 KiB | 150 |

### JSON Contains Comparisons

| Predicate | MiniSQL indexed | MiniSQL sequential | SQLite JSON scan | SQLite expression index |
|---|---|---|---|---|
| Key/value | 20.7 µs / 7.9 KiB | 3.62 ms / 1.94 MiB | 858.0 µs / 424 B | 30.48 µs / 424 B |
| Object subset | 33.4 µs / 9.0 KiB | 3.58 ms / 1.94 MiB | 830.6 µs / 424 B | 140.7 µs / 424 B |

### Maintenance and Explain

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| VACUUM small | 2.15 ms | 583.7 µs | 3.7× | 769 KiB | 91 B | 6,977 / 4 |
| WAL checkpoint | 357.0 µs | 171.8 µs | 2.1× | 3.3 KiB | 441 B | 37 / 12 |
| EXPLAIN | 7.45 µs | 1.75 µs | 4.3× | 6.0 KiB | 680 B | 55 / 18 |

### HNSW Build Index

| Dims | Rows | Time | Memory | Allocs |
|---|---|---|---|---|
| 3 | 1000 | 719.77 ms | 7.07 MiB | 25,895 |
| 3 | 10000 | 10.70 s | 140.8 MiB | 337,309 |
| 128 | 1000 | 1.25 s | 8.59 MiB | 26,809 |
| 128 | 10000 | 32.15 s | 136.7 MiB | 339,033 |
| 768 | 1000 | 3.01 s | 13.80 MiB | 26,586 |
| 768 | 10000 | 53.96 s | 207.7 MiB | 345,509 |

### HNSW ANN Search

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 29.80 µs | 13.4 KiB | 57 |
| 3 | 1000 | 10 | 35.80 µs | 17.1 KiB | 125 |
| 3 | 10000 | 1 | 41.75 µs | 22.7 KiB | 59 |
| 3 | 10000 | 10 | 50.39 µs | 26.4 KiB | 131 |
| 128 | 1000 | 1 | 176.17 µs | 41.7 KiB | 62 |
| 128 | 1000 | 10 | 183.74 µs | 54.3 KiB | 143 |
| 128 | 10000 | 1 | 376.77 µs | 77.8 KiB | 67 |
| 128 | 10000 | 10 | 386.71 µs | 90.3 KiB | 148 |
| 768 | 1000 | 1 | 696.54 µs | 46.7 KiB | 62 |
| 768 | 1000 | 10 | 722.59 µs | 104.2 KiB | 138 |
| 768 | 10000 | 1 | 2.03 ms | 82.8 KiB | 67 |
| 768 | 10000 | 10 | 2.22 ms | 140.3 KiB | 148 |

### HNSW Sequential Scan

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 665.84 µs | 664.7 KiB | 10,819 |
| 128 | 1000 | 1 | 8.35 ms | 5.93 MiB | 11,824 |
| 768 | 1000 | 1 | 46.36 ms | 31.44 MiB | 11,848 |

### HNSW Insert Overhead

| Dims | With index | No index | Time ratio |
|---|---|---|---|
| 3 | 1.24 ms / 220.6 KiB | 21.38 µs / 6.9 KiB | 57.9× |
| 128 | 3.31 ms / 232.1 KiB | 21.60 µs / 7.4 KiB | 153.4× |
| 768 | 12.67 ms / 314.3 KiB | 22.62 µs / 9.8 KiB | 559.9× |

### Memory Outliers

| Area | Benchmark | MiniSQL memory |
|---|---|---|
| HNSW | Build index, dims768, 10k rows | 207.7 MiB |
| HNSW | Build index, dims128, 10k rows | 136.7 MiB |
| HNSW | Build index, dims3, 10k rows | 140.8 MiB |
| Full-text | Build index | 1.43 MiB |
| DISTINCT | High-cardinality distinct (no ORDER BY) | 1.27 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY | 4.53 MiB |
| JSON inverted | Build index | 1.31 MiB |
| SELECT | Full scan | 1.29 MiB |
| Join | Inner join, small-large | 1.03 MiB |
| Join | Left join, unmatched rows | 890 KiB |
| Maintenance | VACUUM small | 769 KiB |
| Subquery | IN list | 559.0 KiB |

### Good Next Optimisation Targets

- HNSW build allocation counts are much lower after typed candidate heaps and per-node neighbor backing allocation, but build memory remains the largest broad-suite outlier at 10k rows.
- Full-text and JSON build paths still allocate multiple MiB per operation and remain the most relevant inverted-index targets.
- GROUP BY / HAVING memory gap vs SQLite (9.6× / 13.1×) is largely structural: Go's runtime map has higher per-entry overhead than SQLite's C hash table. The `groupOrder` redundancy was removed (saved ~1.6–2 KiB/op); arena interning was tried and rejected (old backing arrays accumulate in GC via map string references). Further reduction requires a custom open-address hash table — low ROI at this stage.
- DISTINCT high cardinality without ORDER BY (1.27 MiB vs SQLite 586 KiB, 2.2×): streaming hash-set path; the gap is structural — SQLite sorts/hashes on the C side and delivers rows lazily, avoiding upfront Go heap allocation.
- DISTINCT + ORDER BY high cardinality (4.53 MiB vs SQLite 586 KiB): ORDER BY requires upfront materialization of all rows before sorting, which doubles alloc count vs the no-ORDER-BY streaming path. The sort-then-adjacent-dedup optimization removes the hash-set overhead from deduplication, but the dominant cost is the O(N) row materialization for sorting — unavoidable without a disk-backed sort (see ROADMAP).
- DISTINCT + ORDER BY indexed (3.99 ms / 4.59 MiB / 60,079 allocs): when an index covers ORDER BY, streaming adjacent-compare dedup (`newDistinctAdjacentRowViewIteratorFactory`) eliminates the hash set, saving ~30,018 allocs/op vs the in-memory sort path. Still limited by per-row materialization overhead from the `database/sql` layer.
- VACUUM is much improved after streaming row copy, though it still allocates far more than SQLite because it rebuilds the compacted MiniSQL database through normal table/index write paths.
