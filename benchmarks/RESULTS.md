# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `go test -tags bench ./benchmarks/ -run='^$' -bench='...' -benchmem -count=3` run as a single uninterrupted 43-minute pass (all families together). All rows refreshed 2026-06-11.  
**GOMAXPROCS:** 10

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`) median of 3 runs; memory figures are heap allocations reported by the Go runtime.

This file was refreshed after Tier-2 point-scan optimizations (2026-06-11): `conditionsCanSkipFolding` avoids folding work for bound-parameter WHERE clauses; `buildColumnNames` precomputes `Rows.Columns()` names once at construction; `RuntimeIndexKeys [][]any` decouples per-execution index key injection from the static `Scan` struct (avoids a 264-byte-per-scan copy on the rehydration hot path); read-only transaction object reused via a single-slot cache under the existing `mu` lock instead of allocating a new `Transaction` on every read query. Net effect on point scan: −20% heap (2.5 KiB→2.0 KiB/op), −7% alloc count (42→39/op), −2% time.

Tier-1 (same date): `Statement.Clone()` shares the Fields slice instead of deep-copying it when SELECT field expressions contain no placeholders; `cachedSelectedFields` precomputed once at `PrepareStatement` time; `QueryPlan.CachedFieldIndexes`/`CachedResultColumns` cache the `rowViewProjectionPlan` result for prepared statements. Combined effect: −31% heap, −11% allocs on point scan.

Prior milestone (2026-06-07): GROUP BY + JOIN support addition, `groupOrder` slice removal from `groupByAccumulator`, `computeGroupValues` refactor. GROUP BY / HAVING memory improved after eliminating the redundant per-group string copy.

---

## 2026-06-11 — Tier-2 Optimization

The results are grouped by benchmark family so each table can be read without horizontal scrolling. In comparison tables, a time ratio below `1.0×` means MiniSQL is faster than SQLite; above `1.0×` means slower.

### SELECT

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Point scan | 4.33 µs | 3.44 µs | 1.3× | 2.0 KiB | 679 B | 39 / 26 |
| Limit | 6.97 µs | 8.21 µs | 0.85× | 2.8 KiB | 1.7 KiB | 92 / 104 |
| Full scan | 3.75 ms | 5.30 ms | 0.71× | 1.23 MiB | 1.33 MiB | 79,820 / 99,758 |
| Count star | 6.54 µs | 10.01 µs | 0.65× | 2.4 KiB | 400 B | 26 / 13 |
| Index range scan | 703 µs | 783 µs | 0.90× | 82.9 KiB | 85.9 KiB | 5,534 / 6,581 |
| Secondary index, low selectivity | 1.77 ms | 2.86 ms | 0.62× | 314 KiB | 313 KiB | 24,913 / 29,886 |
| Secondary index, low selectivity limit | 7.82 µs | 8.48 µs | 0.92× | 3.2 KiB | 1.1 KiB | 82 / 64 |
| Range scan | 805 µs | 895 µs | 0.90× | 79.7 KiB | 85.9 KiB | 5,504 / 6,581 |

### INSERT, UPDATE, DELETE, and Constraints

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Insert single row | 13.2 µs | 103 µs | 0.13× | 2.1 KiB | 311 B | 31 / 12 |
| Insert batch | 360 µs | 241 µs | 1.49× | 133 KiB | 31.1 KiB | 2,650 / 1,308 |
| Insert prepared batch | 358 µs | 249 µs | 1.44× | 132 KiB | 31.1 KiB | 2,649 / 1,307 |
| Insert multi-values | 201 µs | 192 µs | 1.05× | 109.5 KiB | 25.2 KiB | 1,763 / 622 |
| Update by PK | 8.08 µs | 39.4 µs | 0.20× | 3.5 KiB | 263 B | 38 / 10 |
| Delete by PK | 14.6 µs | 58.1 µs | 0.25× | 3.0 KiB | 447 B | 53 / 19 |
| ON CONFLICT DO UPDATE | 7.69 µs | 56.0 µs | 0.14× | 1.6 KiB | 260 B | 31 / 10 |
| Foreign key insert | 10.96 µs | 46.4 µs | 0.24× | 1.9 KiB | 192 B | 28 / 8 |
| Foreign key delete cascade | 22.9 µs | 57.8 µs | 0.40× | 7.1 KiB | 128 B | 111 / 5 |

### Aggregates, DISTINCT, CTE, and Subquery

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| GROUP BY aggregate | 989 µs | 2.46 ms | 0.40× | 33.4 KiB | 3.5 KiB | 457 / 309 |
| HAVING filter | 814 µs | 2.26 ms | 0.36× | 25.3 KiB | 1.9 KiB | 262 / 111 |
| DISTINCT high cardinality | 3.37 ms | 6.42 ms | 0.52× | 1.26 MiB | 586.3 KiB | 40,092 / 40,010 |
| DISTINCT + ORDER BY high cardinality | 3.77 ms | 5.31 ms | 0.71× | 4.53 MiB | 586.3 KiB | 90,099 / 40,010 |
| DISTINCT + ORDER BY indexed | 2.89 ms | 3.44 ms | 0.84× | 4.38 MiB | 586.3 KiB | 60,079 / 40,010 |
| CTE materialise | 353 µs | 446 µs | 0.79× | 6.3 KiB | 400 B | 86 / 13 |
| Subquery IN list | 3.04 ms | 3.72 ms | 0.82× | 559 KiB | 234.7 KiB | 15,196 / 20,010 |

### Joins

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Inner join, small-large | 6.13 ms | 5.17 ms | 1.2× | 1.00 MiB | 1.07 MiB | 79,854 / 99,757 |
| Inner join, low selectivity | 113 µs | 765 µs | 0.15× | 21.0 KiB | 11.3 KiB | 1,198 / 1,009 |
| Left join, unmatched rows | 3.82 ms | 4.29 ms | 0.89× | 869 KiB | 708 KiB | 79,643 / 70,157 |

### ORDER BY Disk Spill

MiniSQL-only sub-benchmarks on a 10 000-row table sorted by a `varchar(255)` email column.
`no-spill` uses `sort_mem_limit=0` (pure in-memory sort); `spill-64k` uses `sort_mem_limit=65536`,
which flushes the rows across ~8 sorted run files that are then N-way merged.

| Sub-benchmark | Time | Rows/op | Notes |
|---|---|---|---|
| no-spill | 3.68 ms | 10 000 | pure in-memory sort, baseline |
| spill-64k | 9.94 ms | 10 000 | after buffered I/O (64 KiB); was 55.9 ms unbuffered (~5.6× improvement) |

### Full-Text Inverted Index

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Build index | 3.04 ms | 2.16 ms | 1.41× | 1.42 MiB | 392 B | 12,336 / 20 |
| Insert with index | 39.1 µs | 94.1 µs | 0.42× | 14.8 KiB | 271 B | 139 / 10 |
| Search single term, rare | 4.62 µs | 7.12 µs | 0.65× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, medium | 4.16 µs | 7.98 µs | 0.52× | 2.1 KiB | 408 B | 39 / 13 |
| Search single term, common | 4.36 µs | 64.2 µs | 0.07× | 2.1 KiB | 424 B | 41 / 15 |
| Search multi-term AND | 15.3 µs | 35.0 µs | 0.44× | 11.1 KiB | 408 B | 60 / 13 |
| Search phrase | 16.4 µs | 25.4 µs | 0.64× | 26.0 KiB | 416 B | 276 / 14 |
| Update with index | 37.2 µs | 113.4 µs | 0.33× | 17.7 KiB | 292 B | 189 / 12 |
| Delete with index | 92.6 µs | 210.5 µs | 0.44× | 81.4 KiB | 135 B | 908 / 6 |

### Full-Text MiniSQL-Only Checks

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Search after deletes | 84.4 µs | 10.9 KiB | 47 |

### JSON Inverted Index DML

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Build index | 2.11 ms | 1.26 MiB | 26,671 |
| Insert with index | 73.8 µs | 166 KiB | 154 |
| Contains after deletes | 62.0 µs | 19.0 KiB | 75 |
| Update with index | 6.1 µs | 4.0 KiB | 44 |
| Delete with index | 296 µs | 60.0 KiB | 153 |

### JSON Contains Comparisons

| Predicate | MiniSQL indexed | MiniSQL sequential | SQLite JSON scan | SQLite expression index |
|---|---|---|---|---|
| Key/value | 15.7 µs / 7.5 KiB | 1.99 ms / 1.94 MiB | 729 µs / 424 B | 27.6 µs / 424 B |
| Object subset | 27.4 µs / 8.6 KiB | 2.02 ms / 1.94 MiB | 784 µs / 424 B | 129 µs / 424 B |

### Maintenance and Explain

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| VACUUM small | 1.66 ms | 307 µs | 5.4× | 753 KiB | 88 B | 6,976 / 4 |
| WAL checkpoint | 204 µs | 102 µs | 2.0× | 3.3 KiB | 440 B | 37 / 12 |
| EXPLAIN | 5.36 µs | 1.25 µs | 4.3× | 5.5 KiB | 680 B | 51 / 18 |

### HNSW Build Index

| Dims | Rows | Time | Memory | Allocs |
|---|---|---|---|---|
| 3 | 1000 | 693.5 ms | 5.3 MiB | 26,133 |
| 3 | 10000 | 9.32 s | 120 MiB | 336,053 |
| 128 | 1000 | 826.6 ms | 6.9 MiB | 26,673 |
| 128 | 10000 | 24.0 s | 156 MiB | 347,261 |
| 768 | 1000 | 1.16 s | 13.7 MiB | 26,721 |
| 768 | 10000 | 30.1 s | 208 MiB | 346,050 |

### HNSW ANN Search

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 30.4 µs | 13.2 KiB | 55 |
| 3 | 1000 | 10 | 36.0 µs | 16.9 KiB | 123 |
| 3 | 10000 | 1 | 44.5 µs | 22.5 KiB | 57 |
| 3 | 10000 | 10 | 50.3 µs | 26.2 KiB | 129 |
| 128 | 1000 | 1 | 181 µs | 41.5 KiB | 60 |
| 128 | 1000 | 10 | 192 µs | 54.1 KiB | 141 |
| 128 | 10000 | 1 | 375 µs | 77.6 KiB | 65 |
| 128 | 10000 | 10 | 378 µs | 90.1 KiB | 146 |
| 768 | 1000 | 1 | 731 µs | 46.5 KiB | 60 |
| 768 | 1000 | 10 | 799 µs | 104.1 KiB | 136 |
| 768 | 10000 | 1 | 1.75 ms | 82.6 KiB | 65 |
| 768 | 10000 | 10 | 1.76 ms | 140.1 KiB | 146 |

### HNSW Sequential Scan

| Dims | Rows | Top K | Time | Memory | Allocs |
|---|---|---|---|---|---|
| 3 | 1000 | 1 | 682 µs | 664.8 KiB | 10,820 |
| 128 | 1000 | 1 | 8.40 ms | 5.93 MiB | 11,825 |
| 768 | 1000 | 1 | 45.1 ms | 31.44 MiB | 11,849 |

### HNSW Insert Overhead

| Dims | With index | No index | Time ratio |
|---|---|---|---|
| 3 | 1.01 ms / 222 KiB | 19.4 µs / 6.9 KiB | 52.1× |
| 128 | 3.00 ms / 225 KiB | 21.7 µs / 7.4 KiB | 138.2× |
| 768 | 11.5 ms / 253 KiB | 20.4 µs / 9.9 KiB | 563.7× |

### Memory Outliers

| Area | Benchmark | MiniSQL memory |
|---|---|---|
| HNSW | Build index, dims768, 10k rows | 208 MiB |
| HNSW | Build index, dims128, 10k rows | 156 MiB |
| HNSW | Build index, dims3, 10k rows | 120 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY | 4.53 MiB |
| DISTINCT | High-cardinality distinct + ORDER BY indexed | 4.38 MiB |
| DISTINCT | High-cardinality distinct (no ORDER BY) | 1.26 MiB |
| Full-text | Build index | 1.42 MiB |
| JSON inverted | Build index | 1.26 MiB |
| SELECT | Full scan | 1.23 MiB |
| Join | Inner join, small-large | 1.00 MiB |
| Join | Left join, unmatched rows | 869 KiB |
| Maintenance | VACUUM small | 753 KiB |
| Subquery | IN list | 559 KiB |

### Good Next Optimisation Targets

- HNSW build allocation counts are much lower after typed candidate heaps and per-node neighbor backing allocation, but build memory remains the largest broad-suite outlier at 10k rows.
- Full-text and JSON build paths still allocate multiple MiB per operation and remain the most relevant inverted-index targets.
- GROUP BY / HAVING memory gap vs SQLite (9.6× / 13.3×) is largely structural: Go's runtime map has higher per-entry overhead than SQLite's C hash table. The `groupOrder` redundancy was removed (saved ~1.6–2 KiB/op); arena interning was tried and rejected (old backing arrays accumulate in GC via map string references). Further reduction requires a custom open-address hash table — low ROI at this stage.
- DISTINCT high cardinality without ORDER BY (1.26 MiB vs SQLite 586 KiB, 2.2×): streaming hash-set path; the gap is structural — SQLite sorts/hashes on the C side and delivers rows lazily, avoiding upfront Go heap allocation.
- DISTINCT + ORDER BY high cardinality (4.53 MiB vs SQLite 586 KiB): ORDER BY requires upfront materialization of all rows before sorting, which doubles alloc count vs the no-ORDER-BY streaming path. The sort-then-adjacent-dedup optimization removes the hash-set overhead from deduplication, but the dominant cost is the O(N) row materialization for sorting — unavoidable without a disk-backed sort (see ROADMAP).
- DISTINCT + ORDER BY indexed (2.89 ms / 4.38 MiB / 60,079 allocs): when an index covers ORDER BY, streaming adjacent-compare dedup (`newDistinctAdjacentRowViewIteratorFactory`) eliminates the hash set, saving ~30,018 allocs/op vs the in-memory sort path. Still limited by per-row materialization overhead from the `database/sql` layer.
- VACUUM is much improved after streaming row copy, though it still allocates far more than SQLite because it rebuilds the compacted MiniSQL database through normal table/index write paths.
