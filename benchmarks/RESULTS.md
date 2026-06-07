# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `codex/profile-vacuum-small`  
**Command:** `make bench BENCH_COUNT=1` followed by `make bench-report`; HNSW tables refreshed with targeted `go test -tags bench -bench '^BenchmarkHNSW_...' -benchmem` runs; DISTINCT/subquery rows refreshed with `go test -tags bench -run '^$' -bench 'Benchmark(Distinct_HighCardinality|Subquery_InList)$' -benchmem -count=1 ./benchmarks/`; VACUUM row refreshed with `go test -tags bench -run '^$' -bench '^BenchmarkVacuum_Small$' -benchmem -count=1 ./benchmarks/`  
**GOMAXPROCS:** 10  

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime.

This file was refreshed from scratch after the latest DML allocation work, including transaction write-set inlining, auto-commit transaction reuse, direct prepared DML argument binding, and HNSW typed candidate-heap allocation reduction. DISTINCT/subquery rows were subsequently refreshed after RowView predicate fast paths and DISTINCT seen-set pre-sizing. VACUUM was refreshed after streaming row copy into the temporary compacted database.

---

## 2026-06-06 — Current Baseline

The results are grouped by benchmark family so each table can be read without horizontal scrolling. In comparison tables, a time ratio below `1.0×` means MiniSQL is faster than SQLite; above `1.0×` means slower.

### SELECT

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Point scan | 4.80 µs | 3.47 µs | 1.4× | 3.7 KiB | 679 B | 47 / 26 |
| Limit | 7.13 µs | 7.95 µs | 0.9× | 3.8 KiB | 1.7 KiB | 97 / 104 |
| Full scan | 3.65 ms | 5.12 ms | 0.7× | 1.23 MiB | 1.29 MiB | 79,822 / 99,758 |
| Count star | 6.30 µs | 9.65 µs | 0.7× | 2.6 KiB | 400 B | 27 / 13 |
| Index range scan | 670.66 µs | 752.23 µs | 0.9× | 83.8 KiB | 85.9 KiB | 5,539 / 6,581 |
| Secondary index, low selectivity | 1.76 ms | 2.67 ms | 0.7× | 314.9 KiB | 313.0 KiB | 24,920 / 29,886 |
| Secondary index, low selectivity limit | 7.90 µs | 8.32 µs | 1.0× | 4.1 KiB | 1.1 KiB | 89 / 64 |
| Range scan | 1.49 ms | 879.16 µs | 1.7× | 80.8 KiB | 85.9 KiB | 5,507 / 6,581 |

### INSERT, UPDATE, DELETE, and Constraints

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Insert single row | 10.88 µs | 43.95 µs | 0.2× | 2.1 KiB | 311 B | 31 / 12 |
| Insert batch | 369.66 µs | 253.47 µs | 1.5× | 133.6 KiB | 31.0 KiB | 2,637 / 1,300 |
| Insert prepared batch | 396.14 µs | 234.67 µs | 1.7× | 132.5 KiB | 31.0 KiB | 2,634 / 1,300 |
| Insert multi-values | 198.06 µs | 177.99 µs | 1.1× | 109.6 KiB | 25.2 KiB | 1,757 / 616 |
| Update by PK | 8.26 µs | 37.67 µs | 0.2× | 3.8 KiB | 263 B | 40 / 10 |
| Delete by PK | 16.80 µs | 78.92 µs | 0.2× | 3.3 KiB | 447 B | 55 / 19 |
| ON CONFLICT DO UPDATE | 7.42 µs | 36.39 µs | 0.2× | 1.6 KiB | 260 B | 31 / 10 |
| Foreign key insert | 11.32 µs | 43.07 µs | 0.3× | 1.9 KiB | 192 B | 28 / 8 |
| Foreign key delete cascade | 23.35 µs | 50.96 µs | 0.5× | 7.2 KiB | 128 B | 112 / 5 |

### Aggregates, DISTINCT, CTE, and Subquery

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| GROUP BY aggregate | 979.73 µs | 2.10 ms | 0.5× | 35.5 KiB | 3.5 KiB | 459 / 309 |
| HAVING filter | 741.87 µs | 1.95 ms | 0.4× | 27.4 KiB | 1.9 KiB | 264 / 111 |
| DISTINCT high cardinality | 4.10 ms | 8.44 ms | 0.5× | 1.26 MiB | 586.3 KiB | 40,093 / 40,010 |
| CTE materialise | 793.64 µs | 437.25 µs | 1.8× | 6.6 KiB | 400 B | 86 / 13 |
| Subquery IN list | 4.03 ms | 5.07 ms | 0.8× | 715.8 KiB | 234.7 KiB | 20,200 / 20,010 |

### Joins

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Inner join, small-large | 5.68 ms | 4.66 ms | 1.2× | 1.00 MiB | 1.07 MiB | 79,855 / 99,757 |
| Inner join, low selectivity | 108.11 µs | 735.45 µs | 0.1× | 21.1 KiB | 11.3 KiB | 1,198 / 1,009 |
| Left join, unmatched rows | 3.64 ms | 4.13 ms | 0.9× | 869.4 KiB | 708.2 KiB | 79,643 / 70,157 |

### Full-Text Inverted Index

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| Build index | 2.95 ms | 2.01 ms | 1.5× | 1.39 MiB | 392 B | 12,295 / 20 |
| Insert with index | 36.43 µs | 87.09 µs | 0.4× | 12.8 KiB | 271 B | 132 / 10 |
| Search single term, rare | 4.57 µs | 6.54 µs | 0.7× | 2.4 KiB | 408 B | 41 / 13 |
| Search single term, medium | 4.14 µs | 7.61 µs | 0.5× | 2.4 KiB | 408 B | 41 / 13 |
| Search single term, common | 4.24 µs | 61.16 µs | 0.1× | 2.4 KiB | 424 B | 43 / 15 |
| Search multi-term AND | 14.93 µs | 33.26 µs | 0.4× | 11.4 KiB | 408 B | 62 / 13 |
| Search phrase | 15.61 µs | 24.50 µs | 0.6× | 26.3 KiB | 416 B | 278 / 14 |
| Update with index | 35.41 µs | 97.40 µs | 0.4× | 18.0 KiB | 291 B | 187 / 12 |
| Delete with index | 48.01 µs | 138.41 µs | 0.3× | 17.4 KiB | 135 B | 175 / 6 |

### Full-Text MiniSQL-Only Checks

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Search after deletes | 81.57 µs | 11.1 KiB | 49 |

### JSON Inverted Index DML

| Benchmark | Time | Memory | Allocs |
|---|---|---|---|
| Build index | 1.93 ms | 1.26 MiB | 26,669 |
| Insert with index | 48.91 µs | 59.6 KiB | 146 |
| Contains after deletes | 59.19 µs | 19.2 KiB | 77 |
| Update with index | 5.97 µs | 4.2 KiB | 46 |
| Delete with index | 112.67 µs | 26.6 KiB | 150 |

### JSON Contains Comparisons

| Predicate | MiniSQL indexed | MiniSQL sequential | SQLite JSON scan | SQLite expression index |
|---|---|---|---|---|
| Key/value | 16.39 µs / 7.8 KiB | 1.90 ms / 1.94 MiB | 712.05 µs / 424 B | 26.02 µs / 424 B |
| Object subset | 28.70 µs / 8.8 KiB | 1.98 ms / 1.94 MiB | 730.76 µs / 424 B | 121.03 µs / 424 B |

### Maintenance and Explain

| Benchmark | MiniSQL time | SQLite time | Time ratio | MiniSQL memory | SQLite memory | Allocs |
|---|---|---|---|---|---|---|
| VACUUM small | 2.34 ms | 556.88 µs | 4.2× | 752.5 KiB | 91 B | 6,982 / 4 |
| WAL checkpoint | 194.44 µs | 104.85 µs | 1.9× | 3.3 KiB | 440 B | 37 / 12 |
| EXPLAIN | 5.46 µs | 1.21 µs | 4.5× | 6.0 KiB | 680 B | 55 / 18 |

### HNSW Build Index

| Dims | Rows | Time | Memory | Allocs |
|---|---|---|---|---|
| 3 | 1000 | 658.29 ms | 4.81 MiB | 34,945 |
| 3 | 10000 | 8.88 s | 120.2 MiB | 433,369 |
| 128 | 1000 | 785.49 ms | 6.30 MiB | 36,263 |
| 128 | 10000 | 23.65 s | 134.7 MiB | 432,720 |
| 768 | 1000 | 1.19 s | 13.61 MiB | 37,226 |
| 768 | 10000 | 29.12 s | 208.1 MiB | 442,866 |

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
| HNSW | Build index, dims768, 10k rows | 208.1 MiB |
| HNSW | Build index, dims128, 10k rows | 134.7 MiB |
| HNSW | Build index, dims3, 10k rows | 120.2 MiB |
| Full-text | Build index | 1.39 MiB |
| DISTINCT | High-cardinality distinct | 1.26 MiB |
| JSON inverted | Build index | 1.26 MiB |
| SELECT | Full scan | 1.23 MiB |
| Join | Inner join, small-large | 1.00 MiB |
| Maintenance | VACUUM small | 752.5 KiB |
| Subquery | IN list | 715.8 KiB |

### Good Next Optimisation Targets

- HNSW build allocation growth is much lower after typed candidate heaps, but it remains the largest broad-suite outlier at 10k rows.
- Full-text and JSON build paths still allocate multiple MiB per operation and remain the most relevant inverted-index targets.
- DISTINCT and subquery materialisation are improved after RowView predicate fast paths and DISTINCT seen-set pre-sizing, but DISTINCT remains near the MiB/op outlier group.
- VACUUM is much improved after streaming row copy, though it still allocates far more than SQLite because it rebuilds the compacted MiniSQL database through normal table/index write paths.
