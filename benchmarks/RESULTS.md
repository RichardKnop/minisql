# Benchmark Results

## 2026-06-02 — Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `codex/json-streaming-rowid-merge`
**Command:** `make bench BENCH_COUNT=1`
**GOMAXPROCS:** 10  

SQLite comparisons use the `sqlite` driver compiled into the same test binary. All minisql benchmarks run against a fresh temp-file database per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime.

---

## Aggregate / GROUP BY

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| GroupBy_Aggregate (100 groups) | 892 µs | 2.14 ms | 37.2 KiB | 3.53 KiB | 459 | 309 |
| Having_Filter (100 groups) | 724 µs | 1.91 ms | 28.7 KiB | 1.94 KiB | 264 | 111 |
| Distinct_HighCardinality (10K rows) | 3.05 ms | 5.75 ms | 1.69 MiB | 586 KiB | 40,141 | 40,010 |

---

## DELETE / Foreign Keys

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Delete_ByPK | 24.6 µs | 83.6 µs | 5.08 KiB | 447 B |
| ForeignKey_Insert | 17.2 µs | 48.7 µs | 2.82 KiB | 192 B |
| ForeignKey_DeleteCascade | 47.5 µs | 52.0 µs | 9.81 KiB | 128 B |

---

## INSERT

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Insert_SingleRow | 14.8 µs | 45.5 µs | 3.08 KiB | 311 B | 35 | 12 |
| Insert_Batch (100 rows/op) | 546 µs | 305 µs | 180 KiB | 31.0 KiB | 2,752 | 1,299 |
| Insert_PreparedBatch (100 rows/op) | 440 µs | 266 µs | 178 KiB | 31.0 KiB | 2,751 | 1,300 |
| Insert_MultiValues (100 rows/op) | 376 µs | 189 µs | 156 KiB | 25.2 KiB | 1,861 | 616 |

---

## Full-Text Search (minisql log-structured vs SQLite FTS5)

**Inverted-index sections refreshed:** 2026-06-02 on `codex/json-streaming-rowid-merge` with the full `make bench BENCH_COUNT=1` suite.

**Latest memory improvement:** streaming row-ID and positional document-frequency merges reduced `JSONInverted_Contains_AfterDeletes` from **82.7 KiB/op** to **19.2 KiB/op** and `FullText_Search_AfterDeletes` from **111 KiB/op** to **11.1 KiB/op**.

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| FullText_BuildIndex (1,000 docs/op) | 3.16 ms | 2.10 ms | 1.51 MiB | 392 B | 16,317 | 20 |
| FullText_Insert_WithIndex | 47.2 µs | 97.8 µs | 15.6 KiB | 271 B | 137 | 10 |
| FullText_Search_SingleTerm/rare | 4.40 µs | 6.71 µs | 2.31 KiB | 408 B | 41 | 13 |
| FullText_Search_SingleTerm/medium | 4.03 µs | 7.78 µs | 2.31 KiB | 408 B | 41 | 13 |
| FullText_Search_SingleTerm/common | 4.16 µs | 61.3 µs | 2.32 KiB | 424 B | 43 | 15 |
| FullText_Search_MultiTermAND | 14.8 µs | 33.9 µs | 11.3 KiB | 408 B | 62 | 13 |
| FullText_Search_Phrase | 15.8 µs | 25.0 µs | 26.2 KiB | 416 B | 278 | 14 |
| FullText_Search_AfterDeletes | 82.8 µs | — | 11.1 KiB | — | 49 | — |
| FullText_Update_WithIndex | 45.5 µs | 113 µs | 21.9 KiB | 290 B | 193 | 12 |
| FullText_Delete_WithIndex | 63.6 µs | 146 µs | 22.5 KiB | 135 B | 186 | 6 |

---

## JSON Inverted Index (minisql only, with SQLite expression-index comparison where available)

| Benchmark | minisql indexed | comparison | minisql B/op | minisql allocs |
|---|---:|---:|---:|---:|
| JSONInverted_BuildIndex (1,000 docs/op) | 4.22 ms | — | 3.17 MiB | 63,010 |
| JSONInverted_Insert_WithIndex | 61.2 µs | — | 51.6 KiB | 185 |
| JSONInverted_Contains_KeyValue (334 matches) | 15.3 µs | 26.4 µs (sqlite expr idx) | 7.73 KiB | 75 |
| JSONInverted_Contains_KeyValue seq scan | 1.90 ms | 696 µs (sqlite json scan) | 1.94 MiB | 38,068 |
| JSONInverted_Contains_ObjectSubset (334 matches) | 26.8 µs | 129 µs (sqlite expr idx) | 8.79 KiB | 115 |
| JSONInverted_Contains_AfterDeletes (167 matches) | 60.9 µs | — | 19.2 KiB | 78 |
| JSONInverted_Update_WithIndex | 6.36 µs | — | 4.55 KiB | 51 |
| JSONInverted_Delete_WithIndex | 137 µs | — | 33.0 KiB | 194 |

---

## JOINs

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Join_Inner_SmallLarge (10K rows/op) | 6.57 ms | 4.96 ms | 1.24 MiB | 1.07 MiB | 89,857 | 99,757 |
| Join_Inner_LowSelectivity (100 rows/op) | 116 µs | 789 µs | 23.6 KiB | 11.3 KiB | 1,298 | 1,009 |
| Join_Left_UnmatchedRows (10K rows/op) | 3.78 ms | 4.22 ms | 878 KiB | 708 KiB | 79,744 | 70,157 |

---

## Maintenance

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Vacuum_Small | 20.0 ms | 297 µs | 1.49 MiB | 89 B |
| WAL_Checkpoint | 235 µs | 124 µs | 71.4 KiB | 440 B |

---

## Query Planning

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Explain | 5.21 µs | 1.23 µs | 5.96 KiB | 680 B |

---

## SELECT

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Select_PointScan | 4.89 µs | 3.44 µs | 3.69 KiB | 679 B | 49 | 26 |
| Select_Limit | 6.90 µs | 8.04 µs | 3.72 KiB | 1.69 KiB | 97 | 104 |
| Select_FullScan (10K rows/op) | 3.70 ms | 5.47 ms | 1.24 MiB | 1.29 MiB | 79,823 | 99,758 |
| Select_CountStar | 5.76 µs | 9.98 µs | 2.49 KiB | 400 B | 27 | 13 |
| Select_IndexRangeScan | 819 µs | 754 µs | 111.7 KiB | 85.9 KiB | 6,641 | 6,581 |
| Select_SecondaryIndex_LowSelectivity (5K rows/op) | 2.14 ms | 2.87 ms | 436 KiB | 313 KiB | 29,921 | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit | 9.39 µs | 8.44 µs | 4.34 KiB | 1.07 KiB | 101 | 64 |
| Select_RangeScan | 1.47 ms | 878 µs | 83.6 KiB | 85.9 KiB | 5,508 | 6,581 |

---

## CTE / Subquery

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| CTE_Materialise | 799 µs | 439 µs | 7.93 KiB | 400 B | 85 | 13 |
| Subquery_InList (5K rows/op) | 4.59 ms | 3.73 ms | 872 KiB | 235 KiB | 35,100 | 20,010 |

---

## ON CONFLICT / UPDATE

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| OnConflict_DoUpdate | 9.50 µs | 77.4 µs | 2.49 KiB | 260 B |
| Update_ByPK | 10.5 µs | 38.8 µs | 4.98 KiB | 263 B |

---

## HNSW Vector Index

### Build index (CREATE HNSW INDEX over pre-seeded table)

| Corpus | Dims | ns/op | rows/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 674 ms | 1,000 | 195 MiB | 7,769,153 |
| 10,000 | 3 | 10.4 s | 10,000 | 2.26 GiB | 94,953,606 |
| 1,000 | 128 | 793 ms | 1,000 | 223 MiB | 9,517,170 |
| 10,000 | 128 | 27.3 s | 10,000 | 3.54 GiB | 178,021,702 |
| 1,000 | 768 | 1.51 s | 1,000 | 243 MiB | 10,137,696 |
| 10,000 | 768 | 33.6 s | 10,000 | 3.74 GiB | 186,181,981 |

### ANN search (VEC_L2 ORDER BY … LIMIT k, routed through HNSW index)

| Corpus | Dims | top-k | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 1 | 37.1 µs | 17.3 KiB | 308 |
| 1,000 | 3 | 10 | 44.7 µs | 21.1 KiB | 385 |
| 10,000 | 3 | 1 | 50.4 µs | 26.4 KiB | 298 |
| 10,000 | 3 | 10 | 56.1 µs | 30.2 KiB | 379 |
| 1,000 | 128 | 1 | 206 µs | 49.1 KiB | 541 |
| 1,000 | 128 | 10 | 206 µs | 61.9 KiB | 631 |
| 10,000 | 128 | 1 | 363 µs | 87.0 KiB | 663 |
| 10,000 | 128 | 10 | 372 µs | 99.8 KiB | 753 |
| 1,000 | 768 | 1 | 744 µs | 54.7 KiB | 577 |
| 1,000 | 768 | 10 | 764 µs | 112 KiB | 662 |
| 10,000 | 768 | 1 | 1.47 ms | 92.3 KiB | 678 |
| 10,000 | 768 | 10 | 1.50 ms | 150 KiB | 768 |

### Sequential scan (brute-force, no HNSW index — baseline for speedup comparison)

| Corpus | Dims | top-k | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 1 | 662 µs | 664 KiB | 10,819 |
| 1,000 | 128 | 1 | 8.43 ms | 5.93 MiB | 11,824 |
| 1,000 | 768 | 1 | 46.5 ms | 31.4 MiB | 11,848 |

**HNSW speedup vs sequential scan (top-1, n=1,000):**

| Dims | Sequential | HNSW | Speedup |
|---:|---:|---:|---:|
| 3 | 662 µs | 37.1 µs | **18×** |
| 128 | 8.43 ms | 206 µs | **41×** |
| 768 | 46.5 ms | 744 µs | **62×** |

### Online INSERT overhead (single row, 1,000-row starting corpus)

| Dims | With HNSW index | No index | Overhead |
|---:|---:|---:|---:|
| 3 | 1.52 ms | 29.7 µs | **51×** |
| 128 | 3.57 ms | 29.0 µs | **123×** |
| 768 | 11.9 ms | 27.7 µs | **428×** |

The overhead is dominated by HNSW graph traversal at `efConstruction=200` and the page writes for dirty neighbour nodes — both inherent to the algorithm.

---

## Memory Outliers

Largest per-operation heap consumers (minisql only):

- `HNSW_BuildIndex` dims768/n10000: **3.74 GiB/op** — O(N²) distance matrix during greedy layer construction; dominated by neighbour-list allocations across 10K nodes
- `HNSW_BuildIndex` dims128/n10000: **3.54 GiB/op** — same structural cost, lower per-vector overhead
- `JSONInverted_BuildIndex`: **3.17 MiB/op** — JSON decoding plus in-memory term→row-ID map during bulk build
- `Distinct_HighCardinality`: **1.69 MiB/op** — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: **1.51 MiB/op** — token-position accumulation and posting payload encoding during log-structured segment build
- `Vacuum_Small`: **1.49 MiB/op** — full copy-compact-swap; structural cost
- `Join_Inner_SmallLarge`: **1.24 MiB/op** — INLJ result materialization for 10K matched rows
- `Select_FullScan`: **1.24 MiB/op** — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: **33.0 KiB/op** — reduced by bulk row-ID foldback; no longer a major memory outlier
- `Insert_Batch` / `PreparedBatch`: **~179 KiB/op** — ~1.8 KiB/row vs SQLite's 310 B; remaining cost is per-row clone + B-tree page I/O

## Good Next Optimisation Targets

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Reduce JSON decode/tree-walk allocations during inverted-index build and maintenance
- Add a direct compacting JSON term scanner only if a prototype can preserve duplicate-key semantics without regressing allocations
- Redesign inverted segment compaction around a direct page writer or reusable buffers before replacing the current nested-map reducer
- Reduce per-row clone overhead in `Insert_Batch` (~1.8 KiB/row vs SQLite's 310 B)
