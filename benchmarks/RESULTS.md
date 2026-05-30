# Benchmark Results

## 2026-05-30 — Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `./benchmarks.test -test.run '^$' -test.bench '.' -test.benchmem -test.count 1`  
**GOMAXPROCS:** 10  

SQLite comparisons use the `sqlite` driver compiled into the same test binary. All minisql benchmarks run against a fresh temp-file database per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime.

---

## Aggregate / GROUP BY

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| GroupBy_Aggregate (100 groups) | 887 µs | 2.12 ms | 37.3 KiB | 3.5 KiB | 459 | 309 |
| Having_Filter (100 groups) | 707 µs | 1.87 ms | 28.7 KiB | 1.9 KiB | 264 | 111 |
| Distinct_HighCardinality (10K rows) | 2.96 ms | 5.61 ms | 1.69 MiB | 586 KiB | 40,140 | 40,010 |

---

## DELETE / Foreign Keys

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Delete_ByPK | 22.5 µs | 81.2 µs | 5.3 KiB | 447 B |
| ForeignKey_Insert | 14.4 µs | 47.2 µs | 3.0 KiB | 192 B |
| ForeignKey_DeleteCascade | 45.6 µs | 51.3 µs | 10.1 KiB | 128 B |

---

## INSERT

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Insert_SingleRow | 15.0 µs | 46.1 µs | 3.3 KiB | 311 B | 35 | 12 |
| Insert_Batch (100 rows/op) | 372 µs | 229 µs | 194 KiB | 31.0 KiB | 2,755 | 1,301 |
| Insert_PreparedBatch (100 rows/op) | 356 µs | 228 µs | 193 KiB | 31.0 KiB | 2,754 | 1,300 |
| Insert_MultiValues (100 rows/op) | 209 µs | 174 µs | 172 KiB | 25.2 KiB | 1,875 | 616 |

---

## Full-Text Search (minisql log-structured vs SQLite FTS5)

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| FullText_BuildIndex (1,000 docs/op) | 3.02 ms | 1.95 ms | 1.66 MiB | 392 B | 16,375 | 20 |
| FullText_Insert_WithIndex | 48.5 µs | 87.3 µs | 22.4 KiB | 439 B | 178 | 18 |
| FullText_Search_SingleTerm/rare | 17.1 µs | 10.2 µs | 4.4 KiB | 392 B | 67 | 12 |
| FullText_Search_SingleTerm/medium | 16.7 µs | 11.6 µs | 4.4 KiB | 392 B | 67 | 12 |
| FullText_Search_SingleTerm/common | 17.3 µs | 64.0 µs | 4.4 KiB | 408 B | 69 | 14 |
| FullText_Search_MultiTermAND | 27.3 µs | 37.0 µs | 13.5 KiB | 392 B | 88 | 12 |
| FullText_Search_Phrase | 28.2 µs | 27.9 µs | 28.7 KiB | 400 B | 304 | 13 |
| FullText_Search_AfterDeletes | 86.2 µs | — | 77.7 KiB | — | 90 | — |
| FullText_Update_WithIndex | 45.6 µs | 94.6 µs | 25.9 KiB | 291 B | 214 | 12 |
| FullText_Delete_WithIndex | 60.9 µs | 134 µs | 25.2 KiB | 135 B | 195 | 6 |

---

## JSON Inverted Index (minisql only, with SQLite expression-index comparison where available)

| Benchmark | minisql indexed | comparison | minisql B/op | minisql allocs |
|---|---:|---:|---:|---:|
| JSONInverted_BuildIndex (1,000 docs/op) | 4.40 ms | — | 3.98 MiB | 63,047 |
| JSONInverted_Insert_WithIndex | 61.0 µs | — | 53.0 KiB | 214 |
| JSONInverted_Contains_KeyValue (334 matches) | 27.3 µs | 30.3 µs (sqlite expr idx) | 10.0 KiB | 101 |
| JSONInverted_Contains_KeyValue seq scan | 1.92 ms | 706 µs (sqlite json scan) | 2.00 MiB | 38,096 |
| JSONInverted_Contains_ObjectSubset (334 matches) | 38.7 µs | 126 µs (sqlite expr idx) | 11.1 KiB | 141 |
| JSONInverted_Contains_AfterDeletes (167 matches) | 137 µs | — | 74.6 KiB | 118 |
| JSONInverted_Update_WithIndex | 8.1 µs | — | 5.4 KiB | 65 |
| JSONInverted_Delete_WithIndex | 324 µs | — | 1,011 KiB | 382 |

---

## JOINs

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Join_Inner_SmallLarge (10K rows/op) | 6.58 ms | 4.70 ms | 1.24 MiB | 1.07 MiB | 89,855 | 99,757 |
| Join_Inner_LowSelectivity (100 rows/op) | 111 µs | 752 µs | 23.6 KiB | 11.3 KiB | 1,298 | 1,009 |
| Join_Left_UnmatchedRows (10K rows/op) | 3.60 ms | 4.18 ms | 878 KiB | 708 KiB | 79,743 | 70,157 |

---

## Maintenance

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Vacuum_Small | 19.6 ms | 259 µs | 1.49 MiB | 89 B |
| WAL_Checkpoint | 220 µs | 107 µs | 71.4 KiB | 440 B |

---

## Query Planning

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Explain | 5.0 µs | 1.2 µs | 5.96 KiB | 680 B |

---

## SELECT

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Select_PointScan | 4.79 µs | 3.60 µs | 3.69 KiB | 679 B | 49 | 26 |
| Select_Limit | 7.0 µs | 7.84 µs | 3.72 KiB | 1.69 KiB | 97 | 104 |
| Select_FullScan (10K rows/op) | 3.57 ms | 5.18 ms | 1.24 MiB | 1.29 MiB | 79,822 | 99,758 |
| Select_CountStar | 5.56 µs | 9.70 µs | 2.49 KiB | 400 B | 27 | 13 |
| Select_IndexRangeScan | 749 µs | 739 µs | 111.6 KiB | 85.9 KiB | 6,641 | 6,581 |
| Select_SecondaryIndex_LowSelectivity (5K rows/op) | 1.98 ms | 2.67 ms | 435 KiB | 313 KiB | 29,921 | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit | 8.21 µs | 8.29 µs | 4.33 KiB | 1.07 KiB | 101 | 64 |
| Select_RangeScan | 1.45 ms | 860 µs | 83.5 KiB | 85.9 KiB | 5,507 | 6,581 |

---

## CTE / Subquery

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| CTE_Materialise | 783 µs | 430 µs | 7.89 KiB | 400 B | 85 | 13 |
| Subquery_InList (5K rows/op) | 4.34 ms | 3.62 ms | 871 KiB | 235 KiB | 35,098 | 20,010 |

---

## ON CONFLICT / UPDATE

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| OnConflict_DoUpdate | 9.41 µs | 39.8 µs | 2.54 KiB | 259 B |
| Update_ByPK | 10.4 µs | 40.0 µs | 5.16 KiB | 263 B |

---

## HNSW Vector Index

### Build index (CREATE HNSW INDEX over pre-seeded table)

| Corpus | Dims | ns/op | rows/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 968 ms | 1,000 | 226 MiB | 7,866,021 |
| 10,000 | 3 | 13.2 s | 10,000 | 2.62 GiB | 95,396,072 |
| 1,000 | 128 | 2.24 s | 1,000 | 250 MiB | 9,285,518 |
| 10,000 | 128 | 58.4 s | 10,000 | 3.83 GiB | 176,827,221 |
| 1,000 | 768 | 8.22 s | 1,000 | 270 MiB | 10,047,415 |
| 10,000 | 768 | 208 s | 10,000 | 4.00 GiB | 183,656,828 |

### ANN search (VEC_L2 ORDER BY … LIMIT k, routed through HNSW index)

| Corpus | Dims | top-k | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 1 | 73.8 µs | 23.4 KiB | 329 |
| 1,000 | 3 | 10 | 82.9 µs | 28.3 KiB | 435 |
| 10,000 | 3 | 1 | 90.1 µs | 29.2 KiB | 335 |
| 10,000 | 3 | 10 | 128 µs | 75.6 KiB | 561 |
| 1,000 | 128 | 1 | 261 µs | 75.8 KiB | 581 |
| 1,000 | 128 | 10 | 342 µs | 129 KiB | 696 |
| 10,000 | 128 | 1 | 483 µs | 114 KiB | 721 |
| 10,000 | 128 | 10 | 585 µs | 169 KiB | 844 |
| 1,000 | 768 | 1 | 907 µs | 190 KiB | 613 |
| 1,000 | 768 | 10 | 1.33 ms | 479 KiB | 729 |
| 10,000 | 768 | 1 | 1.77 ms | 246 KiB | 767 |
| 10,000 | 768 | 10 | 2.14 ms | 522 KiB | 885 |

### Sequential scan (brute-force, no HNSW index — baseline for speedup comparison)

| Corpus | Dims | top-k | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 1 | 638 µs | 664 KiB | 10,822 |
| 1,000 | 128 | 1 | 8.15 ms | 6.08 MiB | 11,830 |
| 1,000 | 768 | 1 | 45.0 ms | 31.5 MiB | 11,855 |

**HNSW speedup vs sequential scan (top-1, n=1,000):**

| Dims | Sequential | HNSW | Speedup |
|---:|---:|---:|---:|
| 3 | 638 µs | 73.8 µs | **8.6×** |
| 128 | 8.15 ms | 261 µs | **31×** |
| 768 | 45.0 ms | 907 µs | **50×** |

### Online INSERT overhead (single row, 1,000-row starting corpus)

| Dims | With HNSW index | No index | Overhead |
|---:|---:|---:|---:|
| 3 | 1.71 ms | 25.2 µs | **68×** |
| 128 | 3.89 ms | 25.4 µs | **153×** |
| 768 | 11.4 ms | 28.0 µs | **408×** |

The overhead is dominated by HNSW graph traversal at `efConstruction=200` and the page writes for dirty neighbour nodes — both inherent to the algorithm.

---

## Memory Outliers

Largest per-operation heap consumers (minisql only):

- `HNSW_BuildIndex` dims768/n10000: **4.00 GiB/op** — O(N²) distance matrix during greedy layer construction; dominated by neighbour-list allocations across 10K nodes
- `HNSW_BuildIndex` dims128/n10000: **3.83 GiB/op** — same structural cost, lower per-vector overhead
- `JSONInverted_BuildIndex`: **3.98 MiB/op** — in-memory term→postings map during bulk build
- `Distinct_HighCardinality`: **1.69 MiB/op** — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: **1.66 MiB/op** — per-doc postings map during log-structured segment build
- `Vacuum_Small`: **1.49 MiB/op** — full copy-compact-swap; structural cost
- `Join_Inner_SmallLarge`: **1.24 MiB/op** — INLJ result materialization for 10K matched rows
- `Select_FullScan`: **1.24 MiB/op** — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: **1,011 KiB/op** — full posting list read into memory on delete
- `Insert_Batch` / `PreparedBatch`: **~193 KiB/op** — ~1.9 KiB/row vs SQLite's 310 B; remaining cost is per-row clone + B-tree page I/O

## Good Next Optimisation Targets

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Streaming term extraction for inverted-index build and maintenance
- Reduce per-row clone overhead in `Insert_Batch` (~1.9 KiB/row vs SQLite's 310 B)
