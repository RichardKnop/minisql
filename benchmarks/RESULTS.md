# Benchmark Results

## 2026-06-03 — Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `codex/json-build-memory-reassessment`
**Command:** `make bench BENCH_COUNT=1`
**GOMAXPROCS:** 10  

SQLite comparisons use the `sqlite` driver compiled into the same test binary. All minisql benchmarks run against a fresh temp-file database per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime.

---

## Aggregate / GROUP BY

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| GroupBy_Aggregate (100 groups) | 1.15 ms | 3.04 ms | 37.4 KiB | 3.53 KiB | 459 | 309 |
| Having_Filter (100 groups) | 1.03 ms | 2.80 ms | 29.5 KiB | 1.94 KiB | 264 | 111 |
| Distinct_HighCardinality (10K rows) | 4.55 ms | 7.49 ms | 1.69 MiB | 586 KiB | 40,142 | 40,010 |

---

## DELETE / Foreign Keys

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Delete_ByPK | 32.8 µs | 158 µs | 5.22 KiB | 447 B |
| ForeignKey_Insert | 21.8 µs | 87.1 µs | 2.82 KiB | 191 B |
| ForeignKey_DeleteCascade | 84.1 µs | 105 µs | 9.81 KiB | 128 B |

---

## INSERT

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Insert_SingleRow | 22.5 µs | 81.1 µs | 3.10 KiB | 311 B | 35 | 12 |
| Insert_Batch (100 rows/op) | 499 µs | 423 µs | 179 KiB | 31.0 KiB | 2,741 | 1,297 |
| Insert_PreparedBatch (100 rows/op) | 503 µs | 416 µs | 178 KiB | 31.0 KiB | 2,742 | 1,294 |
| Insert_MultiValues (100 rows/op) | 273 µs | 302 µs | 156 KiB | 25.2 KiB | 1,865 | 614 |

---

## Full-Text Search (minisql log-structured vs SQLite FTS5)

**Inverted-index sections refreshed:** 2026-06-03 on `codex/json-build-memory-reassessment` with the full `make bench BENCH_COUNT=1` suite.

**Latest memory improvement:** streaming row-ID segment-run merges and base foldback keep JSON segment compaction term-at-a-time. On the full-suite baseline, `JSONInverted_Insert_WithIndex` is down from **51.6 KiB/op** to **29.9 KiB/op** and `JSONInverted_Delete_WithIndex` is down from **33.0 KiB/op** to **31.0 KiB/op**.

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| FullText_BuildIndex (1,000 docs/op) | 5.30 ms | 3.73 ms | 1.52 MiB | 392 B | 16,290 | 20 |
| FullText_Insert_WithIndex | 71.9 µs | 146 µs | 15.6 KiB | 271 B | 136 | 10 |
| FullText_Search_SingleTerm/rare | 7.31 µs | 9.51 µs | 2.31 KiB | 408 B | 41 | 13 |
| FullText_Search_SingleTerm/medium | 7.07 µs | 10.5 µs | 2.31 KiB | 408 B | 41 | 13 |
| FullText_Search_SingleTerm/common | 7.77 µs | 83.3 µs | 2.32 KiB | 424 B | 43 | 15 |
| FullText_Search_MultiTermAND | 29.3 µs | 47.1 µs | 11.3 KiB | 408 B | 62 | 13 |
| FullText_Search_Phrase | 40.9 µs | 33.7 µs | 26.2 KiB | 416 B | 278 | 14 |
| FullText_Search_AfterDeletes | 107 µs | — | 11.1 KiB | — | 49 | — |
| FullText_Update_WithIndex | 60.9 µs | 175 µs | 21.7 KiB | 290 B | 193 | 12 |
| FullText_Delete_WithIndex | 101 µs | 204 µs | 22.4 KiB | 135 B | 186 | 6 |

---

## JSON Inverted Index (minisql only, with SQLite expression-index comparison where available)

| Benchmark | minisql indexed | comparison | minisql B/op | minisql allocs |
|---|---:|---:|---:|---:|
| JSONInverted_BuildIndex (1,000 docs/op) | 8.93 ms | — | 3.17 MiB | 63,010 |
| JSONInverted_Insert_WithIndex | 108 µs | — | 29.9 KiB | 183 |
| JSONInverted_Contains_KeyValue (334 matches) | 27.4 µs | 35.6 µs (sqlite expr idx) | 7.73 KiB | 75 |
| JSONInverted_Contains_KeyValue seq scan | 4.05 ms | 1.08 ms (sqlite json scan) | 1.94 MiB | 38,068 |
| JSONInverted_Contains_ObjectSubset (334 matches) | 43.5 µs | 166 µs (sqlite expr idx) | 8.80 KiB | 115 |
| JSONInverted_Contains_AfterDeletes (167 matches) | 100 µs | — | 19.2 KiB | 78 |
| JSONInverted_Update_WithIndex | 11.4 µs | — | 4.61 KiB | 52 |
| JSONInverted_Delete_WithIndex | 180 µs | — | 31.0 KiB | 195 |

---

## JOINs

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Join_Inner_SmallLarge (10K rows/op) | 9.28 ms | 8.11 ms | 1.25 MiB | 1.07 MiB | 89,858 | 99,757 |
| Join_Inner_LowSelectivity (100 rows/op) | 175 µs | 1.48 ms | 23.6 KiB | 11.3 KiB | 1,298 | 1,009 |
| Join_Left_UnmatchedRows (10K rows/op) | 6.23 ms | 7.75 ms | 882 KiB | 708 KiB | 79,745 | 70,157 |

---

## Maintenance

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Vacuum_Small | 78.7 ms | 905 µs | 1.52 MiB | 94 B |
| WAL_Checkpoint | 647 µs | 243 µs | 72.4 KiB | 441 B |

---

## Query Planning

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| Explain | 8.85 µs | 2.26 µs | 5.97 KiB | 680 B |

---

## SELECT

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| Select_PointScan | 7.67 µs | 4.88 µs | 3.69 KiB | 679 B | 49 | 26 |
| Select_Limit | 11.1 µs | 12.0 µs | 3.72 KiB | 1.69 KiB | 97 | 104 |
| Select_FullScan (10K rows/op) | 4.72 ms | 7.74 ms | 1.24 MiB | 1.29 MiB | 79,824 | 99,758 |
| Select_CountStar | 7.85 µs | 11.9 µs | 2.48 KiB | 400 B | 27 | 13 |
| Select_IndexRangeScan | 1.49 ms | 1.11 ms | 112.8 KiB | 85.9 KiB | 6,641 | 6,581 |
| Select_SecondaryIndex_LowSelectivity (5K rows/op) | 2.89 ms | 4.28 ms | 437 KiB | 313 KiB | 29,922 | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit | 13.4 µs | 13.9 µs | 4.34 KiB | 1.07 KiB | 101 | 64 |
| Select_RangeScan | 2.27 ms | 1.18 ms | 85.1 KiB | 85.9 KiB | 5,508 | 6,581 |

---

## CTE / Subquery

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| CTE_Materialise | 1.12 ms | 613 µs | 8.92 KiB | 400 B | 85 | 13 |
| Subquery_InList (5K rows/op) | 6.44 ms | 4.97 ms | 884 KiB | 235 KiB | 35,103 | 20,010 |

---

## ON CONFLICT / UPDATE

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op |
|---|---:|---:|---:|---:|
| OnConflict_DoUpdate | 16.5 µs | 48.5 µs | 2.48 KiB | 259 B |
| Update_ByPK | 15.2 µs | 80.5 µs | 5.03 KiB | 263 B |

---

## HNSW Vector Index

### Build index (CREATE HNSW INDEX over pre-seeded table)

| Corpus | Dims | ns/op | rows/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 847 ms | 1,000 | 199 MiB | 7,799,532 |
| 10,000 | 3 | 10.2 s | 10,000 | 2.27 GiB | 95,244,649 |
| 1,000 | 128 | 1.12 s | 1,000 | 219 MiB | 9,295,527 |
| 10,000 | 128 | 26.7 s | 10,000 | 3.50 GiB | 176,902,209 |
| 1,000 | 768 | 2.35 s | 1,000 | 238 MiB | 9,960,078 |
| 10,000 | 768 | 46.4 s | 10,000 | 3.66 GiB | 182,968,451 |

### ANN search (VEC_L2 ORDER BY … LIMIT k, routed through HNSW index)

| Corpus | Dims | top-k | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 1 | 53.3 µs | 17.4 KiB | 314 |
| 1,000 | 3 | 10 | 60.4 µs | 21.2 KiB | 391 |
| 10,000 | 3 | 1 | 58.2 µs | 26.4 KiB | 301 |
| 10,000 | 3 | 10 | 68.4 µs | 30.3 KiB | 382 |
| 1,000 | 128 | 1 | 227 µs | 48.8 KiB | 518 |
| 1,000 | 128 | 10 | 273 µs | 61.5 KiB | 608 |
| 10,000 | 128 | 1 | 451 µs | 86.6 KiB | 637 |
| 10,000 | 128 | 10 | 521 µs | 99.4 KiB | 727 |
| 1,000 | 768 | 1 | 834 µs | 55.4 KiB | 625 |
| 1,000 | 768 | 10 | 957 µs | 113 KiB | 710 |
| 10,000 | 768 | 1 | 2.03 ms | 92.9 KiB | 721 |
| 10,000 | 768 | 10 | 1.80 ms | 151 KiB | 811 |

### Sequential scan (brute-force, no HNSW index — baseline for speedup comparison)

| Corpus | Dims | top-k | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3 | 1 | 1.23 ms | 663 KiB | 10,819 |
| 1,000 | 128 | 1 | 13.1 ms | 5.92 MiB | 11,823 |
| 1,000 | 768 | 1 | 64.2 ms | 31.4 MiB | 11,849 |

**HNSW speedup vs sequential scan (top-1, n=1,000):**

| Dims | Sequential | HNSW | Speedup |
|---:|---:|---:|---:|
| 3 | 1.23 ms | 53.3 µs | **23×** |
| 128 | 13.1 ms | 227 µs | **58×** |
| 768 | 64.2 ms | 834 µs | **77×** |

### Online INSERT overhead (single row, 1,000-row starting corpus)

| Dims | With HNSW index | No index | Overhead |
|---:|---:|---:|---:|
| 3 | 2.07 ms | 29.4 µs | **70×** |
| 128 | 4.73 ms | 34.1 µs | **139×** |
| 768 | 13.4 ms | 36.2 µs | **370×** |

The overhead is dominated by HNSW graph traversal at `efConstruction=200` and the page writes for dirty neighbour nodes — both inherent to the algorithm.

---

## Memory Outliers

Largest per-operation heap consumers (minisql only):

- `HNSW_BuildIndex` dims768/n10000: **3.66 GiB/op** — O(N²) distance matrix during greedy layer construction; dominated by neighbour-list allocations across 10K nodes
- `HNSW_BuildIndex` dims128/n10000: **3.50 GiB/op** — same structural cost, lower per-vector overhead
- `JSONInverted_BuildIndex`: **3.17 MiB/op** — JSON decoding plus in-memory term→row-ID map during bulk build
- `Distinct_HighCardinality`: **1.69 MiB/op** — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: **1.52 MiB/op** — token-position accumulation and posting payload encoding during log-structured segment build
- `Vacuum_Small`: **1.52 MiB/op** — full copy-compact-swap; structural cost
- `Join_Inner_SmallLarge`: **1.25 MiB/op** — INLJ result materialization for 10K matched rows
- `Select_FullScan`: **1.24 MiB/op** — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: **31.0 KiB/op** — reduced by streaming row-ID segment merges and base foldback; no longer a major memory outlier
- `Insert_Batch` / `PreparedBatch`: **~178-179 KiB/op** — ~1.8 KiB/row vs SQLite's 310 B; remaining cost is per-row clone + B-tree page I/O

## Good Next Optimisation Targets

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Reduce JSON decode/tree-walk allocations during inverted-index build and maintenance
- Add a direct compacting JSON term scanner only if a prototype can preserve duplicate-key semantics without regressing allocations
- Extend direct segment-writer compaction to positional/full-text postings
- Reduce per-row clone overhead in `Insert_Batch` (~1.8 KiB/row vs SQLite's 310 B)
