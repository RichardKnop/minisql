# Benchmark Results

## 2026-06-03 — Baseline (inverted sections refreshed 2026-06-05)

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** full-suite baseline from `codex/json-build-memory-reassessment`; inverted refresh from `codex/profile-inverted-build-memory`  
**Command:** full-suite baseline from `make bench BENCH_COUNT=1`; inverted refresh command shown below  
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

**Inverted-index sections refreshed:** 2026-06-05 on `codex/profile-inverted-build-memory` with:

```bash
LOG_LEVEL=warn go test -tags bench -bench='Benchmark(FullText|JSONInverted)' -benchmem -count=1 -run '^$' ./benchmarks/
```

**Latest memory improvement:** direct JSON term scanning avoids full `encoding/json` tree materialisation for index term extraction, final build flushes avoid one throwaway mutation map, and single-position full-text postings reuse cached immutable slices. `JSONInverted_BuildIndex` is down from **3.17 MiB/op** to **1.26 MiB/op**, while `FullText_BuildIndex` is down from **1.52 MiB/op** to **1.40 MiB/op**.

| Benchmark | minisql | sqlite | minisql B/op | sqlite B/op | minisql allocs | sqlite allocs |
|---|---:|---:|---:|---:|---:|---:|
| FullText_BuildIndex (1,000 docs/op) | 3.01 ms | 2.06 ms | 1.40 MiB | 392 B | 12,297 | 20 |
| FullText_Insert_WithIndex | 45.7 µs | 83.9 µs | 16.8 KiB | 271 B | 138 | 10 |
| FullText_Search_SingleTerm/rare | 4.43 µs | 6.76 µs | 2.31 KiB | 408 B | 41 | 13 |
| FullText_Search_SingleTerm/medium | 4.12 µs | 7.92 µs | 2.31 KiB | 408 B | 41 | 13 |
| FullText_Search_SingleTerm/common | 4.10 µs | 61.1 µs | 2.32 KiB | 424 B | 43 | 15 |
| FullText_Search_MultiTermAND | 15.0 µs | 33.8 µs | 11.3 KiB | 408 B | 62 | 13 |
| FullText_Search_Phrase | 15.5 µs | 24.6 µs | 26.2 KiB | 416 B | 278 | 14 |
| FullText_Search_AfterDeletes | 81.4 µs | — | 11.1 KiB | — | 49 | — |
| FullText_Update_WithIndex | 44.8 µs | 96.2 µs | 22.3 KiB | 291 B | 194 | 12 |
| FullText_Delete_WithIndex | 61.1 µs | 142 µs | 21.8 KiB | 135 B | 180 | 6 |

---

## JSON Inverted Index (minisql only, with SQLite expression-index comparison where available)

| Benchmark | minisql indexed | comparison | minisql B/op | minisql allocs |
|---|---:|---:|---:|---:|
| JSONInverted_BuildIndex (1,000 docs/op) | 2.76 ms | — | 1.26 MiB | 26,669 |
| JSONInverted_Insert_WithIndex | 78.5 µs | — | 38.4 KiB | 148 |
| JSONInverted_Contains_KeyValue (334 matches) | 22.3 µs | 33.9 µs (sqlite expr idx) | 7.71 KiB | 74 |
| JSONInverted_Contains_KeyValue seq scan | 3.38 ms | 812 µs (sqlite json scan) | 1.94 MiB | 38,067 |
| JSONInverted_Contains_ObjectSubset (334 matches) | 38.6 µs | 141 µs (sqlite expr idx) | 8.76 KiB | 112 |
| JSONInverted_Contains_AfterDeletes (167 matches) | 74.4 µs | — | 19.2 KiB | 77 |
| JSONInverted_Update_WithIndex | 8.40 µs | — | 4.55 KiB | 51 |
| JSONInverted_Delete_WithIndex | 164 µs | — | 31.2 KiB | 159 |

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
- `Distinct_HighCardinality`: **1.69 MiB/op** — in-memory dedup set for 10K distinct rows
- `Vacuum_Small`: **1.52 MiB/op** — full copy-compact-swap; structural cost
- `FullText_BuildIndex`: **1.40 MiB/op** — token-position accumulation, posting slice growth, and segment-cell materialisation during log-structured segment build
- `JSONInverted_BuildIndex`: **1.26 MiB/op** — direct term scanner removed most JSON decode/tree-walk allocation; remaining cost is term map and segment build
- `Join_Inner_SmallLarge`: **1.25 MiB/op** — INLJ result materialization for 10K matched rows
- `Select_FullScan`: **1.24 MiB/op** — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: **31.2 KiB/op** — reduced by streaming row-ID segment merges and base foldback; no longer a major memory outlier
- `Insert_Batch` / `PreparedBatch`: **~178-179 KiB/op** — ~1.8 KiB/row vs SQLite's 310 B; remaining cost is per-row clone + B-tree page I/O

## Good Next Optimisation Targets

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Reduce full-text build memory by avoiding large `postingsByTerm[term]` slice growth and segment-cell materialisation
- Consider a dedicated streaming full-text segment builder for bulk CREATE INDEX
- Reduce per-row clone overhead in `Insert_Batch` (~1.8 KiB/row vs SQLite's 310 B)
