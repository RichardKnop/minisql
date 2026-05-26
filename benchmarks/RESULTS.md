# Benchmark Results

## 2026-05-26 — Post ALTER TABLE / Self-Describing Cell Format Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `refactor/alter-table` (merged self-describing cell refactor + ALTER TABLE)  
**Command:** `LOG_LEVEL=warn go test -tags bench -bench=. -benchmem -count=1 -run '^$' ./benchmarks/`  
**Runtime:** `187.669s`

### What changed since the previous baseline (2026-05-25)

The previous baseline was taken immediately after the log-structured inverted-index refactor was merged. Since then:

- **Self-describing cell format** (`type_code.go`, `row_view.go`): every leaf cell now carries its own type codes; RowView reads columns without touching the schema, eliminating a schema lookup per column per row.
- **ALTER TABLE** implemented: ADD/DROP/RENAME COLUMN, RENAME TO. No benchmark impact.

The cell-format change is responsible for the across-the-board latency reductions visible below.

### Notable improvements vs 2026-05-25 baseline

| Benchmark | Old | New | Δ |
|---|---:|---:|---:|
| Select_PointScan/minisql | 13.3 µs | 5.8 µs | −57% |
| Select_IndexRangeScan/minisql | 2.11 ms | 805 µs | −62% |
| Select_FullScan/minisql | 6.63 ms | 3.77 ms | −43% |
| Select_CountStar/minisql | 11.1 µs | 5.7 µs | −49% |
| Explain/minisql | 12.8 µs | 5.2 µs | −59% |
| WAL_Checkpoint/minisql | 652 µs | 244 µs | −63% |
| Update_ByPK/minisql | 20.1 µs | 10.9 µs | −46% |
| OnConflict_DoUpdate/minisql | 16.6 µs | 8.7 µs | −48% |
| Delete_ByPK/minisql | 36.0 µs | 21.9 µs | −39% |
| Insert_Batch/minisql | 607 µs | 398 µs | −34% |
| FullText_Search_Phrase/minisql | 86.6 µs | 28.4 µs | −67% |
| FullText_Search_MultiTermAND/minisql | 55.6 µs | 27.6 µs | −50% |
| FullText_Search_AfterDeletes/minisql | 189 µs | 86.1 µs | −54% |
| ForeignKey_DeleteCascade/minisql | 178 µs | 46.1 µs | −74% |
| Join_Inner_LowSelectivity/minisql | 181 µs | 115 µs | −37% |

### Memory regressions vs 2026-05-25 baseline

| Benchmark | Old | New | Δ |
|---|---:|---:|---:|
| Join_Inner_SmallLarge/minisql | 2.57 MiB | 3.46 MiB | +35% |
| Insert_Batch/minisql | 242.5 KiB | 251.3 KiB | +4% |
| Insert_PreparedBatch/minisql | 240.8 KiB | 250.2 KiB | +4% |
| Insert_MultiValues/minisql | 197.2 KiB | 207.1 KiB | +5% |

The Join_Inner_SmallLarge memory regression warrants investigation (see Memory Outliers section).

## Full Benchmark Baseline

| Benchmark | Time/op | Memory/op | Allocs/op |
|---|---:|---:|---:|
| GroupBy_Aggregate/minisql | 875 µs | 37.4 KiB | 463 |
| GroupBy_Aggregate/sqlite | 2.12 ms | 3.5 KiB | 309 |
| Having_Filter/minisql | 717 µs | 28.9 KiB | 268 |
| Having_Filter/sqlite | 1.92 ms | 1.9 KiB | 111 |
| Distinct_HighCardinality/minisql | 3.09 ms | 1.69 MiB | 40,144 |
| Distinct_HighCardinality/sqlite | 5.63 ms | 586.3 KiB | 40,010 |
| Delete_ByPK/minisql | 21.9 µs | 7.1 KiB | 85 |
| Delete_ByPK/sqlite | 80.2 µs | 447 B | 19 |
| ForeignKey_Insert/minisql | 14.1 µs | 3.6 KiB | 38 |
| ForeignKey_Insert/sqlite | 46.2 µs | 192 B | 8 |
| ForeignKey_DeleteCascade/minisql | 46.1 µs | 11.2 KiB | 141 |
| ForeignKey_DeleteCascade/sqlite | 50.7 µs | 128 B | 5 |
| Insert_SingleRow/minisql | 14.8 µs | 4.0 KiB | 42 |
| Insert_SingleRow/sqlite | 45.9 µs | 311 B | 12 |
| Insert_Batch/minisql | 398 µs | 251.3 KiB | 3,257 |
| Insert_Batch/sqlite | 232 µs | 31.0 KiB | 1,301 |
| Insert_PreparedBatch/minisql | 357 µs | 250.2 KiB | 3,256 |
| Insert_PreparedBatch/sqlite | 229 µs | 31.0 KiB | 1,300 |
| Insert_MultiValues/minisql | 229 µs | 207.1 KiB | 2,277 |
| Insert_MultiValues/sqlite | 179 µs | 25.2 KiB | 616 |
| FullText_BuildIndex/minisql | 3.24 ms | 1.66 MiB | 16,328 |
| FullText_BuildIndex/sqlite | 2.14 ms | 392 B | 20 |
| FullText_Insert_WithIndex/minisql | 50.3 µs | 23.1 KiB | 185 |
| FullText_Insert_WithIndex/sqlite | 90.9 µs | 439 B | 18 |
| FullText_Search_SingleTerm/rare/minisql | 17.4 µs | 4.6 KiB | 71 |
| FullText_Search_SingleTerm/rare/sqlite | 10.1 µs | 392 B | 12 |
| FullText_Search_SingleTerm/medium/minisql | 16.8 µs | 4.6 KiB | 71 |
| FullText_Search_SingleTerm/medium/sqlite | 11.3 µs | 392 B | 12 |
| FullText_Search_SingleTerm/common/minisql | 17.3 µs | 4.6 KiB | 73 |
| FullText_Search_SingleTerm/common/sqlite | 64.0 µs | 408 B | 14 |
| FullText_Search_MultiTermAND/minisql | 27.6 µs | 13.7 KiB | 92 |
| FullText_Search_MultiTermAND/sqlite | 37.8 µs | 392 B | 12 |
| FullText_Search_Phrase/minisql | 28.4 µs | 28.8 KiB | 308 |
| FullText_Search_Phrase/sqlite | 28.6 µs | 400 B | 13 |
| FullText_Search_AfterDeletes/minisql | 86.1 µs | 77.8 KiB | 94 |
| FullText_Update_WithIndex/minisql | 45.7 µs | 27.7 KiB | 231 |
| FullText_Update_WithIndex/sqlite | 108 µs | 291 B | 12 |
| FullText_Delete_WithIndex/minisql | 63.1 µs | 26.6 KiB | 208 |
| FullText_Delete_WithIndex/sqlite | 162 µs | 135 B | 6 |
| JSONInverted_BuildIndex/minisql_indexed | 4.75 ms | 3.99 MiB | 63,050 |
| JSONInverted_Insert_WithIndex/minisql_indexed | 61.7 µs | 56.3 KiB | 221 |
| JSONInverted_Contains_KeyValue/key_value/minisql_indexed | 27.8 µs | 10.1 KiB | 105 |
| JSONInverted_Contains_KeyValue/key_value/minisql_sequential | 2.03 ms | 2.00 MiB | 38,100 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_scan | 752 µs | 408 B | 14 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_expr_index | 31.6 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_indexed | 40.0 µs | 11.3 KiB | 145 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_sequential | 2.22 ms | 1.96 MiB | 38,122 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_scan | 844 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_expr_index | 130 µs | 408 B | 14 |
| JSONInverted_Contains_AfterDeletes/minisql_indexed | 146 µs | 74.8 KiB | 122 |
| JSONInverted_Update_WithIndex/minisql_indexed | 9.05 µs | 6.9 KiB | 81 |
| JSONInverted_Delete_WithIndex/minisql_indexed | 328 µs | 1012.3 KiB | 395 |
| Join_Inner_SmallLarge/minisql | 4.87 ms | 3.46 MiB | 89,778 |
| Join_Inner_SmallLarge/sqlite | 4.82 ms | 1.07 MiB | 99,757 |
| Join_Inner_LowSelectivity/minisql | 115 µs | 22.9 KiB | 1,294 |
| Join_Inner_LowSelectivity/sqlite | 739 µs | 11.3 KiB | 1,009 |
| Join_Left_UnmatchedRows/minisql | 3.71 ms | 878.3 KiB | 79,739 |
| Join_Left_UnmatchedRows/sqlite | 4.24 ms | 725.2 KiB | 70,157 |
| Vacuum_Small/minisql | 19.85 ms | 1.71 MiB | 15,024 |
| Vacuum_Small/sqlite | 298 µs | 90 B | 12 |
| WAL_Checkpoint/minisql | 244 µs | 71.5 KiB | 45 |
| WAL_Checkpoint/sqlite | 112 µs | 440 B | 12 |
| Explain/minisql | 5.17 µs | 6.0 KiB | 58 |
| Explain/sqlite | 1.21 µs | 680 B | 18 |
| Select_PointScan/minisql | 5.76 µs | 5.0 KiB | 62 |
| Select_PointScan/sqlite | 3.50 µs | 679 B | 26 |
| Select_Limit/minisql | 7.41 µs | 4.1 KiB | 101 |
| Select_Limit/sqlite | 8.23 µs | 1.7 KiB | 104 |
| Select_FullScan/minisql | 3.77 ms | 1.24 MiB | 79,827 |
| Select_FullScan/sqlite | 5.28 ms | 1.29 MiB | 99,758 |
| Select_CountStar/minisql | 5.70 µs | 2.6 KiB | 29 |
| Select_CountStar/sqlite | 10.0 µs | 400 B | 13 |
| Select_IndexRangeScan/minisql | 805 µs | 111.7 KiB | 6,645 |
| Select_IndexRangeScan/sqlite | 768 µs | 85.9 KiB | 6,581 |
| Select_SecondaryIndex_LowSelectivity/minisql | 2.15 ms | 437.2 KiB | 29,937 |
| Select_SecondaryIndex_LowSelectivity/sqlite | 2.77 ms | 313.0 KiB | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit/minisql | 9.99 µs | 5.7 KiB | 117 |
| Select_SecondaryIndex_LowSelectivityLimit/sqlite | 8.47 µs | 1.1 KiB | 64 |
| Select_RangeScan/minisql | 1.48 ms | 83.9 KiB | 5,511 |
| Select_RangeScan/sqlite | 894 µs | 85.9 KiB | 6,581 |
| CTE_Materialise/minisql | 773 µs | 8.0 KiB | 89 |
| CTE_Materialise/sqlite | 455 µs | 400 B | 13 |
| Subquery_InList/minisql | 4.51 ms | 871.7 KiB | 35,102 |
| Subquery_InList/sqlite | 3.62 ms | 234.7 KiB | 20,010 |
| OnConflict_DoUpdate/minisql | 8.69 µs | 3.1 KiB | 39 |
| OnConflict_DoUpdate/sqlite | 39.7 µs | 259 B | 10 |
| Update_ByPK/minisql | 10.9 µs | 6.8 KiB | 63 |
| Update_ByPK/sqlite | 40.8 µs | 263 B | 10 |

## Memory Outliers — Profiling Candidates

Ranked by absolute bytes/op for minisql, excluding intentionally-unindexed sequential scan variants.

| Rank | Benchmark | Memory/op | Allocs/op | Notes |
|---:|---|---:|---:|---|
| 1 | `JSONInverted_BuildIndex` | 3.99 MiB | 63,050 | Build-time term extraction and segment materialisation |
| 2 | `Join_Inner_SmallLarge` | 3.46 MiB | 89,778 | **Regressed +35%** vs prior baseline; ~39 B/row overhead vs sqlite |
| 3 | `Vacuum_Small` | 1.71 MiB | 15,024 | Full copy-compact-swap; expected but large vs sqlite's 90 B |
| 4 | `Distinct_HighCardinality` | 1.69 MiB | 40,144 | In-memory dedup set for 10K distinct rows |
| 5 | `FullText_BuildIndex` | 1.66 MiB | 16,328 | Per-doc postings map during bulk index build |
| 6 | `Select_FullScan` | 1.24 MiB | 79,827 | ~13 B/alloc — 8 allocs/row for 10K rows; row materialisation |
| 7 | `JSONInverted_Delete_WithIndex` | 1012 KiB | 395 | Delete-path reads full posting list into memory |
| 8 | `Subquery_InList` | 872 KiB | 35,102 | IN-list materialises outer scan rows |
| 9 | `Join_Left_UnmatchedRows` | 878 KiB | 79,739 | Same root cause as SmallLarge join |
| 10 | `Insert_Batch` / `PreparedBatch` | ~251 KiB | 3,257 | ~2.5 KiB/row vs sqlite's 310 B/row; 8× gap |

### Likely root causes and suggested profiling targets

**Join row materialisation** (`Join_Inner_SmallLarge`, `Join_Left_UnmatchedRows`): the join executor builds a combined `Row` struct with a `[]OptionalValue` slice for every matched pair. For a 10K-row result set this generates ~90K allocations. A RowView-based join path (already exists on the scan side) that avoids materialising `[]OptionalValue` until projection would be the natural next step. This is also the source of the +35% memory regression — something in the join path allocates more per row now.

**Select full-scan materialisation** (`Select_FullScan`): 79,827 allocs for 10K rows = ~8 allocs/row. The scan pipeline still calls `Materialize()` on each RowView, allocating a `Row.Values []OptionalValue` slice per row. A streaming projection that reads directly from RowView into the driver dest slice would collapse this to O(1) allocations per row.

**Insert batch overhead** (`Insert_Batch`, `Insert_PreparedBatch`, `Insert_MultiValues`): 2.5 KiB/row vs SQLite's 310 B suggests heavy per-row statement preparation (clone, bind-args, prepareInsert). Reusing a prepared statement across rows within a multi-row batch would reduce this significantly.

**JSONInverted_BuildIndex** and **FullText_BuildIndex**: these are known targets from the previous analysis — streaming term extraction instead of building a full in-memory term→postings map per document.
