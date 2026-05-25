# Benchmark Results

## 2026-05-25 — Integration Baseline After Log-Structured Inverted-Index Refactor

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `refactor/log-structured-inverted-index`  
**Command:** `LOG_LEVEL=warn go test -tags bench -bench=. -benchmem -count=1 -run '^$' ./benchmarks/`  
**Runtime:** `247.585s`

This file has been reset to a fresh post-refactor baseline. Use this as the comparison point after the log-structured inverted-index branch is integrated into `main`.

The full-text and JSON inverted-index e2e coverage now includes:

- indexed planning and `EXPLAIN ANALYZE` execution
- posting-list intersection, phrase matching, object subsets, nested JSON keys, and array membership
- persistence/reload plus `PRAGMA integrity_check`
- insert/update/delete maintenance
- heavier mutation runs that exercise accumulated log-structured segments and tombstones
- partial full-text and JSON inverted indexes, including predicate transitions and `NULL` indexed documents

The partial-index tests caught and fixed an integrity-check issue where log-structured delete segments were ignored while reconstructing logical inverted-index contents.

## Full Benchmark Baseline

| Benchmark | Time/op | Memory/op | Allocs/op |
|---|---:|---:|---:|
| GroupBy_Aggregate/minisql | 1.22 ms | 37.4 KiB | 463 |
| GroupBy_Aggregate/sqlite | 2.80 ms | 3.5 KiB | 309 |
| Having_Filter/minisql | 921.0 µs | 29.1 KiB | 268 |
| Having_Filter/sqlite | 2.38 ms | 1.9 KiB | 111 |
| Distinct_HighCardinality/minisql | 5.81 ms | 1.69 MiB | 40,145 |
| Distinct_HighCardinality/sqlite | 8.32 ms | 586.3 KiB | 40,010 |
| Delete_ByPK/minisql | 36.0 µs | 7.3 KiB | 85 |
| Delete_ByPK/sqlite | 116.1 µs | 447 B | 19 |
| ForeignKey_Insert/minisql | 17.6 µs | 3.5 KiB | 37 |
| ForeignKey_Insert/sqlite | 53.1 µs | 192 B | 8 |
| ForeignKey_DeleteCascade/minisql | 178.2 µs | 11.3 KiB | 141 |
| ForeignKey_DeleteCascade/sqlite | 69.8 µs | 128 B | 5 |
| Insert_SingleRow/minisql | 18.6 µs | 3.9 KiB | 41 |
| Insert_SingleRow/sqlite | 55.6 µs | 311 B | 12 |
| Insert_Batch/minisql | 607.2 µs | 242.5 KiB | 3,137 |
| Insert_Batch/sqlite | 285.3 µs | 31.0 KiB | 1,299 |
| Insert_PreparedBatch/minisql | 586.7 µs | 240.8 KiB | 3,135 |
| Insert_PreparedBatch/sqlite | 277.4 µs | 31.0 KiB | 1,298 |
| Insert_MultiValues/minisql | 354.4 µs | 197.2 KiB | 2,161 |
| Insert_MultiValues/sqlite | 205.1 µs | 25.2 KiB | 615 |
| FullText_BuildIndex/minisql | 8.63 ms | 1.67 MiB | 16,359 |
| FullText_BuildIndex/sqlite | 2.82 ms | 392 B | 20 |
| FullText_Insert_WithIndex/minisql | 72.3 µs | 19.6 KiB | 164 |
| FullText_Insert_WithIndex/sqlite | 104.3 µs | 437 B | 18 |
| FullText_Search_SingleTerm/rare/minisql | 25.9 µs | 4.5 KiB | 71 |
| FullText_Search_SingleTerm/rare/sqlite | 14.6 µs | 392 B | 12 |
| FullText_Search_SingleTerm/medium/minisql | 26.2 µs | 4.5 KiB | 71 |
| FullText_Search_SingleTerm/medium/sqlite | 15.2 µs | 392 B | 12 |
| FullText_Search_SingleTerm/common/minisql | 35.1 µs | 4.5 KiB | 73 |
| FullText_Search_SingleTerm/common/sqlite | 106.8 µs | 408 B | 14 |
| FullText_Search_MultiTermAND/minisql | 55.6 µs | 13.6 KiB | 92 |
| FullText_Search_MultiTermAND/sqlite | 50.7 µs | 392 B | 12 |
| FullText_Search_Phrase/minisql | 86.6 µs | 28.6 KiB | 308 |
| FullText_Search_Phrase/sqlite | 38.3 µs | 400 B | 13 |
| FullText_Search_AfterDeletes/minisql | 189.0 µs | 77.4 KiB | 94 |
| FullText_Update_WithIndex/minisql | 71.4 µs | 25.7 KiB | 220 |
| FullText_Update_WithIndex/sqlite | 115.7 µs | 290 B | 12 |
| FullText_Delete_WithIndex/minisql | 85.9 µs | 26.1 KiB | 207 |
| FullText_Delete_WithIndex/sqlite | 162.5 µs | 135 B | 6 |
| JSONInverted_BuildIndex/minisql_indexed | 12.34 ms | 3.97 MiB | 63,049 |
| JSONInverted_Insert_WithIndex/minisql_indexed | 105.6 µs | 34.9 KiB | 217 |
| JSONInverted_Contains_KeyValue/key_value/minisql_indexed | 48.0 µs | 10.1 KiB | 105 |
| JSONInverted_Contains_KeyValue/key_value/minisql_sequential | 5.21 ms | 1.95 MiB | 38,100 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_scan | 880.9 µs | 409 B | 14 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_expr_index | 41.0 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_indexed | 77.8 µs | 11.2 KiB | 145 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_sequential | 4.86 ms | 1.95 MiB | 38,122 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_scan | 1.03 ms | 409 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_expr_index | 168.4 µs | 408 B | 14 |
| JSONInverted_Contains_AfterDeletes/minisql_indexed | 258.0 µs | 74.4 KiB | 122 |
| JSONInverted_Update_WithIndex/minisql_indexed | 19.6 µs | 7.0 KiB | 83 |
| JSONInverted_Delete_WithIndex/minisql_indexed | 832.9 µs | 1012.5 KiB | 395 |
| Join_Inner_SmallLarge/minisql | 9.56 ms | 2.57 MiB | 89,778 |
| Join_Inner_SmallLarge/sqlite | 8.96 ms | 1.07 MiB | 99,757 |
| Join_Inner_LowSelectivity/minisql | 180.6 µs | 22.8 KiB | 1,294 |
| Join_Inner_LowSelectivity/sqlite | 923.0 µs | 11.3 KiB | 1,009 |
| Join_Left_UnmatchedRows/minisql | 6.28 ms | 880.0 KiB | 79,740 |
| Join_Left_UnmatchedRows/sqlite | 7.53 ms | 708.2 KiB | 70,157 |
| Vacuum_Small/minisql | 21.89 ms | 1.67 MiB | 13,999 |
| Vacuum_Small/sqlite | 924.4 µs | 92 B | 4 |
| WAL_Checkpoint/minisql | 652.4 µs | 70.2 KiB | 45 |
| WAL_Checkpoint/sqlite | 304.2 µs | 442 B | 12 |
| Explain/minisql | 12.8 µs | 6.0 KiB | 58 |
| Explain/sqlite | 3.0 µs | 680 B | 18 |
| Select_PointScan/minisql | 13.3 µs | 5.0 KiB | 62 |
| Select_PointScan/sqlite | 6.4 µs | 679 B | 26 |
| Select_Limit/minisql | 12.6 µs | 4.0 KiB | 101 |
| Select_Limit/sqlite | 14.4 µs | 1.7 KiB | 104 |
| Select_FullScan/minisql | 6.63 ms | 1.24 MiB | 79,827 |
| Select_FullScan/sqlite | 11.50 ms | 1.29 MiB | 99,758 |
| Select_CountStar/minisql | 11.1 µs | 2.5 KiB | 29 |
| Select_CountStar/sqlite | 16.4 µs | 400 B | 13 |
| Select_IndexRangeScan/minisql | 2.11 ms | 113.5 KiB | 6,645 |
| Select_IndexRangeScan/sqlite | 1.24 ms | 85.9 KiB | 6,581 |
| Select_SecondaryIndex_LowSelectivity/minisql | 3.35 ms | 438.6 KiB | 29,937 |
| Select_SecondaryIndex_LowSelectivity/sqlite | 5.24 ms | 313.0 KiB | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit/minisql | 21.0 µs | 5.7 KiB | 117 |
| Select_SecondaryIndex_LowSelectivityLimit/sqlite | 13.8 µs | 1.1 KiB | 64 |
| Select_RangeScan/minisql | 1.96 ms | 84.8 KiB | 5,511 |
| Select_RangeScan/sqlite | 1.36 ms | 85.9 KiB | 6,581 |
| CTE_Materialise/minisql | 917.6 µs | 7.9 KiB | 89 |
| CTE_Materialise/sqlite | 580.4 µs | 400 B | 13 |
| Subquery_InList/minisql | 6.14 ms | 873.8 KiB | 35,102 |
| Subquery_InList/sqlite | 6.02 ms | 234.7 KiB | 20,010 |
| OnConflict_DoUpdate/minisql | 16.6 µs | 3.1 KiB | 39 |
| OnConflict_DoUpdate/sqlite | 47.2 µs | 259 B | 10 |
| Update_ByPK/minisql | 20.1 µs | 6.6 KiB | 63 |
| Update_ByPK/sqlite | 44.9 µs | 263 B | 10 |

## Inverted-Index Focus

The refactor branch has substantially lowered the inverted-index write-path baseline, especially compared with the pre-refactor multi-MiB DML spikes. The current integration baseline shows:

- Full-text build: `1.67 MiB/op`, `16,359 allocs/op`
- Full-text insert/update/delete with index: `19.6-26.1 KiB/op`
- Full-text indexed lookups: `4.5-28.6 KiB/op`
- JSON build: `3.97 MiB/op`, `63,049 allocs/op`
- JSON insert/update with index: `34.9 KiB/op` and `7.0 KiB/op`
- JSON indexed contains: `10.1-11.2 KiB/op`
- JSON contains after deletes: `74.4 KiB/op`

The largest remaining inverted-index outliers are:

- `JSONInverted_Delete_WithIndex`: `1012.5 KiB/op`
- `JSONInverted_BuildIndex`: `3.97 MiB/op`
- `FullText_BuildIndex`: `1.67 MiB/op`
- sequential JSON containment: `1.95 MiB/op`

Good next branches after integration:

- streaming JSON term extraction for build and maintenance
- streaming JSON containment for simple sequential predicates
- full-text build grouping without a per-row map
- deeper segment writer changes that avoid fully materializing segment cell slices
