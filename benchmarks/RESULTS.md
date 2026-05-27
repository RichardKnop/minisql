# Benchmark Results

## 2026-05-26 — Baseline After Self-Describing Cell Format + ALTER TABLE

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `refactor/alter-table`  
**Command:** `LOG_LEVEL=warn go test -tags bench -bench=. -benchmem -count=1 -run '^$' ./benchmarks/`  
**Runtime:** `187.669s`

This baseline reflects two changes merged since the previous measurement (2026-05-25):

- **Self-describing cell format** (`type_code.go`, `row_view.go`): every leaf cell now stores its own type codes; `RowView` reads columns without touching the schema. This eliminated a per-column schema lookup on every row read and is responsible for the broad latency reductions visible across SELECT, UPDATE, DELETE, and JOIN benchmarks.
- **ALTER TABLE** (ADD/DROP/RENAME COLUMN, RENAME TO): schema-only DDL, no measurable benchmark impact.

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
| Vacuum_Small/sqlite | 298 µs | 90 B | 4 |
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

## Memory Outliers

The largest remaining memory consumers (minisql only, excluding intentional sequential-scan variants):

- `Join_Inner_SmallLarge`: `3.46 MiB/op`, `89,778 allocs/op` — combined-row materialisation per matched pair
- `JSONInverted_BuildIndex`: `3.99 MiB/op`, `63,050 allocs/op` — in-memory term→postings map during build
- `Vacuum_Small`: `1.71 MiB/op` — full copy-compact-swap; structural cost
- `Distinct_HighCardinality`: `1.69 MiB/op`, `40,144 allocs/op` — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: `1.66 MiB/op`, `16,328 allocs/op` — per-doc postings map
- `Select_FullScan`: `1.24 MiB/op`, `79,827 allocs/op` — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: `1012 KiB/op` — full posting list read into memory on delete
- `Insert_Batch` / `PreparedBatch`: `~251 KiB/op` — ~2.5 KiB/row vs SQLite's 310 B; per-row clone+bind+prepare overhead

Good next optimisation targets:

- Streaming join projection to avoid allocating `Row.Values []OptionalValue` per matched pair
- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Batch insert path that reuses prepared state across rows within a single multi-row statement
- Streaming term extraction for inverted-index build and maintenance
