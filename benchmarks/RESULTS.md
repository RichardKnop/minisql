# Benchmark Results

## 2026-05-27 — Greedy Join Planner: Index-Aware Build-Side Selection

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `LOG_LEVEL=warn go test -tags bench -bench=. -benchmem -count=1 -run '^$' ./benchmarks/`  
**Runtime:** `187.066s`

This baseline reflects the greedy join planner improvement merged since the previous measurement (2026-05-26):

- **Index-aware greedy join reordering** (`query_plan_join.go`): `greedyJoinOrder` and `collectJoinGraph` now precompute `indexPartners` — whether each table has an index on its join column for each partner. The start-node selection prefers tables without index-eligible join columns as the probe (base) side, keeping indexed tables as the inner (lookup) side for INLJ. The next-node selection prefers index-eligible candidates over raw row-count minimization. This fixes a regression introduced when greedy reordering was added: the planner was placing `bench_dept` (100 rows, PK on `id`) as the outer probe and `bench_emp` (10K rows, no index on `dept_id`) as the inner hash-build — the wrong direction. The fix restores the pre-greedy INLJ path (emp=probe, dept=inner via PK), reducing `Join_Inner_SmallLarge` memory from **3.46 MiB → 1.24 MiB** (−64%). Time increases from 4.87 ms → 6.39 ms because INLJ does 10K individual PK lookups instead of 100 hash probes; this is the correct trade-off (the previous plan accidentally used the large table as the hash-build side).

## Full Benchmark Baseline

| Benchmark | Time/op | Memory/op | Allocs/op |
|---|---:|---:|---:|
| GroupBy_Aggregate/minisql | 911.5 µs | 37.4 KiB | 463 |
| GroupBy_Aggregate/sqlite | 2.08 ms | 3.5 KiB | 309 |
| Having_Filter/minisql | 733.1 µs | 28.9 KiB | 268 |
| Having_Filter/sqlite | 1.89 ms | 1.9 KiB | 111 |
| Distinct_HighCardinality/minisql | 2.96 ms | 1.69 MiB | 40,144 |
| Distinct_HighCardinality/sqlite | 5.60 ms | 586.3 KiB | 40,010 |
| Delete_ByPK/minisql | 21.1 µs | 7.1 KiB | 85 |
| Delete_ByPK/sqlite | 79.3 µs | 447 B | 19 |
| ForeignKey_Insert/minisql | 14.2 µs | 3.6 KiB | 38 |
| ForeignKey_Insert/sqlite | 56.5 µs | 192 B | 8 |
| ForeignKey_DeleteCascade/minisql | 45.7 µs | 11.2 KiB | 141 |
| ForeignKey_DeleteCascade/sqlite | 50.0 µs | 128 B | 5 |
| Insert_SingleRow/minisql | 13.7 µs | 4.0 KiB | 42 |
| Insert_SingleRow/sqlite | 47.3 µs | 311 B | 12 |
| Insert_Batch/minisql | 365.8 µs | 251.4 KiB | 3,257 |
| Insert_Batch/sqlite | 242.6 µs | 31.0 KiB | 1,300 |
| Insert_PreparedBatch/minisql | 359.7 µs | 250.0 KiB | 3,256 |
| Insert_PreparedBatch/sqlite | 227.2 µs | 31.0 KiB | 1,300 |
| Insert_MultiValues/minisql | 210.8 µs | 206.9 KiB | 2,278 |
| Insert_MultiValues/sqlite | 171.3 µs | 25.2 KiB | 617 |
| FullText_BuildIndex/minisql | 3.17 ms | 1.64 MiB | 16,308 |
| FullText_BuildIndex/sqlite | 1.94 ms | 392 B | 20 |
| FullText_Insert_WithIndex/minisql | 46.1 µs | 22.9 KiB | 184 |
| FullText_Insert_WithIndex/sqlite | 86.8 µs | 439 B | 18 |
| FullText_Search_SingleTerm/rare/minisql | 17.0 µs | 4.6 KiB | 71 |
| FullText_Search_SingleTerm/rare/sqlite | 10.4 µs | 392 B | 12 |
| FullText_Search_SingleTerm/medium/minisql | 16.5 µs | 4.6 KiB | 71 |
| FullText_Search_SingleTerm/medium/sqlite | 11.5 µs | 392 B | 12 |
| FullText_Search_SingleTerm/common/minisql | 18.1 µs | 4.6 KiB | 73 |
| FullText_Search_SingleTerm/common/sqlite | 65.2 µs | 408 B | 14 |
| FullText_Search_MultiTermAND/minisql | 26.9 µs | 13.7 KiB | 92 |
| FullText_Search_MultiTermAND/sqlite | 37.4 µs | 392 B | 12 |
| FullText_Search_Phrase/minisql | 28.9 µs | 28.8 KiB | 308 |
| FullText_Search_Phrase/sqlite | 28.2 µs | 400 B | 13 |
| FullText_Search_AfterDeletes/minisql | 85.5 µs | 78.0 KiB | 94 |
| FullText_Update_WithIndex/minisql | 43.3 µs | 27.5 KiB | 230 |
| FullText_Update_WithIndex/sqlite | 100.2 µs | 291 B | 12 |
| FullText_Delete_WithIndex/minisql | 61.0 µs | 26.1 KiB | 208 |
| FullText_Delete_WithIndex/sqlite | 150.1 µs | 135 B | 6 |
| JSONInverted_BuildIndex/minisql_indexed | 4.51 ms | 3.98 MiB | 63,050 |
| JSONInverted_Insert_WithIndex/minisql_indexed | 59.0 µs | 56.3 KiB | 221 |
| JSONInverted_Contains_KeyValue/key_value/minisql_indexed | 27.8 µs | 10.1 KiB | 105 |
| JSONInverted_Contains_KeyValue/key_value/minisql_sequential | 1.93 ms | 1.96 MiB | 38,100 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_scan | 687.2 µs | 408 B | 14 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_expr_index | 30.3 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_indexed | 38.1 µs | 11.3 KiB | 145 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_sequential | 1.93 ms | 1.96 MiB | 38,122 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_scan | 721.9 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_expr_index | 127.7 µs | 408 B | 14 |
| JSONInverted_Contains_AfterDeletes/minisql_indexed | 136.4 µs | 74.7 KiB | 122 |
| JSONInverted_Update_WithIndex/minisql_indexed | 8.8 µs | 6.9 KiB | 81 |
| JSONInverted_Delete_WithIndex/minisql_indexed | 313.9 µs | 1012.3 KiB | 395 |
| Join_Inner_SmallLarge/minisql | 6.39 ms | 1.24 MiB | 89,859 |
| Join_Inner_SmallLarge/sqlite | 4.76 ms | 1.07 MiB | 99,757 |
| Join_Inner_LowSelectivity/minisql | 111.3 µs | 23.7 KiB | 1,302 |
| Join_Inner_LowSelectivity/sqlite | 732.7 µs | 11.3 KiB | 1,009 |
| Join_Left_UnmatchedRows/minisql | 3.58 ms | 878.4 KiB | 79,747 |
| Join_Left_UnmatchedRows/sqlite | 4.14 ms | 708.2 KiB | 70,157 |
| Vacuum_Small/minisql | 18.48 ms | 1.71 MiB | 15,022 |
| Vacuum_Small/sqlite | 261.4 µs | 89 B | 4 |
| WAL_Checkpoint/minisql | 251.4 µs | 71.6 KiB | 45 |
| WAL_Checkpoint/sqlite | 105.1 µs | 440 B | 12 |
| Explain/minisql | 4.9 µs | 6.0 KiB | 58 |
| Explain/sqlite | 1.2 µs | 680 B | 18 |
| Select_PointScan/minisql | 5.6 µs | 5.0 KiB | 62 |
| Select_PointScan/sqlite | 3.4 µs | 679 B | 26 |
| Select_Limit/minisql | 6.9 µs | 4.1 KiB | 101 |
| Select_Limit/sqlite | 8.0 µs | 1.7 KiB | 104 |
| Select_FullScan/minisql | 3.53 ms | 1.24 MiB | 79,826 |
| Select_FullScan/sqlite | 5.04 ms | 1.29 MiB | 99,758 |
| Select_CountStar/minisql | 5.4 µs | 2.6 KiB | 29 |
| Select_CountStar/sqlite | 9.7 µs | 400 B | 13 |
| Select_IndexRangeScan/minisql | 770.3 µs | 111.7 KiB | 6,645 |
| Select_IndexRangeScan/sqlite | 740.7 µs | 85.9 KiB | 6,581 |
| Select_SecondaryIndex_LowSelectivity/minisql | 2.00 ms | 437.1 KiB | 29,937 |
| Select_SecondaryIndex_LowSelectivity/sqlite | 2.68 ms | 313.0 KiB | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit/minisql | 9.4 µs | 5.7 KiB | 117 |
| Select_SecondaryIndex_LowSelectivityLimit/sqlite | 8.2 µs | 1.1 KiB | 64 |
| Select_RangeScan/minisql | 1.44 ms | 83.8 KiB | 5,511 |
| Select_RangeScan/sqlite | 864.7 µs | 85.9 KiB | 6,581 |
| CTE_Materialise/minisql | 755.8 µs | 8.0 KiB | 89 |
| CTE_Materialise/sqlite | 433.2 µs | 400 B | 13 |
| Subquery_InList/minisql | 4.41 ms | 872.4 KiB | 35,102 |
| Subquery_InList/sqlite | 3.56 ms | 234.7 KiB | 20,010 |
| OnConflict_DoUpdate/minisql | 9.1 µs | 3.1 KiB | 39 |
| OnConflict_DoUpdate/sqlite | 38.6 µs | 259 B | 10 |
| Update_ByPK/minisql | 10.9 µs | 6.8 KiB | 63 |
| Update_ByPK/sqlite | 41.4 µs | 263 B | 10 |

## Memory Outliers

The largest remaining memory consumers (minisql only, excluding intentional sequential-scan variants):

- `JSONInverted_BuildIndex`: `3.98 MiB/op`, `63,050 allocs/op` — in-memory term→postings map during build
- `Vacuum_Small`: `1.71 MiB/op` — full copy-compact-swap; structural cost
- `Distinct_HighCardinality`: `1.69 MiB/op`, `40,144 allocs/op` — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: `1.64 MiB/op`, `16,308 allocs/op` — per-doc postings map
- `Join_Inner_SmallLarge`: `1.24 MiB/op`, `89,859 allocs/op` — INLJ result materialization for 10K matched rows (was 3.46 MiB with wrong hash-build side)
- `Select_FullScan`: `1.24 MiB/op`, `79,826 allocs/op` — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: `1012 KiB/op` — full posting list read into memory on delete
- `Insert_Batch` / `PreparedBatch`: `~251 KiB/op` — ~2.5 KiB/row vs SQLite's 310 B; per-row clone+bind+prepare overhead

Good next optimisation targets:

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Batch insert path that reuses prepared state across rows within a single multi-row statement
- Streaming term extraction for inverted-index build and maintenance
- Hash join with correct small-build-side selection even when inner table has a PK (cost model: when INLJ would do many lookups on a tiny table, prefer hash-build from that small table)
