# Benchmark Results

## 2026-05-27 — Insert Allocation Reduction: Prep Cache, Unsafe String Reuse, logger.Check

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `LOG_LEVEL=warn go test -tags bench -bench=. -benchmem -count=1 -run '^$' ./benchmarks/`  
**Runtime:** `195.108s`

Four targeted optimizations reducing per-row allocation overhead on all write paths:

- **`insertPrepCache`** (`stmt.go`, `database.go`): Prepared INSERT statements now cache the static `colFieldIdx []int` (column→field-index mapping) and the reordered `sortedFields []Field` slice in a `sync.Once`-guarded struct shared across all `Clone()` calls. Previously, `prepareInsert()` recomputed and allocated these on every `Exec`. Eliminated ~132 MB of allocation per 3326-row benchmark run (~40 KB/op). One-shot (non-prepared) INSERTs are unaffected.

- **Zero-copy string→TextPointer** (`stmt.go`): `toInternalArgs` now uses `unsafe.Slice(unsafe.StringData(v), len(v))` to view the string's backing memory as `[]byte` without copying. Safe because `args []driver.NamedValue` keeps the string alive until `ExecContext` returns. Eliminated ~50 MB of string-copy allocation per run (~15 KB/op).

- **`logger.Check` pattern on all hot paths** (`insert.go`, `table_primary_key.go`, `cursor.go`, `table_secondary_index.go`, `table_unique_index.go`, `transaction_manager.go`, `select.go`, `update.go`, `delete.go`, `update_from.go`, `table.go`): `logger.Debug(msg, fields...)` allocates a `[]zap.Field` variadic slice unconditionally even when debug is disabled. Converting every hot-path debug log to `if ce := logger.Check(level, msg); ce != nil { ce.Write(...) }` makes them zero-alloc at `LOG_LEVEL=warn`. Biggest win: Insert_SingleRow −17% allocs, Insert_Batch −16% allocs.

- **`LeafNode.Unmarshal` cap+1** (`leaf_node.go`): Changed `make([]Cell, N)` to `make([]Cell, N, N+1)` so the first `append` after page load (inserting a new cell into a just-read page) does not immediately trigger a backing-array realloc. Small but correct.

**Memory improvements vs previous baseline (2026-05-27 greedy join baseline):**

| Benchmark | Memory before | Memory after | Δ |
|---|---:|---:|---:|
| Insert_SingleRow/minisql | 4.0 KiB | 3.3 KiB | −17% |
| Insert_Batch/minisql | 251.4 KiB | 193.0 KiB | −23% |
| Insert_PreparedBatch/minisql | 250.0 KiB | 192.5 KiB | −23% |
| Insert_MultiValues/minisql | 206.9 KiB | 170.7 KiB | −17% |
| Delete_ByPK/minisql | 7.1 KiB | 6.1 KiB | −14% |
| ForeignKey_Insert/minisql | 3.6 KiB | 3.0 KiB | −17% |
| FullText_Insert_WithIndex/minisql | 22.9 KiB | 19.1 KiB | −17% |

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
| GroupBy_Aggregate/minisql | 951.1 µs | 37.4 KiB | 459 |
| GroupBy_Aggregate/sqlite | 2.35 ms | 3.5 KiB | 309 |
| Having_Filter/minisql | 783.7 µs | 28.7 KiB | 264 |
| Having_Filter/sqlite | 2.05 ms | 1.9 KiB | 111 |
| Distinct_HighCardinality/minisql | 3.68 ms | 1.73 MiB | 40,141 |
| Distinct_HighCardinality/sqlite | 6.17 ms | 586.3 KiB | 40,010 |
| Delete_ByPK/minisql | 25.0 µs | 6.1 KiB | 74 |
| Delete_ByPK/sqlite | 94.9 µs | 447 B | 19 |
| ForeignKey_Insert/minisql | 15.3 µs | 3.0 KiB | 32 |
| ForeignKey_Insert/sqlite | 51.8 µs | 192 B | 8 |
| ForeignKey_DeleteCascade/minisql | 84.0 µs | 10.9 KiB | 136 |
| ForeignKey_DeleteCascade/sqlite | 86.8 µs | 128 B | 5 |
| Insert_SingleRow/minisql | 15.5 µs | 3.3 KiB | 35 |
| Insert_SingleRow/sqlite | 51.6 µs | 311 B | 12 |
| Insert_Batch/minisql | 425.6 µs | 193.0 KiB | 2,746 |
| Insert_Batch/sqlite | 268.1 µs | 31.0 KiB | 1,301 |
| Insert_PreparedBatch/minisql | 400.3 µs | 192.5 KiB | 2,754 |
| Insert_PreparedBatch/sqlite | 243.4 µs | 31.0 KiB | 1,300 |
| Insert_MultiValues/minisql | 240.0 µs | 170.7 KiB | 1,870 |
| Insert_MultiValues/sqlite | 197.5 µs | 25.2 KiB | 615 |
| FullText_BuildIndex/minisql | 5.07 ms | 1.73 MiB | 16,382 |
| FullText_BuildIndex/sqlite | 2.51 ms | 392 B | 20 |
| FullText_Insert_WithIndex/minisql | 52.6 µs | 19.1 KiB | 158 |
| FullText_Insert_WithIndex/sqlite | 99.4 µs | 439 B | 18 |
| FullText_Search_SingleTerm/rare/minisql | 19.3 µs | 4.3 KiB | 67 |
| FullText_Search_SingleTerm/rare/sqlite | 11.8 µs | 392 B | 12 |
| FullText_Search_SingleTerm/medium/minisql | 19.9 µs | 4.3 KiB | 67 |
| FullText_Search_SingleTerm/medium/sqlite | 12.5 µs | 392 B | 12 |
| FullText_Search_SingleTerm/common/minisql | 20.9 µs | 4.3 KiB | 69 |
| FullText_Search_SingleTerm/common/sqlite | 69.4 µs | 408 B | 14 |
| FullText_Search_MultiTermAND/minisql | 37.7 µs | 13.4 KiB | 88 |
| FullText_Search_MultiTermAND/sqlite | 42.7 µs | 392 B | 12 |
| FullText_Search_Phrase/minisql | 51.4 µs | 28.4 KiB | 304 |
| FullText_Search_Phrase/sqlite | 31.6 µs | 400 B | 13 |
| FullText_Search_AfterDeletes/minisql | 144.8 µs | 77.3 KiB | 90 |
| FullText_Update_WithIndex/minisql | 55.5 µs | 25.3 KiB | 212 |
| FullText_Update_WithIndex/sqlite | 106.2 µs | 290 B | 12 |
| FullText_Delete_WithIndex/minisql | 66.6 µs | 26.1 KiB | 202 |
| FullText_Delete_WithIndex/sqlite | 146.1 µs | 135 B | 6 |
| JSONInverted_BuildIndex/minisql_indexed | 7.24 ms | 4.08 MiB | 63,043 |
| JSONInverted_Insert_WithIndex/minisql_indexed | 72.7 µs | 48.4 KiB | 213 |
| JSONInverted_Contains_KeyValue/key_value/minisql_indexed | 32.1 µs | 9.8 KiB | 101 |
| JSONInverted_Contains_KeyValue/key_value/minisql_sequential | 3.15 ms | 2.00 MiB | 38,096 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_scan | 734.1 µs | 408 B | 14 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_expr_index | 32.5 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_indexed | 46.4 µs | 11.0 KiB | 141 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_sequential | 3.18 ms | 2.00 MiB | 38,118 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_scan | 784.9 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_expr_index | 141.3 µs | 408 B | 14 |
| JSONInverted_Contains_AfterDeletes/minisql_indexed | 183.6 µs | 74.3 KiB | 118 |
| JSONInverted_Update_WithIndex/minisql_indexed | 12.8 µs | 6.6 KiB | 76 |
| JSONInverted_Delete_WithIndex/minisql_indexed | 530.2 µs | 1011.9 KiB | 390 |
| Join_Inner_SmallLarge/minisql | 7.04 ms | 1.27 MiB | 89,855 |
| Join_Inner_SmallLarge/sqlite | 6.19 ms | 1.09 MiB | 99,757 |
| Join_Inner_LowSelectivity/minisql | 137.9 µs | 23.4 KiB | 1,298 |
| Join_Inner_LowSelectivity/sqlite | 780.5 µs | 11.3 KiB | 1,009 |
| Join_Left_UnmatchedRows/minisql | 4.26 ms | 878.3 KiB | 79,743 |
| Join_Left_UnmatchedRows/sqlite | 4.89 ms | 708.2 KiB | 70,157 |
| Vacuum_Small/minisql | 22.1 ms | 1.49 MiB | 13,002 |
| Vacuum_Small/sqlite | 612.2 µs | 91 B | 4 |
| WAL_Checkpoint/minisql | 332.0 µs | 71.6 KiB | 42 |
| WAL_Checkpoint/sqlite | 163.8 µs | 441 B | 12 |
| Explain/minisql | 7.2 µs | 6.0 KiB | 56 |
| Explain/sqlite | 1.8 µs | 680 B | 18 |
| Select_PointScan/minisql | 7.3 µs | 4.7 KiB | 58 |
| Select_PointScan/sqlite | 4.3 µs | 679 B | 26 |
| Select_Limit/minisql | 8.5 µs | 3.7 KiB | 97 |
| Select_Limit/sqlite | 10.0 µs | 1.7 KiB | 104 |
| Select_FullScan/minisql | 4.46 ms | 1.27 MiB | 79,823 |
| Select_FullScan/sqlite | 6.30 ms | 1.33 MiB | 99,758 |
| Select_CountStar/minisql | 6.2 µs | 2.5 KiB | 27 |
| Select_CountStar/sqlite | 10.6 µs | 400 B | 13 |
| Select_IndexRangeScan/minisql | 1.43 ms | 113.5 KiB | 6,641 |
| Select_IndexRangeScan/sqlite | 824.5 µs | 85.9 KiB | 6,581 |
| Select_SecondaryIndex_LowSelectivity/minisql | 2.27 ms | 437.1 KiB | 29,932 |
| Select_SecondaryIndex_LowSelectivity/sqlite | 3.03 ms | 313.0 KiB | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit/minisql | 11.7 µs | 5.3 KiB | 112 |
| Select_SecondaryIndex_LowSelectivityLimit/sqlite | 9.8 µs | 1.1 KiB | 64 |
| Select_RangeScan/minisql | 1.60 ms | 83.6 KiB | 5,507 |
| Select_RangeScan/sqlite | 943.7 µs | 85.9 KiB | 6,581 |
| CTE_Materialise/minisql | 803.7 µs | 7.9 KiB | 85 |
| CTE_Materialise/sqlite | 460.1 µs | 400 B | 13 |
| Subquery_InList/minisql | 4.92 ms | 872.4 KiB | 35,098 |
| Subquery_InList/sqlite | 3.94 ms | 234.7 KiB | 20,010 |
| OnConflict_DoUpdate/minisql | 10.4 µs | 2.8 KiB | 35 |
| OnConflict_DoUpdate/sqlite | 41.9 µs | 260 B | 10 |
| Update_ByPK/minisql | 12.3 µs | 6.5 KiB | 57 |
| Update_ByPK/sqlite | 43.5 µs | 263 B | 10 |

## Memory Outliers

The largest remaining memory consumers (minisql only, excluding intentional sequential-scan variants):

- `JSONInverted_BuildIndex`: `4.08 MiB/op`, `63,043 allocs/op` — in-memory term→postings map during build
- `Distinct_HighCardinality`: `1.73 MiB/op`, `40,141 allocs/op` — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: `1.73 MiB/op`, `16,382 allocs/op` — per-doc postings map
- `Vacuum_Small`: `1.49 MiB/op` — full copy-compact-swap; structural cost
- `Join_Inner_SmallLarge`: `1.27 MiB/op`, `89,855 allocs/op` — INLJ result materialization for 10K matched rows (was 3.46 MiB with wrong hash-build side)
- `Select_FullScan`: `1.27 MiB/op`, `79,823 allocs/op` — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: `1012 KiB/op` — full posting list read into memory on delete
- `Insert_Batch` / `PreparedBatch`: `~193 KiB/op` — ~1.9 KiB/row vs SQLite's 310 B; reduced from 251 KiB by prep cache + unsafe string reuse + logger.Check; remaining cost is per-row clone + B-tree page I/O

Good next optimisation targets:

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Streaming term extraction for inverted-index build and maintenance
- Reduce per-row INSERT clone overhead (Statement.Clone allocates inner maps/slices even for identical prepared execs)
