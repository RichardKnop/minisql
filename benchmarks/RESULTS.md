# Benchmark Results

## 2026-05-27 — Plan Cache Extension for Bound-Condition Queries

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `go test -tags bench -run='^$' -bench=Update_ByPK -benchmem -count=5 ./benchmarks/`

One optimization eliminating the per-call query planning cost for indexed DML prepared statements:

- **Plan cache for bound-condition queries** (`query_plan.go`): `PlanQuery` now caches plans for prepared `UPDATE`/`DELETE`/`SELECT` statements where the `WHERE` clause uses simple indexed equality conditions. On a cache hit, `rehydratePlanIndexKeys` rebuilds only the `IndexKeys` slice from current bound values without re-running the full planner. Three guards ensure correctness: `planConditionsAreCacheable` rejects subquery/expression/placeholder operands that would change the plan shape; `planIsCacheableWithConditions` only caches plans composed entirely of `ScanTypeIndexPoint` scans with no secondary filters or sub-scans; and `rehydratePlanIndexKeys` falls back to full re-planning if key extraction fails.

**Memory improvements vs previous baseline (2026-05-27 Update/Query Planning baseline):**

| Benchmark | Memory before | Memory after | Δ allocs |
|---|---:|---:|---:|
| Update_ByPK/minisql | 5.7 KiB / 53 allocs | 5.2 KiB / 46 allocs | −13% |
| Delete_ByPK/minisql | 5.9 KiB / 73 allocs | 5.3 KiB / 65 allocs | −11% |

## 2026-05-27 — Update/Query Planning Allocation Reduction

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `main`  
**Command:** `LOG_LEVEL=warn go test -tags bench -bench=. -benchmem -count=1 -run '^$' ./benchmarks/`  
**Runtime:** `210.164s`

Four targeted optimizations benefiting all write paths and all indexed queries:

- **`matchedConditions []bool` in `tryMatchIndex`** (`query_plan.go`): Replaced `map[int]bool` with `[]bool` indexed by condition position. Condition indices are contiguous 0..len(group)-1, so a slice is correct and avoids map bucket allocations entirely. For a single-condition WHERE clause (the common case), the slice is 1 byte and stack-allocatable. Added `numMatched int` to `indexMatch` to preserve the count of matched conditions without relying on `len()`. Affects every query that uses index-equality planning — UPDATE, DELETE, SELECT with indexed WHERE, ON CONFLICT.

- **`Table.allFields []Field` and `Table.textOverflowCols []Column` cached fields** (`table.go`, `update.go`, `cursor.go`): Both `fieldsFromColumns(t.Columns...)` and the text-overflow column list are pure functions of `t.Columns`, which is immutable after table construction. Pre-compute them once in `NewTable` and reuse. Removed the now-dead `textOverflowColumns()` helper from `stmt.go`. Eliminates two per-`Update()` call allocations.

- **Pre-size `WriteSet` map in `BeginTransaction`** (`transaction_manager.go`): Changed `make(map[PageIndex]WriteInfo)` to `make(map[PageIndex]WriteInfo, 8)`. A single-row write touches ~3–5 pages (data leaf + PK index leaf + secondary index leaf(s)). Pre-allocating 8 slots avoids all bucket-growth allocations for the common case. Halved `TrackWrite` allocation from 104 MB → 50 MB per 100K-iteration run.

- **Share `Fields` slice for UPDATE in `Statement.Clone`** (`stmt.go`): `BindArguments` for UPDATE only reads `stmt.Fields` (to find which `Updates` map keys hold placeholders) and never mutates it. Share the reference rather than copying, same as the existing INSERT optimisation.

**Memory improvements vs previous baseline (2026-05-27 Insert optimisation baseline):**

| Benchmark | Memory before | Memory after | Δ |
|---|---:|---:|---:|
| Update_ByPK/minisql | 6.5 KiB | 5.7 KiB | −12% |
| OnConflict_DoUpdate/minisql | 2.8 KiB | 2.5 KiB | −8% |
| Select_PointScan/minisql | 4.7 KiB | 4.5 KiB | −4% |
| Delete_ByPK/minisql | 6.1 KiB | 5.9 KiB | −3% |
| Explain/minisql | 6.0 KiB | 5.8 KiB | −4% |

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
| GroupBy_Aggregate/minisql | 1.00 ms | 37.2 KiB | 459 |
| GroupBy_Aggregate/sqlite | 2.89 ms | 3.5 KiB | 309 |
| Having_Filter/minisql | 820.1 µs | 28.8 KiB | 264 |
| Having_Filter/sqlite | 2.29 ms | 1.9 KiB | 111 |
| Distinct_HighCardinality/minisql | 3.81 ms | 1.73 MiB | 40,141 |
| Distinct_HighCardinality/sqlite | 6.53 ms | 586.3 KiB | 40,010 |
| Delete_ByPK/minisql | 22.0 µs | 5.3 KiB | 65 |
| Delete_ByPK/sqlite | 110.1 µs | 447 B | 19 |
| ForeignKey_Insert/minisql | 16.6 µs | 3.0 KiB | 32 |
| ForeignKey_Insert/sqlite | 62.9 µs | 192 B | 8 |
| ForeignKey_DeleteCascade/minisql | 67.5 µs | 10.7 KiB | 135 |
| ForeignKey_DeleteCascade/sqlite | 86.9 µs | 128 B | 5 |
| Insert_SingleRow/minisql | 15.3 µs | 3.3 KiB | 35 |
| Insert_SingleRow/sqlite | 58.9 µs | 311 B | 12 |
| Insert_Batch/minisql | 392.4 µs | 193.2 KiB | 2,748 |
| Insert_Batch/sqlite | 266.6 µs | 31.0 KiB | 1,301 |
| Insert_PreparedBatch/minisql | 400.3 µs | 192.3 KiB | 2,753 |
| Insert_PreparedBatch/sqlite | 269.2 µs | 31.0 KiB | 1,297 |
| Insert_MultiValues/minisql | 222.0 µs | 171.2 KiB | 1,874 |
| Insert_MultiValues/sqlite | 242.2 µs | 25.2 KiB | 613 |
| FullText_BuildIndex/minisql | 4.40 ms | 1.71 MiB | 16,280 |
| FullText_BuildIndex/sqlite | 2.51 ms | 392 B | 20 |
| FullText_Insert_WithIndex/minisql | 55.1 µs | 19.0 KiB | 158 |
| FullText_Insert_WithIndex/sqlite | 105.7 µs | 438 B | 18 |
| FullText_Search_SingleTerm/rare/minisql | 19.5 µs | 4.3 KiB | 67 |
| FullText_Search_SingleTerm/rare/sqlite | 12.3 µs | 392 B | 12 |
| FullText_Search_SingleTerm/medium/minisql | 21.3 µs | 4.3 KiB | 67 |
| FullText_Search_SingleTerm/medium/sqlite | 17.8 µs | 392 B | 12 |
| FullText_Search_SingleTerm/common/minisql | 20.5 µs | 4.3 KiB | 69 |
| FullText_Search_SingleTerm/common/sqlite | 74.3 µs | 408 B | 14 |
| FullText_Search_MultiTermAND/minisql | 36.4 µs | 13.4 KiB | 88 |
| FullText_Search_MultiTermAND/sqlite | 43.3 µs | 392 B | 12 |
| FullText_Search_Phrase/minisql | 37.6 µs | 28.5 KiB | 304 |
| FullText_Search_Phrase/sqlite | 33.9 µs | 400 B | 13 |
| FullText_Search_AfterDeletes/minisql | 112.5 µs | 77.4 KiB | 90 |
| FullText_Update_WithIndex/minisql | 53.8 µs | 24.6 KiB | 208 |
| FullText_Update_WithIndex/sqlite | 166.8 µs | 290 B | 12 |
| FullText_Delete_WithIndex/minisql | 79.8 µs | 26.2 KiB | 202 |
| FullText_Delete_WithIndex/sqlite | 175.0 µs | 135 B | 6 |
| JSONInverted_BuildIndex/minisql_indexed | 6.23 ms | 4.08 MiB | 63,045 |
| JSONInverted_Insert_WithIndex/minisql_indexed | 81.3 µs | 41.9 KiB | 212 |
| JSONInverted_Contains_KeyValue/key_value/minisql_indexed | 32.0 µs | 9.9 KiB | 101 |
| JSONInverted_Contains_KeyValue/key_value/minisql_sequential | 2.38 ms | 2.00 MiB | 38,096 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_scan | 967.0 µs | 409 B | 14 |
| JSONInverted_Contains_KeyValue/key_value/sqlite_json_expr_index | 35.3 µs | 408 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_indexed | 45.9 µs | 11.0 KiB | 141 |
| JSONInverted_Contains_ObjectSubset/object_subset/minisql_sequential | 2.87 ms | 2.00 MiB | 38,118 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_scan | 943.7 µs | 409 B | 14 |
| JSONInverted_Contains_ObjectSubset/object_subset/sqlite_json_expr_index | 149.5 µs | 408 B | 14 |
| JSONInverted_Contains_AfterDeletes/minisql_indexed | 180.4 µs | 74.3 KiB | 118 |
| JSONInverted_Update_WithIndex/minisql_indexed | 11.6 µs | 6.2 KiB | 73 |
| JSONInverted_Delete_WithIndex/minisql_indexed | 455.9 µs | 1011.7 KiB | 389 |
| Join_Inner_SmallLarge/minisql | 7.59 ms | 1.27 MiB | 89,855 |
| Join_Inner_SmallLarge/sqlite | 5.78 ms | 1.09 MiB | 99,757 |
| Join_Inner_LowSelectivity/minisql | 126.8 µs | 23.4 KiB | 1,298 |
| Join_Inner_LowSelectivity/sqlite | 810.9 µs | 11.3 KiB | 1,009 |
| Join_Left_UnmatchedRows/minisql | 4.02 ms | 878.0 KiB | 79,743 |
| Join_Left_UnmatchedRows/sqlite | 4.85 ms | 708.2 KiB | 70,157 |
| Vacuum_Small/minisql | 22.9 ms | 1.53 MiB | 13,014 |
| Vacuum_Small/sqlite | 567.3 µs | 91 B | 4 |
| WAL_Checkpoint/minisql | 323.8 µs | 71.6 KiB | 42 |
| WAL_Checkpoint/sqlite | 154.3 µs | 441 B | 12 |
| Explain/minisql | 6.2 µs | 5.8 KiB | 55 |
| Explain/sqlite | 1.7 µs | 680 B | 18 |
| Select_PointScan/minisql | 6.7 µs | 4.5 KiB | 57 |
| Select_PointScan/sqlite | 4.2 µs | 679 B | 26 |
| Select_Limit/minisql | 8.6 µs | 3.7 KiB | 97 |
| Select_Limit/sqlite | 10.0 µs | 1.7 KiB | 104 |
| Select_FullScan/minisql | 4.23 ms | 1.27 MiB | 79,823 |
| Select_FullScan/sqlite | 6.93 ms | 1.33 MiB | 99,758 |
| Select_CountStar/minisql | 7.2 µs | 2.5 KiB | 27 |
| Select_CountStar/sqlite | 12.8 µs | 400 B | 13 |
| Select_IndexRangeScan/minisql | 1.27 ms | 112.6 KiB | 6,641 |
| Select_IndexRangeScan/sqlite | 894.8 µs | 85.9 KiB | 6,581 |
| Select_SecondaryIndex_LowSelectivity/minisql | 2.66 ms | 437.5 KiB | 29,931 |
| Select_SecondaryIndex_LowSelectivity/sqlite | 3.29 ms | 313.0 KiB | 29,886 |
| Select_SecondaryIndex_LowSelectivityLimit/minisql | 11.4 µs | 5.1 KiB | 111 |
| Select_SecondaryIndex_LowSelectivityLimit/sqlite | 10.4 µs | 1.1 KiB | 64 |
| Select_RangeScan/minisql | 1.73 ms | 84.1 KiB | 5,507 |
| Select_RangeScan/sqlite | 1.00 ms | 85.9 KiB | 6,581 |
| CTE_Materialise/minisql | 947.8 µs | 8.0 KiB | 85 |
| CTE_Materialise/sqlite | 515.3 µs | 400 B | 13 |
| Subquery_InList/minisql | 5.55 ms | 875.0 KiB | 35,098 |
| Subquery_InList/sqlite | 4.26 ms | 234.7 KiB | 20,010 |
| OnConflict_DoUpdate/minisql | 11.4 µs | 2.5 KiB | 34 |
| OnConflict_DoUpdate/sqlite | 53.6 µs | 259 B | 10 |
| Update_ByPK/minisql | 10.1 µs | 5.2 KiB | 46 |
| Update_ByPK/sqlite | 60.5 µs | 263 B | 10 |

## Memory Outliers

The largest remaining memory consumers (minisql only, excluding intentional sequential-scan variants):

- `JSONInverted_BuildIndex`: `4.08 MiB/op`, `63,045 allocs/op` — in-memory term→postings map during build
- `Distinct_HighCardinality`: `1.73 MiB/op`, `40,141 allocs/op` — in-memory dedup set for 10K distinct rows
- `FullText_BuildIndex`: `1.71 MiB/op`, `16,280 allocs/op` — per-doc postings map
- `Vacuum_Small`: `1.53 MiB/op` — full copy-compact-swap; structural cost
- `Join_Inner_SmallLarge`: `1.27 MiB/op`, `89,855 allocs/op` — INLJ result materialization for 10K matched rows
- `Select_FullScan`: `1.27 MiB/op`, `79,823 allocs/op` — ~8 allocs/row from `Materialize()` per RowView
- `JSONInverted_Delete_WithIndex`: `1012 KiB/op` — full posting list read into memory on delete
- `Insert_Batch` / `PreparedBatch`: `~193 KiB/op` — ~1.9 KiB/row vs SQLite's 310 B; remaining cost is per-row clone + B-tree page I/O

Good next optimisation targets:

- Streaming SELECT delivery that reads directly from RowView into the driver dest slice (eliminating `Materialize()`)
- Streaming term extraction for inverted-index build and maintenance
- Reduce per-row clone overhead in `Insert_Batch` (~1.9 KiB/row vs SQLite's 310 B)
