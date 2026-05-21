# Benchmark Results

### 2026-05-21 — Row-view refactor baseline (branch refactor/row-view-api)

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=1s -count=3` · mean of 3 runs shown
**Branch:** `refactor/row-view-api` — RowView streaming implemented for sequential
scan, direct SELECT, COUNT(*), and GROUP BY zero-alloc paths. Baseline before
continued join / index-scan / aggregate RowView work.

#### Timing (mean of 3 runs)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 956 µs/op | — | — | 2.52 ms/op | — | — | 0.4× |
| Having_Filter | 691 µs/op | — | — | 2.15 ms/op | — | — | 0.3× |
| Distinct_HighCardinality | 4.98 ms/op | — | — | 6.33 ms/op | — | — | 0.8× |
| Delete_ByPK | 84.45 µs/op | — | — | 570 µs/op | — | — | 0.1× |
| ForeignKey_Insert | 35.86 µs/op | — | — | 219 µs/op | — | — | 0.2× |
| ForeignKey_DeleteCascade | 145 µs/op | — | — | 151 µs/op | — | — | 1.0× |
| Insert_SingleRow | 24.84 µs/op | — | — | 79.1 µs/op | — | — | 0.3× |
| Insert_Batch | 441 µs/op | — | — | 269 µs/op | — | — | 1.6× |
| Insert_PreparedBatch | 432 µs/op | — | — | 290 µs/op | — | — | 1.5× |
| Insert_MultiValues | 301 µs/op | — | — | 208 µs/op | — | — | 1.4× |
| FullText_BuildIndex | 9.71 ms/op | — | — | 6.46 ms/op | — | — | 1.5× |
| FullText_Insert_WithIndex | 283 µs/op | — | — | 214 µs/op | — | — | 1.3× |
| FullText_Search_SingleTerm/rare | 18.27 µs/op | — | — | 11.96 µs/op | — | — | 1.5× |
| FullText_Search_SingleTerm/medium | 19.46 µs/op | — | — | 13.18 µs/op | — | — | 1.5× |
| FullText_Search_SingleTerm/common | 18.02 µs/op | — | — | 79.85 µs/op | — | — | 0.2× |
| FullText_Search_MultiTermAND | 31.87 µs/op | — | — | 47.75 µs/op | — | — | 0.7× |
| FullText_Search_Phrase | 50.92 µs/op | — | — | 39.55 µs/op | — | — | 1.3× |
| FullText_Update_WithIndex | 133 µs/op | — | — | 133 µs/op | — | — | 1.0× |
| FullText_Delete_WithIndex | 256 µs/op | — | — | 155 µs/op | — | — | 1.7× |
| JSONInverted_BuildIndex | — | 31.0 ms/op | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 892 µs/op | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 76.5 µs/op | 4.04 ms/op | — | 36.7 µs/op | 853 µs/op | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 138 µs/op | 4.22 ms/op | — | 145 µs/op | 846 µs/op | — |
| JSONInverted_Update_WithIndex | — | 15.4 µs/op | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 603 µs/op | — | — | — | — | — |
| Join_Inner_SmallLarge | 11.1 ms/op | — | — | 5.52 ms/op | — | — | 2.0× |
| Join_Left_UnmatchedRows | 17.5 ms/op | — | — | 4.75 ms/op | — | — | 3.7× |
| Vacuum_Small | 25.9 ms/op | — | — | 374 µs/op | — | — | 69× |
| WAL_Checkpoint | 264 µs/op | — | — | 98.5 µs/op | — | — | 2.7× |
| Explain | 7.10 µs/op | — | — | 1.71 µs/op | — | — | 4.2× |
| Select_PointScan | 7.80 µs/op | — | — | 4.14 µs/op | — | — | 1.9× |
| Select_Limit | 9.67 µs/op | — | — | 10.05 µs/op | — | — | 1.0× |
| Select_FullScan | 4.04 ms/op | — | — | 6.16 ms/op | — | — | 0.7× |
| Select_CountStar | 5.98 µs/op | — | — | 10.58 µs/op | — | — | 0.6× |
| Select_IndexRangeScan | 1.16 ms/op | — | — | 947 µs/op | — | — | 1.2× |
| Select_SecondaryIndex_LowSelectivity | 2.32 ms/op | — | — | 3.24 ms/op | — | — | 0.7× |
| Select_SecondaryIndex_LowSelectivityLimit | 10.4 µs/op | — | — | 11.5 µs/op | — | — | 0.9× |
| Select_RangeScan | 1.65 ms/op | — | — | 958 µs/op | — | — | 1.7× |
| CTE_Materialise | 848 µs/op | — | — | 492 µs/op | — | — | 1.7× |
| Subquery_InList | 11.9 ms/op | — | — | 4.22 ms/op | — | — | 2.8× |
| OnConflict_DoUpdate | 12.1 µs/op | — | — | 44.4 µs/op | — | — | 0.3× |
| Update_ByPK | 13.3 µs/op | — | — | 52.1 µs/op | — | — | 0.3× |

#### Memory (B/op, mean of 3 runs)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 375.8 KiB | — | — | 3.5 KiB | — | — | **107×** |
| Having_Filter | 287.7 KiB | — | — | 1.9 KiB | — | — | **149×** |
| Distinct_HighCardinality | 2.98 MiB | — | — | 586 KiB | — | — | **5.2×** |
| Delete_ByPK | 45.5 KiB | — | — | 446 B | — | — | **104×** |
| ForeignKey_Insert | 20.7 KiB | — | — | 191 B | — | — | **111×** |
| ForeignKey_DeleteCascade | 15.0 KiB | — | — | 128 B | — | — | **120×** |
| Insert_SingleRow | 19.1 KiB | — | — | 311 B | — | — | **63×** |
| Insert_Batch | 312 KiB | — | — | 31.0 KiB | — | — | **10×** |
| Insert_PreparedBatch | 310 KiB | — | — | 31.7 KiB | — | — | **10×** |
| Insert_MultiValues | 275 KiB | — | — | 25.8 KiB | — | — | **11×** |
| FullText_BuildIndex | 10.7 MiB | — | — | 392 B | — | — | **28,600×** |
| FullText_Insert_WithIndex | 274 KiB | — | — | 438 B | — | — | **640×** |
| FullText_Search_SingleTerm/rare | 4.9 KiB | — | — | 392 B | — | — | **13×** |
| FullText_Search_SingleTerm/medium | 4.9 KiB | — | — | 392 B | — | — | **13×** |
| FullText_Search_SingleTerm/common | 4.9 KiB | — | — | 408 B | — | — | **12×** |
| FullText_Search_MultiTermAND | 14.8 KiB | — | — | 392 B | — | — | **39×** |
| FullText_Search_Phrase | 49.6 KiB | — | — | 400 B | — | — | **127×** |
| FullText_Update_WithIndex | 103 KiB | — | — | 291 B | — | — | **362×** |
| FullText_Delete_WithIndex | 243 KiB | — | — | 135 B | — | — | **1,843×** |
| JSONInverted_BuildIndex | — | 55.6 MiB | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.67 MiB | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 142 KiB | 3.24 MiB | — | 408 B | 408 B | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 272 KiB | 3.36 MiB | — | 408 B | 408 B | — |
| JSONInverted_Update_WithIndex | — | 14.3 KiB | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.30 MiB | — | — | — | — | — |
| Join_Inner_SmallLarge | 10.95 MiB | — | — | 1.07 MiB | — | — | **10.2×** |
| Join_Left_UnmatchedRows | 11.70 MiB | — | — | 690 KiB | — | — | **17.3×** |
| Vacuum_Small | 8.04 MiB | — | — | 90 B | — | — | — |
| WAL_Checkpoint | 71.4 KiB | — | — | 440 B | — | — | **166×** |
| Explain | 6.6 KiB | — | — | 680 B | — | — | **10×** |
| Select_PointScan | 5.7 KiB | — | — | 679 B | — | — | **8.6×** |
| Select_Limit | 5.8 KiB | — | — | 1.73 KiB | — | — | **3.4×** |
| Select_FullScan | **1.24 MiB** | — | — | **1.29 MiB** | — | — | **0.96×** ✓ |
| Select_CountStar | 2.6 KiB | — | — | 400 B | — | — | **6.5×** |
| Select_IndexRangeScan | 242 KiB | — | — | 85.9 KiB | — | — | **2.8×** |
| Select_SecondaryIndex_LowSelectivity | 848 KiB | — | — | 313 KiB | — | — | **2.7×** |
| Select_SecondaryIndex_LowSelectivityLimit | 7.1 KiB | — | — | 1.10 KiB | — | — | **6.5×** |
| Select_RangeScan | 210 KiB | — | — | 85.9 KiB | — | — | **2.4×** |
| CTE_Materialise | 8.4 KiB | — | — | 400 B | — | — | **21×** |
| Subquery_InList | 5.78 MiB | — | — | 235 KiB | — | — | **25×** |
| OnConflict_DoUpdate | 8.8 KiB | — | — | 259 B | — | — | **35×** |
| Update_ByPK | 10.2 KiB | — | — | 263 B | — | — | **40×** |

#### Allocs/op (mean of 3 runs)

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 465 | 309 | 1.5× |
| Having_Filter | 270 | 111 | 2.4× |
| Distinct_HighCardinality | 90,140 | 40,010 | 2.3× |
| Delete_ByPK | 164 | 19 | 8.6× |
| Insert_SingleRow | 56 | 12 | 4.7× |
| Join_Inner_SmallLarge | 150,012 | 99,757 | 1.5× |
| Join_Left_UnmatchedRows | 209,895 | 70,157 | 3.0× |
| Select_FullScan | **79,828** | **99,758** | **0.80×** ✓ |
| Select_IndexRangeScan | 8,873 | 6,581 | 1.3× |
| Select_SecondaryIndex_LowSelectivity | 39,946 | 29,886 | 1.3× |
| Select_RangeScan | 7,719 | 6,581 | 1.2× |
| Subquery_InList | 134,876 | 20,010 | 6.7× |

#### Key observations from this baseline

**Wins:** `Select_FullScan` is **at parity** with SQLite in both memory (0.96×) and allocs (0.80×) — the RowView streaming path for simple sequential SELECT is working. `GroupBy_Aggregate` alloc count is nearly at parity (1.5×). Many DML operations are 2–3× faster than SQLite.

**Largest memory gaps:**
1. **JOIN** — 10–17× gap. `combineRowsWithSchema` allocates `[]OptionalValue` per combined row; hash join stores all inner rows as materialised `Row` structs; results collected into `[]Row` before projection.
2. **Subquery/InList** — 25× gap. IN-list subquery falls through to resolveSubqueries (LIMIT in subquery blocks semi-join lift); sequential IN-key scan materialises every matched row.
3. **GROUP BY/HAVING** — 107–149× gap on **memory** despite only 1.5–2.4× alloc gap. Root cause: `aggStatePool` pre-allocated at `estRows/10 × numAggs` entries, each `aggState` holds two `OptionalValue` (48 bytes for min/max) even for COUNT/SUM queries that never use them.
4. **DISTINCT** — 5.2× gap. Materialises all rows into `[]Row` for deduplication; uses `Row.Key` comparison instead of lazy key computation.
5. **Index scans** — 2.4–2.8× gap. `indexedScanRow` and `rowIDScanRow` still materialise `Row` via `view.MaterializeWithOverflow` for every fetched row, even on paths that already have a RowView iterator.

---

### 2026-05-20 17:10 UTC

#### Timing

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 820.48 µs/op | — | — | 2.16 ms/op | — | — | 0.4× |
| Having_Filter | 789.93 µs/op | — | — | 2.00 ms/op | — | — | 0.4× |
| Distinct_HighCardinality | 4.22 ms/op | — | — | 6.07 ms/op | — | — | 0.7× |
| Delete_ByPK | 36.82 µs/op | — | — | 86.54 µs/op | — | — | 0.4× |
| ForeignKey_Insert | 19.44 µs/op | — | — | 47.38 µs/op | — | — | 0.4× |
| ForeignKey_DeleteCascade | 55.65 µs/op | — | — | 48.74 µs/op | — | — | 1.1× |
| Insert_SingleRow | 17.81 µs/op | — | — | 43.55 µs/op | — | — | 0.4× |
| Insert_Batch | 388.34 µs/op | — | — | 235.68 µs/op | — | — | 1.6× |
| Insert_PreparedBatch | 378.91 µs/op | — | — | 234.35 µs/op | — | — | 1.6× |
| Insert_MultiValues | 240.51 µs/op | — | — | 171.56 µs/op | — | — | 1.4× |
| FullText_BuildIndex | 6.85 ms/op | — | — | 2.04 ms/op | — | — | 3.4× |
| FullText_Insert_WithIndex | 196.10 µs/op | — | — | 90.77 µs/op | — | — | 2.2× |
| FullText_Search_SingleTerm/rare | 15.88 µs/op | — | — | 10.35 µs/op | — | — | 1.5× |
| FullText_Search_SingleTerm/medium | 15.77 µs/op | — | — | 11.17 µs/op | — | — | 1.4× |
| FullText_Search_SingleTerm/common | 15.76 µs/op | — | — | 63.79 µs/op | — | — | 0.2× |
| FullText_Search_MultiTermAND | 27.16 µs/op | — | — | 36.95 µs/op | — | — | 0.7× |
| FullText_Search_Phrase | 35.73 µs/op | — | — | 28.09 µs/op | — | — | 1.3× |
| FullText_Update_WithIndex | 86.81 µs/op | — | — | 100.70 µs/op | — | — | 0.9× |
| FullText_Delete_WithIndex | 179.92 µs/op | — | — | 134.86 µs/op | — | — | 1.3× |
| JSONInverted_BuildIndex | — | 16.82 ms/op | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 639.46 µs/op | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 48.70 µs/op | 2.80 ms/op | — | 30.51 µs/op | 713.32 µs/op | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 83.48 µs/op | 3.10 ms/op | — | 129.47 µs/op | 754.84 µs/op | — |
| JSONInverted_Update_WithIndex | — | 11.37 µs/op | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 421.47 µs/op | — | — | — | — | — |
| Join_Inner_SmallLarge | 8.71 ms/op | — | — | 4.84 ms/op | — | — | 1.8× |
| Join_Left_UnmatchedRows | 15.43 ms/op | — | — | 4.16 ms/op | — | — | 3.7× |
| Vacuum_Small | 20.16 ms/op | — | — | 278.73 µs/op | — | — | 72.3× |
| WAL_Checkpoint | 347.18 µs/op | — | — | 69.09 µs/op | — | — | 5.0× |
| Explain | 5.11 µs/op | — | — | 1.19 µs/op | — | — | 4.3× |
| Select_PointScan | 5.46 µs/op | — | — | 3.59 µs/op | — | — | 1.5× |
| Select_Limit | 7.79 µs/op | — | — | 7.77 µs/op | — | — | 1.0× |
| Select_FullScan | 3.30 ms/op | — | — | 5.06 ms/op | — | — | 0.7× |
| Select_CountStar | 5.31 µs/op | — | — | 9.77 µs/op | — | — | 0.5× |
| Select_IndexRangeScan | 567.80 µs/op | — | — | 750.54 µs/op | — | — | 0.8× |
| Select_SecondaryIndex_LowSelectivity | 2.12 ms/op | — | — | 2.82 ms/op | — | — | 0.8× |
| Select_SecondaryIndex_LowSelectivityLimit | 9.06 µs/op | — | — | 8.21 µs/op | — | — | 1.1× |
| Select_RangeScan | 1.44 ms/op | — | — | 868.41 µs/op | — | — | 1.7× |
| CTE_Materialise | 690.01 µs/op | — | — | 440.30 µs/op | — | — | 1.6× |
| Subquery_InList | 9.21 ms/op | — | — | 3.62 ms/op | — | — | 2.5× |
| OnConflict_DoUpdate | 12.70 µs/op | — | — | 38.02 µs/op | — | — | 0.3× |
| Update_ByPK | 12.27 µs/op | — | — | 36.58 µs/op | — | — | 0.3× |

#### Memory (B/op)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan |
|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 366.6 KiB | — | — | 3.5 KiB | — | — |
| Having_Filter | 281.5 KiB | — | — | 1.9 KiB | — | — |
| Distinct_HighCardinality | 2.9 MiB | — | — | 586.3 KiB | — | — |
| Delete_ByPK | 33.4 KiB | — | — | 447 B | — | — |
| ForeignKey_Insert | 20.2 KiB | — | — | 192 B | — | — |
| ForeignKey_DeleteCascade | 14.7 KiB | — | — | 128 B | — | — |
| Insert_SingleRow | 18.7 KiB | — | — | 311 B | — | — |
| Insert_Batch | 303.7 KiB | — | — | 31.0 KiB | — | — |
| Insert_PreparedBatch | 302.9 KiB | — | — | 31.0 KiB | — | — |
| Insert_MultiValues | 268.3 KiB | — | — | 25.2 KiB | — | — |
| FullText_BuildIndex | 10.5 MiB | — | — | 392 B | — | — |
| FullText_Insert_WithIndex | 267.7 KiB | — | — | 439 B | — | — |
| FullText_Search_SingleTerm/rare | 4.8 KiB | — | — | 392 B | — | — |
| FullText_Search_SingleTerm/medium | 4.8 KiB | — | — | 392 B | — | — |
| FullText_Search_SingleTerm/common | 4.8 KiB | — | — | 408 B | — | — |
| FullText_Search_MultiTermAND | 14.7 KiB | — | — | 392 B | — | — |
| FullText_Search_Phrase | 52.1 KiB | — | — | 400 B | — | — |
| FullText_Update_WithIndex | 87.1 KiB | — | — | 291 B | — | — |
| FullText_Delete_WithIndex | 237.5 KiB | — | — | 135 B | — | — |
| JSONInverted_BuildIndex | — | 55.4 MiB | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.7 MiB | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 142.0 KiB | 3.2 MiB | — | 408 B | 408 B |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 285.2 KiB | 3.3 MiB | — | 408 B | 408 B |
| JSONInverted_Update_WithIndex | — | 12.1 KiB | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.3 MiB | — | — | — | — |
| Join_Inner_SmallLarge | 11.0 MiB | — | — | 1.1 MiB | — | — |
| Join_Left_UnmatchedRows | 11.7 MiB | — | — | 708.2 KiB | — | — |
| Vacuum_Small | 8.0 MiB | — | — | 89 B | — | — |
| WAL_Checkpoint | 69.6 KiB | — | — | 440 B | — | — |
| Explain | 6.4 KiB | — | — | 680 B | — | — |
| Select_PointScan | 5.7 KiB | — | — | 679 B | — | — |
| Select_Limit | 5.7 KiB | — | — | 1.7 KiB | — | — |
| Select_FullScan | 1.2 MiB | — | — | 1.3 MiB | — | — |
| Select_CountStar | 2.5 KiB | — | — | 400 B | — | — |
| Select_IndexRangeScan | 255.5 KiB | — | — | 85.9 KiB | — | — |
| Select_SecondaryIndex_LowSelectivity | 828.0 KiB | — | — | 313.0 KiB | — | — |
| Select_SecondaryIndex_LowSelectivityLimit | 6.8 KiB | — | — | 1.1 KiB | — | — |
| Select_RangeScan | 204.6 KiB | — | — | 85.9 KiB | — | — |
| CTE_Materialise | 8.0 KiB | — | — | 400 B | — | — |
| Subquery_InList | 5.8 MiB | — | — | 234.7 KiB | — | — |
| OnConflict_DoUpdate | 8.7 KiB | — | — | 259 B | — | — |
| Update_ByPK | 9.9 KiB | — | — | 263 B | — | — |




## 2026-05-19 — GROUP BY / HAVING zero-alloc streaming

**GROUP BY zero-alloc sequential scan:** `selectGroupBy` materialised every
matching row into a `[]Row` buffer before grouping — each row required
`make([]OptionalValue, nCols)` from `UnmarshalWithMask` (91.75% of all
allocs). Refactored into a `groupByAccumulator` struct (`process` / `buildResult`
methods) + `selectGroupByZeroAlloc` which, for single sequential scans, iterates
directly over B-tree cells with one reused `[]OptionalValue` buffer (same pattern
as `countSequentialScanZeroAlloc`). GROUP BY column values are copied into the
flat `groupValPool` inside `process`, so the buffer reuse is safe. Falls back to
the materialising path for virtual tables and parallel scans.

Net: **GroupBy_Aggregate** allocs 10,483 → 466 (96% drop), 2,082 µs → 803 µs
(2.6× speedup, now **2.8× faster than SQLite**). **Having_Filter** allocs
10,287 → 271 (97% drop), 1,944 µs → 767 µs (2.5× speedup, now **2.5× faster
than SQLite**).

---

## 2026-05-19 — WAL aliasing fix + FK cascade benchmark redesign

**`Cell.Unmarshal` aliasing fix (correctness):** `Cell.Unmarshal` previously
sub-sliced `c.Value` directly into the WAL frame buffer. When the frame was
returned to `pageDataPool` on the next commit and reused as `pageBuf`, writes
corrupted the cell data still referenced by cached pages — producing a corrupt
WAL frame that panicked on the next cache miss
(`index out of range [65560] with length 3806`). Fix: copy value bytes into
owned memory (`make+copy`, `isOwned=true`). Cost: +1 alloc per cell per
cache-miss page load (~20 allocs/op added to Delete_ByPK, ~22 to Insert).
Panic eliminated at 25 000-iteration cascade-delete benchmark run.

**FK cascade benchmark redesign:** `BenchmarkForeignKey_DeleteCascade`
pre-seeded all `b.N` rows before the timed loop. Calibration saw O(b.N²) cost
(each delete scanned a growing table with no FK-column index), causing the
framework to settle on tiny b.N values (≈ 136 at -benchtime=1s vs ≈ 125 k
previously) where each delete traversed a much larger pre-seeded table. Result:
spurious 7 ms/op vs the correct ≈ 50 µs/op. Fixed by moving insert inside the
loop with `b.StopTimer`/`b.StartTimer`: each iteration inserts 1 parent + 10
children (untimed) then deletes the parent (timed). Table size is always 0 net
per iteration; timing is b.N-independent.

---

## 2026-05-19 — CTE / CountStar zero-alloc optimisation

**CTE_Materialise improvements (2026-05-19):** `UnmarshalWithMask` refactored
to extract a shared `decodeColumnsWithMask` inner loop; added
`unmarshalWithMaskInto` which accepts a caller-supplied values buffer so the
dominant `make([]OptionalValue, n)` allocation can be eliminated for COUNT(\*)
scans. `countSequentialScanZeroAlloc` pre-allocates one reuse buffer before the
scan loop and re-uses it across every row — safe because COUNT(\*) never retains
a row after the predicate check. Net: CTE_Materialise allocs 14,134 → 92
(99% reduction), 1,851 µs → 787 µs (2.4× speedup). CountStar allocs 706 → 30
(96% reduction), 29.5 µs → 6.4 µs (4.6× speedup, now faster than SQLite).

---

## 2026-05-18 — Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26  
**Settings:** `-benchtime=1s -count=3` · median of 3 runs shown  
**Commit:** `fix/delete-cascade-panic` branch (WAL aliasing fix applied)

---

### SELECT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| PointScan | 8,701 | 69 | 4,054 | **2.15×** |
| Limit | 9,980 | 129 | 9,492 | **1.05×** |
| FullScan (10k rows) | 6,243,000 | 109,845 | 6,503,975 | **0.96×** ✓ |
| CountStar | 6,388 | 30 | 10,617 | **0.60×** ✓ |
| IndexRangeScan | 1,020,200 | 11,077 | 865,700 | **1.18×** |
| RangeScan | 2,535,559 | 19,922 | 994,145 | **2.55×** |
| SecondaryIndex_LowSelectivity (5k rows) | 3,883,000 | 54,952 | 3,119,121 | **1.25×** |
| SecondaryIndex_LowSelectivityLimit | 14,840 | 166 | 9,422 | **1.57×** |

### INSERT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| SingleRow | 23,516 | 56 | 63,692 | **0.37×** ✓ |
| Batch (100 rows) | 471,600 | 3,372 | 283,300 | **1.66×** |
| PreparedBatch (100 rows) | 465,576 | 3,378 | 316,700 | **1.47×** |
| MultiValues (100 rows) | 275,068 | 2,497 | 191,739 | **1.43×** |

### UPDATE / DELETE / UPSERT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Update_ByPK | 17,085 | 75 | 41,610 | **0.41×** ✓ |
| OnConflict_DoUpdate | 14,552 | 49 | 39,680 | **0.37×** ✓ |
| Delete_ByPK | 31,370 | 131 | 98,560 | **0.32×** ✓ |

### JOIN

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Join_Inner_SmallLarge (10k rows) | 13,055,000 | 150,000 | 6,025,173 | **2.17×** |
| Join_Left_UnmatchedRows (10k rows) | 22,140,000 | 199,893 | 4,894,000 | **4.52×** |

### GROUP BY / HAVING / DISTINCT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| GroupBy_Aggregate (100 groups) | 803,000 | 466 | 2,218,000 | **0.36×** ✓ |
| Having_Filter (100 groups) | 767,000 | 271 | 1,910,000 | **0.40×** ✓ |
| Distinct_HighCardinality (10k rows) | 5,380,000 | 110,160 | 5,395,000 | **1.00×** ✓ |

### SUBQUERY / CTE

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Subquery_InList (5k rows) | 12,837,000 | 134,879 | 4,019,000 | **3.19×** |
| CTE_Materialise | 787,400 | 92 | 538,400 | **1.46×** |

### FOREIGN KEY

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| ForeignKey_Insert | 18,940 | 51 | 43,970 | **0.43×** ✓ |
| ForeignKey_DeleteCascade | 51,071 | 170 | 49,351 | **1.03×** |

### FULL-TEXT SEARCH

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| BuildIndex (1k docs) | 10,730,000 | 35,097 | 2,383,000 | **4.50×** |
| Insert_WithIndex | 294,100 | 1,234 | 94,280 | **3.12×** |
| Search_SingleTerm/rare (1 match) | 19,600 | 72 | 10,870 | **1.80×** |
| Search_SingleTerm/medium (10 matches) | 19,720 | 72 | 12,080 | **1.63×** |
| Search_SingleTerm/common (1k matches) | 19,690 | 74 | 69,600 | **0.28×** ✓ |
| Search_MultiTermAND (10 matches) | 34,970 | 104 | 42,930 | **0.81×** ✓ |
| Search_Phrase (100 matches) | 56,170 | 526 | 30,630 | **1.84×** |
| Update_WithIndex | 145,600 | 679 | 116,200 | **1.25×** |
| Delete_WithIndex | 251,300 | 1,715 | 146,400 | **1.72×** |

**Search improvements (2026-05-18/19):** Parser pre-computes `strings.ToUpper`
once per query (was per-token×keyword). Single-term COUNT(\*) uses `DocFreq`
from the index entry header — O(log N) vs O(N). Rare/medium dropped from
~11× to ~1.6×; common flipped to 3.6× faster than SQLite. Phrase search:
replaced `map[RowID][]uint32` postings with sorted `[]invertedPosting` +
binary search (eliminating per-row map allocations); phrase adjacency check
replaced with zero-alloc binary search on sorted position arrays; COUNT(\*)
with index-covered predicates skips B-tree row fetch entirely. Phrase dropped
from 9.5× to 1.8×. MultiTermAND now faster than SQLite.

**Maintenance improvements (2026-05-19):** `invertedEntryPage.Marshal` and
`invertedPostingPage.Marshal` now write cells/blocks directly into the
destination page buffer — eliminates one `make([]byte)` allocation per
cell/block. Added `groupInvertedPostingsInPlace` — sorts and groups in-place
with zero allocations for the common all-unique-RowID case. Removed redundant
`groupInvertedPostings` calls throughout the codec. Net: Insert allocs
3,164 → 1,234 (−61%), Update_WithIndex now 1.25× (was 1.39×), Delete_WithIndex
1.72× (was 1.96×).

### JSON INVERTED INDEX

No SQLite equivalent (minisql-only feature).

| Benchmark | minisql ns/op | minisql allocs/op |
|---|---:|---:|
| BuildIndex (1k docs) | 34,750,000 | 144,390 |
| Insert_WithIndex | 989,600 | 724 |
| Update_WithIndex | 18,537 | 93 |
| Delete_WithIndex | 740,400 | 668 |
| Contains_KeyValue (indexed, 334 matches) | 93,670 | 147 |
| Contains_ObjectSubset (indexed, 334 matches) | 143,300 | 218 |

SQLite with expression index: `Contains_KeyValue` ~32 µs (2.9× faster),
`Contains_ObjectSubset` ~140 µs (1.0×, at parity).

### MAINTENANCE

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Vacuum_Small | 26,540,000 | 21,509 | 637,700 | **41.6×** |
| WAL_Checkpoint | 300,262 | 46 | 122,220 | **2.46×** |
| Explain | 6,804 | 68 | 1,683 | **4.04×** |

Vacuum gap is expected — minisql does a full copy-compact-swap; SQLite reclaims
free pages in-place. Not a meaningful comparison.

---

### Summary: biggest gaps vs SQLite

Ranked by ratio (excluding Vacuum):

| Benchmark | Ratio | allocs/op |
|---|---:|---:|
| Join_Left_UnmatchedRows | **4.52×** | 199,893 |
| FullText_BuildIndex | **4.50×** | 35,097 |
| Explain | **4.04×** | 68 |
| Subquery_InList | **3.19×** | 134,879 |
| FullText_Insert_WithIndex | **3.12×** | 1,234 |
| RangeScan | **2.55×** | 19,922 |
| WAL_Checkpoint | **2.46×** | 46 |
| Join_Inner_SmallLarge | **2.17×** | 150,000 |
| PointScan | **2.15×** | 69 |
| FullText_Search_Phrase | **1.84×** | 526 |
| FullText_Search_SingleTerm/rare | **1.80×** | 72 |
| FullText_Search_SingleTerm/medium | **1.63×** | 72 |
| SecondaryIndex_LowSelectivityLimit | **1.57×** | 166 |
| CTE_Materialise | **1.46×** | 92 |
| FullText_Delete_WithIndex | **1.72×** | 1,715 |

### Summary: at parity or faster than SQLite

| Benchmark | Ratio |
|---|---:|
| Delete_ByPK | **0.32×** (3.1× faster) |
| OnConflict_DoUpdate | **0.37×** (2.7× faster) |
| Insert_SingleRow | **0.37×** (2.7× faster) |
| GroupBy_Aggregate | **0.36×** (2.8× faster) |
| Having_Filter | **0.40×** (2.5× faster) |
| Update_ByPK | **0.41×** (2.4× faster) |
| ForeignKey_Insert | **0.43×** (2.3× faster) |
| CountStar | **0.60×** (1.7× faster) |
| FullText_Search_SingleTerm/common | **0.28×** (3.6× faster) |
| FullText_Search_MultiTermAND | **0.81×** (1.2× faster) |
| ForeignKey_DeleteCascade | **1.03×** |
| Select_FullScan | **0.96×** |
| Distinct_HighCardinality | **1.00×** |
| Select_Limit | **1.05×** |
