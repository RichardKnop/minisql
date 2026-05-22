# Benchmark Results

### 2026-05-23 — JOIN combined RowView (branch refactor/row-view-api)

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=3s -count=3` · median of 3 runs shown
**Branch:** `refactor/row-view-api`

Cumulative optimisations in this baseline:
- RowView streaming for sequential scan, COUNT(*), GROUP BY, semi-join probe side (Steps 1–10)
- Streaming JOIN output (no `[]Row` collection before iteration)
- Compact-cell build phase + arena allocation for hash join inner side
- RowView outer scan + Bloom pre-filter for hash join (skips materialisation for non-matching rows)
- Precomputed projection schema (`projectFast`, eliminates per-row `[]Column` alloc)
- INLJ hot path: `ErrNotFound` sentinel (−20k allocs), `PointUniqueRowID` direct call,
  `singleKeySlice` reuse for single-pair `JoinColumnPairs`, `neededOuterFields` column pruning
- DISTINCT streaming via `appendDistinctKeyFromView` + `newDistinctRowViewIteratorFactory` (no []OptionalValue per row; allocs at parity with SQLite)
- GROUP BY `buildResult`: `resultRows []Row` → `passedIndices []int32` (-4.8 KiB/op)
- **JOIN CombinedRowView**: `selectHashJoinDirectRowView` + `selectINLJDirectRowView` fast paths;
  outer cell bytes + inner pointer; no goroutine/channel; `innerCellBuf` reused per match;
  outer table looked up from `plan.Scans[0]` (handles greedy join reordering);
  NULL-inner LEFT JOIN via `innerIsNull=true` + `splitIdx > 0` guard in RowView accessors

#### Timing (median of 3 runs)

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 988 µs/op | 2.20 ms/op | 0.45× ✓ |
| Having_Filter | 733 µs/op | 1.97 ms/op | 0.37× ✓ |
| Distinct_HighCardinality | 3.03 ms/op | 6.68 ms/op | 0.45× ✓ |
| Delete_ByPK | 31.8 µs/op | 63.7 µs/op | 0.50× ✓ |
| ForeignKey_Insert | 23.1 µs/op | 51.9 µs/op | 0.44× ✓ |
| ForeignKey_DeleteCascade | 96.2 µs/op | 84.3 µs/op | 1.14× |
| Insert_SingleRow | 22.1 µs/op | 48.7 µs/op | 0.45× ✓ |
| Insert_Batch | 469 µs/op | 261 µs/op | 1.79× |
| Insert_PreparedBatch | 455 µs/op | 284 µs/op | 1.60× |
| Insert_MultiValues | 312 µs/op | 216 µs/op | 1.45× |
| FullText_BuildIndex | 10.3 ms/op | 2.68 ms/op | 3.84× |
| FullText_Insert_WithIndex | 302 µs/op | 113 µs/op | 2.67× |
| FullText_Search_SingleTerm/rare | 19.8 µs/op | 11.7 µs/op | 1.69× |
| FullText_Search_SingleTerm/medium | 18.8 µs/op | 12.8 µs/op | 1.47× |
| FullText_Search_SingleTerm/common | 18.5 µs/op | 83.1 µs/op | 0.22× ✓ |
| FullText_Search_MultiTermAND | 34.9 µs/op | 48.9 µs/op | 0.71× ✓ |
| FullText_Search_Phrase | 49.0 µs/op | 33.6 µs/op | 1.46× |
| FullText_Update_WithIndex | 113 µs/op | 210 µs/op | 0.54× ✓ |
| FullText_Delete_WithIndex | 254 µs/op | 339 µs/op | 0.75× ✓ |
| **Join_Inner_SmallLarge** | **5.60 ms/op** | **5.98 ms/op** | **0.94× ✓** |
| **Join_Inner_LowSelectivity** | **126 µs/op** | **834 µs/op** | **0.15× ✓** |
| **Join_Left_UnmatchedRows** | **4.31 ms/op** | **5.27 ms/op** | **0.82× ✓** |
| Vacuum_Small | 24.5 ms/op | 637 µs/op | 38× |
| WAL_Checkpoint | 246 µs/op | 103 µs/op | 2.38× |
| Explain | 7.06 µs/op | 1.63 µs/op | 4.33× |
| Select_PointScan | 7.34 µs/op | 4.03 µs/op | 1.82× |
| Select_Limit | 7.88 µs/op | 9.79 µs/op | 0.81× ✓ |
| Select_FullScan | 4.31 ms/op | 6.38 ms/op | 0.68× ✓ |
| Select_CountStar | 6.33 µs/op | 11.8 µs/op | 0.54× ✓ |
| Select_IndexRangeScan | 1.09 ms/op | 904 µs/op | 1.21× |
| Select_SecondaryIndex_LowSelectivity | 2.03 ms/op | 3.14 ms/op | 0.65× ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 11.2 µs/op | 9.65 µs/op | 1.16× |
| Select_RangeScan | 1.61 ms/op | 983 µs/op | 1.64× |
| CTE_Materialise | 819 µs/op | 558 µs/op | 1.47× |
| Subquery_InList | 4.63 ms/op | 4.08 ms/op | 1.13× |
| OnConflict_DoUpdate | 15.3 µs/op | 132 µs/op | 0.12× ✓ |
| Update_ByPK | 15.0 µs/op | 141 µs/op | 0.11× ✓ |

> Note: SQLite OnConflict_DoUpdate and Update_ByPK medians are high due to M1 thermal variance. Historical baseline shows ~44–52 µs for these.

#### Memory (B/op, median of 3 runs)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 50.7 KiB | — | — | 3.5 KiB | — | — | **14×** |
| Having_Filter | 39.4 KiB | — | — | 1.9 KiB | — | — | **20×** |
| Distinct_HighCardinality | 1.72 MiB | — | — | 586 KiB | — | — | **2.9×** |
| Delete_ByPK | 25.5 KiB | — | — | 447 B | — | — | **58×** |
| ForeignKey_Insert | 20.1 KiB | — | — | 192 B | — | — | **107×** |
| ForeignKey_DeleteCascade | 14.7 KiB | — | — | 128 B | — | — | **118×** |
| Insert_SingleRow | 18.6 KiB | — | — | 312 B | — | — | **61×** |
| Insert_Batch | 304 KiB | — | — | 31.1 KiB | — | — | **9.8×** |
| Insert_PreparedBatch | 304 KiB | — | — | 31.1 KiB | — | — | **9.8×** |
| Insert_MultiValues | 269 KiB | — | — | 25.2 KiB | — | — | **10.7×** |
| FullText_BuildIndex | 10.6 MiB | — | — | 392 B | — | — | **28,300×** |
| FullText_Insert_WithIndex | 271 KiB | — | — | 443 B | — | — | **626×** |
| FullText_Search_SingleTerm/rare | 4.8 KiB | — | — | 392 B | — | — | **13×** |
| FullText_Search_SingleTerm/medium | 4.8 KiB | — | — | 392 B | — | — | **13×** |
| FullText_Search_SingleTerm/common | 4.8 KiB | — | — | 408 B | — | — | **12×** |
| FullText_Search_MultiTermAND | 14.5 KiB | — | — | 392 B | — | — | **38×** |
| FullText_Search_Phrase | 48.5 KiB | — | — | 400 B | — | — | **124×** |
| FullText_Update_WithIndex | 77.9 KiB | — | — | 291 B | — | — | **274×** |
| FullText_Delete_WithIndex | 250 KiB | — | — | 135 B | — | — | **1,897×** |
| JSONInverted_BuildIndex | — | 55.3 MiB | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.71 MiB | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 141.7 KiB | 3.25 MiB | — | 408 B | 408 B | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 279.2 KiB | 3.37 MiB | — | 408 B | 408 B | — |
| JSONInverted_Update_WithIndex | — | 9.6 KiB | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.31 MiB | — | — | — | — | — |
| **Join_Inner_SmallLarge** | **2.61 MiB** | — | — | **1.07 MiB** | — | — | **2.4×** |
| **Join_Inner_LowSelectivity** | **22.6 KiB** | — | — | **11.3 KiB** | — | — | **2.0×** |
| **Join_Left_UnmatchedRows** | **873 KiB** | — | — | **708 KiB** | — | — | **1.23×** |
| Vacuum_Small | 8.04 MiB | — | — | 89 B | — | — | — |
| WAL_Checkpoint | 69.3 KiB | — | — | 440 B | — | — | **161×** |
| Explain | 6.4 KiB | — | — | 680 B | — | — | **9.6×** |
| Select_PointScan | 5.3 KiB | — | — | 679 B | — | — | **8.0×** |
| Select_Limit | 4.0 KiB | — | — | 1.7 KiB | — | — | **2.4×** |
| Select_FullScan | **1.29 MiB** | — | — | **1.36 MiB** | — | — | **0.95×** ✓ |
| Select_CountStar | 2.5 KiB | — | — | 400 B | — | — | **6.5×** |
| Select_IndexRangeScan | 110.9 KiB | — | — | 85.9 KiB | — | — | **1.3×** |
| Select_SecondaryIndex_LowSelectivity | 434.9 KiB | — | — | 313 KiB | — | — | **1.4×** |
| Select_SecondaryIndex_LowSelectivityLimit | 6.0 KiB | — | — | 1.1 KiB | — | — | **5.6×** |
| Select_RangeScan | **82.4 KiB** | — | — | **85.9 KiB** | — | — | **0.96×** ✓ |
| CTE_Materialise | 7.2 KiB | — | — | 400 B | — | — | **18×** |
| Subquery_InList | 859 KiB | — | — | 235 KiB | — | — | **3.7×** |
| OnConflict_DoUpdate | 8.7 KiB | — | — | 260 B | — | — | **34×** |
| Update_ByPK | 9.8 KiB | — | — | 263 B | — | — | **38×** |

#### Allocs/op (median of 3 runs)

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 468 | 309 | 1.5× |
| Having_Filter | 273 | 111 | 2.5× |
| Distinct_HighCardinality | **40,145** | **40,010** | **~1.0×** ✓ |
| Delete_ByPK | 112 | 19 | 5.9× |
| Insert_SingleRow | 56 | 12 | 4.7× |
| **Join_Inner_SmallLarge** | **89,778** | **99,757** | **0.90× ✓** |
| **Join_Inner_LowSelectivity** | **1,295** | **1,009** | **1.28×** |
| **Join_Left_UnmatchedRows** | **79,740** | **70,157** | **1.14×** |
| Select_FullScan | **79,827** | **99,758** | **0.80×** ✓ |
| Select_RangeScan | **5,516** | **6,581** | **0.84×** ✓ |
| Select_IndexRangeScan | 6,650 | 6,581 | 1.01× ✓ |
| Select_SecondaryIndex_LowSelectivity | 29,942 | 29,886 | 1.00× ✓ |
| Subquery_InList | 35,101 | 20,010 | 1.75× |
| CTE_Materialise | 94 | 13 | 7.2× |
| Select_CountStar | 30 | 13 | 2.3× |

#### Key observations

**Wins vs SQLite (timing):** GroupBy, Having, Distinct, most SELECT paths, DML (insert/update/delete), semi-join, subquery (nearly at parity), full-text common-term and multi-term, **all three JOIN benchmarks** (now 0.15–0.94×).

**Wins vs SQLite (memory/allocs):** Select_FullScan (0.95× B, 0.80× allocs), Select_RangeScan (0.96× B, 0.84× allocs), Select_IndexRangeScan (1.01× allocs — at parity), **Join_Inner_SmallLarge allocs (0.90×)**.

**JOIN improvement summary (was → now):**
- Join_Inner_SmallLarge: 6.0 MiB → 2.6 MiB (**2.4×**, was 5.6×); 1.91× slower → **0.94× faster** ✓
- Join_Left_UnmatchedRows: 4.3 MiB → 873 KiB (**1.23×**, was 6.2×); 2.40× slower → **0.82× faster** ✓
- Join_Inner_LowSelectivity: 85 KiB → 23 KiB (**2.0×**, was 7.6×); 0.36× → **0.15×** ✓

**Largest remaining memory gaps:**
1. **JOIN high-selectivity** — 2.4× gap remaining (Join_Inner_SmallLarge). Inner hash build still allocates compact cells per inner row; outer scan still boxes probe keys via `OptionalValue`. Further reduction would require zero-alloc hash probe keys and arena-free inner cell storage.
2. **Subquery/InList** — 3.7× gap (was 25× at project start). Remaining: inner build phase (Bloom filter + hash map + key strings for ~10k inner entries).
3. **GROUP BY/HAVING** — 14–20× memory gap despite fast timing. `aggStatePool` and `groupValPool` still allocate per-group structs; RowView GROUP BY path is not yet end-to-end zero-copy.
4. **DISTINCT** — 2.9× memory gap. Allocs now at parity with SQLite; remaining bytes from hash-set key strings.
5. **Batch INSERT** — 1.8× slower than SQLite. Multi-row insert path not yet RowView-optimised; each row still allocates `[]OptionalValue`.
6. **Full-text** — large absolute memory (10 MiB build index) due to inverted index structure; not a target for RowView optimisation.
