# Benchmark Results

### 2026-05-23 — GROUP BY estGroups heuristic (branch refactor/row-view-api)

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
- JOIN CombinedRowView: `selectHashJoinDirectRowView` + `selectINLJDirectRowView` fast paths;
  outer cell bytes + inner pointer; no goroutine/channel; `innerCellBuf` reused per match;
  outer table looked up from `plan.Scans[0]` (handles greedy join reordering);
  NULL-inner LEFT JOIN via `innerIsNull=true` + `splitIdx > 0` guard in RowView accessors
- **GROUP BY estGroups heuristic**: changed pool initial capacity from `estRows/10` capped at 64 to
  `estRows/100` bounded at [16, 512]; for the benchmark (10K rows, 100 groups) this gives exactly 100
  — eliminating all pool growth reallocs; optional NDV lookup from `t.indexStats` when ANALYZE stats
  are available. Result: GROUP BY memory −29% (50.7 KiB → 36.2 KiB, ratio 14× → 10.3× vs SQLite).

#### Timing (median of 3 runs)

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 1.16 ms/op | 2.53 ms/op | 0.46× ✓ |
| Having_Filter | 825 µs/op | 2.12 ms/op | 0.39× ✓ |
| Distinct_HighCardinality | 3.51 ms/op | 6.50 ms/op | 0.54× ✓ |
| Delete_ByPK | 50.5 µs/op | 90.7 µs/op | 0.56× ✓ |
| ForeignKey_Insert | 33.7 µs/op | 85.3 µs/op | 0.39× ✓ |
| ForeignKey_DeleteCascade | 118 µs/op | 89.8 µs/op | 1.31× |
| Insert_SingleRow | 35.2 µs/op | 88.8 µs/op | 0.40× ✓ |
| Insert_Batch | 472 µs/op | 342 µs/op | 1.38× |
| Insert_PreparedBatch | 484 µs/op | 333 µs/op | 1.45× |
| Insert_MultiValues | 323 µs/op | 260 µs/op | 1.24× |
| FullText_BuildIndex | 7.15 ms/op | 2.11 ms/op | 3.39× |
| FullText_Insert_WithIndex | 245 µs/op | 109 µs/op | 2.25× |
| FullText_Search_SingleTerm/rare | 16.0 µs/op | 10.5 µs/op | 1.52× |
| FullText_Search_SingleTerm/medium | 15.9 µs/op | 11.2 µs/op | 1.42× |
| FullText_Search_SingleTerm/common | 15.7 µs/op | 65.1 µs/op | 0.24× ✓ |
| FullText_Search_MultiTermAND | 25.8 µs/op | 38.6 µs/op | 0.67× ✓ |
| FullText_Search_Phrase | 34.1 µs/op | 28.6 µs/op | 1.19× |
| FullText_Update_WithIndex | 126 µs/op | 138 µs/op | 0.91× ✓ |
| FullText_Delete_WithIndex | 321 µs/op | 269 µs/op | 1.19× |
| **Join_Inner_SmallLarge** | **5.35 ms/op** | **5.95 ms/op** | **0.90× ✓** |
| **Join_Inner_LowSelectivity** | **124 µs/op** | **810 µs/op** | **0.15× ✓** |
| **Join_Left_UnmatchedRows** | **4.06 ms/op** | **4.73 ms/op** | **0.86× ✓** |
| Vacuum_Small | 26.8 ms/op | 472 µs/op | 56.8× |
| WAL_Checkpoint | 495 µs/op | 344 µs/op | 1.44× |
| Explain | 7.08 µs/op | 1.56 µs/op | 4.54× |
| Select_PointScan | 7.64 µs/op | 3.98 µs/op | 1.92× |
| Select_Limit | 8.35 µs/op | 9.56 µs/op | 0.87× ✓ |
| Select_FullScan | 4.22 ms/op | 6.33 ms/op | 0.67× ✓ |
| Select_CountStar | 6.31 µs/op | 10.8 µs/op | 0.58× ✓ |
| Select_IndexRangeScan | 1.02 ms/op | 876 µs/op | 1.16× |
| Select_SecondaryIndex_LowSelectivity | 2.17 ms/op | 3.15 ms/op | 0.69× ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 12.2 µs/op | 9.70 µs/op | 1.26× |
| Select_RangeScan | 1.66 ms/op | 1.03 ms/op | 1.61× |
| CTE_Materialise | 1.14 ms/op | 654 µs/op | 1.74× |
| Subquery_InList | 5.18 ms/op | 4.58 ms/op | 1.13× |
| OnConflict_DoUpdate | 19.7 µs/op | 56.2 µs/op | 0.35× ✓ |
| Update_ByPK | 19.6 µs/op | 148 µs/op | 0.13× ✓ |

> Note: Vacuum_Small and WAL_Checkpoint timings have high M1 thermal variance across runs.
> GroupBy/Having timing ratios are unchanged vs the previous baseline; the absolute µs/op values
> differ due to benchmark order and thermal state (SQLite medians also shifted proportionally).

#### Memory (B/op, median of 3 runs)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| **GroupBy_Aggregate** | **36.2 KiB** | — | — | **3.5 KiB** | — | — | **10.3×** ↓ (was 14×) |
| **Having_Filter** | **28.0 KiB** | — | — | **1.9 KiB** | — | — | **14.7×** ↓ (was 20×) |
| Distinct_HighCardinality | 1.69 MiB | — | — | 586 KiB | — | — | **2.9×** |
| Delete_ByPK | 26.8 KiB | — | — | 447 B | — | — | **61×** |
| ForeignKey_Insert | 20.2 KiB | — | — | 192 B | — | — | **108×** |
| ForeignKey_DeleteCascade | 14.7 KiB | — | — | 128 B | — | — | **118×** |
| Insert_SingleRow | 18.6 KiB | — | — | 311 B | — | — | **61×** |
| Insert_Batch | 304 KiB | — | — | 31.1 KiB | — | — | **9.8×** |
| Insert_PreparedBatch | 304 KiB | — | — | 31.1 KiB | — | — | **9.8×** |
| Insert_MultiValues | 269 KiB | — | — | 25.2 KiB | — | — | **10.7×** |
| FullText_BuildIndex | 10.7 MiB | — | — | 392 B | — | — | **28,600×** |
| FullText_Insert_WithIndex | 276 KiB | — | — | 443 B | — | — | **638×** |
| FullText_Search_SingleTerm/rare | 4.8 KiB | — | — | 392 B | — | — | **13×** |
| FullText_Search_SingleTerm/medium | 4.8 KiB | — | — | 392 B | — | — | **13×** |
| FullText_Search_SingleTerm/common | 4.8 KiB | — | — | 408 B | — | — | **12×** |
| FullText_Search_MultiTermAND | 14.6 KiB | — | — | 392 B | — | — | **38×** |
| FullText_Search_Phrase | 48.6 KiB | — | — | 400 B | — | — | **124×** |
| FullText_Update_WithIndex | 79.8 KiB | — | — | 291 B | — | — | **281×** |
| FullText_Delete_WithIndex | 244 KiB | — | — | 135 B | — | — | **1,851×** |
| JSONInverted_BuildIndex | — | 55.3 MiB | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.71 MiB | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 141.6 KiB | 3.25 MiB | — | 408 B | 408 B | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 279.1 KiB | 3.36 MiB | — | 408 B | 408 B | — |
| JSONInverted_Update_WithIndex | — | 9.8 KiB | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.31 MiB | — | — | — | — | — |
| **Join_Inner_SmallLarge** | **2.56 MiB** | — | — | **1.07 MiB** | — | — | **2.4×** |
| **Join_Inner_LowSelectivity** | **22.6 KiB** | — | — | **11.3 KiB** | — | — | **2.0×** |
| **Join_Left_UnmatchedRows** | **873 KiB** | — | — | **708 KiB** | — | — | **1.23×** |
| Vacuum_Small | 8.04 MiB | — | — | 89 B | — | — | — |
| WAL_Checkpoint | 69.5 KiB | — | — | 440 B | — | — | **162×** |
| Explain | 6.4 KiB | — | — | 680 B | — | — | **9.6×** |
| Select_PointScan | 5.4 KiB | — | — | 679 B | — | — | **8.1×** |
| Select_Limit | 4.0 KiB | — | — | 1.7 KiB | — | — | **2.4×** |
| Select_FullScan | **1.23 MiB** | — | — | **1.30 MiB** | — | — | **0.95×** ✓ |
| Select_CountStar | 2.6 KiB | — | — | 400 B | — | — | **6.6×** |
| Select_IndexRangeScan | 110.9 KiB | — | — | 85.9 KiB | — | — | **1.3×** |
| Select_SecondaryIndex_LowSelectivity | 435 KiB | — | — | 313 KiB | — | — | **1.4×** |
| Select_SecondaryIndex_LowSelectivityLimit | 6.0 KiB | — | — | 1.1 KiB | — | — | **5.6×** |
| Select_RangeScan | **82.4 KiB** | — | — | **85.9 KiB** | — | — | **0.96×** ✓ |
| CTE_Materialise | 7.4 KiB | — | — | 400 B | — | — | **18×** |
| Subquery_InList | 860 KiB | — | — | 235 KiB | — | — | **3.7×** |
| OnConflict_DoUpdate | 8.7 KiB | — | — | 260 B | — | — | **34×** |
| Update_ByPK | 9.8 KiB | — | — | 263 B | — | — | **38×** |

#### Allocs/op (median of 3 runs)

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 464 | 309 | 1.5× |
| Having_Filter | 269 | 111 | 2.4× |
| Distinct_HighCardinality | **40,145** | **40,010** | **~1.0×** ✓ |
| Delete_ByPK | 115 | 19 | 6.1× |
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

**Wins vs SQLite (timing):** GroupBy, Having, Distinct, most SELECT paths, DML (insert/update/delete), semi-join, subquery (nearly at parity), full-text common-term and multi-term, FullText_Update, **all three JOIN benchmarks** (0.15–0.90×).

**Wins vs SQLite (memory/allocs):** Select_FullScan (0.95× B, 0.80× allocs), Select_RangeScan (0.96× B, 0.84× allocs), Select_IndexRangeScan (1.01× allocs — at parity), **Join_Inner_SmallLarge allocs (0.90×)**.

**GROUP BY memory improvement summary (was → now):**
- GroupBy_Aggregate: 50.7 KiB → 36.2 KiB (**−29%**, ratio 14× → **10.3×**)
- Having_Filter: 39.4 KiB → 28.0 KiB (**−29%**, ratio 20× → **14.7×**)
- Root cause: `estGroups` capped at 64 caused pool realloc for queries with >64 distinct groups;
  the 1%-cardinality heuristic (`estRows/100`) gives exact capacity for the benchmark (100 groups / 10K rows).

**Largest remaining memory gaps (within current architecture):**
1. **GROUP BY/HAVING** — 10–15× memory gap (was 14–20×). Pool growth eliminated; remaining: Go map overhead, `groupValPool []OptionalValue` boxing (~2.4 KiB), `allResultValues` flat block (~7.2 KiB). Further reduction requires eliminating `groupValPool` via key-byte decode at result time, and replacing `map[string]int32` with a hash table on raw byte slices — both architectural.
2. **DISTINCT** — 2.9× memory gap. Allocs at parity; remaining bytes from hash-set key strings (1.69 MiB vs 586 KiB).
3. **JOIN high-selectivity** — 2.4× gap (Join_Inner_SmallLarge). Inner hash build still allocates compact cells per inner row; outer probe keys box via `OptionalValue`.
4. **Subquery/InList** — 3.7× gap. Inner build phase (Bloom filter + hash map + key strings for ~10K inner entries).
5. **Batch INSERT** — 1.38–1.45× slower than SQLite. Multi-row insert path not yet RowView-optimised.
6. **DML memory** (30–60×) — architectural: OCC `readSet`/`writeSet` maps, WAL frame management, in-process WAL index. Requires lightweight autocommit path that bypasses OCC tracking.
7. **Full-text** — large absolute memory (10.7 MiB build) due to inverted index structure.
