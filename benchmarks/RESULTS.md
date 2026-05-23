# Benchmark Results

### 2026-05-23 — OCC removal + in-place LRU + RowView streaming

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=3s -count=1` · single run
**Branch:** `refactor/enforce-single-writer-remove-OCC-layer`

Cumulative optimisations in this baseline:

**Write-path (OCC removal + in-place LRU):**
- **OCC removed**: write transactions no longer track per-page read-sets or validate
  conflicts at commit; `activeWriters` atomic enforces single writer instead.
  Eliminates `map[PageIndex]versionEntry` allocation per read-page per transaction.
- **In-place LRU modification** (`ModifyPage` fast path): when no snapshot reader
  is active and the page has been committed at least once, the write transaction
  modifies the shared LRU page directly — no `Page.Clone()` call.
  Rollback evicts the slot via `InvalidatePage` so the next read reloads from WAL.
- **Per-file connection guard** in `Driver.Open`: returns `ErrDatabaseAlreadyOpen`
  when a second `sql.Open` targets the same file path.

**Read-path (RowView streaming):**
- RowView streaming for sequential scan, COUNT(*), GROUP BY, HAVING, semi-join probe side
- Streaming JOIN output (no `[]Row` collection before iteration)
- Compact-cell build phase + arena allocation for hash join inner side
- RowView outer scan + Bloom pre-filter for hash join
- Precomputed projection schema (`projectFast`, eliminates per-row `[]Column` alloc)
- INLJ hot path: `ErrNotFound` sentinel, `PointUniqueRowID` direct call,
  `singleKeySlice` reuse, `neededOuterFields` column pruning
- DISTINCT streaming via `appendDistinctKeyFromView` + `newDistinctRowViewIteratorFactory`
- GROUP BY `buildResult`: `resultRows []Row` → `passedIndices []int32`
- JOIN CombinedRowView fast paths (`selectHashJoinDirectRowView`, `selectINLJDirectRowView`)
- GROUP BY `estGroups` heuristic (`estRows/100`, bounded at [16, 512]); optional NDV
  lookup from `t.indexStats` when ANALYZE stats are available

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 962 µs/op | 2.14 ms/op | **0.45×** ✓ |
| Having_Filter | 712 µs/op | 1.92 ms/op | **0.37×** ✓ |
| Distinct_HighCardinality | 2.95 ms/op | 5.67 ms/op | **0.52×** ✓ |
| Delete_ByPK | 18.5 µs/op | 56.8 µs/op | **0.33×** ✓ |
| ForeignKey_Insert | 13.8 µs/op | 42.9 µs/op | **0.32×** ✓ |
| ForeignKey_DeleteCascade | 46.7 µs/op | 48.2 µs/op | **0.97×** ✓ |
| Insert_SingleRow | 13.9 µs/op | 44.6 µs/op | **0.31×** ✓ |
| Insert_Batch (100 rows) | 362 µs/op | 239 µs/op | 1.52× |
| Insert_PreparedBatch (100) | 351 µs/op | 226 µs/op | 1.55× |
| Insert_MultiValues (100) | 222 µs/op | 172 µs/op | 1.29× |
| FullText_BuildIndex (1K docs) | 7.27 ms/op | 2.17 ms/op | 3.35× |
| FullText_Insert_WithIndex | 183 µs/op | 88.3 µs/op | 2.08× |
| FullText_Search_SingleTerm/rare | 15.9 µs/op | 10.3 µs/op | 1.55× |
| FullText_Search_SingleTerm/medium | 16.2 µs/op | 11.5 µs/op | 1.41× |
| FullText_Search_SingleTerm/common | 16.2 µs/op | 64.7 µs/op | **0.25×** ✓ |
| FullText_Search_MultiTermAND | 26.3 µs/op | 37.5 µs/op | **0.70×** ✓ |
| FullText_Search_Phrase | 34.0 µs/op | 29.4 µs/op | 1.16× |
| FullText_Update_WithIndex | 66.3 µs/op | 126 µs/op | **0.53×** ✓ |
| FullText_Delete_WithIndex | 183 µs/op | 170 µs/op | 1.08× |
| **Join_Inner_SmallLarge** | **4.37 ms/op** | **4.77 ms/op** | **0.92×** ✓ |
| **Join_Inner_LowSelectivity** | **108 µs/op** | **734 µs/op** | **0.15×** ✓ |
| **Join_Left_UnmatchedRows** | **3.55 ms/op** | **4.17 ms/op** | **0.85×** ✓ |
| Vacuum_Small | 18.3 ms/op | 269 µs/op | 68.0× |
| WAL_Checkpoint | 158 µs/op | 64.5 µs/op | 2.44× |
| Explain | 5.39 µs/op | 1.25 µs/op | 4.30× |
| Select_PointScan | 5.83 µs/op | 3.38 µs/op | 1.73× |
| Select_Limit | 6.83 µs/op | 7.88 µs/op | **0.87×** ✓ |
| Select_FullScan (10K rows) | 3.60 ms/op | 5.09 ms/op | **0.71×** ✓ |
| Select_CountStar | 5.21 µs/op | 9.63 µs/op | **0.54×** ✓ |
| Select_IndexRangeScan | 749 µs/op | 758 µs/op | **0.99×** ✓ |
| Select_SecondaryIndex_LowSelectivity | 1.92 ms/op | 2.71 ms/op | **0.71×** ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 9.30 µs/op | 8.22 µs/op | **0.88×** ✓ |
| Select_RangeScan | 1.47 ms/op | 874 µs/op | 1.68× |
| CTE_Materialise | 766 µs/op | 435 µs/op | 1.76× |
| Subquery_InList (5K rows) | 4.26 ms/op | 3.64 ms/op | 1.17× |
| OnConflict_DoUpdate | 9.17 µs/op | 37.4 µs/op | **0.25×** ✓ |
| Update_ByPK | 10.7 µs/op | 36.6 µs/op | **0.29×** ✓ |

#### Memory (B/op)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 36.2 KiB | — | — | 3.5 KiB | — | — | 10.3× |
| Having_Filter | 28.0 KiB | — | — | 1.9 KiB | — | — | 14.4× |
| Distinct_HighCardinality | 1.68 MiB | — | — | 586 KiB | — | — | 2.9× |
| Delete_ByPK | **6.8 KiB** | — | — | 447 B | — | — | **15.6×** |
| ForeignKey_Insert | **3.5 KiB** | — | — | 192 B | — | — | **18.7×** |
| ForeignKey_DeleteCascade | **11.2 KiB** | — | — | 128 B | — | — | **89.9×** |
| Insert_SingleRow | **3.97 KiB** | — | — | 312 B | — | — | **13.0×** |
| Insert_Batch (100) | 248 KiB | — | — | 31.1 KiB | — | — | 8.0× |
| Insert_PreparedBatch (100) | 247 KiB | — | — | 31.1 KiB | — | — | 8.0× |
| Insert_MultiValues (100) | 213 KiB | — | — | 25.2 KiB | — | — | 8.4× |
| FullText_BuildIndex | 10.5 MiB | — | — | 392 B | — | — | — |
| FullText_Insert_WithIndex | 221 KiB | — | — | 443 B | — | — | 511× |
| FullText_Search_SingleTerm/rare | 4.8 KiB | — | — | 392 B | — | — | 12.6× |
| FullText_Search_SingleTerm/medium | 4.8 KiB | — | — | 392 B | — | — | 12.6× |
| FullText_Search_SingleTerm/common | 4.8 KiB | — | — | 408 B | — | — | 12.1× |
| FullText_Search_MultiTermAND | 14.6 KiB | — | — | 392 B | — | — | 38.1× |
| FullText_Search_Phrase | 48.6 KiB | — | — | 400 B | — | — | 124× |
| FullText_Update_WithIndex | 35.2 KiB | — | — | 292 B | — | — | 123× |
| FullText_Delete_WithIndex | 196 KiB | — | — | 135 B | — | — | 1,490× |
| JSONInverted_BuildIndex | — | 55.2 MiB | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.66 MiB | — | — | — | — | — |
| JSONInverted_Contains_KeyValue | — | 142 KiB | 3.26 MiB | — | 408 B | 408 B | — |
| JSONInverted_Contains_ObjectSubset | — | 280 KiB | 3.37 MiB | — | 408 B | 408 B | — |
| JSONInverted_Update_WithIndex | — | 7.9 KiB | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.25 MiB | — | — | — | — | — |
| **Join_Inner_SmallLarge** | **2.56 MiB** | — | — | **1.07 MiB** | — | — | 2.4× |
| **Join_Inner_LowSelectivity** | **22.6 KiB** | — | — | **11.3 KiB** | — | — | 2.0× |
| **Join_Left_UnmatchedRows** | **873 KiB** | — | — | **708 KiB** | — | — | 1.23× |
| Vacuum_Small | 1.68 MiB | — | — | 88 B | — | — | — |
| WAL_Checkpoint | 69.1 KiB | — | — | 440 B | — | — | 161× |
| Explain | 6.4 KiB | — | — | 680 B | — | — | 9.7× |
| Select_PointScan | 5.4 KiB | — | — | 679 B | — | — | 8.1× |
| Select_Limit | 4.0 KiB | — | — | 1.7 KiB | — | — | 2.4× |
| Select_FullScan | **1.23 MiB** | — | — | **1.30 MiB** | — | — | **0.95×** ✓ |
| Select_CountStar | 2.6 KiB | — | — | 400 B | — | — | 6.6× |
| Select_IndexRangeScan | 111 KiB | — | — | 85.9 KiB | — | — | 1.3× |
| Select_SecondaryIndex_LowSelectivity | 435 KiB | — | — | 313 KiB | — | — | 1.4× |
| Select_SecondaryIndex_LowSelectivityLimit | 6.0 KiB | — | — | 1.1 KiB | — | — | 5.6× |
| Select_RangeScan | **82.3 KiB** | — | — | **85.9 KiB** | — | — | **0.96×** ✓ |
| CTE_Materialise | 7.2 KiB | — | — | 400 B | — | — | 18.4× |
| Subquery_InList | 859 KiB | — | — | 235 KiB | — | — | 3.7× |
| OnConflict_DoUpdate | **3.1 KiB** | — | — | 260 B | — | — | **12.2×** |
| Update_ByPK | **6.6 KiB** | — | — | 263 B | — | — | **25.6×** |

#### Allocs/op

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 464 | 309 | 1.5× |
| Having_Filter | 269 | 111 | 2.4× |
| Distinct_HighCardinality | **40,145** | **40,010** | **~1.0×** ✓ |
| Delete_ByPK | 85 | 19 | 4.5× |
| ForeignKey_Insert | 38 | 8 | 4.8× |
| ForeignKey_DeleteCascade | 141 | 5 | 28.2× |
| Insert_SingleRow | 43 | 12 | 3.6× |
| OnConflict_DoUpdate | 39 | 10 | 3.9× |
| Update_ByPK | 65 | 10 | 6.5× |
| **Join_Inner_SmallLarge** | **89,778** | **99,757** | **0.90×** ✓ |
| **Join_Inner_LowSelectivity** | **1,295** | **1,009** | 1.28× |
| **Join_Left_UnmatchedRows** | **79,740** | **70,157** | 1.14× |
| Select_FullScan | **79,827** | **99,758** | **0.80×** ✓ |
| Select_RangeScan | **5,516** | **6,581** | **0.84×** ✓ |
| Select_IndexRangeScan | 6,650 | 6,581 | 1.01× ✓ |
| Select_SecondaryIndex_LowSelectivity | 29,942 | 29,886 | **~1.0×** ✓ |
| Subquery_InList | 35,101 | 20,010 | 1.75× |
| CTE_Materialise | 94 | 13 | 7.2× |
| Select_CountStar | 30 | 13 | 2.3× |

#### Delta vs OCC-era baseline (bench_baseline_old.txt)

| Benchmark | Old ns/op | New ns/op | Δ speed | Old B/op | New B/op | Δ memory |
|---|---|---|---|---|---|---|
| Delete_ByPK | 23.0 µs | 18.5 µs | **−20%** | 23.5 KiB | 6.8 KiB | **−71%** |
| Insert_SingleRow | 17.9 µs | 13.9 µs | **−22%** | 18.6 KiB | 4.0 KiB | **−79%** |
| Insert_Batch (100) | 385 µs | 362 µs | **−6%** | 304 KiB | 248 KiB | **−18%** |
| Insert_MultiValues (100) | 243 µs | 222 µs | **−9%** | 269 KiB | 213 KiB | **−21%** |
| Update_ByPK | 12.7 µs | 10.7 µs | **−16%** | 9.5 KiB | 6.6 KiB | **−31%** |
| OnConflict_DoUpdate | 11.1 µs | 9.2 µs | **−17%** | 8.7 KiB | 3.1 KiB | **−65%** |
| ForeignKey_Insert | 17.5 µs | 13.8 µs | **−21%** | 20.1 KiB | 3.5 KiB | **−83%** |
| ForeignKey_DeleteCascade | 49.6 µs | 46.7 µs | **−6%** | 14.7 KiB | 11.2 KiB | **−24%** |

#### Key observations

**Major wins vs SQLite (timing):** GroupBy (0.45×), Having (0.37×), CountStar (0.54×), all DML — Delete (0.33×), Insert (0.31×), Update (0.29×), OnConflict (0.25×), FK_Insert (0.32×), FK_DeleteCascade (0.97×) — all JOIN benchmarks (0.15–0.92×), full-text search/common (0.25×) and multi-term (0.70×), FullText_Update (0.53×), Select_FullScan (0.71×), Select_IndexRangeScan (0.99×), Select_SecondaryIndex_LowSelectivity (0.71×).

**Major wins vs SQLite (memory/allocs):** Select_FullScan (0.95× B, 0.80× allocs), Select_RangeScan (0.96× B, 0.84× allocs), Select_IndexRangeScan (1.01× allocs — at parity), Join_Inner_SmallLarge allocs (0.90×), Distinct allocs (1.0×).

**OCC removal memory impact (vs old baseline):** DML paths dropped from 30–60× memory gap to 4–90× vs SQLite — but the absolute numbers improved dramatically: Delete_ByPK −71%, Insert_SingleRow −79%, ForeignKey_Insert −83%, OnConflict_DoUpdate −65%. The previous baseline noted DML memory as "architectural"; the in-place LRU fast path now eliminates the per-page `Page.Clone()` allocation on the write hot path.

**WAL_Checkpoint timing note:** The 2.44× gap vs SQLite (~158 µs vs 65 µs) reflects WAL frame writes, WAL truncation, and fsync overhead in the Go implementation. The earlier OCC-era baseline showed 168 µs which was a warm-cache artifact of `-count=3`. Both old and new code give ~158–300 µs in practice depending on filesystem page cache state.

**Largest remaining memory gaps (within current architecture):**
1. **GROUP BY/HAVING** — 10–14× memory gap. Pool growth eliminated; remaining: Go map overhead, `groupValPool []OptionalValue` boxing, `allResultValues` flat block.
2. **Batch INSERT** — 1.5× slower than SQLite. Multi-row insert path not yet RowView-optimised.
3. **Full-text** — large absolute memory for build/insert due to inverted index structure; search and update are now competitive.
4. **CTE, Subquery, Distinct** — 1.75–3.7× memory gap; remaining from hash-set key strings and hash map overhead.
5. **WAL overhead** — WAL_Checkpoint at 2.4× SQLite; Explain at 4.3×; PointScan at 1.7×.
