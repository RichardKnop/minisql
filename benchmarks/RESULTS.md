# Benchmark Results

### 2026-05-23 — Reduce insert allocations (rowValues reuse + inline-text skip)

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=3s -count=1` · single run
**Branch:** `refactor/insert-multi-values`

Cumulative optimisations in this baseline:

**Write-path (OCC removal + in-place LRU + insert alloc reduction):**
- **OCC removed**: write transactions no longer track per-page read-sets or validate
  conflicts at commit; `activeWriters` atomic enforces single writer instead.
- **In-place LRU modification** (`ModifyPage` fast path): when no snapshot reader is
  active and the page has been committed at least once, writes modify the shared LRU
  page directly — no `Page.Clone()`. Rollback evicts via `InvalidatePage`.
- **`rowValues` slice reused across multi-values INSERT loop**: allocated once before
  the loop; `copy` overwrites it each iteration; `clear` before the slow path prevents
  stale values. Saves 1 alloc per row for every multi-row `INSERT … VALUES` call.
- **Inline-text re-boxing eliminated** (`storeOverflowTexts`): for text values that
  fit inline (≤255 B), the `TextPointer` is unchanged after `storeOverflowText` returns.
  The unconditional `r.Values[i]` re-assignment was boxing the 32-byte `TextPointer`
  into `any` (heap alloc) for nothing. Now skipped for inline text. Saves 1 alloc per
  text column per row across all write paths (INSERT, batch INSERT, UPDATE).
- **Per-file connection guard** in `Driver.Open`: returns `ErrDatabaseAlreadyOpen`
  when a second `sql.Open` targets the same file path.

**Read-path (RowView streaming):**
- RowView streaming for sequential scan, COUNT(*), GROUP BY, HAVING, semi-join probe side
- Streaming JOIN output; compact-cell build phase + arena for hash join inner side
- RowView outer scan + Bloom pre-filter for hash join
- Precomputed projection schema (`projectFast`)
- INLJ hot path: `ErrNotFound` sentinel, `PointUniqueRowID`, `singleKeySlice` reuse,
  `neededOuterFields` column pruning
- DISTINCT streaming; GROUP BY `passedIndices []int32`; JOIN CombinedRowView fast paths
- GROUP BY `estGroups` heuristic (`estRows/100`, bounded [16, 512])

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 1.03 ms/op | 2.31 ms/op | **0.45×** ✓ |
| Having_Filter | 704 µs/op | 1.89 ms/op | **0.37×** ✓ |
| Distinct_HighCardinality | 2.87 ms/op | 5.60 ms/op | **0.51×** ✓ |
| Delete_ByPK | 18.3 µs/op | 124 µs/op | **0.15×** ✓ |
| ForeignKey_Insert | 13.9 µs/op | 40.8 µs/op | **0.34×** ✓ |
| ForeignKey_DeleteCascade | 47.6 µs/op | 49.4 µs/op | **0.96×** ✓ |
| Insert_SingleRow | 13.4 µs/op | 41.2 µs/op | **0.33×** ✓ |
| Insert_Batch (100 rows) | 353 µs/op | 233 µs/op | 1.51× |
| Insert_PreparedBatch (100) | 348 µs/op | 221 µs/op | 1.57× |
| Insert_MultiValues (100) | 206 µs/op | 168 µs/op | 1.23× |
| FullText_BuildIndex (1K docs) | 6.87 ms/op | 1.99 ms/op | 3.45× |
| FullText_Insert_WithIndex | 177 µs/op | 87.3 µs/op | 2.03× |
| FullText_Search_SingleTerm/rare | 16.0 µs/op | 10.6 µs/op | 1.51× |
| FullText_Search_SingleTerm/medium | 15.9 µs/op | 11.5 µs/op | 1.38× |
| FullText_Search_SingleTerm/common | 16.5 µs/op | 64.9 µs/op | **0.25×** ✓ |
| FullText_Search_MultiTermAND | 27.0 µs/op | 37.6 µs/op | **0.72×** ✓ |
| FullText_Search_Phrase | 34.4 µs/op | 28.5 µs/op | 1.21× |
| FullText_Update_WithIndex | 64.6 µs/op | 110 µs/op | **0.59×** ✓ |
| FullText_Delete_WithIndex | 164 µs/op | 143 µs/op | 1.15× |
| **Join_Inner_SmallLarge** | **4.48 ms/op** | **5.00 ms/op** | **0.90×** ✓ |
| **Join_Inner_LowSelectivity** | **112 µs/op** | **752 µs/op** | **0.15×** ✓ |
| **Join_Left_UnmatchedRows** | **3.69 ms/op** | **4.30 ms/op** | **0.86×** ✓ |
| Vacuum_Small | 18.7 ms/op | 302 µs/op | 61.9× |
| WAL_Checkpoint | 171 µs/op | 67.8 µs/op | 2.52× |
| Explain | 5.49 µs/op | 1.25 µs/op | 4.39× |
| Select_PointScan | 5.83 µs/op | 3.53 µs/op | 1.65× |
| Select_Limit | 7.10 µs/op | 8.51 µs/op | **0.83×** ✓ |
| Select_FullScan (10K rows) | 3.69 ms/op | 5.30 ms/op | **0.70×** ✓ |
| Select_CountStar | 5.41 µs/op | 9.87 µs/op | **0.55×** ✓ |
| Select_IndexRangeScan | 772 µs/op | 782 µs/op | **0.99×** ✓ |
| Select_SecondaryIndex_LowSelectivity | 2.00 ms/op | 2.81 ms/op | **0.71×** ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 9.36 µs/op | 8.64 µs/op | 1.08× |
| Select_RangeScan | 1.50 ms/op | 918 µs/op | 1.64× |
| CTE_Materialise | 782 µs/op | 469 µs/op | 1.67× |
| Subquery_InList (5K rows) | 4.30 ms/op | 3.79 ms/op | 1.14× |
| OnConflict_DoUpdate | 9.16 µs/op | 41.5 µs/op | **0.22×** ✓ |
| Update_ByPK | 10.8 µs/op | 39.7 µs/op | **0.27×** ✓ |

#### Memory (B/op)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 36.1 KiB | — | — | 3.5 KiB | — | — | 10.3× |
| Having_Filter | 27.9 KiB | — | — | 1.9 KiB | — | — | 14.4× |
| Distinct_HighCardinality | 1.68 MiB | — | — | 586 KiB | — | — | 2.9× |
| Delete_ByPK | 6.7 KiB | — | — | 447 B | — | — | 15.3× |
| ForeignKey_Insert | **3.5 KiB** | — | — | 192 B | — | — | 18.5× |
| ForeignKey_DeleteCascade | **11.2 KiB** | — | — | 128 B | — | — | 89.9× |
| Insert_SingleRow | **3.90 KiB** | — | — | 312 B | — | — | **12.8×** |
| Insert_Batch (100) | **242 KiB** | — | — | 31.1 KiB | — | — | **7.8×** |
| Insert_PreparedBatch (100) | **241 KiB** | — | — | 31.1 KiB | — | — | **7.8×** |
| Insert_MultiValues (100) | **197 KiB** | — | — | 25.2 KiB | — | — | **7.8×** |
| FullText_BuildIndex | 10.7 MiB | — | — | 392 B | — | — | — |
| FullText_Insert_WithIndex | 221 KiB | — | — | 444 B | — | — | 510× |
| FullText_Search_SingleTerm/rare | 4.8 KiB | — | — | 392 B | — | — | 12.5× |
| FullText_Search_SingleTerm/medium | 4.8 KiB | — | — | 392 B | — | — | 12.5× |
| FullText_Search_SingleTerm/common | 4.8 KiB | — | — | 408 B | — | — | 12.0× |
| FullText_Search_MultiTermAND | 14.5 KiB | — | — | 392 B | — | — | 37.8× |
| FullText_Search_Phrase | 48.5 KiB | — | — | 400 B | — | — | 124× |
| FullText_Update_WithIndex | 35.8 KiB | — | — | 292 B | — | — | 125× |
| FullText_Delete_WithIndex | 198 KiB | — | — | 135 B | — | — | 1,504× |
| JSONInverted_BuildIndex | — | 55.2 MiB | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.66 MiB | — | — | — | — | — |
| JSONInverted_Contains_KeyValue | — | 142 KiB | 3.26 MiB | — | 408 B | 408 B | — |
| JSONInverted_Contains_ObjectSubset | — | 280 KiB | 3.37 MiB | — | 408 B | 408 B | — |
| JSONInverted_Update_WithIndex | — | 7.9 KiB | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.25 MiB | — | — | — | — | — |
| **Join_Inner_SmallLarge** | **2.56 MiB** | — | — | **1.07 MiB** | — | — | 2.4× |
| **Join_Inner_LowSelectivity** | **22.6 KiB** | — | — | **11.3 KiB** | — | — | 2.0× |
| **Join_Left_UnmatchedRows** | **873 KiB** | — | — | **708 KiB** | — | — | 1.23× |
| Vacuum_Small | 1.66 MiB | — | — | 88 B | — | — | — |
| WAL_Checkpoint | 69.1 KiB | — | — | 440 B | — | — | 161× |
| Explain | 6.4 KiB | — | — | 680 B | — | — | 9.6× |
| Select_PointScan | 5.3 KiB | — | — | 679 B | — | — | 8.0× |
| Select_Limit | 3.99 KiB | — | — | 1.7 KiB | — | — | 2.4× |
| Select_FullScan | **1.23 MiB** | — | — | **1.30 MiB** | — | — | **0.95×** ✓ |
| Select_CountStar | 2.5 KiB | — | — | 400 B | — | — | 6.4× |
| Select_IndexRangeScan | 111 KiB | — | — | 85.9 KiB | — | — | 1.3× |
| Select_SecondaryIndex_LowSelectivity | 435 KiB | — | — | 313 KiB | — | — | 1.4× |
| Select_SecondaryIndex_LowSelectivityLimit | 5.97 KiB | — | — | 1.1 KiB | — | — | 5.5× |
| Select_RangeScan | **82.3 KiB** | — | — | **85.9 KiB** | — | — | **0.96×** ✓ |
| CTE_Materialise | 7.2 KiB | — | — | 400 B | — | — | 18.4× |
| Subquery_InList | 859 KiB | — | — | 235 KiB | — | — | 3.7× |
| OnConflict_DoUpdate | **3.1 KiB** | — | — | 260 B | — | — | **12.2×** |
| Update_ByPK | **6.5 KiB** | — | — | 263 B | — | — | **25.2×** |

#### Allocs/op

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 463 | 309 | 1.5× |
| Having_Filter | 268 | 111 | 2.4× |
| Distinct_HighCardinality | **40,144** | **40,010** | **~1.0×** ✓ |
| Delete_ByPK | 83 | 19 | 4.4× |
| ForeignKey_Insert | 37 | 8 | 4.6× |
| ForeignKey_DeleteCascade | 141 | 5 | 28.2× |
| Insert_SingleRow | **41** | 12 | 3.4× |
| Insert_Batch (100) | **3,169** | 1,308 | 2.4× |
| Insert_PreparedBatch (100) | **3,168** | 1,307 | 2.4× |
| Insert_MultiValues (100) | **2,183** | 622 | 3.5× |
| OnConflict_DoUpdate | 39 | 10 | 3.9× |
| Update_ByPK | **63** | 10 | 6.3× |
| **Join_Inner_SmallLarge** | **89,777** | **99,757** | **0.90×** ✓ |
| **Join_Inner_LowSelectivity** | **1,294** | **1,009** | 1.28× |
| **Join_Left_UnmatchedRows** | **79,739** | **70,157** | 1.14× |
| Select_FullScan | **79,826** | **99,758** | **0.80×** ✓ |
| Select_RangeScan | **5,515** | **6,581** | **0.84×** ✓ |
| Select_IndexRangeScan | 6,649 | 6,581 | 1.01× ✓ |
| Select_SecondaryIndex_LowSelectivity | 29,941 | 29,886 | **~1.0×** ✓ |
| Subquery_InList | 35,100 | 20,010 | 1.75× |
| CTE_Materialise | 93 | 13 | 7.2× |
| Select_CountStar | 29 | 13 | 2.2× |

#### Delta vs previous baseline (OCC removal + in-place LRU)

| Benchmark | Old B/op | New B/op | Δ memory | Old allocs | New allocs | Δ allocs |
|---|---|---|---|---|---|---|
| Insert_SingleRow | 4,064 B | 3,994 B | **−70 B** | 43 | 41 | **−2** |
| Insert_Batch (100) | 254 KiB | 242 KiB | **−6.8 KiB** | 3,369 | 3,169 | **−200** |
| Insert_PreparedBatch (100) | 247 KiB | 241 KiB | **−6.8 KiB** | 3,368 | 3,168 | **−200** |
| Insert_MultiValues (100) | 213 KiB | 197 KiB | **−16 KiB (−7%)** | 2,482 | 2,183 | **−299 (−12%)** |
| Update_ByPK | 6,733 B | 6,666 B | **−67 B** | 65 | 63 | **−2** |
| Delete_ByPK | 6,916 B | 6,857 B | **−59 B** | 85 | 83 | **−2** |

The −2 allocs/op on single-row paths (Insert_SingleRow, Update_ByPK, Delete_ByPK) comes
entirely from the `storeOverflowTexts` inline-text fix (1 alloc saved per text column; the
benchmark table has 2 text columns). The larger savings on batch and multi-values paths are
`storeOverflowTexts` (200 per 100 rows) plus `rowValues` hoisting (100 per 100 rows).

#### Key observations

**Major wins vs SQLite (timing):** GroupBy (0.45×), Having (0.37×), CountStar (0.55×), all DML — Delete (0.15×), Insert (0.33×), Update (0.27×), OnConflict (0.22×), FK_Insert (0.34×), FK_DeleteCascade (0.96×) — all JOIN benchmarks (0.15–0.90×), full-text search/common (0.25×) and multi-term (0.72×), FullText_Update (0.59×), Select_FullScan (0.70×), Select_IndexRangeScan (0.99×), Select_SecondaryIndex_LowSelectivity (0.71×).

**Major wins vs SQLite (memory/allocs):** Select_FullScan (0.95× B, 0.80× allocs), Select_RangeScan (0.96× B, 0.84× allocs), Select_IndexRangeScan (1.01× allocs — at parity), Join_Inner_SmallLarge allocs (0.90×), Distinct allocs (1.0×).

**WAL_Checkpoint timing note:** The 2.5× gap vs SQLite (~171 µs vs 68 µs) reflects WAL frame writes, WAL truncation, and fsync overhead in the Go implementation.

**Largest remaining memory gaps (within current architecture):**
1. **GROUP BY/HAVING** — 10–14× memory gap. Remaining: Go map overhead for group-key strings, `OptionalValue` boxing per aggregate slot. Closing the gap requires changing the group-value encoding away from `[]OptionalValue`.
2. **Batch INSERT** — 1.5–1.6× slower than SQLite. Each of the 100 separate autocommit transactions pays full WAL + transaction overhead; not addressable without a batch-write API.
3. **Insert_MultiValues** — 7.8× memory gap vs SQLite (197 KiB vs 25 KiB). Remaining: `Statement.Clone` for 100-row payload (~15 KiB), WAL frame allocation per modified page, index node copies.
4. **Full-text** — large absolute memory for build/insert due to inverted index structure; search and update are competitive.
5. **CTE, Subquery, Distinct** — 1.75–3.7× memory gap; remaining from hash-set key strings and hash map overhead.
