# Benchmark Results

### 2026-05-23 — CTE alias-tolerant inlining

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=3s -count=1` · single run
**Branch:** `refactor/CTE-materialise`

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

**CTE optimisation:**
- **CTE alias-tolerant inlining**: `cteIsInlineable` previously blocked inlining whenever
  the CTE body had any column alias (e.g. `SELECT name AS display_name`). Extended to a
  two-phase check: if the body is otherwise inlineable, `cteBodyAliasesConflictWithOuter`
  checks whether the outer query's fields/conditions/ORDER BY/GROUP BY/HAVING actually
  reference any alias. When no alias is used (e.g. `SELECT COUNT(*) FROM cte` or
  `SELECT id FROM cte`), the CTE is merged directly without materialisation.
  Impact on a non-inlineable CTE benchmark: **531,855 B/op → 7,665 B/op (69×),
  6,093 → 96 allocs/op (63×)**. Existing `CTE_Materialise` benchmark is unchanged
  (it was already inlined before this fix).

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
| GroupBy_Aggregate | 977 µs/op | 2.18 ms/op | **0.45×** ✓ |
| Having_Filter | 724 µs/op | 2.08 ms/op | **0.35×** ✓ |
| Distinct_HighCardinality | 3.05 ms/op | 5.83 ms/op | **0.52×** ✓ |
| Delete_ByPK | 19.9 µs/op | 145 µs/op | **0.14×** ✓ |
| ForeignKey_Insert | 14.8 µs/op | 50.0 µs/op | **0.30×** ✓ |
| ForeignKey_DeleteCascade | 50.7 µs/op | 51.3 µs/op | **0.99×** ✓ |
| Insert_SingleRow | 14.1 µs/op | 43.8 µs/op | **0.32×** ✓ |
| Insert_Batch (100 rows) | 355 µs/op | 226 µs/op | 1.57× |
| Insert_PreparedBatch (100) | 360 µs/op | 227 µs/op | 1.59× |
| Insert_MultiValues (100) | 210 µs/op | 205 µs/op | **1.03×** ✓ |
| FullText_BuildIndex (1K docs) | 7.63 ms/op | 2.60 ms/op | 2.93× |
| FullText_Insert_WithIndex | 205 µs/op | 86.9 µs/op | 2.36× |
| FullText_Search_SingleTerm/rare | 16.0 µs/op | 10.1 µs/op | 1.59× |
| FullText_Search_SingleTerm/medium | 16.0 µs/op | 11.2 µs/op | 1.42× |
| FullText_Search_SingleTerm/common | 15.7 µs/op | 64.6 µs/op | **0.24×** ✓ |
| FullText_Search_MultiTermAND | 26.3 µs/op | 37.2 µs/op | **0.71×** ✓ |
| FullText_Search_Phrase | 36.8 µs/op | 28.9 µs/op | 1.28× |
| FullText_Update_WithIndex | 60.8 µs/op | 103 µs/op | **0.59×** ✓ |
| FullText_Delete_WithIndex | 167 µs/op | 144 µs/op | 1.16× |
| **Join_Inner_SmallLarge** | **4.38 ms/op** | **4.78 ms/op** | **0.92×** ✓ |
| **Join_Inner_LowSelectivity** | **119 µs/op** | **749 µs/op** | **0.16×** ✓ |
| **Join_Left_UnmatchedRows** | **3.68 ms/op** | **4.40 ms/op** | **0.84×** ✓ |
| Vacuum_Small | 19.2 ms/op | 291 µs/op | 65.9× |
| WAL_Checkpoint | 173 µs/op | 73.5 µs/op | 2.35× |
| Explain | 5.93 µs/op | 1.26 µs/op | 4.70× |
| Select_PointScan | 5.69 µs/op | 3.40 µs/op | 1.67× |
| Select_Limit | 6.97 µs/op | 8.08 µs/op | **0.86×** ✓ |
| Select_FullScan (10K rows) | 3.68 ms/op | 5.24 ms/op | **0.70×** ✓ |
| Select_CountStar | 5.44 µs/op | 9.83 µs/op | **0.55×** ✓ |
| Select_IndexRangeScan | 823 µs/op | 754 µs/op | 1.09× |
| Select_SecondaryIndex_LowSelectivity | 1.97 ms/op | 2.73 ms/op | **0.72×** ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 9.42 µs/op | 8.28 µs/op | 1.14× |
| Select_RangeScan | 1.49 ms/op | 881 µs/op | 1.69× |
| CTE_Materialise | 766 µs/op | 482 µs/op | 1.59× |
| Subquery_InList (5K rows) | 4.24 ms/op | 3.90 ms/op | 1.09× |
| OnConflict_DoUpdate | 9.62 µs/op | 38.3 µs/op | **0.25×** ✓ |
| Update_ByPK | 10.8 µs/op | 38.6 µs/op | **0.28×** ✓ |

#### Memory (B/op)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 36.1 KiB | — | — | 3.5 KiB | — | — | 10.2× |
| Having_Filter | 27.9 KiB | — | — | 1.9 KiB | — | — | 14.4× |
| Distinct_HighCardinality | 1.72 MiB | — | — | 586 KiB | — | — | 2.9× |
| Delete_ByPK | 6.7 KiB | — | — | 447 B | — | — | 15.3× |
| ForeignKey_Insert | **3.5 KiB** | — | — | 192 B | — | — | 18.5× |
| ForeignKey_DeleteCascade | **11.2 KiB** | — | — | 128 B | — | — | 89.9× |
| Insert_SingleRow | **3.90 KiB** | — | — | 312 B | — | — | **12.8×** |
| Insert_Batch (100) | **242 KiB** | — | — | 31.1 KiB | — | — | **7.8×** |
| Insert_PreparedBatch (100) | **241 KiB** | — | — | 31.1 KiB | — | — | **7.8×** |
| Insert_MultiValues (100) | **197 KiB** | — | — | 25.3 KiB | — | — | **7.8×** |
| FullText_BuildIndex | 10.5 MiB | — | — | 392 B | — | — | — |
| FullText_Insert_WithIndex | 220 KiB | — | — | 443 B | — | — | 508× |
| FullText_Search_SingleTerm/rare | 4.8 KiB | — | — | 392 B | — | — | 12.5× |
| FullText_Search_SingleTerm/medium | 4.8 KiB | — | — | 392 B | — | — | 12.5× |
| FullText_Search_SingleTerm/common | 4.8 KiB | — | — | 408 B | — | — | 12.0× |
| FullText_Search_MultiTermAND | 14.5 KiB | — | — | 392 B | — | — | 37.8× |
| FullText_Search_Phrase | 48.5 KiB | — | — | 400 B | — | — | 124× |
| FullText_Update_WithIndex | 35.2 KiB | — | — | 292 B | — | — | 123× |
| FullText_Delete_WithIndex | 199 KiB | — | — | 135 B | — | — | 1,511× |
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
| WAL_Checkpoint | 69.0 KiB | — | — | 440 B | — | — | 161× |
| Explain | 6.4 KiB | — | — | 680 B | — | — | 9.6× |
| Select_PointScan | 5.3 KiB | — | — | 679 B | — | — | 7.8× |
| Select_Limit | 3.99 KiB | — | — | 1.7 KiB | — | — | 2.4× |
| Select_FullScan | **1.23 MiB** | — | — | **1.30 MiB** | — | — | **0.95×** ✓ |
| Select_CountStar | 2.5 KiB | — | — | 400 B | — | — | 6.4× |
| Select_IndexRangeScan | 111 KiB | — | — | 85.9 KiB | — | — | 1.3× |
| Select_SecondaryIndex_LowSelectivity | 435 KiB | — | — | 313 KiB | — | — | 1.4× |
| Select_SecondaryIndex_LowSelectivityLimit | 5.98 KiB | — | — | 1.1 KiB | — | — | 5.5× |
| Select_RangeScan | **82.3 KiB** | — | — | **85.9 KiB** | — | — | **0.96×** ✓ |
| CTE_Materialise | 7.2 KiB | — | — | 400 B | — | — | 18.5× |
| Subquery_InList | 859 KiB | — | — | 235 KiB | — | — | 3.7× |
| OnConflict_DoUpdate | **3.1 KiB** | — | — | 259 B | — | — | **12.3×** |
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

#### Delta vs previous baseline (insert alloc reduction)

The CTE fix has no measurable effect on `CTE_Materialise` (that CTE was already inlined in
the previous baseline too). The gain is visible only for previously non-inlineable CTEs; a
purpose-built benchmark shows the full impact:

| Benchmark | Old B/op | New B/op | Δ memory | Old allocs | New allocs | Δ allocs |
|---|---|---|---|---|---|---|
| CTE_NonInlineable† | 531,855 B | 7,665 B | **−524 KiB (−69×)** | 6,093 | 96 | **−5,997 (−63×)** |
| CTE_Materialise | 7,171 B | 7,405 B | ±noise | 93 | 93 | 0 |

† `CTE_NonInlineable` is a purpose-built benchmark (not part of the regular suite) that uses
`SELECT id, name AS display_name FROM bench_rows WHERE age >= 80` as the CTE body — a column
alias prevents inlining under the old rules. The outer query is `SELECT COUNT(*) FROM seniors`
which never references `display_name`. After the fix, `cteBodyAliasesConflictWithOuter`
returns false and the CTE is inlined, eliminating all `materializeResultRows` / `projectRowView`
/ `RowView.ValueAt` allocations (which together accounted for 94.8% of total memory per iteration).

#### Key observations

**Major wins vs SQLite (timing):** GroupBy (0.45×), Having (0.35×), CountStar (0.55×), all DML — Delete (0.14×), Insert (0.32×), Update (0.28×), OnConflict (0.25×), FK_Insert (0.30×), FK_DeleteCascade (0.99×) — all JOIN benchmarks (0.16–0.92×), full-text search/common (0.24×) and multi-term (0.71×), FullText_Update (0.59×), Select_FullScan (0.70×), Select_SecondaryIndex_LowSelectivity (0.72×).

**Insert_MultiValues now at 1.03×:** With the `rowValues` reuse + inline-text skip optimisations from the previous baseline, multi-values INSERT is effectively at SQLite parity on timing. The remaining 7.8× memory gap is from WAL frame writes, index node copies, and `Statement.Clone` for the 100-row payload — not from row-processing overhead.

**Major wins vs SQLite (memory/allocs):** Select_FullScan (0.95× B, 0.80× allocs), Select_RangeScan (0.96× B, 0.84× allocs), Select_IndexRangeScan (1.01× allocs — at parity), Join_Inner_SmallLarge allocs (0.90×), Distinct allocs (1.0×).

**WAL_Checkpoint timing note:** The 2.35× gap vs SQLite (~173 µs vs 73.5 µs) reflects WAL frame writes, WAL truncation, and fsync overhead in the Go implementation.

**Largest remaining memory gaps (within current architecture):**
1. **GROUP BY/HAVING** — 10–14× memory gap. Remaining: Go map overhead for group-key strings, `OptionalValue` boxing per aggregate slot. Closing the gap requires changing the group-value encoding away from `[]OptionalValue`.
2. **Batch INSERT** — 1.57–1.59× slower than SQLite. Each of the 100 separate autocommit transactions pays full WAL + transaction overhead; not addressable without a batch-write API.
3. **Insert_MultiValues** — 7.8× memory gap vs SQLite (197 KiB vs 25 KiB). Remaining: `Statement.Clone` for 100-row payload (~15 KiB), WAL frame allocation per modified page, index node copies.
4. **Full-text** — large absolute memory for build/insert due to inverted index structure; search and update are competitive.
5. **CTE, Subquery, Distinct** — 1.75–3.7× memory gap; remaining from hash-set key strings and hash map overhead.
