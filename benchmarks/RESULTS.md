# Benchmark Results

### 2026-05-24 — Inverted-index write-path memory reduction

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** focused inverted-index DML run, `-benchmem`, `-count=1`
**Branch:** `codex-inverted-index-memory-wins`

Cumulative optimisations in this baseline:

- **Append-only posting-block insert path**: when new row IDs are increasing and the target
  posting block has room, MiniSQL now appends the delta-encoded posting bytes directly instead
  of decoding, regrouping, and re-encoding the whole block.
- **Full-text per-row term aggregation**: INSERT/DELETE maintenance now groups all positions
  for the same row+term into one posting before touching the inverted index.
- **Allocation-free inverted page fit checks**: entry/posting page fit checks use byte
  accounting instead of allocating and marshaling a temporary 4 KiB page.
- **Reusable JSON path escaper**: JSON inverted term generation reuses a package-level
  `strings.Replacer` instead of rebuilding it for every path segment.

Command:

```bash
LOG_LEVEL=warn go test -tags bench -bench='Benchmark(FullText|JSONInverted)_(BuildIndex|Insert_WithIndex|Update_WithIndex|Delete_WithIndex)$' -benchmem -run '^$' -count=1 -memprofile=/tmp/minisql_inverted_dml_after_mem.prof ./benchmarks/
```

#### Timing

| Benchmark | minisql | sqlite |
|---|---:|---:|
| FullText_BuildIndex | 6.26 ms/op | 3.39 ms/op |
| FullText_Insert_WithIndex | 152 µs/op | 134 µs/op |
| FullText_Update_WithIndex | 80.1 µs/op | 144 µs/op |
| FullText_Delete_WithIndex | 226 µs/op | 256 µs/op |
| JSONInverted_BuildIndex | 5.87 ms/op | — |
| JSONInverted_Insert_WithIndex | 158 µs/op | — |
| JSONInverted_Update_WithIndex | 10.7 µs/op | — |
| JSONInverted_Delete_WithIndex | 354 µs/op | — |

#### Memory (B/op)

| Benchmark | before | after | delta |
|---|---:|---:|---:|
| FullText_BuildIndex | 10.7 MiB | 3.0 MiB | −71% |
| FullText_Insert_WithIndex | 203 KiB | 29.0 KiB | −86% |
| FullText_Update_WithIndex | 43.3 KiB | 27.4 KiB | −37% |
| FullText_Delete_WithIndex | 175 KiB | 141 KiB | −20% |
| JSONInverted_BuildIndex | 55.3 MiB | 5.3 MiB | −90% |
| JSONInverted_Insert_WithIndex | 1.66 MiB | 153 KiB | −91% |
| JSONInverted_Update_WithIndex | 10.5 KiB | 9.9 KiB | −5% |
| JSONInverted_Delete_WithIndex | 1.25 MiB | 1.13 MiB | −8% |

#### Allocs/op

| Benchmark | before | after | delta |
|---|---:|---:|---:|
| FullText_BuildIndex | 35,797 | 34,056 | −5% |
| FullText_Insert_WithIndex | 822 | 226 | −73% |
| FullText_Update_WithIndex | 231 | 245 | +6% |
| FullText_Delete_WithIndex | 1,357 | 1,350 | −1% |
| JSONInverted_BuildIndex | 143,587 | 85,895 | −40% |
| JSONInverted_Insert_WithIndex | 526 | 214 | −59% |
| JSONInverted_Update_WithIndex | 85 | 82 | −4% |
| JSONInverted_Delete_WithIndex | 460 | 392 | −15% |

#### Key observations

The append-only path and allocation-free fit checks remove the worst insert/build allocation
spikes from the shared inverted-index storage layer. The largest remaining allocation source
is now delete maintenance, especially JSON deletes, because deletion still decodes existing
posting blocks into `[]invertedPosting`, removes a row ID, and re-encodes the block. That
remaining shape is the main evidence for the next segment/tombstone refactor.

---

### 2026-05-24 — Window function implementation (no regressions)

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=1×` (`-count=1`) · single run
**Branch:** `refactor/row-view-api`

New feature: full window function support (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD,
FIRST_VALUE, LAST_VALUE, NTH_VALUE, SUM/AVG/COUNT/MIN/MAX OVER).  The `HasWindowFuncs()`
guard on `Statement` ensures existing queries pay zero overhead — every alloc/op count
is identical to the prior baseline.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 1.21 ms/op | 2.79 ms/op | **0.43×** ✓ |
| Having_Filter | 960 µs/op | 2.26 ms/op | **0.42×** ✓ |
| Distinct_HighCardinality | 3.58 ms/op | 6.84 ms/op | **0.52×** ✓ |
| Delete_ByPK | 27.4 µs/op | 129 µs/op | **0.21×** ✓ |
| ForeignKey_Insert | 17.5 µs/op | 55.1 µs/op | **0.32×** ✓ |
| ForeignKey_DeleteCascade | 111 µs/op | 70.1 µs/op | 1.58× |
| Insert_SingleRow | 16.0 µs/op | 56.4 µs/op | **0.28×** ✓ |
| Insert_Batch | 417 µs/op | 263 µs/op | 1.58× |
| Insert_PreparedBatch | 411 µs/op | 260 µs/op | 1.58× |
| Insert_MultiValues | 246 µs/op | 180 µs/op | 1.37× |
| Join_Inner_SmallLarge | 5.26 ms/op | 5.91 ms/op | **0.89×** ✓ |
| Join_Inner_LowSelectivity | 127 µs/op | 840 µs/op | **0.15×** ✓ |
| Join_Left_UnmatchedRows | 4.08 ms/op | 4.87 ms/op | **0.84×** ✓ |
| Explain | 7.25 µs/op | 1.69 µs/op | 4.29× |
| Select_PointScan | 7.33 µs/op | 3.96 µs/op | 1.85× |
| Select_Limit | 9.06 µs/op | 9.49 µs/op | **0.95×** ✓ |
| Select_FullScan | 4.31 ms/op | 6.50 ms/op | **0.66×** ✓ |
| Select_CountStar | 6.69 µs/op | 11.1 µs/op | **0.60×** ✓ |
| Select_IndexRangeScan | 1.43 ms/op | 869 µs/op | 1.64× |
| Select_SecondaryIndex_LowSelectivity | 2.15 ms/op | 3.24 ms/op | **0.66×** ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 11.6 µs/op | 9.57 µs/op | 1.21× |
| Select_RangeScan | 1.94 ms/op | 1.11 ms/op | 1.75× |
| CTE_Materialise | 1.16 ms/op | 579 µs/op | 2.00× |
| Subquery_InList | 5.83 ms/op | 4.38 ms/op | 1.33× |
| OnConflict_DoUpdate | 10.7 µs/op | 42.5 µs/op | **0.25×** ✓ |
| Update_ByPK | 13.1 µs/op | 44.0 µs/op | **0.30×** ✓ |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| GroupBy_Aggregate | 38,241 | 3,611 |
| Join_Inner_SmallLarge | 2,686,285 | 1,120,442 |
| Explain | **6,089** | 680 |
| Select_PointScan | 5,100 | 679 |
| Select_FullScan | 1,297,643 | 1,357,756 |

#### Allocs/op (key paths, unchanged from prior baseline)

| Benchmark | minisql | sqlite |
|---|---|---|
| Explain | **58** | 18 |
| Select_PointScan | 62 | 26 |
| GroupBy_Aggregate | 463 | 309 |
| Join_Inner_SmallLarge | 89,778 | 99,757 |
| Join_Left_UnmatchedRows | 79,739 | 70,157 |

---

### 2026-05-23 — EXPLAIN allocation reduction

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26
**Settings:** `-benchtime=1×` (`-count=1`) · single run
**Branch:** `refactor/explain`

Cumulative optimisations in this baseline (adds EXPLAIN improvements on top of previous):

**EXPLAIN path:**
- **`[]byte` detail building**: `scanDetail`, `joinDetail`, `orderByDetail`, and `rangeDetail`
  now build output via direct `[]byte` append instead of `[]string{}` + `strings.Join`,
  eliminating the intermediate slice allocation and the `strings.Join` output string.
  `explainRow.detail` changed to `[]byte` so `NewTextPointer` receives bytes directly
  without a `string→[]byte` conversion per row.
- **`liftINSubqueriesToSemiJoins` fast path**: added `hasINSubqueryConditions` guard that
  returns early when no `IN/NOT IN (subquery)` conditions are present, avoiding the
  `outerTableNames` map allocation and join-tree walk on every non-IN query (including
  every EXPLAIN call).

Combined impact: **67 → 58 allocs/op**, **6,533 → 6,087 B/op** on `BenchmarkExplain/minisql`
(−13% allocs, −6.8% memory). SQLite baseline remains 18 allocs/op; gap closed from 49 → 40.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 1.04 ms/op | 2.54 ms/op | **0.41×** ✓ |
| Having_Filter | 811 µs/op | 2.27 ms/op | **0.36×** ✓ |
| Distinct_HighCardinality | 4.05 ms/op | 6.42 ms/op | **0.63×** ✓ |
| Delete_ByPK | 31.5 µs/op | 149 µs/op | **0.21×** ✓ |
| ForeignKey_Insert | 18.7 µs/op | 77.0 µs/op | **0.24×** ✓ |
| ForeignKey_DeleteCascade | 97.6 µs/op | 50.7 µs/op | 1.92× |
| Insert_SingleRow | 14.5 µs/op | 43.7 µs/op | **0.33×** ✓ |
| Insert_Batch (100 rows) | 364 µs/op | 248 µs/op | 1.47× |
| Insert_PreparedBatch (100) | 385 µs/op | 234 µs/op | 1.64× |
| Insert_MultiValues (100) | 218 µs/op | 174 µs/op | 1.25× |
| FullText_BuildIndex (1K docs) | 7.02 ms/op | 1.99 ms/op | 3.53× |
| FullText_Insert_WithIndex | 173 µs/op | 92.4 µs/op | 1.88× |
| FullText_Search_SingleTerm/rare | 16.4 µs/op | 10.3 µs/op | 1.58× |
| FullText_Search_SingleTerm/medium | 15.5 µs/op | 11.3 µs/op | 1.38× |
| FullText_Search_SingleTerm/common | 16.2 µs/op | 65.2 µs/op | **0.25×** ✓ |
| FullText_Search_MultiTermAND | 26.1 µs/op | 37.5 µs/op | **0.70×** ✓ |
| FullText_Search_Phrase | 33.7 µs/op | 28.1 µs/op | 1.20× |
| FullText_Update_WithIndex | 63.0 µs/op | 95.4 µs/op | **0.66×** ✓ |
| FullText_Delete_WithIndex | 150 µs/op | 155 µs/op | **0.96×** ✓ |
| **Join_Inner_SmallLarge** | **4.32 ms/op** | **4.75 ms/op** | **0.91×** ✓ |
| **Join_Inner_LowSelectivity** | **109 µs/op** | **741 µs/op** | **0.15×** ✓ |
| **Join_Left_UnmatchedRows** | **3.76 ms/op** | **4.20 ms/op** | **0.90×** ✓ |
| Vacuum_Small | 18.5 ms/op | 278 µs/op | 66.5× |
| WAL_Checkpoint | 192 µs/op | 77.6 µs/op | 2.48× |
| Explain | 5.38 µs/op | 1.23 µs/op | 4.39× |
| Select_PointScan | 5.56 µs/op | 3.41 µs/op | 1.63× |
| Select_Limit | 7.11 µs/op | 8.07 µs/op | **0.88×** ✓ |
| Select_FullScan (10K rows) | 3.59 ms/op | 5.30 ms/op | **0.68×** ✓ |
| Select_CountStar | 5.64 µs/op | 10.1 µs/op | **0.56×** ✓ |
| Select_IndexRangeScan | 819 µs/op | 769 µs/op | 1.06× |
| Select_SecondaryIndex_LowSelectivity | 2.00 ms/op | 2.78 ms/op | **0.72×** ✓ |
| Select_SecondaryIndex_LowSelectivityLimit | 9.34 µs/op | 8.45 µs/op | 1.10× |
| Select_RangeScan | 1.47 ms/op | 867 µs/op | 1.70× |
| CTE_Materialise | 766 µs/op | 455 µs/op | 1.68× |
| Subquery_InList (5K rows) | 4.47 ms/op | 3.83 ms/op | 1.17× |
| OnConflict_DoUpdate | 9.14 µs/op | 36.6 µs/op | **0.25×** ✓ |
| Update_ByPK | 10.8 µs/op | 108 µs/op | **0.10×** ✓ |

#### Memory (B/op)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 37.5 KiB | — | — | 3.5 KiB | — | — | 10.6× |
| Having_Filter | 28.9 KiB | — | — | 1.9 KiB | — | — | 14.9× |
| Distinct_HighCardinality | 1.69 MiB | — | — | 586 KiB | — | — | 2.9× |
| Delete_ByPK | 7.2 KiB | — | — | 447 B | — | — | 16.6× |
| ForeignKey_Insert | **3.5 KiB** | — | — | 191 B | — | — | 18.6× |
| ForeignKey_DeleteCascade | **11.3 KiB** | — | — | 128 B | — | — | 90.1× |
| Insert_SingleRow | **3.92 KiB** | — | — | 311 B | — | — | **12.9×** |
| Insert_Batch (100) | **243 KiB** | — | — | 31.0 KiB | — | — | **7.8×** |
| Insert_PreparedBatch (100) | **241 KiB** | — | — | 31.0 KiB | — | — | **7.8×** |
| Insert_MultiValues (100) | **197 KiB** | — | — | 25.2 KiB | — | — | **7.8×** |
| FullText_BuildIndex | 10.7 MiB | — | — | 392 B | — | — | — |
| FullText_Insert_WithIndex | 203 KiB | — | — | 439 B | — | — | 474× |
| FullText_Search_SingleTerm/rare | 4.4 KiB | — | — | 392 B | — | — | 11.6× |
| FullText_Search_SingleTerm/medium | 4.4 KiB | — | — | 392 B | — | — | 11.6× |
| FullText_Search_SingleTerm/common | 4.4 KiB | — | — | 408 B | — | — | 11.1× |
| FullText_Search_MultiTermAND | 14.2 KiB | — | — | 392 B | — | — | 37.0× |
| FullText_Search_Phrase | 48.2 KiB | — | — | 400 B | — | — | 123× |
| FullText_Update_WithIndex | 43.3 KiB | — | — | 291 B | — | — | 152× |
| FullText_Delete_WithIndex | 175 KiB | — | — | 135 B | — | — | 1,328× |
| JSONInverted_BuildIndex | — | 55.3 MiB | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | 1.66 MiB | — | — | — | — | — |
| JSONInverted_Contains_KeyValue | — | 142 KiB | 3.26 MiB | — | 408 B | 408 B | — |
| JSONInverted_Contains_ObjectSubset | — | 279 KiB | 3.37 MiB | — | 408 B | 408 B | — |
| JSONInverted_Update_WithIndex | — | 10.5 KiB | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 1.25 MiB | — | — | — | — | — |
| **Join_Inner_SmallLarge** | **2.56 MiB** | — | — | **1.07 MiB** | — | — | 2.4× |
| **Join_Inner_LowSelectivity** | **22.7 KiB** | — | — | **11.3 KiB** | — | — | 2.0× |
| **Join_Left_UnmatchedRows** | **878 KiB** | — | — | **708 KiB** | — | — | 1.24× |
| Vacuum_Small | 1.66 MiB | — | — | 90 B | — | — | — |
| WAL_Checkpoint | 69.3 KiB | — | — | 440 B | — | — | 161× |
| Explain | **5.94 KiB** | — | — | 680 B | — | — | **8.95×** |
| Select_PointScan | 4.98 KiB | — | — | 679 B | — | — | 7.3× |
| Select_Limit | 4.00 KiB | — | — | 1.7 KiB | — | — | 2.3× |
| Select_FullScan | **1.24 MiB** | — | — | **1.30 MiB** | — | — | **0.95×** ✓ |
| Select_CountStar | 2.5 KiB | — | — | 400 B | — | — | 6.4× |
| Select_IndexRangeScan | 112 KiB | — | — | 85.9 KiB | — | — | 1.3× |
| Select_SecondaryIndex_LowSelectivity | 437 KiB | — | — | 313 KiB | — | — | 1.4× |
| Select_SecondaryIndex_LowSelectivityLimit | 5.65 KiB | — | — | 1.1 KiB | — | — | 5.2× |
| Select_RangeScan | **83.7 KiB** | — | — | **85.9 KiB** | — | — | **0.97×** ✓ |
| CTE_Materialise | 7.9 KiB | — | — | 400 B | — | — | 20.3× |
| Subquery_InList | 870 KiB | — | — | 235 KiB | — | — | 3.7× |
| OnConflict_DoUpdate | **3.1 KiB** | — | — | 259 B | — | — | **12.3×** |
| Update_ByPK | **6.6 KiB** | — | — | 263 B | — | — | **25.8×** |

#### Allocs/op

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| GroupBy_Aggregate | 463 | 309 | 1.5× |
| Having_Filter | 268 | 111 | 2.4× |
| Distinct_HighCardinality | **40,145** | **40,010** | **~1.0×** ✓ |
| Delete_ByPK | 85 | 19 | 4.5× |
| ForeignKey_Insert | 37 | 8 | 4.6× |
| ForeignKey_DeleteCascade | 141 | 5 | 28.2× |
| Insert_SingleRow | **41** | 12 | 3.4× |
| Insert_Batch (100) | **3,154** | 1,302 | 2.4× |
| Insert_PreparedBatch (100) | **3,150** | 1,300 | 2.4× |
| Insert_MultiValues (100) | **2,175** | 617 | 3.5× |
| OnConflict_DoUpdate | 39 | 10 | 3.9× |
| Update_ByPK | **63** | 10 | 6.3× |
| **Join_Inner_SmallLarge** | **89,778** | **99,757** | **0.90×** ✓ |
| **Join_Inner_LowSelectivity** | **1,294** | **1,009** | 1.28× |
| **Join_Left_UnmatchedRows** | **79,739** | **70,157** | 1.14× |
| Select_FullScan | **79,826** | **99,758** | **0.80×** ✓ |
| Select_RangeScan | **5,511** | **6,581** | **0.84×** ✓ |
| Select_IndexRangeScan | 6,645 | 6,581 | 1.01× ✓ |
| Select_SecondaryIndex_LowSelectivity | 29,937 | 29,886 | **~1.0×** ✓ |
| Subquery_InList | 35,102 | 20,010 | 1.75× |
| CTE_Materialise | 89 | 13 | 6.9× |
| Select_CountStar | 29 | 13 | 2.2× |
| **Explain** | **58** | **18** | **3.2×** |

#### Delta vs previous baseline (CTE alias-tolerant inlining)

| Benchmark | Old B/op | New B/op | Δ memory | Old allocs | New allocs | Δ allocs |
|---|---|---|---|---|---|---|
| Explain/minisql | 6,533 B | 6,087 B | −446 B (−6.8%) | 67 | 58 | **−9 (−13%)** |

The 9-alloc saving comes from: (a) eliminating `outerTableNames` map + walk in
`liftINSubqueriesToSemiJoins` via `hasINSubqueryConditions` fast-path guard (~3–4 allocs);
(b) replacing `[]string` + `strings.Join` in `scanDetail`/`joinDetail`/`orderByDetail` with
direct `[]byte` append (~4–5 allocs); (c) eliminating `[]byte(row.detail)` conversion in
`buildExplainResult` (~1–2 allocs per row). Gap vs SQLite closed from 49 → 40 allocs/op.

#### Key observations

**Major wins vs SQLite (timing):** GroupBy (0.41×), Having (0.36×), CountStar (0.56×), all DML — Delete (0.21×), Insert (0.33×), Update (0.10×), OnConflict (0.25×), FK_Insert (0.24×), FK_DeleteCascade n/a — all JOIN benchmarks (0.15–0.91×), full-text search/common (0.25×) and multi-term (0.70×), FullText_Update (0.66×), FullText_Delete (0.96×), Select_FullScan (0.68×), Select_SecondaryIndex_LowSelectivity (0.72×).

**Major wins vs SQLite (memory/allocs):** Select_FullScan (0.95× B, 0.80× allocs), Select_RangeScan (0.97× B, 0.84× allocs), Select_IndexRangeScan (1.01× allocs — at parity), Join_Inner_SmallLarge allocs (0.90×), Distinct allocs (1.0×).

**Largest remaining memory gaps (within current architecture):**
1. **GROUP BY/HAVING** — 10–15× memory gap. Remaining: Go map overhead for group-key strings, `OptionalValue` boxing per aggregate slot.
2. **Batch INSERT** — 1.47–1.64× slower than SQLite. Each of the 100 separate autocommit transactions pays full WAL + transaction overhead.
3. **Insert_MultiValues** — 7.8× memory gap vs SQLite. Remaining: `Statement.Clone` for 100-row payload, WAL frame allocation per modified page, index node copies.
4. **Full-text** — large absolute memory for build/insert due to inverted index structure; search and update are competitive.
5. **Explain** — 4.4× slower, 9× more memory than SQLite; remaining gap is in query preparation, planning, and page read paths — not addressable with string-building tricks alone.

---

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
### 2026-05-24 23:08 UTC

#### Timing

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan | ratio |
|---|---|---|---|---|---|---|---|
| FullText_BuildIndex | 5.97 ms/op | — | — | 2.68 ms/op | — | — | 2.2× |
| JSONInverted_BuildIndex | — | 27.73 ms/op | — | — | — | — | — |
| FullText_Insert_WithIndex | 87.13 µs/op | — | — | 178.00 µs/op | — | — | 0.5× |
| FullText_Search_SingleTerm/rare | 140.20 µs/op | — | — | 490.49 µs/op | — | — | 0.3× |
| FullText_Search_SingleTerm/medium | 107.76 µs/op | — | — | 480.07 µs/op | — | — | 0.2× |
| FullText_Search_SingleTerm/common | 107.66 µs/op | — | — | 433.68 µs/op | — | — | 0.2× |
| FullText_Search_MultiTermAND | 148.90 µs/op | — | — | 392.73 µs/op | — | — | 0.4× |
| FullText_Search_Phrase | 129.37 µs/op | — | — | 384.30 µs/op | — | — | 0.3× |
| FullText_Search_AfterDeletes | 929.97 µs/op | — | — | — | — | — | — |
| FullText_Update_WithIndex | 1.18 ms/op | — | — | 594.12 µs/op | — | — | 2.0× |
| FullText_Delete_WithIndex | 67.74 µs/op | — | — | 162.14 µs/op | — | — | 0.4× |
| JSONInverted_Insert_WithIndex | — | 87.53 µs/op | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 579.93 µs/op | 5.78 ms/op | — | 311.73 µs/op | 982.44 µs/op | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 1.03 ms/op | 4.03 ms/op | — | 386.94 µs/op | 1.25 ms/op | — |
| JSONInverted_Contains_AfterDeletes | — | 492.62 µs/op | — | — | — | — | — |
| JSONInverted_Update_WithIndex | — | 179.17 µs/op | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 106.66 µs/op | — | — | — | — | — |

#### Memory (B/op)

| Benchmark | minisql | minisql_indexed | minisql_sequential | sqlite | sqlite_json_expr_index | sqlite_json_scan |
|---|---|---|---|---|---|---|
| FullText_BuildIndex | 5.4 MiB | — | — | 696 B | — | — |
| JSONInverted_BuildIndex | — | 15.2 MiB | — | — | — | — |
| FullText_Insert_WithIndex | 39.2 KiB | — | — | 716 B | — | — |
| FullText_Search_SingleTerm/rare | 38.3 KiB | — | — | 533 B | — | — |
| FullText_Search_SingleTerm/medium | 34.7 KiB | — | — | 533 B | — | — |
| FullText_Search_SingleTerm/common | 38.3 KiB | — | — | 549 B | — | — |
| FullText_Search_MultiTermAND | 43.8 KiB | — | — | 533 B | — | — |
| FullText_Search_Phrase | 62.3 KiB | — | — | 538 B | — | — |
| FullText_Search_AfterDeletes | 68.8 KiB | — | — | — | — | — |
| FullText_Update_WithIndex | 669.8 KiB | — | — | 404 B | — | — |
| FullText_Delete_WithIndex | 32.8 KiB | — | — | 257 B | — | — |
| JSONInverted_Insert_WithIndex | — | 47.2 KiB | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | 93.4 KiB | 3.2 MiB | — | 549 B | 549 B |
| JSONInverted_Contains_ObjectSubset/object_subset | — | 155.4 KiB | 3.4 MiB | — | 549 B | 549 B |
| JSONInverted_Contains_AfterDeletes | — | 127.7 KiB | — | — | — | — |
| JSONInverted_Update_WithIndex | — | 63.9 KiB | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | 38.4 KiB | — | — | — | — |


