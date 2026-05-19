# Benchmark Results

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
