# Benchmark Results

## 2026-05-18 — Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26  
**Settings:** `-benchtime=5s -count=3` · median of 3 runs shown  
**Commit:** `814f492` (zero-alloc qualified column name lookup) + `fix: appendRowID uint64 underflow`

All benchmarks pass. `BenchmarkFullText_Delete_WithIndex` was previously
panicking (uint64 underflow in `appendRowID` when `freeBytes < 8`); fixed in
the same session before this baseline was taken.

---

### SELECT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| PointScan | 6,042 | 69 | 3,436 | **1.76×** |
| Limit | 7,961 | 129 | 8,087 | **1.0×** |
| FullScan (10k rows) | 4,810,296 | 112,321 | 5,186,549 | **0.93×** ✓ |
| CountStar | 29,477 | 706 | 9,760 | **3.02×** |
| IndexRangeScan | 739,527 | 11,077 | 730,900 | **1.01×** |
| RangeScan | 1,510,027 | 19,922 | 869,343 | **1.74×** |
| SecondaryIndex_LowSelectivity (5k rows) | 3,079,429 | 54,951 | 2,785,947 | **1.11×** |
| SecondaryIndex_LowSelectivityLimit | 10,687 | 166 | 8,126 | **1.32×** |

### INSERT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| SingleRow | 17,865 | 56 | 44,282 | **0.35×** ✓ |
| Batch (100 rows) | 385,420 | 3,401 | 234,899 | **1.64×** |
| PreparedBatch (100 rows) | 384,286 | 3,400 | 229,264 | **1.68×** |
| MultiValues (100 rows) | 242,745 | 2,530 | 171,803 | **1.41×** |

### UPDATE / DELETE / UPSERT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Update_ByPK | 12,196 | 75 | 38,080 | **0.32×** ✓ |
| OnConflict_DoUpdate | 11,062 | 49 | 56,635 | **0.20×** ✓ |
| Delete_ByPK | 23,031 | 109 | 113,479 | **0.20×** ✓ |

### JOIN

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Join_Inner_SmallLarge (10k rows) | 8,185,594 | 153,371 | 4,592,612 | **1.78×** |
| Join_Left_UnmatchedRows (10k rows) | 11,358,331 | 203,249 | 3,993,609 | **2.84×** |

### GROUP BY / HAVING / DISTINCT

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| GroupBy_Aggregate (100 groups) | 2,120,776 | 12,347 | 2,197,493 | **0.97×** ✓ |
| Having_Filter (100 groups) | 2,089,395 | 12,174 | 1,910,146 | **1.09×** |
| Distinct_HighCardinality (10k rows) | 5,510,803 | 111,455 | 5,563,584 | **0.99×** ✓ |

### SUBQUERY / CTE

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Subquery_InList (5k rows) | 8,380,671 | 139,776 | 3,653,376 | **2.29×** |
| CTE_Materialise | 1,850,823 | 14,134 | 435,575 | **4.25×** |

### FOREIGN KEY

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| ForeignKey_Insert | 17,387 | 51 | 43,157 | **0.40×** ✓ |
| ForeignKey_DeleteCascade | 49,555 | 170 | 50,359 | **0.98×** ✓ |

### FULL-TEXT SEARCH

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| BuildIndex (1k docs) | 10,671,270 | 196,574 | 2,930,337 | **3.64×** |
| Insert_WithIndex | 382,788 | 3,164 | 101,730 | **3.76×** |
| Search_SingleTerm/rare (1 match) | 16,113 | — | 10,558 | **1.53×** |
| Search_SingleTerm/medium (10 matches) | 15,909 | — | 11,676 | **1.36×** |
| Search_SingleTerm/common (1k matches) | 15,762 | — | 66,954 | **0.24×** ✓ |
| Search_MultiTermAND (10 matches) | 31,044 | — | 38,480 | **0.81×** ✓ |
| Search_Phrase (100 matches) | 98,544 | — | 28,108 | **3.51×** |
| Update_WithIndex | 152,371 | 1,114 | 126,335 | **1.21×** |
| Delete_WithIndex | 349,550 | 3,158 | 178,476 | **1.96×** |

**Search improvements (2026-05-18):** Parser pre-computes `strings.ToUpper` once per query (was per-token×keyword). Single-term COUNT(\*) uses `DocFreq` from index entry header instead of iterating all postings — O(log N) vs O(N). Rare/medium dropped from ~11× to ~1.5×; common flipped from 8.9× slower to 4× faster than SQLite.

### JSON INVERTED INDEX

No SQLite equivalent (minisql-only feature).

| Benchmark | minisql ns/op | minisql allocs/op |
|---|---:|---:|
| BuildIndex (1k docs) | 35,274,625 | 215,818 |
| Insert_WithIndex | 720,165 | 1,015 |
| Update_WithIndex | 9,436 | 87 |
| Delete_WithIndex | 508,629 | 892 |
| Contains_KeyValue (indexed, 334 matches) | 122,021 | 2,076 |
| Contains_ObjectSubset (indexed, 334 matches) | 154,298 | 2,147 |

SQLite with expression index: `Contains_KeyValue` ~30µs (4.1× faster), `Contains_ObjectSubset` ~129µs (1.2× faster).

### MAINTENANCE

| Benchmark | minisql ns/op | minisql allocs/op | SQLite ns/op | Ratio |
|---|---:|---:|---:|---:|
| Vacuum_Small | 20,101,857 | 25,791 | 239,274 | **84×** |
| WAL_Checkpoint | 167,806 | 305 | 64,177 | **2.62×** |
| Explain | 5,038 | 68 | 1,163 | **4.33×** |

Vacuum gap is expected — minisql does a full copy-compact-swap; SQLite reclaims
free pages in-place. Not a meaningful comparison.

---

### Summary: biggest gaps vs SQLite

Ranked by ratio (excluding Vacuum):

| Benchmark | Ratio | allocs/op |
|---|---:|---:|
| Explain | 4.33× | 68 |
| CTE_Materialise | 4.25× | 14,134 |
| FullText_Insert_WithIndex | 3.76× | 3,164 |
| FullText_BuildIndex | 3.64× | 196,574 |
| FullText_Search_Phrase | 3.51× | — |
| CountStar | 3.02× | 706 |
| Join_Left_UnmatchedRows | 2.84× | 203,249 |
| WAL_Checkpoint | 2.62× | 305 |
| Subquery_InList | 2.29× | 139,776 |
| Join_Inner_SmallLarge | 1.78× | 153,371 |
| PointScan | 1.76× | 69 |
| RangeScan | 1.74× | 19,922 |
| FullText_Search_SingleTerm/rare | 1.53× | — |
| FullText_Search_SingleTerm/medium | 1.36× | — |

### Summary: at parity or faster than SQLite

| Benchmark | Ratio |
|---|---:|
| FullText_Search_SingleTerm/common | **0.24×** (4.2× faster) |
| Delete_ByPK | **0.20×** (5× faster) |
| OnConflict_DoUpdate | **0.20×** (5× faster) |
| Update_ByPK | **0.32×** (3× faster) |
| Insert_SingleRow | **0.35×** (2.9× faster) |
| ForeignKey_Insert | **0.40×** (2.5× faster) |
| FullText_Search_MultiTermAND | **0.81×** (1.2× faster) |
| ForeignKey_DeleteCascade | **0.98×** |
| Select_FullScan | **0.93×** |
| GroupBy_Aggregate | **0.97×** |
| Distinct_HighCardinality | **0.99×** |
| Select_Limit | **1.0×** |
| Select_IndexRangeScan | **1.01×** |

---

### Benchmark run-time anomalies

Two benchmarks took far longer than expected to complete the 3-run suite:

- `BenchmarkForeignKey_DeleteCascade`: **333s** — benchmark setup is likely
  not isolated behind `b.ResetTimer()` correctly, or teardown is running inside
  the timed window.
- `BenchmarkFullText_BuildIndex`: **2368s (~40 min)** — SQLite FTS5 setup at
  high `b.N` is very expensive. Consider capping `b.N` or using a fixed dataset
  instead of `b.N`-scaled seeding.
- `BenchmarkVacuum_Small`: **242s** — 20ms/op × large `b.N` × 3 runs; expected
  given the algorithm cost.
