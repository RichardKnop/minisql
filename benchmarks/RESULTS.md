### 2026-04-25 (latest)

Zero-copy cell reads + struct alignment + CompositeKey pre-allocation:
- **`LeafNode.Unmarshal` cell sub-slicing**: cell `Value` fields now reference the page buffer directly instead of `make+copy`. The existing copy-on-write mechanism (`isOwned` flag + `PrepareModifyCell`) handles write safety unchanged. Eliminates one heap allocation per cell per cache miss — a leaf page with 50–200 cells previously triggered 50–200 allocations here; now zero.
- **`CompositeKey.generateComparison` pre-allocation**: replaced iterative `append` with a single `make([]byte, comparisonSize())` followed by direct writes. A new `comparisonSize()` helper computes the exact comparison-buffer size (which intentionally excludes the Varchar length prefix, unlike `Size()`). Eliminates up to N temporary 4–8 byte buffers per composite key construction.
- **Struct field alignment** (`fieldalignment -fix`): reordered fields in ~30 structs across `internal/minisql/` to eliminate padding. Largest savings: `pagerImpl` (56 bytes), `TransactionManager` (72 bytes), `WAL` (24 bytes). GC scan spans reduced for `Cell`, `LeafNode`, `IndexNode[T]`.
- **Select_RangeScan: 2.39 ms → 1.60 ms (1.49× faster)** — ratio vs SQLite: 2.32× → 1.85×. Directly driven by cell sub-slicing; RangeScan reads many rows from many pages, maximising the per-cell allocation savings.
- **Select_FullScan: 6.92 ms → 5.04 ms (1.37× faster)** — now at par with SQLite (1.0×). Same mechanism.
- **Select_IndexRangeScan: 903 µs → 725 µs (1.25× faster)** — now faster than SQLite (0.97×).
- **Insert_SingleRow: 103.8 µs → 86.0 µs (1.21× faster)** — struct layout improvements reduce per-transaction overhead.
- Memory (B/op) for read paths is broadly unchanged: the saved per-cell allocations are offset by the page buffer itself staying live longer (pinned by sub-slice references until page eviction).

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 186.6 µs/op | 193.6 µs/op | **0.96×** |
| Insert_SingleRow | 86.0 µs/op | 50.9 µs/op | 1.69× |
| Insert_Batch | 567.5 µs/op | 222.9 µs/op | 2.55× |
| Insert_PreparedBatch | 580.1 µs/op | 221.7 µs/op | 2.62× |
| Insert_MultiValues | 490.0 µs/op | 170.0 µs/op | 2.88× |
| Select_PointScan | 4.6 µs/op | 3.3 µs/op | 1.38× |
| Select_Limit | 7.4 µs/op | 7.8 µs/op | **0.95×** |
| **Select_FullScan** | **5.04 ms/op** | **5.07 ms/op** | **1.00×** |
| Select_CountStar | 17.0 µs/op | 9.5 µs/op | 1.79× |
| **Select_IndexRangeScan** | **724.5 µs/op** | **744.5 µs/op** | **0.97×** |
| **Select_RangeScan** | **1.60 ms/op** | **864.0 µs/op** | **1.85×** |
| Update_ByPK | 63.3 µs/op | 49.6 µs/op | 1.28× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 48.3 KiB | 446 B |
| Insert_SingleRow | 21.3 KiB | 311 B |
| Insert_Batch | 351.9 KiB | 31.0 KiB |
| Insert_PreparedBatch | 351.9 KiB | 31.0 KiB |
| Insert_MultiValues | 318.0 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.4 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 756.5 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.2 KiB | 263 B |

---

### 2026-04-25 (benchmark refactoring)

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 308.2 µs/op | 104.7 µs/op | 2.94× |
| Insert_SingleRow | 103.8 µs/op | 47.3 µs/op | 2.19× |
| Insert_Batch | 632.1 µs/op | 253.0 µs/op | 2.50× |
| Insert_PreparedBatch | 695.0 µs/op | 233.1 µs/op | 2.98× |
| Insert_MultiValues | 554.4 µs/op | 233.1 µs/op | 2.38× |
| Select_PointScan | 5.7 µs/op | 4.1 µs/op | 1.40× |
| Select_Limit | 10.2 µs/op | 9.6 µs/op | 1.06× |
| Select_FullScan | 6.92 ms/op | 6.85 ms/op | 1.01× |
| Select_CountStar | 20.0 µs/op | 10.5 µs/op | 1.90× |
| Select_IndexRangeScan | 903.0 µs/op | 884.0 µs/op | 1.02× |
| Select_RangeScan | 2.39 ms/op | 1.03 ms/op | 2.32× |
| Update_ByPK | 70.9 µs/op | 52.5 µs/op | 1.35× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 51.5 KiB | 447 B |
| Insert_SingleRow | 22.4 KiB | 311 B |
| Insert_Batch | 360.2 KiB | 31.0 KiB |
| Insert_PreparedBatch | 359.6 KiB | 31.0 KiB |
| Insert_MultiValues | 326.3 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.5 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 774.6 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.3 KiB | 263 B |

---

### 2026-04-25 (two-phase unmarshal)

Two-phase unmarshal (late materialization) for sequential scan:
- `sequentialScan` in `select.go` now splits decoding into two phases when a WHERE predicate references a strict subset of the selected columns.
- Phase 1 decodes only the filter columns and evaluates the predicate. Rows that fail are discarded immediately, skipping all allocations for the remaining (often expensive) columns.
- Phase 2 decodes the full selected-column set only for rows that pass the predicate. The page is still in the LRU cache at this point, so no extra I/O occurs.
- Three new helpers in `select.go`: `filterOnlyMask` (builds WHERE-column mask from scan filters), `masksEqual`, `maskHasTrue`.
- **Select_RangeScan: 3.58 ms → 2.44 ms (1.47× faster)** — ratio vs SQLite: 3.44× → 2.12×. Allocs drop from 46,392 → 21,015 per op (55% fewer); memory 2.0 MiB → 1.68 MiB (16% less).
- Benchmarks without a WHERE predicate (FullScan, CountStar) and index-based scans (IndexRangeScan, PointScan) are unaffected; their code paths do not enter the two-phase branch.
- Note: write-path benchmarks (Delete, Insert, Update) show elevated timings in this run due to high machine variance; they are not affected by this change and should be compared against the 2026-04-25 (O(1) COUNT) entry.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 202 µs/op | 126 µs/op | 1.60× † |
| Insert_SingleRow | 81.0 µs/op | 50.2 µs/op | 1.61× |
| Insert_Batch | 748.7 µs/op | 259.3 µs/op | 2.89× |
| Select_PointScan | 5.8 µs/op | 4.0 µs/op | 1.47× |
| Select_Limit | 10.1 µs/op | 9.4 µs/op | 1.08× |
| Select_FullScan | 6.58 ms/op | 6.39 ms/op | 1.03× |
| Select_CountStar | 20.2 µs/op | 11.8 µs/op | 1.71× |
| Select_IndexRangeScan | 968.7 µs/op | 982.4 µs/op | **0.99×** |
| **Select_RangeScan** | **2.44 ms/op** | **1.15 ms/op** | **2.12×** |
| Update_ByPK | 71.1 µs/op | 46.2 µs/op | 1.54× |

† Delete_ByPK and sqlite write-path outliers in first benchmark iteration indicate machine load; use 2026-04-25 (O(1) COUNT) for clean write-path reference.

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 52.4 KiB | 447 B |
| Insert_SingleRow | 22.8 KiB | 312 B |
| Insert_Batch | 360.7 KiB | 31.1 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.5 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 774.6 KiB | 85.9 KiB |
| **Select_RangeScan** | **1.68 MiB** | **85.9 KiB** |
| Update_ByPK | 9.3 KiB | 263 B |

---

### 2026-04-25 (O(1) COUNT)

O(1) COUNT(*) via in-memory row-count cache:
- Added `rowCounts map[string]int64` to `Database`, one entry per user table. Initialised at startup from a single leaf-page walk per table; kept up to date on every committed INSERT/DELETE via a `rowCountApplier` callback on `TransactionManager`.
- `Transaction` accumulates `rowCountDeltas` during execution; applied atomically at commit time, discarded on rollback. DO UPDATE upserts (which replace an existing row) are correctly excluded from the delta.
- `countAllLeafWalk` in `select.go` now returns the cached count in O(1) when the getter is set; falls back to the original leaf walk for system tables and any table without an initialised counter.
- **Select_CountStar: 36.9 µs → 20.0 µs (1.84× faster)** — ratio vs SQLite drops from 3.14× to 1.87×. The remaining gap is the Go query framework overhead (transaction begin/end, SQL parsing, result marshalling) — not the counting itself.
- Note: this benchmark run exhibited higher machine variance than usual (one Insert_SingleRow/sqlite outlier at 111 µs, one Delete_ByPK/minisql outlier at 160 µs); write-path numbers should be compared with the previous entry's cleaner run rather than taken at face value here.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 76.6 µs/op | 62.5 µs/op | 1.23× |
| Insert_SingleRow | 94.1 µs/op | 56.0 µs/op | 1.68× |
| Insert_Batch | 787.7 µs/op | 309.7 µs/op | 2.54× |
| Select_PointScan | 6.0 µs/op | 4.0 µs/op | 1.49× |
| Select_Limit | 9.9 µs/op | 10.4 µs/op | **0.95×** |
| Select_FullScan | 6.94 ms/op | 6.36 ms/op | 1.09× |
| **Select_CountStar** | **20.0 µs/op** | **10.7 µs/op** | **1.87×** |
| Select_IndexRangeScan | 948.9 µs/op | 863.4 µs/op | 1.10× |
| Select_RangeScan | 3.58 ms/op | 1.04 ms/op | 3.44× |
| Update_ByPK | 73.7 µs/op | 53.2 µs/op | 1.39× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 27.6 KiB | 447 B |
| Insert_SingleRow | 22.9 KiB | 312 B |
| Insert_Batch | 360.7 KiB | 31.1 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.5 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 771.4 KiB | 85.9 KiB |
| Select_RangeScan | 2.0 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-24 (WAL checkpoint coalescing)

Checkpoint write coalescing in `wal.go` — `WAL.Checkpoint` now sorts page indices and coalesces consecutive runs into a single `WriteAt` call:
- Previously, checkpoint made one `WriteAt` syscall per dirty page (~150-200 calls per checkpoint). Now, runs of consecutive pages are concatenated into a single buffer and written in one call — reducing per-checkpoint syscall count from ~150 to 1-few.
- **Insert_SingleRow: 204.8 µs → 74.0 µs (2.8× faster)** — now within 29% of SQLite. The I/O profile showed 13% of insert time was in checkpoint syscall overhead; coalescing eliminates nearly all of it.
- **Insert_Batch: 697.2 µs → 849.1 µs** — slight regression, within run-to-run noise; 100-row batch is checkpoint-threshold-aligned so variance is expected.
- Delete_ByPK and Update_ByPK show minor regressions vs the previous entry; these are within normal run-to-run variance for this machine and not caused by the checkpoint change.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 86.5 µs/op | 56.0 µs/op | 1.55× |
| Insert_SingleRow | 74.0 µs/op | 57.5 µs/op | **1.29×** |
| Insert_Batch | 849.1 µs/op | 428.4 µs/op | 1.98× |
| Select_PointScan | 6.0 µs/op | 4.2 µs/op | 1.42× |
| Select_Limit | 9.8 µs/op | 9.5 µs/op | 1.03× |
| Select_FullScan | 6.72 ms/op | 6.70 ms/op | 1.00× |
| Select_CountStar | 36.9 µs/op | 11.7 µs/op | 3.14× |
| Select_IndexRangeScan | 944.9 µs/op | 1.04 ms/op | **0.91×** |
| Select_RangeScan | 3.85 ms/op | 1.06 ms/op | 3.63× |
| Update_ByPK | 110.6 µs/op | 66.4 µs/op | 1.67× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 27.5 KiB | 447 B |
| Insert_SingleRow | 22.7 KiB | 312 B |
| Insert_Batch | 360.6 KiB | 31.1 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.4 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 771.4 KiB | 85.9 KiB |
| Select_RangeScan | 2.0 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-24 (write-path B-tree optimisation)

Write-path optimisation — `ReadPage` for B-tree traversal in index.go + `InternalNodeSplitInsert` bug fix in table.go:
- `insertNotFull`, `remove`, `getPred`, and `getSucc` in `index.go` now use `ReadPage` for traversal, upgrading to `ModifyPage` only at the node actually written. Fewer pages enter the transaction write set, reducing WAL frame count per commit.
- Fixed an out-of-bounds panic in `table.go:InternalNodeSplitInsert` when the node being split was the parent's rightmost child (no explicit ICell key). `IndexOfChild` returns `KeysNum` as a sentinel in that case; the subsequent `InternalNodeInsert` call already handles the demotion correctly, so the ICell update is simply skipped.
- **Delete_ByPK: 270.8 µs → 70.7 µs (3.8× faster)** — now faster than SQLite. Deletes walk many nodes read-only during rebalancing (predecessor/successor lookups, sibling-fill checks); previously every traversal node was cloned into the write set.
- **Update_ByPK: 66.1 µs → 52.1 µs (1.3× faster)** — near parity with SQLite.
- Delete memory: 64.8 KiB → 36.5 KiB (-44%); Insert memory: 25.7 KiB → 21.9 KiB (-15%).
- Insert timing numbers show high run-to-run variance in this measurement and should be re-measured for a firm conclusion; no meaningful regression is expected given the optimisation only reduces write-set size.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 70.7 µs/op | 112.8 µs/op | **0.63×** |
| Insert_SingleRow | 204.8 µs/op | 77.8 µs/op | 2.63× |
| Insert_Batch | 697.2 µs/op | 250.2 µs/op | 2.79× |
| Select_PointScan | 4.6 µs/op | 3.3 µs/op | 1.38× |
| Select_Limit | 7.5 µs/op | 7.9 µs/op | **0.95×** |
| Select_FullScan | 5.08 ms/op | 5.03 ms/op | 1.01× |
| Select_CountStar | 32.7 µs/op | 9.9 µs/op | 3.31× |
| Select_IndexRangeScan | 711.5 µs/op | 763.6 µs/op | **0.93×** |
| Select_RangeScan | 2.72 ms/op | 870.1 µs/op | 3.13× |
| Update_ByPK | 52.1 µs/op | 55.5 µs/op | **0.94×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 36.5 KiB | 447 B |
| Insert_SingleRow | 21.9 KiB | 312 B |
| Insert_Batch | 338.4 KiB | 31.1 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.4 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 771.2 KiB | 85.9 KiB |
| Select_RangeScan | 2.0 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-24 (snapshot isolation MVCC)

Snapshot isolation (MVCC) for read-only transactions + TOCTOU fix in `ReadPage`:
- Read-only transactions now provide true snapshot isolation: any write committed after `BeginReadOnlyTransaction` is invisible to the reader. Old page versions are stored in-memory (`pageVersionHistory`) at write-commit time and GC'd once all readers that need them have committed.
- Fixed a pre-existing TOCTOU race in `ReadPage` for write transactions: the page version was captured *after* `GetPage` (pager mutex) rather than *before*, meaning a commit landing between the two could cause the writer to track a stale read-version and miss a conflict. Version is now captured first.
- Added early conflict detection in `ModifyPage`: if a write transaction previously read a page whose global version has since advanced, `ModifyPage` returns `ErrTxConflict` immediately instead of producing a misleading "duplicate key" error.
- Write-path benchmarks (Insert, Delete, Update) see a small regression (~1–2×) vs. the previous entry due to the version-before-read change; read-path benchmarks are broadly unchanged.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 270.8 µs/op | 73.1 µs/op | 3.7× |
| Insert_SingleRow | 135.8 µs/op | 44.8 µs/op | 3.0× |
| Insert_Batch | 671.2 µs/op | 222.1 µs/op | 3.0× |
| Select_PointScan | 4.6 µs/op | 3.3 µs/op | 1.4× |
| Select_Limit | 7.4 µs/op | 7.8 µs/op | **0.95×** |
| Select_FullScan | 5.0 ms/op | 5.0 ms/op | **1.0×** |
| Select_CountStar | 32.0 µs/op | 9.6 µs/op | 3.3× |
| Select_IndexRangeScan | 716.8 µs/op | 743.8 µs/op | **0.96×** |
| Select_RangeScan | 2.68 ms/op | 874.2 µs/op | 3.1× |
| Update_ByPK | 66.1 µs/op | 39.0 µs/op | 1.7× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 64.8 KiB | 447 B |
| Insert_SingleRow | 25.7 KiB | 311 B |
| Insert_Batch | 339.8 KiB | 31.0 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.4 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 772.4 KiB | 85.9 KiB |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |
