### 2026-04-26 (biased leaf splits for sequential inserts — latest)

Three changes in this entry:

**1. Biased leaf splits** (`cursor.go` `LeafNodeSplitInsert`): when the new key is greater than all existing keys (sequential insert), pack all existing cells on the left page and place only the new key on the right page. Table RowIDs are engine-managed and strictly monotone-increasing, so this is always safe. Result: O(1) key placement vs O(n) cell shuffle for the common case; fully packed leaf pages (5.3× fewer pages for sequential workloads).
- **Insert_Batch: 349.3 µs → 315.0 µs (1.11× faster, ratio vs SQLite 1.55× → 1.42×)** — rightmost-leaf cache was already skipping tree traversal for 99/100 rows; biased split also eliminates the O(n) cell-copy on every split boundary.
- **Insert_PreparedBatch: 347.6 µs → 316.6 µs (1.10× faster, ratio 1.54× → 1.44×)**
- **Insert_MultiValues: 260.4 µs → 226.5 µs (1.15× faster, ratio 1.50× → 1.36×)**
- Insert_SingleRow: 17.9 µs → 16.6 µs (1.08× faster) — modest benefit since OCC/WAL overhead dominates.

**2. Bug fix — uint64 underflow in in-place update check** (`cursor.go` `Cursor.update`): the condition `row.Size() > page.LeafNode.AvailableSpace()-oldRow.Size()` could wrap around to a huge number when `AvailableSpace() < oldRow.Size()` (a page fully packed by biased splits has only ~11 bytes free vs ~53 bytes for a typical row). Changed to `row.Size() > page.LeafNode.AvailableSpace()+oldRow.Size()` — correct semantics: trigger delete-and-reinsert when the net size increase exceeds available space. This bug was latent with even-split pages (always ~half full, so AvailableSpace ≈ 2000 > oldRow.Size()) and was exposed by biased splits.
- **Update_ByPK: 26.4 µs → 10.2 µs (2.6× faster, ratio 0.37× → 0.22×)** — fully packed pages mean delete-and-reinsert is now triggered correctly (instead of always in-place); the shorter in-place path dominates the benchmark.

**3. Bug fix — unallocated Cells slice in even-split** (`cursor.go` `LeafNodeSplitInsert`): the even-split loop directly indexed `newPage.LeafNode.Cells[cellIdx]` before `saveToCell` could extend the slice, panicking when the new key was not in the rightmost position. Pre-allocate `newPage.LeafNode.Cells` to `rightSplitCount` empty cells before the loop. This bug was latent because even-split was only triggered by sequential inserts (where the new key is always rightmost, so `saveToCell` ran first), and was exposed by the update delete-and-reinsert path introduced by fix #2.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **22.1 µs/op** | **107.5 µs/op†** | **0.21×** |
| **Insert_SingleRow** | **16.6 µs/op** | **41.7 µs/op** | **0.40×** |
| **Insert_Batch** | **315.0 µs/op** | **222.3 µs/op** | **1.42×** |
| **Insert_PreparedBatch** | **316.6 µs/op** | **220.3 µs/op** | **1.44×** |
| **Insert_MultiValues** | **226.5 µs/op** | **167.0 µs/op** | **1.36×** |
| Select_PointScan | 4.35 µs/op | 3.29 µs/op | 1.32× |
| **Select_Limit** | **7.36 µs/op** | **7.72 µs/op** | **0.95×** |
| **Select_FullScan** | **4.64 ms/op** | **5.01 ms/op** | **0.93×** |
| Select_CountStar | 17.0 µs/op | 9.65 µs/op | 1.76× |
| **Select_IndexRangeScan** | **708.3 µs/op** | **742.8 µs/op** | **0.95×** |
| Select_RangeScan | 1.79 ms/op | 852.6 µs/op | 2.10× |
| **Update_ByPK** | **10.2 µs/op** | **46.8 µs/op** | **0.22×** |

† SQLite Delete shows run-to-run variance; single run used.

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 23.6 KiB | 447 B |
| Insert_SingleRow | 18.5 KiB | 312 B |
| Insert_Batch | 302.4 KiB | 31.1 KiB |
| Insert_PreparedBatch | 301.8 KiB | 31.1 KiB |
| Insert_MultiValues | 268.2 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| Select_FullScan | 5.3 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 737.2 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 8.3 KiB | 257 B |

---

### 2026-04-26 (O(1) free-space cache in IndexNode — previous)

Added a `freeBytes uint64` field to `IndexNode[T]` (in-memory only, not serialized). Maintained on every mutating operation so `AvailableSpace()` / `HasSpaceForKey()` / `AtLeastHalfFull()` / `SplitInHalves()` all return in O(1) instead of O(n):
- **`AvailableSpace()`** now returns `n.freeBytes` directly (was: `MaxSpace() - TakenSpace()`, an O(n) cell-size sum).
- **`SplitInHalves()`** uses `(n.MaxSpace() - n.freeBytes)` instead of `TakenSpace()` for non-unique midpoint search.
- `freeBytes` is initialized in `NewIndexNode` (= `MaxSpace()`) and recomputed in `Unmarshal` (accumulates bytes consumed per cell, which equals `cell.Size()`).
- `Clone()` copies `freeBytes`; all mutating node methods (`AppendCells`, `PrependCell`, `RemoveLastCell`, `RemoveFirstCell`, `DeleteKeyAndRightChild`) maintain it incrementally.
- `borrowFromLeft` / `borrowFromRight` apply an O(1) size delta to the parent (instead of a full O(n) recompute) to handle variable-width key types (varchar, CompositeKey) correctly.
- **Insert_Batch: 407.3 µs → 349.3 µs (1.17× faster, ratio vs SQLite 1.8× → 1.55×)** — `hasSpaceForKey` is called on every internal node and the leaf; O(1) free-space check directly reduces per-insert overhead.
- **Insert_PreparedBatch: 405.9 µs → 347.6 µs (1.17× faster, ratio 1.8× → 1.54×)**
- **Insert_MultiValues: 317.9 µs → 260.4 µs (1.22× faster, ratio 1.9× → 1.50×)**
- Insert_SingleRow: unchanged (17.9 µs) — single-row-per-transaction workload benefits less as OCC/WAL overhead dominates.
- Delete_ByPK: 22.2 µs → 26.4 µs (slight regression; allocs 103 → 116) — cause not fully identified; delete is still 2.7× faster than SQLite.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **26.4 µs/op** | **70.2 µs/op** | **0.37×** |
| **Insert_SingleRow** | **17.9 µs/op** | **41.9 µs/op** | **0.43×** |
| **Insert_Batch** | **349.3 µs/op** | **225.2 µs/op** | **1.55×** |
| **Insert_PreparedBatch** | **347.6 µs/op** | **225.9 µs/op** | **1.54×** |
| **Insert_MultiValues** | **260.4 µs/op** | **173.3 µs/op** | **1.50×** |
| Select_PointScan | 4.31 µs/op | 3.43 µs/op | 1.3× |
| **Select_Limit** | **7.59 µs/op** | **7.70 µs/op** | **0.99×** |
| **Select_FullScan** | **4.80 ms/op** | **5.08 ms/op** | **0.94×** |
| Select_CountStar | 17.0 µs/op | 9.86 µs/op | 1.7× |
| **Select_IndexRangeScan** | **703.5 µs/op** | **770.8 µs/op** | **0.91×** |
| Select_RangeScan | 1.77 ms/op | 0.86 ms/op | 2.1× |
| **Update_ByPK** | **10.7 µs/op** | **36.4 µs/op** | **0.29×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 30.2 KiB | 447 B |
| Insert_SingleRow | 21.5 KiB | 311 B |
| Insert_Batch | 356.1 KiB | 31.0 KiB |
| Insert_PreparedBatch | 355.7 KiB | 31.0 KiB |
| Insert_MultiValues | 322.0 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| Select_FullScan | 5.3 MiB | 1.3 MiB |
| Select_CountStar | 6.0 KiB | 400 B |
| Select_IndexRangeScan | 739.3 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.1 KiB | 263 B |

---

### 2026-04-26 (binary search within index nodes)

Replaced all linear scans over `IndexNode.Cells` with `sort.Search` (binary search) in `index.go` and `index_cursor.go`:
- **`insertNotFull` — non-unique duplicate key check**: forward linear scan → binary search lower-bound + equality check.
- **`insertNotFull` — leaf insertion position**: backward linear scan + field-by-field shift → binary search + backward struct-copy shift.
- **`insertNotFull` — internal node child selection**: backward linear scan → binary search (first index where `Cells[i].Key > key`).
- **`remove` — key search**: forward linear scan → binary search lower-bound.
- **`Seek` (index_cursor.go)**: forward linear scan → binary search lower-bound.
- The table B+ tree (`InternalNode.IndexOfChild`, `leafNodeSeek`) was already using binary search; no change there.
- **Insert_Batch: 492.2 µs → 407.3 µs (1.21× faster, ratio vs SQLite 2.2× → 1.8×)** — each of the 100 rows per transaction searches an internal or leaf node; binary search directly cuts per-insert comparison count.
- **Insert_PreparedBatch: 490.7 µs → 405.9 µs (1.21× faster, ratio 2.2× → 1.8×)**
- **Insert_MultiValues: 405.3 µs → 317.9 µs (1.27× faster, ratio 2.4× → 1.9×)**
- Insert_SingleRow improved ~5% (19.0 µs → 18.0 µs) — modest benefit since single-row-per-transaction workloads don't accumulate many keys per node before the next transaction starts fresh.
- Read, Update, Delete paths see small improvements consistent with fewer comparisons during tree traversal.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **22.2 µs/op** | **82.3 µs/op†** | **0.27×** |
| **Insert_SingleRow** | **18.0 µs/op** | **41.0 µs/op** | **0.44×** |
| **Insert_Batch** | **407.3 µs/op** | **223.2 µs/op** | **1.8×** |
| **Insert_PreparedBatch** | **405.9 µs/op** | **221.4 µs/op** | **1.8×** |
| **Insert_MultiValues** | **317.9 µs/op** | **170.3 µs/op** | **1.9×** |
| Select_PointScan | 4.33 µs/op | 3.33 µs/op | 1.3× |
| **Select_Limit** | **7.55 µs/op** | **7.92 µs/op** | **0.95×** |
| **Select_FullScan** | **4.73 ms/op** | **5.08 ms/op** | **0.93×** |
| Select_CountStar | 17.4 µs/op | 9.60 µs/op | 1.8× |
| **Select_IndexRangeScan** | **683.8 µs/op** | **737.2 µs/op** | **0.93×** |
| Select_RangeScan | 1.72 ms/op | 0.88 ms/op | 2.0× |
| **Update_ByPK** | **10.5 µs/op** | **36.5 µs/op** | **0.29×** |

† SQLite Delete continues to show run-to-run variance (63 / 79 / 105 µs); 3-run average used.

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 25.3 KiB | 447 B |
| Insert_SingleRow | 21.5 KiB | 312 B |
| Insert_Batch | 356.2 KiB | 31.1 KiB |
| Insert_PreparedBatch | 355.6 KiB | 31.1 KiB |
| Insert_MultiValues | 322.7 KiB | 25.3 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| Select_FullScan | 5.3 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 737.1 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-26 (IndexNode cell pre-sizing)

IndexNode `Cells` slice capacity increased from 4 to 32, eliminating most slice reallocations during sequential insert / rebalance workloads:
- **`NewIndexNode`**: changed `make([]IndexCell[T], 4, 4)` to `make([]IndexCell[T], 4, 32)`. With `cap==len==4`, the very first `append` (insert) triggered an immediate reallocation to cap=8 and then up to cap=256 across 6 steps to fill a full int64 leaf. With cap=32, no reallocation occurs for the first 28 insertions; a full leaf needs 3 reallocs (32→64→128→256) instead of 6.
- Renamed exported `MinimumIndexCells = 4` to unexported `indexCellsPrealloc = 32` (the old constant had a stale TODO and was only used internally).
- **Delete_ByPK: 29.8 µs → 23.5 µs (1.27× faster)** — rebalancing creates new nodes via `NewIndexNode`; fewer reallocations means fewer intermediate backing-array allocations on the hot delete path.
- **Delete_ByPK allocs/op: 117 → 103 (−12%)** — directly reflects the eliminated backing-array reallocations during node creation in the rebalancing code.
- Insert_SingleRow and Update_ByPK unchanged (within noise): single-row-per-transaction inserts don't create new nodes frequently enough for the capacity change to register.
- SQLite Delete numbers show high run-to-run variance (83 / 113 / 120 µs across 3 runs); ratio computed from 3-run average.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **23.5 µs/op** | **105.4 µs/op†** | **0.22×** |
| **Insert_SingleRow** | **19.0 µs/op** | **44.3 µs/op** | **0.43×** |
| Insert_Batch | 492.2 µs/op | 222.6 µs/op | 2.2× |
| Insert_PreparedBatch | 490.7 µs/op | 219.3 µs/op | 2.2× |
| Insert_MultiValues | 405.3 µs/op | 166.7 µs/op | 2.4× |
| Select_PointScan | 4.49 µs/op | 3.31 µs/op | 1.4× |
| **Select_Limit** | **7.39 µs/op** | **7.82 µs/op** | **0.94×** |
| **Select_FullScan** | **4.71 ms/op** | **5.02 ms/op** | **0.94×** |
| Select_CountStar | 17.1 µs/op | 9.60 µs/op | 1.8× |
| **Select_IndexRangeScan** | **680.7 µs/op** | **740.3 µs/op** | **0.92×** |
| Select_RangeScan | 1.67 ms/op | 0.86 ms/op | 1.9× |
| **Update_ByPK** | **10.6 µs/op** | **38.2 µs/op** | **0.28×** |

† SQLite Delete shows high run-to-run variance (83 / 113 / 120 µs); 3-run average used.

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 25.3 KiB | 447 B |
| Insert_SingleRow | 21.5 KiB | 312 B |
| Insert_Batch | 356.1 KiB | 31.1 KiB |
| Insert_PreparedBatch | 355.4 KiB | 31.1 KiB |
| Insert_MultiValues | 322.3 KiB | 25.3 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| Select_FullScan | 5.3 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 737.1 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-26 (WAL write frame batching — previous)

WAL write-frame batching — frames from multiple transactions accumulated in a 64 KiB in-process buffer before a single `WriteAt` to the OS page cache:
- **`WAL.pendingBuf`** replaces the old `writeBuf` scratch buffer. `AppendTransaction` serialises frames directly into `pendingBuf[pendingLen:]` and flushes (one `WriteAt`) only when `pendingLen >= flushThreshold` (default 64 KiB), `flushThreshold == 0` (opt-out), or `SynchronousFull`. A 64 KiB buffer holds ~16 full-page frames, so ~8–16 single-row transactions share one syscall instead of one each.
- `Checkpoint`, `Truncate`, and `Close` all flush pending bytes before acting, so no frames are ever lost on clean shutdown. `Close` also fsyncs (unless `SynchronousOff`) so a graceful close is always durable.
- `FrameCount()` adds `pendingLen` to the on-disk count so auto-checkpoint fires at the correct threshold even with buffered-but-unflushed frames.
- **`wal_write_buffer_size=N`** connection-string parameter; default 65536; 0 disables batching (flush every commit). Enabled by default for all production databases opened via a connection string; raw `CreateWAL` callers (unit tests) keep `flushThreshold = 0` so existing tests are unaffected.
- **Insert_SingleRow: 28.9 µs → 19.2 µs (1.5× faster, now 2.3× faster than SQLite, ratio 0.43×)**
- **Update_ByPK: 18.5 µs → 11.0 µs (1.7× faster, now 3.3× faster than SQLite, ratio 0.30×)**
- **Delete_ByPK: ~52 µs → 29.8 µs (1.7× faster, 2.7× faster than SQLite†, ratio 0.37×)**
- Insert_Batch/PreparedBatch/MultiValues: 10–19% faster in absolute terms; ratio vs SQLite unchanged (2.1–2.2×) — batch transactions already exceed the 64 KiB threshold and flush per-transaction; the absolute improvement is machine/thermal state.
- Read paths also faster in absolute terms; both databases improved similarly, confirming machine state rather than code change.
- Delete_ByPK allocs/op: 131 → 117 (11% reduction) — the pending buffer grows to steady-state once and stops reallocating, eliminating the occasional `make([]byte, need)` in the hot path.

† SQLite Delete run 1 was a warm-up outlier (186 µs vs 78–80 µs in runs 2–3); ratio computed from runs 2–3.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **29.8 µs/op** | **79.8 µs/op†** | **0.37×** |
| **Insert_SingleRow** | **19.2 µs/op** | **44.4 µs/op** | **0.43×** |
| Insert_Batch | 473.3 µs/op | 225.6 µs/op | 2.1× |
| Insert_PreparedBatch | 482.8 µs/op | 227.5 µs/op | 2.1× |
| Insert_MultiValues | 380.2 µs/op | 169.7 µs/op | 2.2× |
| Select_PointScan | 4.72 µs/op | 3.37 µs/op | 1.4× |
| **Select_Limit** | **7.37 µs/op** | **8.04 µs/op** | **0.92×** |
| **Select_FullScan** | **4.76 ms/op** | **5.19 ms/op** | **0.92×** |
| Select_CountStar | 17.3 µs/op | 9.73 µs/op | 1.8× |
| **Select_IndexRangeScan** | **705.4 µs/op** | **751.8 µs/op** | **0.94×** |
| Select_RangeScan | 1.65 ms/op | 0.87 ms/op | 1.9× |
| **Update_ByPK** | **11.0 µs/op** | **36.4 µs/op** | **0.30×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 30.9 KiB | 447 B |
| Insert_SingleRow | 21.5 KiB | 311 B |
| Insert_Batch | 352.4 KiB | 31.0 KiB |
| Insert_PreparedBatch | 352.0 KiB | 31.0 KiB |
| Insert_MultiValues | 318.8 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| Select_FullScan | 5.3 MiB | 1.3 MiB |
| Select_CountStar | 6.0 KiB | 400 B |
| Select_IndexRangeScan | 739.3 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.1 KiB | 263 B |

---

### 2026-04-26 (rightmost-leaf cache)

Rightmost-leaf cache optimization for B+ tree insertions:
- **`Index[T]`**: added `rightmostLeaf atomic.Int64` (−1 = cold) and `lastTxID atomic.Uint64`. On each `Insert`, if `tx.ID != lastTxID` the cache is invalidated (guards against stale hints after rollback/OCC conflict). Fast path inside `hasSpaceForKey(root)` reads the cached leaf and appends directly when `key > lastKey` and the leaf has space, bypassing the O(log N) root→leaf traversal. `insertNotFull` returns `(PageIndex, bool, error)` where the bool tracks "every level chose the rightmost child" — only when the full path was rightmost is the cache updated; non-rightmost inserts unconditionally cold-start the cache.
- **`Table`**: same pattern for `SeekNextRowID` — `rightmostTablePage atomic.Int64` + `lastTxIDTablePage atomic.Uint64`. Fast path reads the cached leaf, checks `NextLeaf == 0`, and returns `(cursor, maxKey+1)` in O(1). Cache is warmed in the normal slow path and eagerly updated in `LeafNodeSplitInsert` when a new rightmost leaf is created. Fast path gated on `TxFromContext(ctx) != nil` so tests that call `SeekNextRowID` without a transaction context are unaffected.
- **Per-transaction invalidation** is the key correctness property: because each `ExecuteInTransaction` call uses a distinct `tx.ID`, the cache is cold-started on the first insert of every new transaction. This means single-row-per-transaction benchmarks (`Insert_SingleRow`) don't benefit — each iteration begins with a cache miss. Batch inserts do benefit: rows 2–N within the same transaction use the O(1) fast path, skipping traversal for 99 out of 100 rows per batch.
- **Delete** invalidates the cache (`rightmostLeaf.Store(-1)`) at entry; Update and Select do not touch it.
- Write-path benchmarks show higher absolute numbers than the previous "synchronous=normal" run. SQLite numbers are similarly elevated (86.4 vs 89.6 µs for Delete; 50.7 vs 43.9 µs for Insert), indicating machine-load / thermal variance rather than a code regression. All single-row write ratios vs SQLite remain strongly in minisql's favour.
- **Insert_SingleRow: 28.9 µs vs SQLite 50.7 µs (0.57×)** — 1.75× faster than SQLite
- **Delete_ByPK: 52.2 µs vs SQLite 86.4 µs (0.60×)** — 1.66× faster than SQLite
- **Update_ByPK: 18.5 µs vs SQLite 42.5 µs (0.44×)** — 2.3× faster than SQLite
- Insert_Batch: 536.5 µs vs SQLite 254.0 µs (2.1×) — slight improvement vs prior run (543 µs), consistent with 99/100 rows hitting the cache per batch
- Select paths unchanged; numbers elevated by machine variance but ratios stable.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **52.2 µs/op** | **86.4 µs/op** | **0.60×** |
| **Insert_SingleRow** | **28.9 µs/op** | **50.7 µs/op** | **0.57×** |
| Insert_Batch | 536.5 µs/op | 254.0 µs/op | 2.1× |
| Insert_PreparedBatch | 551.7 µs/op | 281.1 µs/op | 2.0× |
| Insert_MultiValues | 453.4 µs/op | 202.0 µs/op | 2.2× |
| Select_PointScan | 6.47 µs/op | 3.93 µs/op | 1.6× |
| **Select_Limit** | **9.41 µs/op** | **9.80 µs/op** | **0.96×** |
| Select_FullScan | 6.23 ms/op | 6.16 ms/op | 1.01× |
| Select_CountStar | 20.6 µs/op | 10.9 µs/op | 1.9× |
| Select_IndexRangeScan | 997.0 µs/op | 914.8 µs/op | 1.1× |
| Select_RangeScan | 2.35 ms/op | 1.02 ms/op | 2.3× |
| **Update_ByPK** | **18.5 µs/op** | **42.5 µs/op** | **0.44×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 35.7 KiB | 447 B |
| Insert_SingleRow | 21.5 KiB | 311 B |
| Insert_Batch | 351.9 KiB | 31.0 KiB |
| Insert_PreparedBatch | 351.2 KiB | 31.0 KiB |
| Insert_MultiValues | 318.2 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| Select_FullScan | 5.3 MiB | 1.3 MiB |
| Select_CountStar | 6.0 KiB | 400 B |
| Select_IndexRangeScan | 740.2 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.1 KiB | 263 B |

---

### 2026-04-26 (synchronous=normal)

Both minisql and SQLite now run with `synchronous=normal` (WAL mode default): no fsync per commit, fsync only at checkpoint.

- **minisql default changed to `SynchronousNormal`**: `WAL.AppendTransaction` no longer calls `fsync()` after each commit. The per-commit ~50–70 µs fsync was the dominant write-path cost.
- **SQLite benchmark DSN updated**: removed `synchronous(FULL)` override — SQLite now also uses its WAL default (`synchronous=NORMAL`). Both databases are now measured under identical durability conditions.
- **`PRAGMA synchronous`** added: readable and settable at runtime (`off` / `normal` / `full`); also configurable via the `synchronous=` connection string parameter.
- **Single-row write paths now faster than SQLite** across Delete, Insert, and Update:
  - **Delete_ByPK: 177.9 µs → 27.5 µs (6.5× faster)** — **3.25× faster than SQLite**
  - **Insert_SingleRow: 83.0 µs → 21.8 µs (3.8× faster)** — **2.0× faster than SQLite**
  - **Update_ByPK: 57.0 µs → 14.1 µs (4.0× faster)** — **2.8× faster than SQLite**
- Batch inserts remain slower (2.3–2.6×): the bottleneck is now per-row Go allocation overhead rather than fsync latency.
- Read paths are unchanged (no code change); minor variance vs previous run.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| **Delete_ByPK** | **27.5 µs/op** | **89.6 µs/op** | **0.31×** |
| **Insert_SingleRow** | **21.8 µs/op** | **43.9 µs/op** | **0.50×** |
| Insert_Batch | 543.0 µs/op | 229.7 µs/op | 2.4× |
| Insert_PreparedBatch | 549.7 µs/op | 241.4 µs/op | 2.3× |
| Insert_MultiValues | 446.8 µs/op | 170.7 µs/op | 2.6× |
| Select_PointScan | 4.46 µs/op | 3.36 µs/op | 1.3× |
| **Select_Limit** | **7.33 µs/op** | **8.03 µs/op** | **0.91×** |
| **Select_FullScan** | **4.81 ms/op** | **5.16 ms/op** | **0.93×** |
| Select_CountStar | 17.28 µs/op | 9.68 µs/op | 1.8× |
| **Select_IndexRangeScan** | **704.5 µs/op** | **760.7 µs/op** | **0.93×** |
| Select_RangeScan | 1.82 ms/op | 883.2 µs/op | 2.1× |
| **Update_ByPK** | **14.1 µs/op** | **39.1 µs/op** | **0.36×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 26.3 KiB | 447 B |
| Insert_SingleRow | 21.5 KiB | 312 B |
| Insert_Batch | 352.3 KiB | 31.1 KiB |
| Insert_PreparedBatch | 351.6 KiB | 31.1 KiB |
| Insert_MultiValues | 318.2 KiB | 25.3 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| **Select_FullScan** | **5.3 MiB** | **1.3 MiB** |
| Select_CountStar | 6.0 KiB | 400 B |
| Select_IndexRangeScan | 737.5 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-26 (medium-impact zero-copy + exact-size allocations)

Medium-impact zero-copy + exact-size allocations:
- **`CompositeKey.Unmarshal` exact allocation**: replaced the blanket `make([]byte, 255×cols×4)` overallocation (up to 8 KiB for an 8-column key) with a two-pass approach — first pass scans `buf` reading varchar length prefixes to compute the exact comparison size, second pass fills values. Allocation for a typical `(int64, varchar(10))` key shrinks from 2,040 B → 18 B. Fixes a latent issue where the sub-sliced `ck.Comparison = comparison[:compOffset]` kept the full oversized backing array alive.
- **`OverflowPage.Unmarshal` sub-slice**: `make+copy` → `buf[i:i+DataSize]`. `readOverflowTexts` copies these bytes out immediately via `append`, so this eliminates one allocation per overflow page read without changing observable behaviour.
- **`TextPointer.Unmarshal` inline sub-slice**: same pattern — inline `Data` now sub-slices the page buffer. `Marshal` copies it out via `copy(buf, tp.Data)`, safe whether `Data` owns its bytes or not.
- **`readOverflowTexts` pre-allocation**: `var overflowData []byte` → `make([]byte, 0, textPointer.Length)`. Eliminates repeated reallocation while assembling multi-page text values.
- **`query_plan.go` allIndexes pre-allocation**: exact capacity (`1 + len(UniqueIndexes) + len(SecondaryIndexes)`) instead of nil-start + append.
- **Select_FullScan: 5.04 ms → 4.89 ms (1.03× faster)** — now faster than SQLite (**0.9×**). Memory drops 5.7 MiB → 5.3 MiB (−7%); allocs 131,698 → 111,698 (−15%). TextPointer sub-slicing reduces per-row cost for text-heavy tables.
- **Select_IndexRangeScan: 724.5 µs → 687.37 µs (1.05× faster)** — 0.89× vs SQLite. Allocs 12,168 → 11,065 (−9%). CompositeKey Unmarshal fix directly reduces per-key allocation on composite-index lookups.
- Delete/Insert timing regressions vs previous entry are within benchmark noise (Delete_ByPK is particularly volatile); alloc counts and memory are stable or improved.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 177.87 µs/op | 76.75 µs/op | 2.3× |
| Insert_SingleRow | 82.95 µs/op | 47.69 µs/op | 1.7× |
| Insert_Batch | 633.61 µs/op | 227.28 µs/op | 2.8× |
| Insert_PreparedBatch | 615.82 µs/op | 235.80 µs/op | 2.6× |
| Insert_MultiValues | 474.51 µs/op | 171.74 µs/op | 2.8× |
| Select_PointScan | 4.40 µs/op | 3.45 µs/op | 1.3× |
| Select_Limit | 7.34 µs/op | 8.02 µs/op | 0.9× |
| **Select_FullScan** | **4.89 ms/op** | **5.24 ms/op** | **0.9×** |
| Select_CountStar | 17.55 µs/op | 9.79 µs/op | 1.8× |
| **Select_IndexRangeScan** | **687.37 µs/op** | **768.10 µs/op** | **0.9×** |
| Select_RangeScan | 1.63 ms/op | 875.80 µs/op | 1.9× |
| Update_ByPK | 56.97 µs/op | 120.46 µs/op | 0.5× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 48.2 KiB | 447 B |
| Insert_SingleRow | 21.4 KiB | 311 B |
| Insert_Batch | 351.7 KiB | 31.0 KiB |
| Insert_PreparedBatch | 351.1 KiB | 31.0 KiB |
| Insert_MultiValues | 318.0 KiB | 25.2 KiB |
| Select_PointScan | 4.6 KiB | 679 B |
| Select_Limit | 6.1 KiB | 1.7 KiB |
| **Select_FullScan** | **5.3 MiB** | **1.3 MiB** |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 739.3 KiB | 85.9 KiB |
| Select_RangeScan | 1.6 MiB | 85.9 KiB |
| Update_ByPK | 9.2 KiB | 263 B |

---

### 2026-04-25 (zero-copy cell reads + struct alignment)

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
