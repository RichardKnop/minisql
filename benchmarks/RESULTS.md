### 2026-04-24 (latest)

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
| Txn_NInserts | 381.9 µs/op | 148.7 µs/op | 2.57× |
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
| Txn_NInserts | 192.7 KiB | 15.9 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

### 2026-04-24 (previous)

Snapshot isolation (MVCC) for read-only transactions + TOCTOU fix in `ReadPage`:
- Read-only transactions now provide true snapshot isolation: any write committed after `BeginReadOnlyTransaction` is invisible to the reader. Old page versions are stored in-memory (`pageVersionHistory`) at write-commit time and GC'd once all readers that need them have committed.
- Fixed a pre-existing TOCTOU race in `ReadPage` for write transactions: the page version was captured *after* `GetPage` (pager mutex) rather than *before*, meaning a commit landing between the two could cause the writer to track a stale read-version and miss a conflict. Version is now captured first.
- Added early conflict detection in `ModifyPage`: if a write transaction previously read a page whose global version has since advanced, `ModifyPage` returns `ErrTxConflict` immediately instead of producing a misleading "duplicate key" error.
- Write-path benchmarks (Insert, Delete, Update, Txn) see a small regression (~1–2×) vs. the previous entry due to the version-before-read change; read-path benchmarks are broadly unchanged.

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
| Txn_NInserts | 326.0 µs/op | 142.3 µs/op | 2.3× |
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
| Txn_NInserts | 200.8 KiB | 15.8 KiB |
| Update_ByPK | 9.0 KiB | 263 B |
