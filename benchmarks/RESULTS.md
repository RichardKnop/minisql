### 2026-04-24 (latest)

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
