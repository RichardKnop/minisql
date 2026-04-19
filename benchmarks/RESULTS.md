### 2026-04-19 17:10 UTC

WAL allocation optimizations + read-only transaction fast path:
- `WAL.AppendTransaction`: reuse `writeBuf` (eliminates per-commit 4KB×N batch buffer allocation)
- `WALIndex.Update`: take ownership of page slice (no defensive copy on store)
- `WALIndex.Lookup`: return direct reference (no defensive copy on read)
- `WAL.Checkpoint`: inline frame scan into latest-page map (eliminates intermediate `[]WALReadFrame` slice)
- `Transaction.ReadOnly`: skip `TrackRead` / ReadSet allocation for SELECT queries; skip OCC read conflict check

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 105.64 µs/op | 97.79 µs/op | 1.1× |
| Insert_SingleRow | 88.60 µs/op | 49.90 µs/op | 1.8× |
| Insert_Batch | 900.61 µs/op | 266.61 µs/op | 3.4× |
| Select_PointScan | 5.01 µs/op | 3.56 µs/op | 1.4× |
| Select_FullScan | 8.41 ms/op | 5.50 ms/op | 1.5× |
| Select_CountStar | 37.54 µs/op | 9.92 µs/op | 3.8× |
| Select_RangeScan | 3.51 ms/op | 1.02 ms/op | 3.4× |
| Txn_NInserts | 482.84 µs/op | 159.79 µs/op | 3.0× |
| Update_ByPK | 51.99 µs/op | 44.47 µs/op | 1.2× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 84.0 KiB | 447 B |
| Insert_SingleRow | 68.6 KiB | 312 B |
| Insert_Batch | 759 KiB | 31.1 KiB |
| Select_PointScan | 4.4 KiB | 679 B |
| Select_FullScan | 12.3 MiB | 1.3 MiB |
| Select_CountStar | 6.0 KiB | 400 B |
| Select_RangeScan | 2.4 MiB | 85.9 KiB |
| Txn_NInserts | 414 KiB | 15.9 KiB |
| Update_ByPK | 15.3 KiB | 263 B |

