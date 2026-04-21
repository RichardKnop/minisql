### 2026-04-21 18:31 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 227.21 µs/op | 234.71 µs/op | 1.0× |
| Insert_SingleRow | 118.85 µs/op | 43.64 µs/op | 2.7× |
| Insert_Batch | 582.11 µs/op | 225.83 µs/op | 2.6× |
| Select_PointScan | 4.56 µs/op | 3.38 µs/op | 1.3× |
| Select_Limit | 96.41 µs/op | 11.42 µs/op | 8.4× |
| Select_FullScan | 6.74 ms/op | 5.06 ms/op | 1.3× |
| Select_CountStar | 32.90 µs/op | 9.77 µs/op | 3.4× |
| Select_IndexRangeScan | 738.68 µs/op | 753.95 µs/op | 1.0× |
| Select_RangeScan | 3.11 ms/op | 869.10 µs/op | 3.6× |
| Txn_NInserts | 330.07 µs/op | 137.99 µs/op | 2.4× |
| Update_ByPK | 56.15 µs/op | 39.43 µs/op | 1.4× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 80.5 KiB | 447 B |
| Insert_SingleRow | 42.1 KiB | 311 B |
| Insert_Batch | 360.0 KiB | 31.0 KiB |
| Select_PointScan | 4.3 KiB | 679 B |
| Select_Limit | 32.0 KiB | 1.7 KiB |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 835.9 KiB | 85.9 KiB |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 205.9 KiB | 15.8 KiB |
| Update_ByPK | 8.9 KiB | 263 B |
