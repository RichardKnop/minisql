### 2026-04-19 02:20 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 386.42 µs/op | 145.41 µs/op | 2.7× |
| Insert_SingleRow | 134.46 µs/op | 53.41 µs/op | 2.5× |
| Insert_Batch | 970.47 µs/op | 241.00 µs/op | 4.0× |
| Select_PointScan | 12.94 µs/op | 3.50 µs/op | 3.7× |
| Select_FullScan | 5.74 ms/op | 5.28 ms/op | 1.1× |
| Select_CountStar | 62.38 µs/op | 10.01 µs/op | 6.2× |
| Select_RangeScan | 3.59 ms/op | 905.22 µs/op | 4.0× |
| Txn_NInserts | 504.36 µs/op | 154.57 µs/op | 3.3× |
| Update_ByPK | 75.66 µs/op | 45.81 µs/op | 1.7× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 205.9 KiB | 447 B |
| Insert_SingleRow | 94.4 KiB | 311 B |
| Insert_Batch | 899.2 KiB | 31.0 KiB |
| Select_PointScan | 19.6 KiB | 679 B |
| Select_FullScan | 9.0 MiB | 1.3 MiB |
| Select_CountStar | 34.3 KiB | 400 B |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 511.3 KiB | 15.8 KiB |
| Update_ByPK | 29.1 KiB | 263 B |
