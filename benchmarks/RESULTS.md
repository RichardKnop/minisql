### 2026-04-19 01:29 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 366.31 µs/op | 104.44 µs/op | 3.5× |
| Insert_SingleRow | 136.07 µs/op | 57.82 µs/op | 2.4× |
| Insert_Batch | 1.14 ms/op | 298.39 µs/op | 3.8× |
| Select_PointScan | 27.10 µs/op | 4.45 µs/op | 6.1× |
| Select_FullScan | 17.94 ms/op | 6.70 ms/op | 2.7× |
| Select_CountStar | 82.45 µs/op | 11.61 µs/op | 7.1× |
| Select_RangeScan | 8.28 ms/op | 1.09 ms/op | 7.6× |
| Txn_NInserts | 645.41 µs/op | 205.45 µs/op | 3.1× |
| Update_ByPK | 91.73 µs/op | 45.27 µs/op | 2.0× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 205.9 KiB | 447 B |
| Insert_SingleRow | 94.9 KiB | 311 B |
| Insert_Batch | 895.8 KiB | 31.0 KiB |
| Select_PointScan | 22.7 KiB | 679 B |
| Select_FullScan | 11.5 MiB | 1.3 MiB |
| Select_CountStar | 34.2 KiB | 400 B |
| Select_RangeScan | 4.6 MiB | 85.9 KiB |
| Txn_NInserts | 510.1 KiB | 15.8 KiB |
| Update_ByPK | 29.4 KiB | 263 B |


