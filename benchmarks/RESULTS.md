### 2026-04-21 18:43 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 240.09 µs/op | 170.96 µs/op | 1.4× |
| Insert_SingleRow | 113.70 µs/op | 47.39 µs/op | 2.4× |
| Insert_Batch | 670.16 µs/op | 238.17 µs/op | 2.8× |
| Select_PointScan | 5.19 µs/op | 3.82 µs/op | 1.4× |
| Select_Limit | 10.66 µs/op | 9.29 µs/op | 1.1× |
| Select_FullScan | 8.47 ms/op | 6.02 ms/op | 1.4× |
| Select_CountStar | 35.75 µs/op | 10.54 µs/op | 3.4× |
| Select_IndexRangeScan | 989.00 µs/op | 856.93 µs/op | 1.2× |
| Select_RangeScan | 3.85 ms/op | 942.68 µs/op | 4.1× |
| Txn_NInserts | 356.50 µs/op | 151.25 µs/op | 2.4× |
| Update_ByPK | 62.26 µs/op | 41.92 µs/op | 1.5× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 80.4 KiB | 447 B |
| Insert_SingleRow | 42.1 KiB | 311 B |
| Insert_Batch | 360.3 KiB | 31.0 KiB |
| Select_PointScan | 4.3 KiB | 679 B |
| Select_Limit | 9.8 KiB | 1.7 KiB |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 836.3 KiB | 85.9 KiB |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 205.5 KiB | 15.8 KiB |
| Update_ByPK | 9.0 KiB | 263 B |


