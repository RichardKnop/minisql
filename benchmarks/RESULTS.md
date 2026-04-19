### 2026-04-19 00:38 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 9.33 ms/op | 81.85 µs/op | 114.0× |
| Insert_SingleRow | 4.48 ms/op | 50.19 µs/op | 89.3× |
| Insert_Batch | 7.83 ms/op | 240.68 µs/op | 32.5× |
| Select_PointScan | 15.84 µs/op | 3.50 µs/op | 4.5× |
| Select_FullScan | 7.64 ms/op | 5.37 ms/op | 1.4× |
| Select_CountStar | 4.92 ms/op | 10.13 µs/op | 485.1× |
| Select_RangeScan | 6.04 ms/op | 914.01 µs/op | 6.6× |
| Txn_NInserts | 5.28 ms/op | 152.37 µs/op | 34.7× |
| Update_ByPK | 4.59 ms/op | 39.08 µs/op | 117.5× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 218.8 KiB | 447 B |
| Insert_SingleRow | 71.8 KiB | 311 B |
| Insert_Batch | 871.1 KiB | 31.1 KiB |
| Select_PointScan | 22.7 KiB | 679 B |
| Select_FullScan | 11.5 MiB | 1.3 MiB |
| Select_CountStar | 3.4 MiB | 400 B |
| Select_RangeScan | 4.6 MiB | 85.9 KiB |
| Txn_NInserts | 467.4 KiB | 15.8 KiB |
| Update_ByPK | 35.5 KiB | 263 B |


