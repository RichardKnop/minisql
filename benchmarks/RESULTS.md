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


### 2026-04-19 00:57 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 10.39 ms/op | 110.40 µs/op | 94.1× |
| Insert_SingleRow | 4.97 ms/op | 54.92 µs/op | 90.4× |
| Insert_Batch | 6.63 ms/op | 295.07 µs/op | 22.5× |
| Select_PointScan | 28.49 µs/op | 4.56 µs/op | 6.2× |
| Select_FullScan | 20.61 ms/op | 7.27 ms/op | 2.8× |
| Select_CountStar | 86.49 µs/op | 11.69 µs/op | 7.4× |
| Select_RangeScan | 9.62 ms/op | 1.04 ms/op | 9.2× |
| Txn_NInserts | 6.18 ms/op | 170.61 µs/op | 36.2× |
| Update_ByPK | 5.30 ms/op | 52.24 µs/op | 101.4× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 223.5 KiB | 447 B |
| Insert_SingleRow | 70.7 KiB | 311 B |
| Insert_Batch | 843.4 KiB | 31.0 KiB |
| Select_PointScan | 22.7 KiB | 679 B |
| Select_FullScan | 11.5 MiB | 1.3 MiB |
| Select_CountStar | 34.2 KiB | 400 B |
| Select_RangeScan | 4.6 MiB | 85.9 KiB |
| Txn_NInserts | 437.5 KiB | 15.8 KiB |
| Update_ByPK | 38.2 KiB | 263 B |


