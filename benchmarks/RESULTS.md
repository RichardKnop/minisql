### 2026-04-21 22:32 UTC

`IndexCell.UniqueRowID` inline storage — eliminate heap-allocated `[]RowID` slice for unique index cells:
- Added `UniqueRowID RowID` field to `IndexCell`; for unique indexes (primary key, unique indexes) the single RowID is stored inline in the struct rather than in a `[]RowID` slice.
- `NewIndexCell`: skips `make([]RowID, 0, 1)` for unique cells.
- `Clone()`: for unique cells, copies the scalar `UniqueRowID` — no `make([]RowID, N)` per cell.
- `Marshal`/`Unmarshal`: reads/writes `UniqueRowID` directly.
- All B+ tree mutation sites updated: `insertNotFull`, `splitChild`, `removeFromInternal`, `borrowFromLeft`, `borrowFromRight`, `merge`, `scanAscending/Descending/Range*`.
- Primary win: each `ModifyPage` on an index page with K unique-index cells previously did K×`make([]RowID,1)` in `IndexNode.Clone`. For a 10K-row table the root had ~85 cells → 85 allocs saved per cloned page.

#### Alloc improvement (stable across 5 runs)

| Benchmark | Before | After | Δ |
|---|---|---|---|
| Insert_SingleRow | 262 allocs/op | **59 allocs/op** | **−77%** |
| Insert_Batch (100 rows) | ~4170 allocs/op | ~3369 allocs/op | −19% |

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 185.19 µs/op | 71.99 µs/op | 2.6× |
| Insert_SingleRow | 86.10 µs/op | 41.79 µs/op | 2.1× |
| Insert_Batch | 588.82 µs/op | 225.33 µs/op | 2.6× |
| Select_PointScan | 4.28 µs/op | 3.25 µs/op | 1.3× |
| Select_Limit | 8.45 µs/op | 7.66 µs/op | 1.1× |
| Select_FullScan | 6.36 ms/op | 4.93 ms/op | 1.3× |
| Select_CountStar | 31.76 µs/op | 9.41 µs/op | 3.4× |
| Select_IndexRangeScan | 708.43 µs/op | 723.12 µs/op | 1.0× |
| Select_RangeScan | 3.00 ms/op | 855.50 µs/op | 3.5× |
| Txn_NInserts | 322.63 µs/op | 134.91 µs/op | 2.4× |
| Update_ByPK | 58.35 µs/op | 67.15 µs/op | 0.9× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 82.4 KiB | 447 B |
| Insert_SingleRow | 46.5 KiB | 311 B |
| Insert_Batch | 366.2 KiB | 31.0 KiB |
| Select_PointScan | 4.3 KiB | 679 B |
| Select_Limit | 9.8 KiB | 1.7 KiB |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 835.9 KiB | 85.9 KiB |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 209.3 KiB | 15.8 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

---

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
