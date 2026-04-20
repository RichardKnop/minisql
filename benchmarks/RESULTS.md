### 2026-04-20 10:00 UTC

Remove `Row.columnCache` entirely — replace with O(n) linear scan in `GetColumn`/`GetValue`/`SetValue`:
- `Row.columnCache map[string]int` field removed from the `Row` struct entirely
- `NewRow`, `NewRowWithValues`, `Unmarshal`, `Clone`, `Table.newRow`: no longer allocate or copy the map
- `GetColumn`, `GetValue`, `SetValue`: now do O(n) linear scan over `r.Columns`; for typical tables (≤16 columns) this is faster than a map lookup due to zero allocation
- `buildColumnCache` helper removed; `collectRows` and `checkRowsWithPrimaryKey` test helpers simplified
- Row struct shrinks from 5 fields to 3 (Key + Columns + Values)

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 106.28 µs/op | 77.15 µs/op | 1.4× |
| Insert_SingleRow | 80.96 µs/op | 41.08 µs/op | 2.0× |
| Insert_Batch | 770.46 µs/op | 222.35 µs/op | 3.5× |
| Select_PointScan | 4.41 µs/op | 3.29 µs/op | **1.34×** |
| Select_FullScan | 6.47 ms/op | 5.01 ms/op | **1.29×** |
| Select_CountStar | 32.53 µs/op | 9.85 µs/op | 3.3× |
| Select_IndexRangeScan | 762 µs/op | 818 µs/op | **0.93×** ✓ faster than SQLite |
| Select_RangeScan (no index) | 3.16 ms/op | 0.91 ms/op | 3.5× |
| Txn_NInserts | 454.65 µs/op | 145.28 µs/op | 3.1× |
| Update_ByPK | 101.34 µs/op | 77.71 µs/op | 1.3× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 83.1 KiB | 447 B |
| Insert_SingleRow | 68.6 KiB | 312 B |
| Insert_Batch | 778 KiB | 31.1 KiB |
| Select_PointScan | 4.2 KiB | 679 B |
| Select_FullScan | 9.8 MiB | 1.3 MiB |
| Select_CountStar | 5.9 KiB | 400 B |
| Select_IndexRangeScan | 854 KiB | 88 KiB |
| Select_RangeScan (no index) | 2.1 MiB | 88 KiB |
| Txn_NInserts | 424 KiB | 15.9 KiB |
| Update_ByPK | 15.3 KiB | 263 B |

---

### 2026-04-20 09:30 UTC

`Row.OnlyFields` columnCache elimination + BenchmarkSelect_IndexRangeScan (new):
- `Row.OnlyFields`: stop allocating `columnCache` map for projected rows — all downstream consumers access values positionally; saves ~256 B per projected row in the hot path (~2.6 GB per benchmark run)
- `Row.GetColumn` / `GetValue`: added O(n) linear scan fallback for nil-cache rows so correctness is maintained
- `BenchmarkSelect_IndexRangeScan`: new benchmark that creates a secondary index on `age` and exercises the index range scan planner path; shows minisql at ~1.04× parity with SQLite

#### Timing (intermediate — superseded by 10:00 UTC entry above)

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Select_IndexRangeScan | 828 µs/op | 793 µs/op | 1.04× |
| Select_FullScan | 7.32 ms/op | 5.41 ms/op | 1.35× |
| Select_PointScan | 4.89 µs/op | 3.48 µs/op | 1.40× |

---

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

