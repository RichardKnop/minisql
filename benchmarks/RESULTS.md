### 2026-04-23 (latest)

Skip `GlobalPageVersion` for read-only transactions:
- `TransactionalPager.ReadPage`: `GlobalPageVersion` (RLock + map lookup + RUnlock on the transaction manager) was called on every single page read, even for read-only transactions where the version is passed straight to `TrackRead` which is a no-op for read-only. Moved the call inside `if !tx.ReadOnly` so it is skipped entirely for SELECT queries.
- Benefit scales with the number of pages read per query: COUNT(*) scans ~180 leaf pages (15% faster), IndexRangeScan touches hundreds of index + table pages (8% faster, now beats SQLite), FullScan reads ~180 leaf pages (now beats SQLite).

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 81.5 µs/op | 88.3 µs/op | **0.9×** |
| Insert_SingleRow | 70.1 µs/op | 41.5 µs/op | 1.7× |
| Insert_Batch | 590.0 µs/op | 222.0 µs/op | 2.7× |
| Select_PointScan | 4.5 µs/op | 3.4 µs/op | 1.35× |
| Select_Limit | 7.2 µs/op | 7.8 µs/op | **0.93×** |
| Select_FullScan | 5.0 ms/op | 5.1 ms/op | **0.97×** |
| Select_CountStar | 28.6 µs/op | 9.6 µs/op | 3.0× |
| Select_IndexRangeScan | 684 µs/op | 748 µs/op | **0.91×** |
| Select_RangeScan | 2.67 ms/op | 875 µs/op | 3.1× |
| Txn_NInserts | 343.0 µs/op | 137.6 µs/op | 2.5× |
| Update_ByPK | 51.5 µs/op | 62.0 µs/op | **0.8×** |

#### Memory (B/op)

_(not measured this run)_

---

### 2026-04-22 (latest)

`AppendCells` pre-growth + `Clone` extra capacity + `insert.go` fieldPositions map elimination:
- `IndexNode.AppendCells`: replaced per-cell `NewIndexCell`+append loop with a single bulk pre-grow, eliminating O(N) slice reallocations when many cells are moved at once (common during B+ tree splits/merges).
- `IndexNode.Clone`: allocate `Header.Keys+4` capacity instead of exact `Header.Keys`, so `splitChild` median-key append and `AppendCells` don't immediately trigger a reallocation.
- `insert.go`: eliminated `fieldPositions` map when `prepareInsert` has been called (detected by `len(values) == len(t.Columns)`); falls back to map for direct-call paths (tests).

The `AppendCells` and `Clone` changes benefit the delete and update paths heavily — delete rebalancing (`merge`/`fill`/`borrowFromLeft/Right`) moves many cells at once, previously triggering repeated slice reallocations.

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 81.5 µs/op | 88.3 µs/op | **0.9×** |
| Insert_SingleRow | 70.1 µs/op | 41.5 µs/op | 1.7× |
| Insert_Batch | 590.0 µs/op | 222.0 µs/op | 2.7× |
| Select_PointScan | 4.5 µs/op | 3.3 µs/op | 1.4× |
| Select_Limit | 7.5 µs/op | 7.8 µs/op | 1.0× |
| Select_FullScan | 5.1 ms/op | 5.1 ms/op | 1.0× |
| Select_CountStar | 33.7 µs/op | 9.7 µs/op | 3.5× |
| Select_IndexRangeScan | 722.0 µs/op | 736.0 µs/op | 1.0× |
| Select_RangeScan | 2.83 ms/op | 869.0 µs/op | 3.3× |
| Txn_NInserts | 343.0 µs/op | 137.6 µs/op | 2.5× |
| Update_ByPK | 51.5 µs/op | 62.0 µs/op | **0.8×** |

#### Memory (B/op)

_(not measured this run)_

---

### 2026-04-22 00:42 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 181.79 µs/op | 72.17 µs/op | 2.5× |
| Insert_SingleRow | 85.55 µs/op | 53.05 µs/op | 1.6× |
| Insert_Batch | 564.62 µs/op | 217.97 µs/op | 2.6× |
| Select_PointScan | 4.51 µs/op | 3.22 µs/op | 1.4× |
| Select_Limit | 7.33 µs/op | 7.62 µs/op | 1.0× |
| Select_FullScan | 4.95 ms/op | 5.06 ms/op | 1.0× |
| Select_CountStar | 31.86 µs/op | 9.33 µs/op | 3.4× |
| Select_IndexRangeScan | 721.36 µs/op | 733.11 µs/op | 1.0× |
| Select_RangeScan | 2.74 ms/op | 852.27 µs/op | 3.2× |
| Txn_NInserts | 322.02 µs/op | 135.54 µs/op | 2.4× |
| Update_ByPK | 60.76 µs/op | 37.64 µs/op | 1.6× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 82.4 KiB | 447 B |
| Insert_SingleRow | 46.0 KiB | 311 B |
| Insert_Batch | 366.3 KiB | 31.0 KiB |
| Select_PointScan | 4.5 KiB | 679 B |
| Select_Limit | 6.3 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 772.3 KiB | 85.9 KiB |
| Select_RangeScan | 2.0 MiB | 85.9 KiB |
| Txn_NInserts | 209.3 KiB | 15.8 KiB |
| Update_ByPK | 8.9 KiB | 263 B |

### 2026-04-22 00:03 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 201.27 µs/op | 143.77 µs/op | 1.4× |
| Insert_SingleRow | 88.20 µs/op | 45.46 µs/op | 1.9× |
| Insert_Batch | 587.77 µs/op | 222.94 µs/op | 2.6× |
| Select_PointScan | 4.44 µs/op | 3.31 µs/op | 1.3× |
| Select_Limit | 7.39 µs/op | 7.94 µs/op | 0.9× |
| Select_FullScan | 5.04 ms/op | 5.55 ms/op | 0.9× |
| Select_CountStar | 32.68 µs/op | 9.66 µs/op | 3.4× |
| Select_IndexRangeScan | 728.41 µs/op | 747.02 µs/op | 1.0× |
| Select_RangeScan | 2.83 ms/op | 928.63 µs/op | 3.0× |
| Txn_NInserts | 341.37 µs/op | 140.91 µs/op | 2.4× |
| Update_ByPK | 61.60 µs/op | 40.56 µs/op | 1.5× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 82.4 KiB | 447 B |
| Insert_SingleRow | 46.8 KiB | 311 B |
| Insert_Batch | 366.1 KiB | 31.0 KiB |
| Select_PointScan | 4.5 KiB | 679 B |
| Select_Limit | 6.3 KiB | 1.7 KiB |
| Select_FullScan | 5.7 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 772.4 KiB | 85.9 KiB |
| Select_RangeScan | 2.0 MiB | 85.9 KiB |
| Txn_NInserts | 209.1 KiB | 15.8 KiB |
| Update_ByPK | 9.0 KiB | 263 B |

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
