### 2026-04-21 13:00 UTC

Step 5 — `AppendValues` O(C²) → O(C) per row using pre-built field-position map:
- `AppendValues` performed a nested loop: for each column it linearly scanned `stmt.Fields` to find the matching field, giving O(C²) per row and multiple `append` growth allocations for the Values slice.
- Replaced with: a `fieldPositions map[string]int` (field-name → index in the values slice) built **once** per `Insert()` call before the row loop. Per row, a single `make([]OptionalValue, C)` + O(C) map lookups fills `rowValues` in column order.
- `NewRow` + `AppendValues` calls replaced with `NewRowWithValues(t.Columns, rowValues)`.
- For DO UPDATE: `stmt.Fields` has update-clause fields appended after insert fields; the map only covers the first `len(values)` fields to avoid out-of-range accesses.
- Primary win: **allocation count per row** — `append` growing from nil allocates ~3× for a 4-column row; `make([]OptionalValue, N)` is always 1 allocation.

#### Alloc reductions (stable across 5 runs)

| Benchmark | Phase 3 allocs | Step 5 allocs | Δ per row |
|---|---|---|---|
| Insert_Batch (100 rows) | 4577 | 4418 | −1.6/row |
| Txn_NInserts (50 rows) | 2487 | 2411 | −1.5/row |

Note: timing numbers are within M1 benchmark noise (±15%). The allocation reduction directly reduces GC pressure for high-throughput insert workloads.

---

### 2026-04-21 12:00 UTC

Step 4 — Lazy streaming early termination for SELECT … LIMIT:
- For SELECT queries with a LIMIT that have no ORDER BY, no GROUP BY, no aggregates, and no JOINs, `selectStreamingDirect` projects each row inline during `plan.Execute` and stops the scan as soon as the LIMIT is satisfied (via `errLimitReached` sentinel propagated through all non-JOIN scan types).
- The projected slice is pre-sized to exactly the LIMIT so there are zero reallocs.
- Queries **without** a LIMIT are unaffected — they continue using the collect-then-project path, which is better for those cases because it can pre-size the slice to the exact row count.
- JOIN queries are also excluded (plan.Execute uses an internal goroutine; returning an error from the callback mid-stream would leak it).
- New `BenchmarkSelect_Limit` benchmark added to measure LIMIT 10 on a 10K-row table.

Key result: `SELECT … LIMIT 10` on a 10K-row table drops from ~6.5 ms (full scan) to **~110 µs** — approximately **59× faster**.

#### New Benchmark: Select_Limit (LIMIT 10, table has 10K rows)

| | minisql | sqlite | ratio |
|---|---|---|---|
| Select_Limit | 110.27 µs/op | 13.36 µs/op | 8.3× |
| Memory | 32.7 KiB | 1.67 KiB | |
| Allocs | 2333 | 103 | |

#### Other benchmarks: no change from Phase 3 baseline (non-LIMIT paths unchanged)

---

### 2026-04-21 11:00 UTC

Phase 3 — WAL page buffer pooling via `sync.Pool`:
- `pageDataPool sync.Pool` at package level in `transaction_manager.go` pools `[]byte` slices of `PageSize` (4 KB)
- `serializeWritesForWAL`: replaces `make([]byte, PageSize)` per dirty page with `pageDataPool.Get()`
- `serializePage0ForWAL`: uses pool for temporary `pageBuf` (copied into the combined frame, then returned)
- `WALIndex.Update` now returns old `[]byte` (if any); `commitWithWAL` recycles displaced buffers back to pool
- Net effect: reduces GC-visible heap allocations for dirty-page serialization on every commit

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 214.83 µs/op | 174.67 µs/op | 1.2× |
| Insert_SingleRow | 106.35 µs/op | 47.81 µs/op | 2.2× |
| Insert_Batch | 612.52 µs/op | 221.60 µs/op | 2.8× |
| Select_PointScan | 4.43 µs/op | 3.32 µs/op | **1.33×** |
| Select_FullScan | 6.56 ms/op | 5.09 ms/op | **1.29×** |
| Select_CountStar | 32.64 µs/op | 9.57 µs/op | 3.4× |
| Select_IndexRangeScan | 751 µs/op | 756 µs/op | **0.99×** ✓ parity with SQLite |
| Select_RangeScan (no index) | 3.07 ms/op | 867 µs/op | 3.5× |
| Txn_NInserts | 356.71 µs/op | 138.21 µs/op | 2.6× |
| Update_ByPK | 57.47 µs/op | 38.87 µs/op | **1.48×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 80.9 KiB | 437 B |
| Insert_SingleRow | 42.6 KiB | 304 B |
| Insert_Batch | 406 KiB | 31.0 KiB |
| Select_PointScan | 4.3 KiB | 664 B |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 391 B |
| Select_IndexRangeScan | 836 KiB | 85.9 KiB |
| Select_RangeScan (no index) | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 228 KiB | 15.8 KiB |
| Update_ByPK | 8.9 KiB | 257 B |

---

### 2026-04-21 09:30 UTC

Replace `Sugar().With(...).Debug(...)` with zero-allocation typed zap fields in all hot-path logging:
- `insert.go`: `Sugar().With(...).Debug("inserting row")` → `logger.Debug("inserting row", zap.Int(...))`
- `cursor.go`: `LeafNodeSplitInsert` split log
- `table.go`: `createNewRoot` and `internalNodeSplitInsert` logs
- `table_primary_key.go`: primary key insert/autoincrement logs (3 call sites)
- `table_secondary_index.go`, `table_unique_index.go`: index insert logs
- `delete.go`, `update.go`, `select.go`: query-plan and row-count logs
- zap's `Sugar().With()` allocates a field buffer + clones the logger even when the debug level is disabled; the typed API (`zap.Int`, `zap.String`, `zap.Any`) stack-allocates `zap.Field` structs and short-circuits immediately on disabled levels

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 165.68 µs/op | 60.69 µs/op | 2.7× |
| Insert_SingleRow | 101.25 µs/op | 48.99 µs/op | 2.1× |
| Insert_Batch | 722.73 µs/op | 281.12 µs/op | 2.6× |
| Select_PointScan | 5.70 µs/op | 4.32 µs/op | **1.32×** |
| Select_FullScan | 9.83 ms/op | 6.23 ms/op | **1.58×** |
| Select_CountStar | 37.47 µs/op | 10.91 µs/op | 3.4× |
| Select_IndexRangeScan | 1006 µs/op | 908 µs/op | **1.11×** |
| Select_RangeScan (no index) | 3.98 ms/op | 1.00 ms/op | 3.97× |
| Txn_NInserts | 375.35 µs/op | 154.69 µs/op | 2.4× |
| Update_ByPK | 59.04 µs/op | 44.99 µs/op | **1.31×** |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 84.4 KiB | 447 B |
| Insert_SingleRow | 61.8 KiB | 312 B |
| Insert_Batch | 438 KiB | 31.1 KiB |
| Select_PointScan | 4.3 KiB | 679 B |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 835 KiB | 85.9 KiB |
| Select_RangeScan (no index) | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 253 KiB | 15.9 KiB |
| Update_ByPK | 12.7 KiB | 263 B |

---

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

### 2026-04-21 17:39 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 311.04 µs/op | 92.14 µs/op | 3.4× |
| Insert_SingleRow | 117.64 µs/op | 48.31 µs/op | 2.4× |
| Insert_Batch | 750.45 µs/op | 255.71 µs/op | 2.9× |
| Select_PointScan | 6.46 µs/op | 4.15 µs/op | 1.6× |
| Select_Limit | 125.02 µs/op | 13.81 µs/op | 9.1× |
| Select_FullScan | 9.14 ms/op | 6.63 ms/op | 1.4× |
| Select_CountStar | 34.79 µs/op | 10.86 µs/op | 3.2× |
| Select_IndexRangeScan | 1.02 ms/op | 877.43 µs/op | 1.2× |
| Select_RangeScan | 3.88 ms/op | 1.47 ms/op | 2.6× |
| Txn_NInserts | 402.56 µs/op | 157.73 µs/op | 2.6× |
| Update_ByPK | 60.49 µs/op | 42.91 µs/op | 1.4× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 81.0 KiB | 447 B |
| Insert_SingleRow | 42.4 KiB | 311 B |
| Insert_Batch | 398.3 KiB | 31.0 KiB |
| Select_PointScan | 4.4 KiB | 679 B |
| Select_Limit | 31.9 KiB | 1.7 KiB |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 836.3 KiB | 85.9 KiB |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 225.4 KiB | 15.8 KiB |
| Update_ByPK | 8.9 KiB | 263 B |


### 2026-04-21 17:39 UTC

#### Timing

| Benchmark | minisql | sqlite | ratio |
|---|---|---|---|
| Delete_ByPK | 311.04 µs/op | 92.14 µs/op | 3.4× |
| Insert_SingleRow | 117.64 µs/op | 48.31 µs/op | 2.4× |
| Insert_Batch | 750.45 µs/op | 255.71 µs/op | 2.9× |
| Select_PointScan | 6.46 µs/op | 4.15 µs/op | 1.6× |
| Select_Limit | 125.02 µs/op | 13.81 µs/op | 9.1× |
| Select_FullScan | 9.14 ms/op | 6.63 ms/op | 1.4× |
| Select_CountStar | 34.79 µs/op | 10.86 µs/op | 3.2× |
| Select_IndexRangeScan | 1.02 ms/op | 877.43 µs/op | 1.2× |
| Select_RangeScan | 3.88 ms/op | 1.47 ms/op | 2.6× |
| Txn_NInserts | 402.56 µs/op | 157.73 µs/op | 2.6× |
| Update_ByPK | 60.49 µs/op | 42.91 µs/op | 1.4× |

#### Memory (B/op)

| Benchmark | minisql | sqlite |
|---|---|---|
| Delete_ByPK | 81.0 KiB | 447 B |
| Insert_SingleRow | 42.4 KiB | 311 B |
| Insert_Batch | 398.3 KiB | 31.0 KiB |
| Select_PointScan | 4.4 KiB | 679 B |
| Select_Limit | 31.9 KiB | 1.7 KiB |
| Select_FullScan | 9.6 MiB | 1.3 MiB |
| Select_CountStar | 5.8 KiB | 400 B |
| Select_IndexRangeScan | 836.3 KiB | 85.9 KiB |
| Select_RangeScan | 2.1 MiB | 85.9 KiB |
| Txn_NInserts | 225.4 KiB | 15.8 KiB |
| Update_ByPK | 8.9 KiB | 263 B |


