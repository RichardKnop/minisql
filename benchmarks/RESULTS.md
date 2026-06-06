# Benchmark Results

## Current Baseline

**Platform:** Apple M1 Max · darwin/arm64 · Go 1.26.3  
**Branch:** `codex/profile-delete-overflow-fields`  
**Command:** `make bench BENCH_COUNT=1` followed by `make bench-report`  
**GOMAXPROCS:** 10  

SQLite comparisons use the `sqlite` driver compiled into the same test binary. MiniSQL benchmarks run against fresh temp-file databases per sub-benchmark. Times are wall-clock (`ns/op`); memory figures are heap allocations reported by the Go runtime.

This file was refreshed from scratch after the latest DML allocation work, including transaction write-set inlining, auto-commit transaction reuse, and direct prepared DML argument binding.

---

## 2026-06-06 — Current Baseline

#### Timing

| Benchmark | minisql | dims128 | dims3 | dims768 | minisql_indexed | minisql_sequential | n1000 | n10000 | sqlite | sqlite_json_expr_index | sqlite_json_scan | top1 | top10 | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 979.73 µs/op | — | — | — | — | — | — | — | 2.10 ms/op | — | — | — | — | 0.5× |
| Having_Filter | 741.87 µs/op | — | — | — | — | — | — | — | 1.95 ms/op | — | — | — | — | 0.4× |
| Distinct_HighCardinality | 2.97 ms/op | — | — | — | — | — | — | — | 5.86 ms/op | — | — | — | — | 0.5× |
| Delete_ByPK | 16.80 µs/op | — | — | — | — | — | — | — | 78.92 µs/op | — | — | — | — | 0.2× |
| ForeignKey_Insert | 11.32 µs/op | — | — | — | — | — | — | — | 43.07 µs/op | — | — | — | — | 0.3× |
| ForeignKey_DeleteCascade | 23.35 µs/op | — | — | — | — | — | — | — | 50.96 µs/op | — | — | — | — | 0.5× |
| Insert_SingleRow | 10.88 µs/op | — | — | — | — | — | — | — | 43.95 µs/op | — | — | — | — | 0.2× |
| Insert_Batch | 369.66 µs/op | — | — | — | — | — | — | — | 253.47 µs/op | — | — | — | — | 1.5× |
| Insert_PreparedBatch | 396.14 µs/op | — | — | — | — | — | — | — | 234.67 µs/op | — | — | — | — | 1.7× |
| Insert_MultiValues | 198.06 µs/op | — | — | — | — | — | — | — | 177.99 µs/op | — | — | — | — | 1.1× |
| FullText_BuildIndex | 2.95 ms/op | — | — | — | — | — | — | — | 2.01 ms/op | — | — | — | — | 1.5× |
| FullText_Insert_WithIndex | 36.43 µs/op | — | — | — | — | — | — | — | 87.09 µs/op | — | — | — | — | 0.4× |
| FullText_Search_SingleTerm/rare | 4.57 µs/op | — | — | — | — | — | — | — | 6.54 µs/op | — | — | — | — | 0.7× |
| FullText_Search_SingleTerm/medium | 4.14 µs/op | — | — | — | — | — | — | — | 7.61 µs/op | — | — | — | — | 0.5× |
| FullText_Search_SingleTerm/common | 4.24 µs/op | — | — | — | — | — | — | — | 61.16 µs/op | — | — | — | — | 0.1× |
| FullText_Search_MultiTermAND | 14.93 µs/op | — | — | — | — | — | — | — | 33.26 µs/op | — | — | — | — | 0.4× |
| FullText_Search_Phrase | 15.61 µs/op | — | — | — | — | — | — | — | 24.50 µs/op | — | — | — | — | 0.6× |
| FullText_Search_AfterDeletes | 81.57 µs/op | — | — | — | — | — | — | — | — | — | — | — | — | — |
| FullText_Update_WithIndex | 35.41 µs/op | — | — | — | — | — | — | — | 97.40 µs/op | — | — | — | — | 0.4× |
| FullText_Delete_WithIndex | 48.01 µs/op | — | — | — | — | — | — | — | 138.41 µs/op | — | — | — | — | 0.3× |
| JSONInverted_BuildIndex | — | — | — | — | 1.93 ms/op | — | — | — | — | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | — | — | — | 48.91 µs/op | — | — | — | — | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | — | — | — | 16.39 µs/op | 1.90 ms/op | — | — | — | 26.02 µs/op | 712.05 µs/op | — | — | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | — | — | — | 28.70 µs/op | 1.98 ms/op | — | — | — | 121.03 µs/op | 730.76 µs/op | — | — | — |
| JSONInverted_Contains_AfterDeletes | — | — | — | — | 59.19 µs/op | — | — | — | — | — | — | — | — | — |
| JSONInverted_Update_WithIndex | — | — | — | — | 5.97 µs/op | — | — | — | — | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | — | — | — | 112.67 µs/op | — | — | — | — | — | — | — | — | — |
| Join_Inner_SmallLarge | 5.68 ms/op | — | — | — | — | — | — | — | 4.66 ms/op | — | — | — | — | 1.2× |
| Join_Inner_LowSelectivity | 108.11 µs/op | — | — | — | — | — | — | — | 735.45 µs/op | — | — | — | — | 0.1× |
| Join_Left_UnmatchedRows | 3.64 ms/op | — | — | — | — | — | — | — | 4.13 ms/op | — | — | — | — | 0.9× |
| Vacuum_Small | 18.78 ms/op | — | — | — | — | — | — | — | 256.38 µs/op | — | — | — | — | 73.2× |
| WAL_Checkpoint | 194.44 µs/op | — | — | — | — | — | — | — | 104.85 µs/op | — | — | — | — | 1.9× |
| Explain | 5.46 µs/op | — | — | — | — | — | — | — | 1.21 µs/op | — | — | — | — | 4.5× |
| Select_PointScan | 4.80 µs/op | — | — | — | — | — | — | — | 3.47 µs/op | — | — | — | — | 1.4× |
| Select_Limit | 7.13 µs/op | — | — | — | — | — | — | — | 7.95 µs/op | — | — | — | — | 0.9× |
| Select_FullScan | 3.65 ms/op | — | — | — | — | — | — | — | 5.12 ms/op | — | — | — | — | 0.7× |
| Select_CountStar | 6.30 µs/op | — | — | — | — | — | — | — | 9.65 µs/op | — | — | — | — | 0.7× |
| Select_IndexRangeScan | 670.66 µs/op | — | — | — | — | — | — | — | 752.23 µs/op | — | — | — | — | 0.9× |
| Select_SecondaryIndex_LowSelectivity | 1.76 ms/op | — | — | — | — | — | — | — | 2.67 ms/op | — | — | — | — | 0.7× |
| Select_SecondaryIndex_LowSelectivityLimit | 7.90 µs/op | — | — | — | — | — | — | — | 8.32 µs/op | — | — | — | — | 1.0× |
| Select_RangeScan | 1.49 ms/op | — | — | — | — | — | — | — | 879.16 µs/op | — | — | — | — | 1.7× |
| CTE_Materialise | 793.64 µs/op | — | — | — | — | — | — | — | 437.25 µs/op | — | — | — | — | 1.8× |
| Subquery_InList | 4.46 ms/op | — | — | — | — | — | — | — | 3.59 ms/op | — | — | — | — | 1.2× |
| OnConflict_DoUpdate | 7.42 µs/op | — | — | — | — | — | — | — | 36.39 µs/op | — | — | — | — | 0.2× |
| Update_ByPK | 8.26 µs/op | — | — | — | — | — | — | — | 37.67 µs/op | — | — | — | — | 0.2× |
| HNSW_BuildIndex/dims3 | — | — | — | — | — | — | 662.52 ms/op | 9.04 s/op | — | — | — | — | — | — |
| HNSW_BuildIndex/dims128 | — | — | — | — | — | — | 777.36 ms/op | 27.14 s/op | — | — | — | — | — | — |
| HNSW_BuildIndex/dims768 | — | — | — | — | — | — | 1.23 s/op | 28.44 s/op | — | — | — | — | — | — |
| HNSW_ANNSearch/dims3/n1000 | — | — | — | — | — | — | — | — | — | — | — | 36.53 µs/op | 43.31 µs/op | — |
| HNSW_ANNSearch/dims3/n10000 | — | — | — | — | — | — | — | — | — | — | — | 46.20 µs/op | 52.13 µs/op | — |
| HNSW_ANNSearch/dims128/n1000 | — | — | — | — | — | — | — | — | — | — | — | 187.28 µs/op | 196.64 µs/op | — |
| HNSW_ANNSearch/dims128/n10000 | — | — | — | — | — | — | — | — | — | — | — | 362.34 µs/op | 364.70 µs/op | — |
| HNSW_ANNSearch/dims768/n1000 | — | — | — | — | — | — | — | — | — | — | — | 743.72 µs/op | 751.65 µs/op | — |
| HNSW_ANNSearch/dims768/n10000 | — | — | — | — | — | — | — | — | — | — | — | 1.49 ms/op | 1.50 ms/op | — |
| HNSW_SeqScan/dims3/n1000 | — | — | — | — | — | — | — | — | — | — | — | 665.84 µs/op | — | — |
| HNSW_SeqScan/dims128/n1000 | — | — | — | — | — | — | — | — | — | — | — | 8.35 ms/op | — | — |
| HNSW_SeqScan/dims768/n1000 | — | — | — | — | — | — | — | — | — | — | — | 46.36 ms/op | — | — |
| HNSW_Insert_WithIndex | — | 3.54 ms/op | 1.39 ms/op | 12.62 ms/op | — | — | — | — | — | — | — | — | — | — |
| HNSW_Insert_NoIndex | — | 21.60 µs/op | 21.38 µs/op | 22.62 µs/op | — | — | — | — | — | — | — | — | — | — |

#### Memory (B/op)

| Benchmark | minisql | dims128 | dims3 | dims768 | minisql_indexed | minisql_sequential | n1000 | n10000 | sqlite | sqlite_json_expr_index | sqlite_json_scan | top1 | top10 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| GroupBy_Aggregate | 35.5 KiB | — | — | — | — | — | — | — | 3.5 KiB | — | — | — | — |
| Having_Filter | 27.4 KiB | — | — | — | — | — | — | — | 1.9 KiB | — | — | — | — |
| Distinct_HighCardinality | 1.7 MiB | — | — | — | — | — | — | — | 586.3 KiB | — | — | — | — |
| Delete_ByPK | 3.3 KiB | — | — | — | — | — | — | — | 447 B | — | — | — | — |
| ForeignKey_Insert | 1.9 KiB | — | — | — | — | — | — | — | 192 B | — | — | — | — |
| ForeignKey_DeleteCascade | 7.2 KiB | — | — | — | — | — | — | — | 128 B | — | — | — | — |
| Insert_SingleRow | 2.1 KiB | — | — | — | — | — | — | — | 311 B | — | — | — | — |
| Insert_Batch | 133.6 KiB | — | — | — | — | — | — | — | 31.0 KiB | — | — | — | — |
| Insert_PreparedBatch | 132.5 KiB | — | — | — | — | — | — | — | 31.0 KiB | — | — | — | — |
| Insert_MultiValues | 109.6 KiB | — | — | — | — | — | — | — | 25.2 KiB | — | — | — | — |
| FullText_BuildIndex | 1.4 MiB | — | — | — | — | — | — | — | 392 B | — | — | — | — |
| FullText_Insert_WithIndex | 12.8 KiB | — | — | — | — | — | — | — | 271 B | — | — | — | — |
| FullText_Search_SingleTerm/rare | 2.4 KiB | — | — | — | — | — | — | — | 408 B | — | — | — | — |
| FullText_Search_SingleTerm/medium | 2.4 KiB | — | — | — | — | — | — | — | 408 B | — | — | — | — |
| FullText_Search_SingleTerm/common | 2.4 KiB | — | — | — | — | — | — | — | 424 B | — | — | — | — |
| FullText_Search_MultiTermAND | 11.4 KiB | — | — | — | — | — | — | — | 408 B | — | — | — | — |
| FullText_Search_Phrase | 26.3 KiB | — | — | — | — | — | — | — | 416 B | — | — | — | — |
| FullText_Search_AfterDeletes | 11.1 KiB | — | — | — | — | — | — | — | — | — | — | — | — |
| FullText_Update_WithIndex | 18.0 KiB | — | — | — | — | — | — | — | 291 B | — | — | — | — |
| FullText_Delete_WithIndex | 17.4 KiB | — | — | — | — | — | — | — | 135 B | — | — | — | — |
| JSONInverted_BuildIndex | — | — | — | — | 1.3 MiB | — | — | — | — | — | — | — | — |
| JSONInverted_Insert_WithIndex | — | — | — | — | 59.6 KiB | — | — | — | — | — | — | — | — |
| JSONInverted_Contains_KeyValue/key_value | — | — | — | — | 7.8 KiB | 1.9 MiB | — | — | — | 424 B | 424 B | — | — |
| JSONInverted_Contains_ObjectSubset/object_subset | — | — | — | — | 8.8 KiB | 1.9 MiB | — | — | — | 424 B | 424 B | — | — |
| JSONInverted_Contains_AfterDeletes | — | — | — | — | 19.2 KiB | — | — | — | — | — | — | — | — |
| JSONInverted_Update_WithIndex | — | — | — | — | 4.2 KiB | — | — | — | — | — | — | — | — |
| JSONInverted_Delete_WithIndex | — | — | — | — | 26.6 KiB | — | — | — | — | — | — | — | — |
| Join_Inner_SmallLarge | 1.0 MiB | — | — | — | — | — | — | — | 1.1 MiB | — | — | — | — |
| Join_Inner_LowSelectivity | 21.1 KiB | — | — | — | — | — | — | — | 11.3 KiB | — | — | — | — |
| Join_Left_UnmatchedRows | 869.4 KiB | — | — | — | — | — | — | — | 708.2 KiB | — | — | — | — |
| Vacuum_Small | 1.3 MiB | — | — | — | — | — | — | — | 89 B | — | — | — | — |
| WAL_Checkpoint | 3.3 KiB | — | — | — | — | — | — | — | 440 B | — | — | — | — |
| Explain | 6.0 KiB | — | — | — | — | — | — | — | 680 B | — | — | — | — |
| Select_PointScan | 3.7 KiB | — | — | — | — | — | — | — | 679 B | — | — | — | — |
| Select_Limit | 3.8 KiB | — | — | — | — | — | — | — | 1.7 KiB | — | — | — | — |
| Select_FullScan | 1.2 MiB | — | — | — | — | — | — | — | 1.3 MiB | — | — | — | — |
| Select_CountStar | 2.6 KiB | — | — | — | — | — | — | — | 400 B | — | — | — | — |
| Select_IndexRangeScan | 83.8 KiB | — | — | — | — | — | — | — | 85.9 KiB | — | — | — | — |
| Select_SecondaryIndex_LowSelectivity | 314.9 KiB | — | — | — | — | — | — | — | 313.0 KiB | — | — | — | — |
| Select_SecondaryIndex_LowSelectivityLimit | 4.1 KiB | — | — | — | — | — | — | — | 1.1 KiB | — | — | — | — |
| Select_RangeScan | 80.8 KiB | — | — | — | — | — | — | — | 85.9 KiB | — | — | — | — |
| CTE_Materialise | 6.6 KiB | — | — | — | — | — | — | — | 400 B | — | — | — | — |
| Subquery_InList | 853.3 KiB | — | — | — | — | — | — | — | 234.7 KiB | — | — | — | — |
| OnConflict_DoUpdate | 1.6 KiB | — | — | — | — | — | — | — | 260 B | — | — | — | — |
| Update_ByPK | 3.8 KiB | — | — | — | — | — | — | — | 263 B | — | — | — | — |
| HNSW_BuildIndex/dims3 | — | — | — | — | — | — | 194.0 MiB | 2279.6 MiB | — | — | — | — | — |
| HNSW_BuildIndex/dims128 | — | — | — | — | — | — | 223.7 MiB | 3529.3 MiB | — | — | — | — | — |
| HNSW_BuildIndex/dims768 | — | — | — | — | — | — | 243.9 MiB | 3755.6 MiB | — | — | — | — | — |
| HNSW_ANNSearch/dims3/n1000 | — | — | — | — | — | — | — | — | — | — | — | 17.3 KiB | 20.9 KiB |
| HNSW_ANNSearch/dims3/n10000 | — | — | — | — | — | — | — | — | — | — | — | 26.4 KiB | 30.1 KiB |
| HNSW_ANNSearch/dims128/n1000 | — | — | — | — | — | — | — | — | — | — | — | 49.1 KiB | 61.6 KiB |
| HNSW_ANNSearch/dims128/n10000 | — | — | — | — | — | — | — | — | — | — | — | 87.0 KiB | 99.5 KiB |
| HNSW_ANNSearch/dims768/n1000 | — | — | — | — | — | — | — | — | — | — | — | 54.6 KiB | 112.1 KiB |
| HNSW_ANNSearch/dims768/n10000 | — | — | — | — | — | — | — | — | — | — | — | 92.6 KiB | 150.1 KiB |
| HNSW_SeqScan/dims3/n1000 | — | — | — | — | — | — | — | — | — | — | — | 664.7 KiB | — |
| HNSW_SeqScan/dims128/n1000 | — | — | — | — | — | — | — | — | — | — | — | 5.9 MiB | — |
| HNSW_SeqScan/dims768/n1000 | — | — | — | — | — | — | — | — | — | — | — | 31.4 MiB | — |
| HNSW_Insert_WithIndex | — | 516.2 KiB | 449.6 KiB | 614.5 KiB | — | — | — | — | — | — | — | — | — |
| HNSW_Insert_NoIndex | — | 7.4 KiB | 6.9 KiB | 9.8 KiB | — | — | — | — | — | — | — | — | — |
