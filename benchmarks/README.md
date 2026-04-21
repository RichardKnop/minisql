# Benchmarking Suite

The `benchmarks/` directory contains a formal suite that compares MiniSQL against SQLite (via [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), a pure-Go driver). Benchmarks are guarded by a `//go:build bench` tag so they never run as part of `make test`.

## Running benchmarks

```sh
# Run the full suite once and save raw output to benchmarks/raw.txt
make bench

# Run each benchmark 5 times for better statistical confidence
make bench BENCH_COUNT=5

# Run only a specific benchmark group
make bench BENCH=BenchmarkInsert
make bench BENCH=BenchmarkSelect
```

## Benchmark groups

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkInsert_SingleRow` | One INSERT per transaction (prepared statement) |
| `BenchmarkInsert_Batch` | 100 INSERTs inside a single explicit transaction |
| `BenchmarkSelect_PointScan` | Single-row lookup by primary key |
| `BenchmarkSelect_FullScan` | Sequential full-table scan (10 000 rows, no WHERE) |
| `BenchmarkSelect_CountStar` | `COUNT(*)` over 10 000 rows |
| `BenchmarkSelect_IndexRangeScan` | Range query on a column with secondary index |
| `BenchmarkSelect_RangeScan` | Range query on a non-indexed column |
| `BenchmarkUpdate_ByPK` | UPDATE a single row by primary key |
| `BenchmarkDelete_ByPK` | DELETE a single row by primary key |
| `BenchmarkTxn_NInserts` | Commit overhead: 50 INSERTs per explicit transaction |

Both drivers are configured for fair durability comparison: MiniSQL uses its default WAL mode; SQLite is opened with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=FULL`.

## Generating charts

Charts require `benchmarks/raw.txt` to exist (produced by `make bench`).

```sh
# Generate PNG bar charts to benchmarks/charts/
make bench-chart
```

Charts compare MiniSQL (blue) against SQLite (red) for each benchmark and are written to `benchmarks/charts/`.

## Statistical comparison with benchstat

For a statistically rigorous comparison install [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
go install golang.org/x/perf/cmd/benchstat@latest
```

Then run multiple iterations and compare:

```sh
make bench BENCH_COUNT=5
benchstat benchmarks/raw.txt
```

Or append results to `benchmarks/RESULTS.md`:

```sh
make bench-report
```

# Profiling

Ad-hoc profiling commands for investigating specific workloads:

```sh
# CPU profile concurrent workload
go test -cpuprofile=cpu.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -top cpu.prof | head -30

# When CPU profile is dominated by runtime scheduling overhead, look at specific database operations
go tool pprof -cum -top cpu_reads.prof | grep "minisql" | head -20

# Memory profile
go test -memprofile=mem.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -alloc_space -top mem.prof | head -30

# Mutex contention
go test -mutexprofile=mutex.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -top mutex.prof | head -25
```