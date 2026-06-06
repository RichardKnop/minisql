# Benchmarking Suite

The `benchmarks/` directory contains a formal suite that compares MiniSQL against SQLite (via [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), a pure-Go driver). Benchmarks are guarded by a `//go:build bench` tag so they never run as part of `make test`.

## Running benchmarks

```sh
# Run the full suite once and save raw output to benchmarks/raw.txt
make bench

# Run each benchmark 5 times for better statistical confidence
make bench BENCH_COUNT=5

# Run setup-heavy benchmarks exactly once per iteration
make bench BENCH=BenchmarkFullText_BuildIndex BENCH_TIME=1x BENCH_COUNT=5

# Run only a specific benchmark group
make bench BENCH=BenchmarkInsert
make bench BENCH=BenchmarkSelect
make bench BENCH=BenchmarkHNSW

# Run only the inverted-index benchmark families
make bench-inverted BENCH_COUNT=5
make bench-inverted-build BENCH_COUNT=5
make bench-inverted-runtime BENCH_COUNT=5
make bench-fulltext
make bench-json
```

Raw output is written to `benchmarks/raw.txt`. Human-readable baseline summaries live in `benchmarks/RESULTS.md`.

## Benchmark families

| Family | What it covers |
|---|---|
| `BenchmarkSelect_*` | Point lookups, full scans, count, range scans, and secondary-index reads |
| `BenchmarkInsert_*` | Single-row inserts, batches, prepared batches, and multi-value inserts |
| `BenchmarkUpdate_*` / `BenchmarkDelete_*` | Primary-key DML paths |
| `BenchmarkTxn_*` | Explicit transaction overhead |
| `BenchmarkJoin_*` | Join execution and planner choices |
| `BenchmarkFullText_*` | Full-text index build, query, and DML maintenance |
| `BenchmarkJSONInverted_*` | JSON inverted-index build, containment lookup, and DML maintenance |
| `BenchmarkHNSW_*` | VECTOR/HNSW build, ANN search, sequential vector scan, and insert overhead |
| `BenchmarkVacuum_*` / `BenchmarkWAL_*` | Maintenance and durability paths |

Both drivers are configured for fair durability comparison: MiniSQL uses its default WAL mode; SQLite is opened with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=FULL`.

Full-text benchmarks compare MiniSQL against SQLite FTS5 when the linked SQLite
driver supports `CREATE VIRTUAL TABLE ... USING fts5`. JSON benchmarks always
include MiniSQL indexed and sequential `JSON_CONTAINS` variants. SQLite JSON
benchmarks are contextual only: `sqlite_json_scan` uses JSON/path predicates and
`sqlite_json_expr_index` uses a fixed-path expression index, which is not
equivalent to MiniSQL's JSON containment inverted index.

HNSW/VECTOR benchmarks are MiniSQL-only because SQLite has no directly comparable
HNSW extension in this benchmark binary. Sequential vector scans are included as
a local baseline for ANN search speedups.

`make bench-inverted` intentionally splits build and steady-state benchmarks.
Index builds use `BENCH_INVERTED_BUILD_TIME=1x` by default because they rebuild
the full fixture on every iteration. Runtime query and maintenance benchmarks
use `BENCH_INVERTED_RUNTIME_TIME=10x` by default so repeated baseline runs stay
practical while still averaging several operations.

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

## Profiling

Ad-hoc profiling commands for investigating specific workloads. Keep profiles in
`/tmp` so they do not become repository artifacts.

```sh
# CPU profile a benchmark workload
go test -tags bench -run '^$' -bench=BenchmarkInsert_SingleRow -benchtime=10s -cpuprofile=/tmp/minisql_insert.cpu ./benchmarks/
go tool pprof -top /tmp/minisql_insert.cpu

# Memory profile a benchmark workload
go test -tags bench -run '^$' -bench=BenchmarkInsert_SingleRow -benchtime=10s -memprofile=/tmp/minisql_insert.mem ./benchmarks/
go tool pprof -alloc_space -top /tmp/minisql_insert.mem

# CPU profile an e2e concurrency workload
go test -run '^$' -bench=BenchmarkConcurrent -benchtime=10s -cpuprofile=/tmp/minisql_concurrent.cpu ./e2e_tests/
go tool pprof -top /tmp/minisql_concurrent.cpu

# Mutex contention
go test -run '^$' -bench=BenchmarkConcurrent -benchtime=10s -mutexprofile=/tmp/minisql_mutex.prof ./e2e_tests/
go tool pprof -top /tmp/minisql_mutex.prof
```

See `benchmarks/AGENTS.md` for agent-specific benchmark hygiene.
