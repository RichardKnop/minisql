# AGENTS.md — `benchmarks`

## Scope

Benchmarks compare MiniSQL against SQLite where possible and track memory/time baselines in `benchmarks/RESULTS.md`. Benchmark changes should make performance questions easier to answer, not just produce more numbers.

## Benchmark Hygiene

- Use `-tags bench` and `-run '^$'` for benchmark-only runs.
- Start with targeted slices while profiling, then run the full suite before integration when the branch is ready.
- Put profiles in `/tmp`, for example `/tmp/minisql_<target>.cpu` and `/tmp/minisql_<target>.mem`.
- Remove generated binaries such as `benchmarks.test` before committing.
- Use `-benchmem` for all performance comparisons so allocation changes are visible.
- Keep benchmark data deterministic where possible: fixed random seeds, fresh temp DBs, and clear setup outside the timed loop.

## Updating `RESULTS.md`

- Update `benchmarks/RESULTS.md` only after benchmark data changes.
- Include platform, branch, command, and GOMAXPROCS in the baseline header.
- Prefer small category tables over very wide tables.
- Call out major memory outliers and likely next optimisation targets.
- If only a subset was refreshed, say so in the command or notes so readers do not confuse it with a full-suite baseline.

## Useful Commands

```bash
go test -tags bench -run '^$' -bench '^BenchmarkSelect_PointScan' -benchmem ./benchmarks/
go test -tags bench -run '^$' -bench=. -benchmem ./benchmarks/
go test -tags bench -run '^$' -bench '^BenchmarkHNSW_' -benchmem ./benchmarks/
go test -tags bench -run '^$' -bench '^BenchmarkInsert_SingleRow' -benchmem -cpuprofile=/tmp/minisql_insert.cpu -memprofile=/tmp/minisql_insert.mem ./benchmarks/
go tool pprof -alloc_space -top /tmp/minisql_insert.mem
```
