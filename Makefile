.PHONY: build test coverage lint bench bench-inverted bench-inverted-build bench-inverted-runtime bench-fulltext bench-json bench-report bench-chart

SHELL := /bin/bash

# Fail CI if total coverage drops below this percentage.
# Current baseline: ~70.8% total (internal/minisql: 70.1%, internal/parser: 87.4%).
# Raise this value as coverage improves toward the 80% target.
COVERAGE_THRESHOLD := 70

# Number of benchmark iterations (increase for statistical stability, e.g. BENCH_COUNT=5).
BENCH_COUNT ?= 1

# Benchmark filter: run all by default, or pass e.g. BENCH=BenchmarkInsert to narrow.
BENCH ?= .

# Benchmark duration/count passed to go test -benchtime. Use e.g. BENCH_TIME=1x
# for one-shot setup-heavy benchmarks such as index builds.
BENCH_TIME ?= 1s

# Inverted-index benchmarks mix setup-heavy index builds with steady-state
# query/mutation benchmarks, so the grouped target uses separate defaults.
BENCH_INVERTED_BUILD_TIME ?= 1x
BENCH_INVERTED_RUNTIME_TIME ?= 10x

build:
	go build -v ./...

build-cli:
	go build -o minisql cmd/minisql/*.go

test:
	LOG_LEVEL=info go test ./... -count=1

coverage:
	@LOG_LEVEL=info go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic
	@go tool cover -func=coverage.out
	@go tool cover -html=coverage.out -o coverage.html
	@echo ""
	@echo "HTML report: coverage.html"

lint:
	golangci-lint run ./...

# bench: run the benchmarking suite and save raw output.
# Results are written to benchmarks/raw.txt for use with bench-report / bench-chart.
#
#   make bench                     # run all benchmarks once
#   make bench BENCH_COUNT=5       # five runs per benchmark (better statistics)
#   make bench BENCH=BenchmarkInsert  # run only insert benchmarks
bench:
	@mkdir -p benchmarks
	set -o pipefail; go test -tags bench -bench='$(BENCH)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) \
		-run '^$$' ./benchmarks/ | tee benchmarks/raw.txt

# bench-inverted: run only full-text and JSON inverted-index benchmarks.
bench-inverted:
	$(MAKE) bench-inverted-build
	$(MAKE) bench-inverted-runtime
	@cat benchmarks/raw_inverted_build.txt benchmarks/raw_inverted_runtime.txt > benchmarks/raw.txt

# bench-inverted-build: run setup-heavy inverted-index build benchmarks.
bench-inverted-build:
	@mkdir -p benchmarks
	set -o pipefail; go test -tags bench -bench='Benchmark(FullText|JSONInverted)_BuildIndex' -benchmem \
		-benchtime=$(BENCH_INVERTED_BUILD_TIME) -count=$(BENCH_COUNT) -run '^$$' ./benchmarks/ | tee benchmarks/raw_inverted_build.txt

# bench-inverted-runtime: run steady-state inverted-index query and maintenance benchmarks.
bench-inverted-runtime:
	@mkdir -p benchmarks
	set -o pipefail; go test -tags bench \
		-bench='BenchmarkFullText_(Insert|Search|Update|Delete)|BenchmarkJSONInverted_(Insert|Contains|Update|Delete)' \
		-benchmem -benchtime=$(BENCH_INVERTED_RUNTIME_TIME) -count=$(BENCH_COUNT) -run '^$$' ./benchmarks/ | tee benchmarks/raw_inverted_runtime.txt

# bench-fulltext: run only full-text index benchmarks.
bench-fulltext:
	$(MAKE) bench BENCH='BenchmarkFullText'

# bench-json: run only JSON inverted-index benchmarks.
bench-json:
	$(MAKE) bench BENCH='BenchmarkJSONInverted'

# bench-report: append a formatted Markdown table to benchmarks/RESULTS.md.
bench-report:
	@if [ ! -f benchmarks/raw.txt ]; then \
		echo "No raw.txt found — run 'make bench' first."; exit 1; \
	fi
	go run ./benchmarks/cmd/report/ -out $(CURDIR)/benchmarks/RESULTS.md benchmarks/raw.txt
