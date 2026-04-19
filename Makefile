.PHONY: build test coverage lint bench bench-report bench-chart

# Fail CI if total coverage drops below this percentage.
# Current baseline: ~70.8% total (internal/minisql: 70.1%, internal/parser: 87.4%).
# Raise this value as coverage improves toward the 80% target.
COVERAGE_THRESHOLD := 70

# Number of benchmark iterations (increase for statistical stability, e.g. BENCH_COUNT=5).
BENCH_COUNT ?= 1

# Benchmark filter: run all by default, or pass e.g. BENCH=BenchmarkInsert to narrow.
BENCH ?= .

build:
	go build -v ./...

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
	go test -tags bench -bench=$(BENCH) -benchmem -count=$(BENCH_COUNT) \
		-run '^$$' ./benchmarks/ | tee benchmarks/raw.txt

# bench-report: append a formatted Markdown table to benchmarks/RESULTS.md.
bench-report:
	@if [ ! -f benchmarks/raw.txt ]; then \
		echo "No raw.txt found — run 'make bench' first."; exit 1; \
	fi
	go run ./benchmarks/cmd/report/ -out $(CURDIR)/benchmarks/RESULTS.md benchmarks/raw.txt

# bench-chart: generate PNG bar charts from benchmarks/raw.txt.
# Charts are written to benchmarks/charts/.
# $(CURDIR) is the absolute path to the repo root, so the output directory is
# always correct regardless of where make is invoked from.
bench-chart:
	@if [ ! -f benchmarks/raw.txt ]; then \
		echo "No raw.txt found — run 'make bench' first."; exit 1; \
	fi
	go run ./benchmarks/cmd/chart/ -out $(CURDIR)/benchmarks/charts benchmarks/raw.txt
