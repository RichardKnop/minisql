.PHONY: build test coverage

# Fail CI if total coverage drops below this percentage.
# Current baseline: ~70.8% total (internal/minisql: 70.1%, internal/parser: 87.4%).
# Raise this value as coverage improves toward the 80% target.
COVERAGE_THRESHOLD := 70

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
