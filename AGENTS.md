# AGENTS.md — MiniSQL Codebase Guide for AI Coding Agents

## Project Overview

MiniSQL is an embedded, single-file SQL database written in Go, inspired by SQLite. It implements a hand-written state-machine SQL parser, a B+ tree storage engine with 4 KB pages, an LRU page cache, a Write-Ahead Log (WAL) for crash recovery, and optimistic concurrency control (OCC). It registers itself as a `database/sql` driver.

**Module:** `github.com/RichardKnop/minisql`
**Go version:** 1.26
**Not production-ready.** Treat it as a research/learning project.

---

## Repository Layout

```
/
├── minisql.go              # Driver, Conn, Stmt — database/sql/driver registration
├── tx.go                   # Tx — database/sql/driver.Tx
├── rows.go                 # Rows, Result — database/sql/driver.Rows / .Result
├── connection_string.go    # Connection string parameter parsing
├── go.mod / go.sum
│
├── internal/
│   ├── minisql/            # Core database engine (~110 .go files)
│   │   ├── stmt.go         # Statement struct + all statement kinds
│   │   ├── condition.go    # WHERE condition types and evaluation
│   │   ├── ports.go        # All key interfaces (Parser, Pager, TxPager, …)
│   │   ├── database.go     # Top-level Database: parse → validate → execute dispatch
│   │   ├── table.go        # Table struct: columns, indexes, query plan entry-point
│   │   ├── insert.go       # INSERT execution
│   │   ├── select.go       # SELECT execution (streaming + sort paths)
│   │   ├── update.go       # UPDATE execution
│   │   ├── delete.go       # DELETE execution
│   │   ├── query_plan.go   # Query planner: scan-type selection, index optimisation
│   │   ├── query_plan_order.go  # ORDER BY planning
│   │   ├── query_plan_join.go   # JOIN planning (star-schema inner join)
│   │   ├── sort.go         # sortRows + compareValues (used by selectWithSort)
│   │   ├── row_heap.go     # Min-heap for ORDER BY … LIMIT efficiency
│   │   ├── row.go          # Row struct + OptionalValue
│   │   ├── stmt_result.go  # StatementResult + Iterator pattern
│   │   ├── pager.go        # In-memory page cache + LRU eviction
│   │   ├── page.go         # Page: LeafNode / InternalNode / OverflowPage
│   │   ├── transaction.go  # Transaction struct + context helpers
│   │   ├── transaction_manager.go  # OCC manager: read-set tracking, conflict detection
│   │   ├── index.go        # B+ tree index (primary, unique, secondary)
│   │   ├── cursor.go       # Row cursor for B+ tree traversal
│   │   ├── wal.go          # Write-Ahead Log: append frames, replay, checkpoint
│   │   ├── wal_index.go    # In-memory WAL index: page→latest-bytes map
│   │   ├── analyze.go      # ANALYZE statement: build index statistics
│   │   ├── config.go       # Database config constants (page size, limits, …)
│   │   └── mocks_test.go   # Auto-generated mocks (do not edit by hand)
│   │
│   ├── parser/             # SQL parser
│   │   ├── parser.go       # State machine, tokeniser, reserved words, step constants
│   │   ├── select.go       # SELECT parsing
│   │   ├── insert.go       # INSERT parsing
│   │   ├── update.go       # UPDATE parsing
│   │   ├── delete.go       # DELETE parsing
│   │   ├── table.go        # CREATE/DROP TABLE parsing
│   │   ├── index.go        # CREATE/DROP INDEX parsing
│   │   ├── where.go        # WHERE clause parsing
│   │   └── analyze.go      # ANALYZE parsing
│   │
│   └── pkg/
│       └── logging/        # Zap logger configuration helpers
│
├── pkg/
│   ├── lrucache/           # Generic LRU cache
│   └── bitwise/            # Bitwise helpers for NULL bitmask
│
└── e2e_tests/              # End-to-end tests (testify suite, real DB files)
    ├── e2e_test.go         # TestSuite setup/teardown, shared helpers, shared table DDL
    ├── select_test.go      # SELECT e2e tests + helpers (execQuery, collectUsers, …)
    ├── distinct_test.go    # SELECT DISTINCT e2e tests
    ├── order_by_test.go    # Multi-column ORDER BY e2e tests
    ├── join_test.go        # INNER JOIN e2e tests
    ├── join_star_schema_test.go
    ├── delete_test.go
    ├── update_test.go
    ├── tx_test.go
    ├── prepared_stmts_test.go
    ├── composite_index_test.go
    ├── concurrency_test.go
    └── concurrency_bench_test.go
```

---

## Commands

### Run all tests
```bash
make test
# or directly:
LOG_LEVEL=info go test ./... -count=1
```
`LOG_LEVEL=info` suppresses verbose debug output; errors are still visible. Use `warn` for even quieter output.

### Run a specific package
```bash
LOG_LEVEL=warn go test ./internal/minisql/... -count=1
LOG_LEVEL=warn go test ./internal/parser/... -count=1
LOG_LEVEL=warn go test ./e2e_tests/... -count=1
```

### Run a specific test by name
```bash
LOG_LEVEL=warn go test ./e2e_tests/... -run TestTestSuite/TestSelectDistinct -count=1 -v
LOG_LEVEL=warn go test ./internal/minisql/... -run TestTable_Select -count=1 -v
```

### Lint
```bash
make lint
# or directly:
golangci-lint run ./...
```

### Build
```bash
make build
# or directly:
go build -v ./...
```

### Coverage
```bash
make coverage
```
Runs tests with `-coverprofile`, prints a per-function summary to stdout, and writes `coverage.html` (open in a browser for the full annotated report).

**Baseline (March 2026):** `internal/minisql` 70.1%, `internal/parser` 87.4%, total 70.8%.
**CI threshold:** 70% total — CI fails if coverage drops below this. Raise `COVERAGE_THRESHOLD` in `Makefile` and `.github/workflows/go.yml` as coverage improves toward the 80% target.

### Regenerate mocks (after changing an interface in `ports.go`)
```bash
go install github.com/vektra/mockery/v3@v3.6.1
mockery
```
Mocks are written to `internal/minisql/mocks_test.go`. **Never edit this file by hand.**

### Benchmarks
```bash
# Page access
go test -bench=BenchmarkPageAccess -benchtime=100000x ./internal/minisql

# Concurrent workload (CPU profile)
go test -cpuprofile=cpu.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -top cpu.prof | head -30

# Memory profile
go test -memprofile=mem.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -alloc_space -top mem.prof | head -30
```

---

## CI

GitHub Actions runs on every push and PR to `main` with two parallel jobs:

1. **lint** — `golangci-lint run ./...` using `.golangci.yml`. Must pass.
2. **build** — `go build -v ./...` + `go test ./...` + coverage threshold check. Must pass.

---

## How to Add a New SQL Feature

Adding any SQL feature touches four layers in a fixed order. Use existing features as reference implementations.

### Step 1 — Parser (`internal/parser/`)

1. If needed, add new **reserved words** to the `reservedWords` slice in `parser.go`. These are recognised before identifiers, so order matters (longer strings first within the same prefix).

2. Add new **step constants** for the feature's parsing states to `parser.go` and register them in the appropriate `case` block of the main `doParse` switch.

3. Create or extend a `doParseXXX()` method (e.g., `doParseSelect()` in `select.go`). Each step reads one token with `p.peek()`, advances with `p.pop()`, populates fields on `p.Statement`, and transitions `p.step` to the next step.

4. Add **parser tests** to `internal/parser/<feature>_test.go` following the `testCase` table pattern.

Reference: implementing `DISTINCT` added `"DISTINCT"` to `reservedWords`, handled it inline in `stepSelectField` inside `doParseSelect()`, and added test cases in `select_test.go`.

### Step 2 — Statement struct (`internal/minisql/stmt.go`)

Add any new fields to `Statement` that the parser populates.

- Also update `Clone()` when adding value fields (booleans and ints are copied automatically by the struct literal; slices and maps need explicit copying).
- Add helpers like `IsSelectCountAll()` or `IsSelectAll()` when useful for readability downstream.
- If a new statement kind is introduced, add it to the `StatementKind` const block and `String()`.
- Update `Validate(aTable *Table)` and the appropriate `validateXXX()` method to enforce constraints on the new field.
- Update `Prepare()` if the new field affects how values are resolved before execution.

### Step 3 — Execution (`internal/minisql/`)

The execution layer is split by operation:

| Feature type | Where to change |
|---|---|
| New DML statement | New method on `*Table` mirroring `Insert`, `Select`, `Update`, `Delete` |
| Extension to SELECT | `select.go` — `Select()`, `selectStreaming()`, `selectWithSort()` |
| Extension to query planning | `query_plan.go` / `query_plan_order.go` / `query_plan_join.go` |
| New WHERE operator | `condition.go` — add to `Operator` const, implement evaluation in `checkCondition()` |
| New data type | `stmt.go` (`ColumnKind`), `row.go` (marshal/unmarshal), `condition.go` (compare) |

The `Database.execute()` in `database.go` dispatches on `stmt.Kind` — add a new case there for new statement kinds.

### Step 4 — Tests

Every new feature needs tests at **two levels**:

#### Unit tests (`internal/minisql/` or `internal/parser/`)

- File: `internal/minisql/<feature>_test.go` (pair it with the implementation file).
- Package: `package minisql` (same package as implementation — not `_test`).
- Use `testify/assert` and `testify/require` directly (no suites).
- Mark independent subtests with `t.Parallel()`.
- Use `initTest(t)` to get a pager and temp DB file when you need real storage.
- For parser tests follow the `testCase` struct pattern in `internal/parser/*_test.go`.

#### E2E tests (`e2e_tests/`)

- File: `e2e_tests/<feature>_test.go`.
- Package: `package e2etests`.
- Add a new `func (s *TestSuite) TestXxx()` method — the testify suite wires it into `go test` automatically.
- Each test gets a fresh DB via `SetupTest()` (temporary file, auto-cleaned).
- Use the helpers already in `select_test.go`: `execQuery(query, expectedRowsAffected)`, `collectUsers(query)`, `collectOrders(query)`, etc. Add new helpers there when needed.
- Shared DDL strings (`createUsersTableSQL`, etc.) live in `e2e_test.go`.

---

## Key Interfaces (`internal/minisql/ports.go`)

```go
Parser        Parse(context.Context, string) ([]Statement, error)
TableProvider GetTable(ctx context.Context, name string) (*Table, bool)
PagerFactory  ForTable([]Column) Pager / ForIndex(columns []Column, unique bool) Pager
Pager         GetPage / GetHeader / TotalPages
PageSaver     SavePage / SaveHeader + Flusher
TxPager       ReadPage / ModifyPage / GetFreePage / AddFreePage / GetOverflowPage
BTreeIndex    FindRowIDs / Insert / Delete / ScanAll / ScanRange
LRUCache[T]   Get / GetAndPromote / Put / EvictIfNeeded
```

When you add or change an interface in `ports.go`, run `mockery` to regenerate `mocks_test.go`.

---

## Mocks

Mocked interfaces (configured in `.mockery.yml`):

| Interface | Used in |
|---|---|
| `Parser` | `database_test.go` |
| `Pager` | pager and storage tests |
| `PageSaver` | pager flush tests |
| `TxPager` | table operation tests |

Usage pattern:
```go
mockParser := new(MockParser)
mockParser.EXPECT().Parse(mock.Anything, mock.Anything).Return([]Statement{...}, nil)
// ... test code ...
mockParser.AssertExpectations(t)
```

---

## Agent OS Standards

Tribal knowledge, design decisions, and gotchas for specific subsystems are documented in `agent-os/standards/`. Before working in any of the areas below, read the relevant standard(s).

**Index:** `agent-os/standards/index.yml`

| Area | Standards |
|---|---|
| SQL Parser | reserved words, state machine, WHERE recursive-descent, peek/pop cursor |
| Query Execution | plan pipeline, index selection, DNF fanout, sort path |
| Storage Engine | page layout, pager cache, OCC transactions, WAL |
| Testing | e2e suite, unit test setup, dataGen, row size presets |

Standards explain the *why* behind non-obvious patterns. Code conventions (formatting, error handling, etc.) remain in this file.

---

## Coding Conventions

The style baseline is **[Effective Go](https://go.dev/doc/effective_go)** and the **[Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)** wiki. The rules below add project-specific detail on top of those guides. All rules are enforced by `golangci-lint` (see `.golangci.yml`); run `make lint` before pushing.

---

### Naming

**No `a`/`an` prefixes on local variables.** This pattern (`aRow`, `aColumn`, `aField`) is not idiomatic Go and was a historical accident. Use plain descriptive names.

```go
// wrong
for aRow := range filteredPipe { ... }
aColumn, ok := t.ColumnByName(name)

// correct
for row := range filteredPipe { ... }
col, ok := t.ColumnByName(name)
```

**Receiver names** are one or two lowercase letters derived from the type name. They must be consistent across all methods of a type.

```go
func (t *Table) Insert(...)  { ... }  // not "tbl", not "this", not "self"
func (p *parserItem) peek()  { ... }
func (ck CompositeKey) Size() { ... }
```

**Abbreviations** follow Go conventions: `id` not `ID` inside names, but `ID` when it is a standalone exported name (`UserID`, `ErrInvalidID`). Acronyms are all-caps: `SQL`, `URL`, `HTTP`.

**Error variable names:** exported errors are `ErrXxx`; unexported are `errXxx`.

---

### Comments / Godoc

Every **exported** identifier must have a godoc comment beginning with the identifier's name.

```go
// Row holds all column values for a single database row.
type Row struct { ... }

// NewRow returns an empty Row with the given column schema.
func NewRow(columns []Column) Row { ... }
```

Unexported helpers benefit from comments too, but it is not enforced by the linter.

---

### Error handling

**Static error values** use `errors.New`, not `fmt.Errorf` (reserve `fmt.Errorf` for formatting or wrapping):

```go
// wrong — fmt.Errorf without format args or %w
var errTableDoesNotExist = fmt.Errorf("table does not exist")

// correct
var errTableDoesNotExist = errors.New("table does not exist")
```

**Wrapping** uses `%w` so callers can use `errors.Is` / `errors.As`:

```go
// wrong — drops the error chain
return fmt.Errorf("seek row: %v", err)

// correct
return fmt.Errorf("seek row: %w", err)
```

**Error strings** are lower-case and have no trailing punctuation (enforced by revive `error-strings` rule):

```go
// wrong
return errors.New("Table does not exist.")

// correct
return errors.New("table does not exist")
```

**Sentinel errors:** public errors are `ErrXxx`; unexported are `errXxx`. Callers check with `errors.Is`.

---

### Early returns

Prefer early returns over nested conditionals. Each guard clause should handle the error / edge case and return immediately.

```go
// wrong — pyramids
func foo() error {
    if ok {
        if err == nil {
            // actual logic
        }
    }
    return nil
}

// correct
func foo() error {
    if !ok {
        return errors.New("not ok")
    }
    if err != nil {
        return fmt.Errorf("foo: %w", err)
    }
    // actual logic
    return nil
}
```

---

### Context

- `context.Context` is always the **first** argument (enforced by revive `context-as-argument` rule).
- Transactions are stored in the context: `TxFromContext(ctx)` / `WithTransaction(ctx, tx)`.
- Pass `ctx` through every layer without modification (except when injecting a transaction).

---

### Tests

**`require` vs `assert`:** use `require` (fail-fast) when the test cannot safely continue if the assertion fails (e.g. a nil pointer would follow). Use `assert` (continue) for independent property checks.

```go
result, err := t.Select(ctx, stmt)
require.NoError(t, err)          // can't continue if err != nil
assert.Len(t, rows, 3)           // independent check, test can still report others
assert.Equal(t, "alice", rows[0])
```

**`t.Parallel()`** should be called at the top of every test and subtest that does not write shared state.

**Table-driven tests** are preferred when the same logic is exercised with multiple inputs. Use `t.Run` with a descriptive name for each case. For a single scenario with distinct setup, a named subtest is fine.

---

### Channel-based pipelines (SELECT)

SELECT uses a producer/consumer goroutine pipeline:

```go
filteredPipe := make(chan Row, 128)  // buffered — reduces goroutine blocking
errorsPipe   := make(chan error, N)

wg.Go(func() {
    if err := plan.Execute(ctx, provider, fields, filteredPipe); err != nil {
        errorsPipe <- err
    }
})
go func() { wg.Wait(); close(filteredPipe) }()

// Consumer (selectStreaming or selectWithSort)
for row := range filteredPipe { ... }
```

When adding filtering/transformation stages, insert them as goroutines between `filteredPipe` and the consumer.

---

### Row projection

`row.OnlyFields(fields...)` returns a new `Row` with only the requested columns. Always call it just before sending a row to the caller — scan phases should fetch all fields needed by WHERE conditions, not just the SELECT list.

---

### Statement result / iterator

`StatementResult.Rows` is a lazy `Iterator`. Do not materialise all rows into a slice unless you need to sort or deduplicate them. The streaming path (`selectStreaming`) never loads all rows into memory.

---

### NULL handling

- Each row has a 64-bit NULL bitmask (column limit: 64).
- `OptionalValue{Valid: false}` represents NULL.
- Indexes do not store NULL keys; equality conditions on NULL use `IS NULL` / `IS NOT NULL`, not `=`.

---

### TIMESTAMP semantics

- `TIMESTAMP` is a timestamp-without-time-zone type stored as microseconds since `2000-01-01 00:00:00 UTC`.
- The implementation uses the proleptic Gregorian calendar across the full supported range.
- Timezone-qualified literals such as `Z`, `UTC`, or `+01:00` are rejected; do not silently normalize them.
- `NOW()` is evaluated in UTC and stored as a timezone-naive timestamp value.
- Fractional seconds accept 1 to 6 digits and are scaled to microseconds.

---

### Database header

- The first `RootPageConfigSize` bytes of page `0` are the on-disk MiniSQL database header.
- Current header contract: magic `minisql\0`, format version `1`, page size `4096`, first free page, free page count, then reserved bytes.
- Opening a database now requires a valid header magic/version/page size; old header layouts are intentionally rejected during the unstable pre-1.0 period.
- When changing the header format, update both `internal/minisql/config.go` and the storage-engine standards/docs in the same change.
- WAL commits write frames to `{dbpath}-wal` and update the in-memory WAL index; the main database file is only written during a checkpoint. Keep code, tests, README, and standards aligned if the WAL protocol changes.

---

### Text storage

- `TextPointer` wraps `[]byte`. Always use `TextPointer.String()` for logical comparison; never compare `TextPointer.Data` bytes directly (inline vs overflow representations differ).
- VARCHAR ≤ 255 bytes is stored inline. Larger TEXT uses overflow pages.

---

### Transactions

- Read operations: call `txPager.ReadPage()` — tracked in the read-set.
- Write operations: call `txPager.ModifyPage()` — page is copied into the write-set.
- All Table methods run inside a transaction context supplied by `TransactionManager.ExecuteInTransaction`.

---

## File Naming Conventions

| Pattern | Purpose |
|---|---|
| `feature.go` | Implementation |
| `feature_test.go` | Unit tests for that feature |
| `feature_bench_test.go` | Benchmarks (separate file to keep test files readable) |
| `feature_primary_key_test.go` | Tests scoped to primary key behaviour of a feature |
| `feature_unique_index_test.go` | Tests scoped to unique index behaviour |
| `mocks_test.go` | Auto-generated mocks — never edit by hand |
| `query_plan_*.go` | Each query planning concern in its own file |

Parser files mirror engine files: `parser/select.go` ↔ `internal/minisql/select.go`.

---

## Constraints and Known Limits

- **Maximum 64 columns per table** — enforced by the 64-bit NULL bitmask in each row.
- **Maximum row size: 4,065 bytes** — a row must fit in a single page.
- **VARCHAR/TEXT key columns in indexes are limited**: TEXT columns cannot be primary-key or unique-index columns (enforced in `validateCreateTable`).
- **Single connection recommended** — OCC with multiple connections to the same file causes high conflict rates.
- **No `database/sql` connection pooling** — always `db.SetMaxOpenConns(1)` / `db.SetMaxIdleConns(1)`.
- **WHERE clause nesting depth**: currently limited to one level of AND/OR nesting (see `OneOrMore` type in `stmt.go`).
- **INNER JOIN topology**: only star-schema (multiple tables joining to one base table). Nested joins are parsed but not fully executed.
- **Multi-column `ORDER BY` index optimisation**: when all `ORDER BY` directions are the same (all ASC or all DESC) and a composite secondary index exists whose columns match the `ORDER BY` columns exactly (same order), the planner uses that index and avoids an in-memory sort. Mixed directions (e.g., `a ASC, b DESC`) always fall back to an in-memory sort because the index scan direction is a single bit (`SortReverse`) with no per-column direction support.
- **Multi-column `CREATE INDEX`**: supported — a composite `BTreeIndex[CompositeKey]` is created. The index is usable for ORDER BY optimisation (see above) and for multi-column unique constraints.

---

## Branch and Commit Conventions

**Branch names:** `feat/`, `refactor/`, `fix/`, `test/`, `chore/`, `docs/`
**Commit message format:** `<type>: <short description>`

```
feat: add SELECT DISTINCT support
fix: dedup DISTINCT + LIMIT interaction with heap optimisation
test: add e2e tests for multi-column ORDER BY
refactor: extract query plan ordering into separate file
docs: update README planned features list
```

PRs target `main`. The CI gate is `golangci-lint` + `go build ./...` + `go test ./...`.
