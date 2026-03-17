# AGENTS.md — MiniSQL Codebase Guide for AI Coding Agents

## Project Overview

MiniSQL is an embedded, single-file SQL database written in Go, inspired by SQLite. It implements a hand-written state-machine SQL parser, a B+ tree storage engine with 4 KB pages, an LRU page cache, a rollback journal for crash recovery, and optimistic concurrency control (OCC). It registers itself as a `database/sql` driver.

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
│   │   ├── journal.go      # Write-ahead rollback journal
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

### Build
```bash
go build -v ./...
```

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

GitHub Actions runs on every push and PR to `main`:
1. `go build -v ./...` — must pass
2. `go test -v ./...` — must pass

There is no linter step in CI. There is no Makefile.

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
| Storage Engine | page layout, pager cache, OCC transactions, rollback journal |
| Testing | e2e suite, unit test setup, dataGen, row size presets |

Standards explain the *why* behind non-obvious patterns. Code conventions (formatting, error handling, etc.) remain in this file.

---

## Coding Conventions

### Error handling

- **Sentinel errors** are `var`-declared at package level and wrapped with `%w`:
  ```go
  var ErrDuplicateKey = fmt.Errorf("duplicate key")
  return fmt.Errorf("failed to insert primary key %s: %w", name, ErrDuplicateKey)
  ```
- Callers check with `errors.Is(err, minisql.ErrDuplicateKey)`.
- **Public** errors: `ErrDuplicateKey`, `ErrTxConflict`, `ErrNoMoreRows`.
- **Private** package-level errors: `errTableDoesNotExist`, `errIndexDoesNotExist`, etc.

### Context

- `context.Context` is always the first argument.
- Transactions are stored in the context: `TxFromContext(ctx)` / `WithTransaction(ctx, tx)`.
- Pass `ctx` through every layer without modification (except when injecting a transaction).

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
for aRow := range filteredPipe { ... }
```

When adding filtering/transformation stages, insert them as goroutines between `filteredPipe` and the consumer.

### Row projection

`row.OnlyFields(fields...)` returns a new `Row` with only the requested columns. Always call it just before sending a row to the caller — scan phases should fetch all fields needed by WHERE conditions, not just the SELECT list.

### Statement result / iterator

`StatementResult.Rows` is a lazy `Iterator`. Do not materialise all rows into a slice unless you need to sort or deduplicate them. The streaming path (`selectStreaming`) never loads all rows into memory.

### NULL handling

- Each row has a 64-bit NULL bitmask (column limit: 64).
- `OptionalValue{Valid: false}` represents NULL.
- Indexes do not store NULL keys; equality conditions on NULL use `IS NULL` / `IS NOT NULL`, not `=`.

### Text storage

- `TextPointer` wraps `[]byte`. Always use `TextPointer.String()` for logical comparison; never compare `TextPointer.Data` bytes directly (inline vs overflow representations differ).
- VARCHAR ≤ 255 bytes is stored inline. Larger TEXT uses overflow pages.

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

PRs target `main`. The CI gate is `go build ./...` + `go test ./...`.
