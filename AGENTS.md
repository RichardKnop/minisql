# AGENTS.md — MiniSQL Codebase Guide for AI Coding Agents

## Project Overview

MiniSQL is an embedded, single-file SQL database written in Go, inspired by SQLite. It implements a hand-written recursive-descent + state-machine SQL parser, a B+ tree storage engine with 4 KB pages, an LRU page cache, a Write-Ahead Log (WAL) for crash recovery, Optimistic Concurrency Control (OCC) for write transactions, and in-memory MVCC snapshot isolation for read-only transactions. It registers itself as a `database/sql` driver.

**Module:** `github.com/RichardKnop/minisql`
**Go version:** 1.26
**Not production-ready.** Treat it as a research/learning project.

**Feature summary:** INSERT/SELECT/UPDATE/DELETE, ON CONFLICT DO NOTHING/DO UPDATE, UNION/UNION ALL, DISTINCT, GROUP BY/HAVING, ORDER BY (multi-column), LIMIT/OFFSET, INNER/LEFT/RIGHT JOIN (arbitrary chain topology), CASE WHEN, CAST, INTERVAL, arithmetic, scalar functions, subqueries (non-correlated scalar + IN/NOT IN in WHERE, derived tables in FROM, CTEs, correlated UPDATE FROM), CHECK/FOREIGN KEY/NOT NULL/UNIQUE constraints, RETURNING, EXPLAIN/EXPLAIN ANALYZE, VACUUM, ANALYZE, PRAGMA, prepared statements (`?` placeholders), data types BOOLEAN/INT4/INT8/REAL/DOUBLE/TEXT/VARCHAR/TIMESTAMP/JSON/UUID, B-tree indexes (primary, unique, secondary, composite, covering, partial, expression), full-text inverted index, JSON inverted index, parallel full table scans, slow query logging.

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
│   ├── minisql/            # Core database engine (~140 .go files, listed selectively below)
│   │   │
│   │   │  ── Statement & Planning ──
│   │   ├── stmt.go               # Statement struct, all statement kinds, Clone(), Validate(), Prepare()
│   │   ├── stmt_join.go          # JoinClause, join statement helpers
│   │   ├── stmt_result.go        # StatementResult + lazy Iterator pattern
│   │   ├── condition.go          # WHERE condition evaluation (checkCondition, likeMatch, etc.)
│   │   ├── condition_node.go     # ConditionNode tree + ToDNF()
│   │   ├── expr.go               # Expr struct + Eval: arithmetic, CASE WHEN, functions
│   │   ├── compare.go            # compareValues helper used by sort and condition evaluation
│   │   ├── composite_key.go      # CompositeKey for multi-column indexes
│   │   │
│   │   │  ── Query Execution ──
│   │   ├── database.go           # Top-level Database: parse → validate → execute dispatch
│   │   ├── database_schema.go    # Schema introspection helpers (createTableDDL, createIndexDDL)
│   │   ├── database_options.go   # DatabaseOption functional options
│   │   ├── table.go              # Table struct: columns, indexes, query plan entry-point
│   │   ├── table_options.go      # TableOption functional options
│   │   ├── table_pager.go        # Table-level pager wiring
│   │   ├── table_primary_key.go  # Primary key B-tree operations
│   │   ├── table_secondary_index.go  # Secondary/unique index DML helpers
│   │   ├── table_unique_index.go # Unique index enforcement
│   │   ├── insert.go             # INSERT execution (incl. ON CONFLICT)
│   │   ├── select.go             # SELECT execution: selectStreaming + selectWithSort paths
│   │   ├── update.go             # UPDATE execution
│   │   ├── update_from.go        # UPDATE FROM (correlated subquery via context injection)
│   │   ├── delete.go             # DELETE execution
│   │   ├── returning.go          # RETURNING clause projection for INSERT/UPDATE/DELETE
│   │   ├── subquery.go           # Non-correlated subquery pre-evaluation (resolveSubqueries)
│   │   ├── correlated_subquery.go # Correlated subquery execution for UPDATE FROM
│   │   ├── derived_table.go      # FROM subquery: materialises into VirtualTable
│   │   ├── cte.go                # WITH clauses: CTE registry via context, VirtualTable injection
│   │   ├── check.go              # CHECK constraint evaluation
│   │   ├── foreign_key.go        # FOREIGN KEY enforcement, FK callbacks, CASCADE/SET NULL
│   │   ├── explain.go            # EXPLAIN / EXPLAIN ANALYZE output
│   │   ├── query_plan.go         # Query planner: scan-type selection, index optimisation
│   │   ├── query_plan_order.go   # ORDER BY planning (index skip-sort, heap, full sort)
│   │   ├── query_plan_join.go    # JOIN planning: flattenJoinTree, hash join selection
│   │   ├── query_plan_stats.go   # ANALYZE statistics: equi-depth histograms, selectivity
│   │   ├── hash_join.go          # Hash join build/probe executor
│   │   ├── parallel_scan.go      # Parallel full table scan (PRAGMA parallel_scan)
│   │   ├── index_scan.go         # Index scan helpers (point, range, intersection)
│   │   ├── covering_index.go     # Covering index eligibility check
│   │   ├── sort.go               # sortRows + compareValues (selectWithSort)
│   │   ├── row_heap.go           # Min/max heap for ORDER BY … LIMIT efficiency
│   │   │
│   │   │  ── Indexes ──
│   │   ├── index.go              # BTreeIndex[T]: FindRowIDs, Insert, Delete, ScanAll, ScanRange
│   │   ├── index_node.go         # Index B-tree node marshal/unmarshal
│   │   ├── index_overflow.go     # Overflow for non-unique index row ID lists
│   │   ├── index_cursor.go       # Index cursor for range traversal
│   │   ├── index_pager.go        # Index-specific pager wiring
│   │   ├── expr_index.go         # Expression index: evalExprIndexKey, FindExpressionIndex
│   │   ├── full_text_search.go   # Full-text search: tokeniser, TF-IDF scoring, MATCH queries
│   │   ├── full_text_posting.go  # Inverted index posting list encoding/decoding
│   │   ├── analyze.go            # ANALYZE: build equi-depth histograms for index statistics
│   │   ├── integrity_check.go    # Integrity check: B-tree invariants + index consistency
│   │   │
│   │   │  ── Storage Engine ──
│   │   ├── pager.go              # In-memory page cache + LRU eviction
│   │   ├── pager_factory.go      # PagerFactory: creates typed pagers for table and index
│   │   ├── page.go               # Page: tagged union (LeafNode/InternalNode/OverflowPage/…)
│   │   ├── internal_node.go      # B+ tree internal node operations
│   │   ├── overflow_page.go      # Row overflow page (TEXT/JSON > 255 bytes)
│   │   ├── free_page.go          # Free page list management
│   │   ├── header.go             # DatabaseHeader marshal/unmarshal
│   │   ├── transaction.go        # Transaction struct + context helpers
│   │   ├── transaction_pager.go  # TransactionalPager: wraps pager with OCC tracking
│   │   ├── transaction_manager.go # OCC (write txns) + MVCC snapshot isolation (read-only txns)
│   │   ├── wal.go                # Write-Ahead Log: append frames, replay, checkpoint
│   │   ├── wal_index.go          # In-memory WAL index: PageIndex → latest committed bytes
│   │   ├── vacuum.go             # VACUUM: 8-phase copy-compact-swap
│   │   ├── sync_file.go          # Fsync helpers
│   │   │
│   │   │  ── Types & Values ──
│   │   ├── row.go                # Row struct + OptionalValue + marshal/unmarshal
│   │   ├── cursor.go             # Row cursor for B+ tree leaf traversal
│   │   ├── text_pointer.go       # TextPointer: inline (≤255B) vs overflow-page TEXT/JSON
│   │   ├── timestamp.go          # TimestampMicros: microseconds since 2000-01-01 UTC
│   │   ├── uuid.go               # UUIDValue: 16-byte fixed storage, ParseUUID/FormatUUID
│   │   ├── interval.go           # INTERVAL literal evaluation for timestamp arithmetic
│   │   ├── cast_test.go          # (paired with cast evaluation in expr.go)
│   │   │
│   │   │  ── Infrastructure ──
│   │   ├── ports.go              # All key interfaces (Parser, Pager, TxPager, BTreeIndex, …)
│   │   ├── pragma.go             # PRAGMA handler (synchronous, parallel_scan, foreign_keys, …)
│   │   ├── config.go             # Constants: PageSize, MaxColumns, LRU defaults, …
│   │   └── mocks_test.go         # Auto-generated mocks (never edit by hand)
│   │
│   ├── parser/                   # SQL parser (~15 .go files)
│   │   ├── parser.go             # State machine, tokeniser, reservedWords, step constants, doParse
│   │   ├── select.go             # SELECT parsing (fields, FROM, GROUP BY, HAVING, UNION)
│   │   ├── insert.go             # INSERT / ON CONFLICT parsing
│   │   ├── update.go             # UPDATE / UPDATE FROM parsing
│   │   ├── delete.go             # DELETE parsing
│   │   ├── table.go              # CREATE/DROP TABLE (columns, constraints, CHECK, FK)
│   │   ├── index.go              # CREATE/DROP INDEX (all index types, partial, expression)
│   │   ├── where.go              # WHERE recursive-descent parser + subquery extraction
│   │   ├── expr.go               # Scalar expression parser (arithmetic, CASE, CAST, functions)
│   │   ├── cte.go                # WITH clause (CTE) parsing
│   │   ├── explain.go            # EXPLAIN / EXPLAIN ANALYZE parsing
│   │   ├── foreign_key.go        # Foreign key constraint parsing helpers
│   │   ├── pragma.go             # PRAGMA parsing
│   │   ├── vacuum.go             # VACUUM parsing
│   │   └── analyze.go            # ANALYZE parsing
│   │
│   └── pkg/
│       └── logging/              # Zap logger configuration helpers
│
├── pkg/
│   ├── lrucache/                 # Generic LRU cache
│   └── bitwise/                  # Bitwise helpers for NULL bitmask
│
├── benchmarks/                   # Comparative benchmarks (MiniSQL vs SQLite)
│   ├── bench_test.go             # Shared setup: dbDriver, openDB, seedRows helpers
│   ├── select_bench_test.go      # SELECT benchmarks: point scan, range, full scan, count, limit
│   ├── insert_bench_test.go      # INSERT benchmarks: single row, batch, multi-values, prepared
│   ├── update_bench_test.go      # UPDATE benchmarks: by PK
│   ├── delete_bench_test.go      # DELETE benchmarks: by PK
│   ├── inverted_bench_test.go    # Full-text + JSON inverted index benchmarks
│   └── txn_bench_test.go         # Transaction benchmarks (build-tag: bench)
│
├── e2e_tests/                    # End-to-end tests (testify suite, real DB files)
│   ├── e2e_test.go               # TestSuite setup/teardown, shared helpers, shared DDL fixtures
│   ├── select_test.go            # SELECT tests + helpers (execQuery, collectUsers, collectOrders)
│   ├── aggregate_test.go         # GROUP BY, HAVING, COUNT/SUM/AVG/MIN/MAX
│   ├── arithmetic_test.go        # Arithmetic expressions in SELECT/WHERE
│   ├── between_test.go           # BETWEEN / NOT BETWEEN
│   ├── case_when_test.go         # CASE WHEN (searched + simple)
│   ├── cast_test.go              # CAST expressions
│   ├── check_test.go             # CHECK constraints
│   ├── composite_index_test.go   # Multi-column composite indexes
│   ├── concurrency_test.go       # OCC conflict + concurrent MVCC reads
│   ├── concurrency_bench_test.go # Concurrent insert/read benchmarks (e2e-level)
│   ├── correlated_subquery_update_test.go  # UPDATE FROM with correlated subquery
│   ├── covering_index_test.go    # Covering (index-only) scan
│   ├── cte_test.go               # WITH (non-recursive CTEs)
│   ├── database_test.go          # Database open/close, connection string params
│   ├── delete_test.go            # DELETE
│   ├── derived_table_test.go     # Subquery in FROM clause
│   ├── distinct_test.go          # SELECT DISTINCT
│   ├── explain_test.go           # EXPLAIN output
│   ├── explain_join_test.go      # EXPLAIN on join queries
│   ├── expression_index_test.go  # Expression indexes (LOWER, arithmetic, JSON path)
│   ├── foreign_key_test.go       # FOREIGN KEY constraints (all actions)
│   ├── full_text_test.go         # MATCH() full-text search
│   ├── functions_test.go         # Scalar functions (COALESCE, SUBSTR, DATE_TRUNC, etc.)
│   ├── hash_join_test.go         # Hash join selection and execution
│   ├── index_method_test.go      # Index type validation (FULLTEXT, INVERTED)
│   ├── insert_on_conflict_test.go # ON CONFLICT DO NOTHING / DO UPDATE
│   ├── interval_test.go          # INTERVAL timestamp arithmetic
│   ├── join_test.go              # INNER JOIN
│   ├── join_chain_test.go        # 3/4/5-table chain joins
│   ├── join_star_schema_test.go  # Star-schema multi-join
│   ├── json_inverted_index_test.go # JSON inverted index (JSON_CONTAINS)
│   ├── json_test.go              # JSON column type, ->, ->>, JSON functions
│   ├── like_test.go              # LIKE / NOT LIKE
│   ├── multi_index_intersect_test.go # Multi-index AND intersection
│   ├── nested_where_test.go      # Arbitrary AND/OR WHERE nesting
│   ├── order_by_test.go          # Multi-column ORDER BY, index skip-sort
│   ├── outer_join_test.go        # LEFT JOIN, RIGHT JOIN
│   ├── parallel_scan_test.go     # PRAGMA parallel_scan
│   ├── partial_index_test.go     # CREATE INDEX … WHERE (partial indexes)
│   ├── pragma_test.go            # PRAGMA statements
│   ├── predicate_pushdown_test.go # Predicate push-down through joins
│   ├── prepared_stmts_test.go    # Prepared statement lifecycle
│   ├── returning_test.go         # RETURNING clause on INSERT/UPDATE/DELETE
│   ├── subquery_test.go          # Non-correlated subqueries in WHERE
│   ├── tx_test.go                # BEGIN/COMMIT/ROLLBACK
│   ├── union_test.go             # UNION / UNION ALL
│   ├── update_from_test.go       # UPDATE … FROM (subquery-based updates)
│   ├── update_test.go            # UPDATE
│   ├── uuid_test.go              # UUID column type
│   ├── vacuum_test.go            # VACUUM
│   └── wal_test.go               # WAL persistence across reopen
│
└── agent-os/standards/           # Tribal knowledge standards (read before working on subsystems)
    ├── index.yml                 # Index of all standards
    ├── parser/                   # Parser subsystem standards
    ├── query-execution/          # Query planner and executor standards
    ├── storage-engine/           # Storage, pager, WAL, transaction standards
    └── testing/                  # Test setup and convention standards
```

---

## Commands

### Run all tests
```bash
make test
# or directly:
LOG_LEVEL=info go test ./... -count=1
```
`LOG_LEVEL=info` suppresses verbose debug output; errors are still visible. Use `warn` for even quieter output. The `benchmarks/` package is excluded from the normal test run (it requires `-tags bench`).

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

**CI threshold:** 70% total — CI fails if coverage drops below this. Raise `COVERAGE_THRESHOLD` in `Makefile` and `.github/workflows/go.yml` as coverage improves toward the 80% target.

### Regenerate mocks (after changing an interface in `ports.go`)
```bash
go install github.com/vektra/mockery/v3@v3.6.1
mockery
```
Mocks are written to `internal/minisql/mocks_test.go`. **Never edit this file by hand.**

### Benchmarks

Benchmarks live in `benchmarks/` and require the `bench` build tag. They run both MiniSQL and SQLite (`modernc.org/sqlite`) side-by-side for direct comparison.

```bash
# Run all benchmarks (MiniSQL + SQLite comparison)
go test -tags bench -bench=. -benchmem ./benchmarks/

# Run a specific benchmark
go test -tags bench -bench=BenchmarkSelect_PointScan -benchmem ./benchmarks/

# CPU profile
go test -tags bench -cpuprofile=cpu.prof -bench=BenchmarkInsert_SingleRow -benchtime=10s ./benchmarks/
go tool pprof -top cpu.prof | head -30

# Memory profile
go test -tags bench -memprofile=mem.prof -bench=BenchmarkInsert_SingleRow -benchtime=10s ./benchmarks/
go tool pprof -alloc_space -top mem.prof | head -30

# Concurrency benchmarks (in e2e_tests — no build tag required)
go test -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests/

# Low-level unit benchmarks (pager, cell, row marshaling)
go test -bench=. -benchmem ./internal/minisql/
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

1. If needed, add new **reserved words** to the `reservedWords` slice in `parser.go`. These are recognised before identifiers, so order matters — **longer strings must come before shorter strings with the same prefix** (e.g. `"DO UPDATE"` before `"DO"`, `"UNION ALL"` before `"UNION"`). See `agent-os/standards/parser/reserved-word-ordering.md`.

2. Add new **step constants** for the feature's parsing states to `parser.go` and register them in the appropriate `case` block of the main `doParse` switch.

3. Create or extend a `doParseXXX()` method (e.g., `doParseSelect()` in `select.go`). Each step reads one token with `p.peek()`, advances with `p.pop()`, populates fields on `p.Statement`, and transitions `p.step` to the next step.

4. For WHERE conditions and expressions, use the **recursive-descent** functions (`parseCondExpr`, `parseExpr`, `parseFuncCall`) — do **not** add new step constants for expression parsing. See `agent-os/standards/parser/where-recursive-descent.md`.

5. Add **parser tests** to `internal/parser/<feature>_test.go` following the `testCase` table pattern. See `agent-os/standards/testing/parser-test-structure.md`.

### Step 2 — Statement struct (`internal/minisql/stmt.go`)

Add any new fields to `Statement` that the parser populates.

- Update `Clone()` for new slice/map/pointer fields (scalar fields are copied automatically).
- Add helpers like `IsSelectCountAll()` when useful for readability downstream.
- If a new statement kind is introduced, add it to the `StatementKind` const block and `String()`.
- Update `Validate(table *Table)` and the appropriate `validateXXX()` method.
- Update `Prepare()` if the new field affects how values are resolved before execution.

### Step 3 — Execution (`internal/minisql/`)

The execution layer is split by operation:

| Feature type | Where to change |
|---|---|
| New DML statement | New method on `*Table` mirroring `Insert`, `Select`, `Update`, `Delete`; dispatch in `database.go` |
| Extension to SELECT | `select.go` — `Select()`, `selectStreaming()`, `selectWithSort()` |
| New aggregation / window operation | Follow `selectWithSort` pattern: collect from `filteredPipe`, transform, build `StatementResult` |
| Extension to query planning | `query_plan.go` / `query_plan_order.go` / `query_plan_join.go` |
| New WHERE operator | `condition.go` — add to `Operator` const, implement in `checkCondition()`; update index-selection rules in `query_plan.go` |
| New data type | `stmt.go` (`ColumnKind`), `row.go` (marshal/unmarshal), `condition.go` (compare), `index.go` (IndexKey constraint), `pager_factory.go` (new pager kind) |
| New index type | `index.go` (new `BTreeIndex[T]` instantiation or separate struct), `database.go` (DDL wiring), `table_secondary_index.go` (DML hooks) |
| New scalar function | `expr.go` — `evalFuncCall()` switch case; `parser/expr.go` — `parseFuncCall()` token recognition |
| Subquery execution | `subquery.go` (non-correlated pre-eval) or `correlated_subquery.go` (per-row eval); both inject via context |
| CTE execution | `cte.go` — materialise into `VirtualTable`, inject into context registry for `getTable()` lookup |
| Schema DDL changes | `database_schema.go` — update `createTableDDL()` and `createIndexDDL()` for round-trip |
| New constraint type | Validation in `insert.go` + `update.go`; store serialized expression in schema; add sentinel error `ErrXxxViolation` |

`Database.executeStatement()` in `database.go` dispatches on `stmt.Kind` — add a new case there for new statement kinds.

### Step 4 — Tests

Every new feature needs tests at **two levels**:

#### Unit tests (`internal/minisql/` or `internal/parser/`)

- File: `internal/minisql/<feature>_test.go` (pair it with the implementation file).
- Package: `package minisql` (same package as implementation — not `_test`).
- Use `testify/assert` and `testify/require` directly (no suites).
- Mark independent subtests with `t.Parallel()`.
- Use `initTest(t)` to get a pager and temp DB file when you need real storage. See `agent-os/standards/testing/unit-test-setup.md`.
- When a test exercises both table and index operations, all pagers must share one `TransactionManager`. See `agent-os/standards/testing/same-transaction-manager.md`.

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
PagerFactory  ForTable(columns []Column) Pager
              ForIndex(columns []Column, unique bool) Pager
              ForFullTextIndex(col Column) Pager
              ForInvertedIndex(col Column) Pager
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

Tribal knowledge, design decisions, and gotchas for specific subsystems are documented in `agent-os/standards/`. **Read the relevant standard(s) before working in any of these areas.**

**Full index:** `agent-os/standards/index.yml`

| Area | Standard file | What it covers |
|---|---|---|
| **Parser** | `parser/reserved-words.md` | Keyword tokenizer rules, how to add new keywords |
| | `parser/reserved-word-ordering.md` | Longest-match ordering rule — critical for correctness |
| | `parser/state-machine.md` | Two-level dispatch pattern; adding new statement types |
| | `parser/step-machine.md` | Step iota constants + doParse*() dispatch pattern |
| | `parser/where-recursive-descent.md` | Recursive-descent WHERE/expr parser; DNF normalisation; BETWEEN AND trap |
| | `parser/peek-pop-errors.md` | peek/pop helpers, error message format |
| | `parser/error-construction.md` | errorf vs wrapErr, sentinel vs inline errors |
| **Query Execution** | `query-execution/plan-pipeline.md` | Channel-based pipeline; streaming vs sort paths |
| | `query-execution/index-selection.md` | Operator eligibility, index priority, composite prefix matching |
| | `query-execution/dnf-scans-fanout.md` | OR groups → Scans fanout; residual Scan.Filters |
| | `query-execution/sort-path.md` | ORDER BY path selection; heap for LIMIT; DISTINCT interaction |
| | `query-execution/covering-index.md` | CoveringIndex eligibility; SELECT-only, IS NULL disqualifier |
| | `query-execution/hash-join-selection.md` | Hash join vs nested-loop; 1M threshold; RIGHT JOIN exception |
| | `query-execution/join-topology.md` | flattenJoinTree + scanIndexByAlias; LeftScanIndex is never hardcoded 0 |
| **Storage Engine** | `storage-engine/page-layout.md` | 4 KB tagged-union page, page 0 header format, usable space |
| | `storage-engine/pager-cache.md` | Sparse page array + LRU; I/O outside the lock |
| | `storage-engine/occ-transactions.md` | OCC ReadSet/WriteSet lifecycle; tx travels via context |
| | `storage-engine/snapshot-isolation.md` | MVCC read-only snapshots; pageVersionHistory; TOCTOU rule; checkpoint blocking |
| | `storage-engine/wal.md` | WAL frame format, commit protocol, crash recovery, checkpoint |
| | `storage-engine/wal-write-buffering.md` | pendingBuf accumulation; flush-before-checkpoint rule |
| | `storage-engine/biased-leaf-split.md` | Sequential-insert optimisation in LeafNodeSplitInsert |
| | `storage-engine/rightmost-leaf-cache.md` | Per-transaction rightmost leaf hint; lastTxID guard |
| | `storage-engine/rollback-journal.md` | Tombstone — the journal was replaced by WAL |
| **Testing** | `testing/e2e-suite.md` | TestSuite lifecycle, SQL fixture placement, assertion style |
| | `testing/unit-test-setup.md` | initTest helper, TransactionalPager wiring, ExecuteInTransaction |
| | `testing/same-transaction-manager.md` | Table + index pagers must share one TxManager |
| | `testing/datagen.md` | dataGen factory, uniqueness guarantees, naming convention |
| | `testing/row-size-presets.md` | testColumns / testMediumColumns / testBigColumns — when to use each |
| | `testing/must-helpers.md` | mustXxx helper pattern; t.Helper, require.NoError |
| | `testing/parser-test-structure.md` | testCase struct, table-driven loop, assert.ErrorIs rule |

---

## Coding Conventions

The style baseline is **[Effective Go](https://go.dev/doc/effective_go)** and the **[Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)** wiki. The rules below add project-specific detail on top of those guides. All rules are enforced by `golangci-lint` (see `.golangci.yml`); run `make lint` before pushing.

---

### Naming

**No `a`/`an` prefixes on local variables.** This pattern (`aRow`, `aColumn`, `aField`, `aTable`, `aSchema`) is not idiomatic Go and was a historical accident. Use plain descriptive names. This pattern still appears in some older code paths and should be cleaned up when editing those files.

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
- Several subsystems use context for injection: CTEs (`ctxWithCTERegistry`), correlated subquery SET values (`contextWithCorrelatedSetUpdates`). Follow this pattern when adding new context-injected state.
- Pass `ctx` through every layer without modification (except when injecting a transaction or other state).
- Use `context.Background()` (not the request context) when creating temporary databases inside operations like VACUUM — the request context carries an OCC transaction that would contaminate the temp DB.

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

### JSON type semantics

- `JSON` columns are stored as compact UTF-8 text via `TextPointer` (overflow-page enabled for large payloads).
- Values are normalised to compact form (`json.Marshal` round-trip) on write.
- JSON path operators: `col->>'$.field'` returns text; `col->'$.field'` returns JSON.
- JSON functions: `JSON_EXTRACT`, `JSON_VALID`, `JSON_TYPE`, `JSON_ARRAY_LENGTH`.
- `JSON_CONTAINS(col, '{"key":"val"}')` uses the JSON inverted index when one exists.
- `CAST(x AS JSON)` and `CAST(uuid AS TEXT)` are supported.
- Parser does not support negative integer literals in SQL — use `?` placeholder with `int64` arg instead.

---

### Database header

- The first `RootPageConfigSize` bytes of page `0` are the on-disk MiniSQL database header.
- Current header contract: magic `minisql\0`, format version `1`, page size `4096`, first free page, free page count, then reserved bytes.
- Opening a database requires a valid header magic/version/page size; old header layouts are intentionally rejected during the unstable pre-1.0 period.
- When changing the header format, update both `internal/minisql/config.go` and `agent-os/standards/storage-engine/page-layout.md` in the same change.
- WAL commits write frames to `{dbpath}-wal` and update the in-memory WAL index; the main database file is only written during a checkpoint.

---

### Text storage

- `TextPointer` wraps `[]byte`. Always use `TextPointer.String()` for logical comparison; never compare `TextPointer.Data` bytes directly (inline vs overflow representations differ).
- VARCHAR ≤ 255 bytes is stored inline. Larger TEXT and all JSON values use overflow pages.

---

### WAL durability modes

The `synchronous` setting on `WAL` controls when `fsync()` is called. It matches SQLite's `PRAGMA synchronous` for WAL mode.

| Mode | Value | Behaviour |
|------|-------|-----------|
| `SynchronousNormal` | 1 | **Default.** No fsync per commit; fsync only at checkpoint. |
| `SynchronousFull` | 2 | fsync after every WAL commit (maximum durability). |
| `SynchronousOff` | 0 | No fsyncs anywhere. Fastest; data loss possible on OS crash. |

- Configured at open time via the `synchronous=normal|full|off` connection string parameter.
- Changeable at runtime via `PRAGMA synchronous = normal|full|off` (takes effect on the next commit).
- Read at runtime via `PRAGMA synchronous` (returns 0/1/2).
- Implementation: `WAL.synchronous` is an `atomic.Int32`. `AppendTransaction` reads it on each call; no lock needed.

---

### Transactions

MiniSQL uses two complementary concurrency mechanisms:

**Write transactions — Optimistic Concurrency Control (OCC)**
- `txPager.ReadPage()` captures the global page version *before* reading the LRU cache (important: version is read first to avoid a TOCTOU race with concurrent commits), then records it in the read-set.
- `txPager.ModifyPage()` clones the page into the write-set. It also performs early conflict detection: if the page was previously read and the global version has advanced since then, it returns `ErrTxConflict` immediately rather than waiting for commit-time validation.
- At commit time, each read-set version is checked against the current global page version; any mismatch returns `ErrTxConflict`.
- All Table methods run inside a write transaction context supplied by `TransactionManager.ExecuteInTransaction`.

**Read-only transactions — Snapshot Isolation (MVCC)**
- `BeginReadOnlyTransaction` captures `tm.commitSeq` as the transaction's `SnapshotSeq` (under `tm.mu` to prevent races with concurrent commits).
- `ReadPage` for read-only transactions checks `pageLastCommittedSeq[pageIdx]` against `tx.SnapshotSeq`. If the cached page was committed after the snapshot, it retrieves the historical version from `pageVersionHistory` instead.
- At write commit time, the pre-modification page (`WriteInfo.OriginalPage`) is saved in `pageVersionHistory` with `validUntilSeq = commitSeq - 1` if any snapshot readers need it.
- `trimPageVersionHistoryLocked` GCs historical versions no longer needed by any active reader (called on each commit/rollback).
- Checkpoint (WAL truncation) is blocked while snapshot readers are active (`ErrCheckpointBlockedByReaders`).
- Use `ExecuteReadOnlyTransaction` for the read-only wrapper; `BeginReadOnlyTransaction` + `CommitTransaction` manually if you need the snapshot seq.

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
- **Maximum row size: ~4,065 bytes** — a row must fit in a single page (overflow pages handle TEXT/JSON column data, but the row header + fixed-width fields must fit).
- **TEXT/VARCHAR key columns in indexes** — TEXT columns cannot be primary-key or unique-index key columns (enforced in `validateCreateTable`). VARCHAR up to `MaxIndexKeySize` is permitted.
- **Single connection recommended** — OCC with multiple connections to the same file causes high conflict rates.
- **No `database/sql` connection pooling** — always `db.SetMaxOpenConns(1)` / `db.SetMaxIdleConns(1)`.
- **FULL OUTER JOIN not yet implemented** — only INNER JOIN, LEFT JOIN, and RIGHT JOIN are supported.
- **No savepoints** — `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` are not yet implemented.
- **No window functions** — `RANK()`, `ROW_NUMBER()`, `SUM() OVER`, `LAG()`, etc. are not yet implemented.
- **No ALTER TABLE** — schema evolution requires CREATE + INSERT SELECT + DROP + RENAME.
- **No hash indexes** — all indexes use B+ tree; O(1) hash equality lookups are not yet implemented.
- **Multi-column `ORDER BY` index optimisation** — requires all directions to be uniform (all ASC or all DESC) and the composite index column order to match exactly. Mixed ASC/DESC falls back to in-memory sort.
- **Partial index implication check** — conservative syntactic containment: each condition in the index WHERE must appear verbatim in the query WHERE. Semantically equivalent but textually different conditions are not recognised.
- **CHECK constraints** — column-level only; table-level CHECK is not yet implemented.
- **FOREIGN KEY** — single-column only; composite FK is not yet implemented. Actions: RESTRICT, NO ACTION, SET NULL, CASCADE.
- **Parser negative integer literals** — the parser does not support negative integer literals directly in SQL. Use `?` placeholder with a negative `int64` argument instead.

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
