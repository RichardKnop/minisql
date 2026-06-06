# AGENTS.md — `e2e_tests`

## Scope

End-to-end tests exercise MiniSQL through `database/sql` against real temporary database files. Use these tests for user-visible SQL behaviour, persistence, planner choices, and integration across parser, execution, storage, and indexes.

## Test Style

- Add tests as `func (s *TestSuite) TestXxx()` methods; the suite handles setup and teardown.
- Each test gets a fresh DB via `SetupTest()`.
- Use existing helpers such as `execQuery`, `collectUsers`, `collectOrders`, and local helpers near related tests.
- Keep shared DDL fixtures in `e2e_test.go` only when they are genuinely reused.
- Prefer descriptive subtest names for multi-scenario tests.
- For `database/sql`, set one open connection when tests create their own DB handles.

## Coverage Expectations

- Parser-only changes still need e2e coverage when syntax reaches execution.
- DML/index changes should cover INSERT, UPDATE, DELETE, persistence after reopen when relevant, and integrity checks for page-owning structures.
- Transaction, WAL, and MVCC changes should include commit, rollback, reopen, and concurrent-reader scenarios where applicable.
- Query-planner changes should include result correctness and `EXPLAIN` checks when planner selection matters.

## Validation

```bash
LOG_LEVEL=warn go test ./e2e_tests/... -count=1
LOG_LEVEL=warn go test ./e2e_tests/... -run 'TestTestSuite/TestSelect' -count=1 -v
LOG_LEVEL=warn go test ./e2e_tests/... -run 'TestTestSuite/TestHNSWIndex' -count=1 -v
```
