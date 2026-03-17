---
name: E2E Test Suite Structure
description: TestSuite setup/teardown lifecycle, SQL fixture placement, sub-test vs method rules
type: standard
---

# E2E Test Suite Structure

All e2e tests live in `e2e_tests/` (package `e2etests`) and use a testify suite.

## Setup

```go
type TestSuite struct {
    suite.Suite
    dbFile *os.File
    db     *sql.DB
}
func TestTestSuite(t *testing.T) { suite.Run(t, new(TestSuite)) }
```

- `SetupTest` — creates a fresh temp file and calls `sql.Open("minisql", tempFile.Name())` before every test.
- `TearDownTest` — closes the DB and removes the temp file after every test.
- `SetupSuite` / `TearDownSuite` — intentionally empty; do not move per-test state there.

## SQL fixtures

DDL and recurring queries are package-level `var` strings (`createUsersTableSQL`, `createProductsTableSQL`, etc.) defined in `e2e_test.go`. Do not inline DDL inside test methods.

## Sub-tests

Use `s.Run("description", func() { ... })` for sub-cases within a test that share pre-inserted data. Use separate `Test*` methods for independent scenarios.

## Assertions

Use `s.Require()` (fatal) for setup steps. Use `s.Equal` / `s.ErrorIs` for outcome assertions.
