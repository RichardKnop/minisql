---
name: Unit Test Setup
description: initTest helper, TransactionalPager wiring, t.Cleanup convention, ExecuteInTransaction requirement
type: standard
---

# Unit Test Setup

Unit tests for `internal/minisql` are plain `Test*` functions (not a suite). All use `t.Parallel()`.

## initTest helper

```go
func initTest(t *testing.T) (*pagerImpl, *os.File) {
    t.Parallel()
    tempFile, _ := os.CreateTemp("", testDbName)
    t.Cleanup(func() { os.Remove(tempFile.Name()) })
    aPager, _ := NewPager(tempFile, PageSize, 1000)
    return aPager, tempFile
}
```

From there, tests wire up:
```go
tablePager  = aPager.ForTable(testColumns)
txManager   = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
txPager     = NewTransactionalPager(tablePager, txManager, testTableName, "")
aTable      = NewTable(testLogger, txPager, txManager, testTableName, testColumns, 0, nil)
```

## Rules

- All writes must go through `txManager.ExecuteInTransaction`.
- Use `zap.NewNop()` for the logger in tests.
- Clean up temp files with `t.Cleanup`, not `defer` — ensures cleanup runs even on `t.Fatal`.
