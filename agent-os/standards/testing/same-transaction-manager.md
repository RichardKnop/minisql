---
name: Same TransactionManager in Index Unit Tests
description: Table and index pagers must share one TransactionManager or OCC commits fail with ErrTxConflict
type: standard
---

# Same TransactionManager in Index Unit Tests

When a unit test exercises both table operations AND secondary/unique index operations,
all pagers must be wired to the **same** `TransactionManager` instance.

## Why

Each `TransactionManager` tracks its own page version state independently. If a table
pager uses TxManager-A and an index pager uses TxManager-B, commits from TxManager-B
look like "unseen concurrent writes" to TxManager-A → `ErrTxConflict` at commit time.
This is a spurious conflict that would never occur in production (single TxManager).

## `newTestTable` + `mockPagerFactory`

```go
// newTestTable wires everything to one TxManager — use it for all table tests.
table, txManager, pager := newTestTable(t, testColumns)

// For index pagers that must share the same TxManager:
indexPager := NewTransactionalPager(pager.ForIndex(...), txManager, tableName, indexName)
```

`mockPagerFactory(pager)` returns a factory that always returns the same pager,
ensuring the TxManager's version tracking is consistent across table and index reads.

## Rule

Never create a second `NewTransactionManager(...)` in a test that already has one from
`newTestTable`. Pass the existing `txManager` to all pager constructors in that test.
