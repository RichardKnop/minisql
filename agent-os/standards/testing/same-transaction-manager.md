---
name: Same TransactionManager in Index Unit Tests
description: Table and index pagers must share one TransactionManager or MVCC version tracking becomes inconsistent
type: standard
---

# Same TransactionManager in Index Unit Tests

When a unit test exercises both table operations AND secondary/unique index operations,
all pagers must be wired to the **same** `TransactionManager` instance.

## Why

Each `TransactionManager` maintains its own `commitSeq`, `pageLastCommittedSeq`, and
`pageVersionHistory` independently. If a table pager uses TxManager-A and an index pager
uses TxManager-B, each manager only sees commits from pagers registered with it. Version
tracking diverges: snapshot readers on TxManager-A would not see history written by
TxManager-B, leading to stale reads. This inconsistency never occurs in production where
a single `TransactionManager` governs all pagers for a database file.

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
