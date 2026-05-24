---
name: Write Transactions
description: Single-writer enforcement via activeWriters atomic; in-place LRU fast path vs clone slow path; transaction travels via context, never as a parameter
type: standard
---

# Write Transactions

All write operations go through a `Transaction` that is stored in the `context.Context`.
Only one write transaction may be active at a time — enforced by `activeWriters atomic.Int32`
on `TransactionManager`.

## Transaction lifecycle

1. **Begin** — `TransactionManager.BeginTransaction` increments `activeWriters` (blocks if already > 0), creates a `Transaction` with a unique ID, injects it into ctx via `WithTransaction(ctx, tx)`.
2. **Modify** — `txPager.ModifyPage(ctx, pageIdx)` returns a writable page. It has two paths:
   - **Fast path (in-place):** no snapshot readers active AND page committed at least once. Returns the shared LRU page directly; `WriteInfo.InPlace = true`, `OriginalPage = nil`.
   - **Slow path (clone):** new page, or snapshot readers are active. Clones the page; `WriteInfo.InPlace = false`, `WriteInfo.OriginalPage` = pointer to the pre-clone LRU page (used by MVCC history at commit time).
3. **Commit** — Flushes `WriteSet` to WAL via `commitWithWAL`. For cloned pages when snapshot readers exist, `OriginalPage` is saved in `pageVersionHistory` for MVCC readers. Increments `commitSeq`. Decrements `activeWriters`.
4. **Rollback** — In-place pages are evicted from LRU via `saver.InvalidatePage(pageIdx)` so the next read reloads the pre-write version from WAL. Cloned pages are discarded. `Abort()` clears `WriteSet` and `DbHeaderWrite`. Decrements `activeWriters`.

## In-place vs clone decision

```go
// Fast path: sole writer, no snapshot readers, previously committed page.
if !isNewPage && tm.PageLastCommittedSeq(pageIdx) > 0 {
    tm.mu.RLock()
    hasReaders := tm.hasActiveSnapshotReadersLocked()
    tm.mu.RUnlock()
    if !hasReaders {
        tx.TrackWrite(pageIdx, originalPage, nil, table, index, true) // inPlace=true
        return originalPage, nil
    }
}
// Slow path: clone
modifiedPage = originalPage.Clone()
tx.TrackWrite(pageIdx, modifiedPage, originalPage, table, index, false)
```

Pages that have never been committed (`PageLastCommittedSeq == 0`) always use the clone path:
their LRU cells have `unique`-seeded state that may mismatch the index's own unique flag.

## Context convention

- **Always** retrieve tx via `TxFromContext(ctx)` or `MustTxFromContext(ctx)`.
- **Never** pass `*Transaction` as a function parameter — it must travel via context.
- `MustTxFromContext` panics if no tx is present; only use it in code paths guaranteed to run inside a transaction.
