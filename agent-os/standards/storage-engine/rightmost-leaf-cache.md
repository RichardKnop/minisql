---
name: Rightmost-Leaf Cache
description: Per-transaction hint caching the last leaf page for sequential inserts; lastTxID guard, invalidation on split
type: standard
---

# Rightmost-Leaf Cache

Skips the O(log n) B+ tree traversal on sequential inserts by caching the
rightmost leaf page index. Effective for auto-increment / monotonically
increasing key workloads.

## Key fields (`Index[T]` and `Table`)

| Field           | Purpose                                                   |
|-----------------|-----------------------------------------------------------|
| `rightmostLeaf` | `atomic.Int64` — cached page index (-1 = cold/invalid)   |
| `lastTxID`      | `atomic.Uint64` — transaction ID that last warmed cache   |

## Cache lifecycle

1. **Cold start per transaction**: on the first insert of each new transaction,
   `lastTxID != tx.ID` so the cache is ignored and the full tree traversal runs.
   `lastTxID` is then updated to `tx.ID`.
2. **Warm hit**: subsequent inserts in the same transaction call
   `tryInsertIntoRightmostLeaf` with the cached page index, bypassing traversal.
3. **Invalidation on split**: `insertNotFull` returns `(pageIdx, isRightmost, err)`.
   If `isRightmost == false` at any level, the fast path is abandoned and
   `rightmostLeaf` is reset to -1.
4. **After successful full traversal**: if all levels chose the rightmost child,
   `rightmostLeaf` is updated to the leaf page returned.

## Critical rule: lastTxID guard

Always check `lastTxID == tx.ID` before using the cached hint. Skipping the
guard allows a stale hint from a prior (possibly rolled-back) transaction to
direct an insert to the wrong leaf, silently producing an out-of-order B+ tree.

```go
// Correct — cold-start on new transaction
if t.lastTxID.Load() == tx.ID {
    // try cached leaf
    if hit, err := t.tryInsertIntoRightmostLeaf(ctx, tx, row); hit {
        return err
    }
}
// fall through to full traversal
t.lastTxID.Store(tx.ID)
```

## `insertNotFull` isRightmost return value

`insertNotFull(ctx, page, key, value)` returns a bool indicating whether the
insert stayed on the rightmost path at this level. The caller propagates the
flag up; if any level returns `false` (e.g. due to a split redirecting to a
non-rightmost child), the cache is invalidated.

## When the cache does NOT help

- Random-key inserts: every insert picks a different leaf; cache is always cold.
- Single-row-per-transaction workloads: `lastTxID` changes every iteration,
  so the cache is cold-started each time. No benefit, no harm.
- After a leaf split that creates a new rightmost leaf: one cache miss, then
  the new leaf is cached for subsequent inserts in the same transaction.

## Rules

- Cache fields are `atomic` for read safety, but correctness depends on
  `lastTxID` being checked before every use of `rightmostLeaf`.
- Never bypass the guard even in single-threaded contexts — a transaction
  rollback and retry can reuse the same struct instance with a new tx ID.
- The cache is per-`Index`/`Table` instance, not per-transaction; the guard
  is what makes it safe across transaction boundaries.
