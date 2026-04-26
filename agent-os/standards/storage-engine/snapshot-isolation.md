---
name: Snapshot Isolation (MVCC)
description: Read-only transaction snapshot semantics, pageVersionHistory lifecycle, TOCTOU fix in ReadPage, and checkpoint blocking
type: standard
---

# Snapshot Isolation (MVCC)

Read-only transactions see a consistent point-in-time snapshot. Write transactions use OCC (see `occ-transactions.md`).

## How it works

1. `BeginReadOnlyTransaction` captures `SnapshotSeq = commitSeq` (current global commit counter).
2. At write-transaction commit, old `*Page` copies are stored in `pageVersionHistory[pageIdx]` before the new version replaces the cache.
3. `ReadPage` for a read-only transaction calls `PageVersionAtSnapshot` — binary search returning the newest page version with `commitSeq <= tx.SnapshotSeq`.
4. After the last reader commits/aborts, `trimPageVersionHistoryLocked` GCs stale history entries.

## ReadPage snapshot path

```go
// Read-only tx: find the historical version visible at snapshot time
if tx.ReadOnly {
    return tm.PageVersionAtSnapshot(pageIdx, tx.SnapshotSeq)
}
```

## TOCTOU rule (critical)

**Always capture the page version BEFORE calling `GetPage`.** Capturing after is a TOCTOU bug: a concurrent write commit between `GetPage` and version-capture gives the wrong (newer) version number to OCC.

```go
// CORRECT — version captured before GetPage
version := tm.pageLastCommittedSeq[pageIdx]
page, err := pager.GetPage(ctx, pageIdx)
tx.ReadSet[pageIdx] = version

// WRONG — version captured after GetPage (TOCTOU race)
page, err := pager.GetPage(ctx, pageIdx)
version := tm.pageLastCommittedSeq[pageIdx] // may have advanced
tx.ReadSet[pageIdx] = version
```

## `pageVersionHistory` lifecycle

- Written at write-commit time for every page in the write-set.
- Entries accumulate while any read-only transaction is active.
- `trimPageVersionHistoryLocked` runs after each transaction ends; it drops versions older than `minActiveSnapshotSeq`.
- The history is in-memory only — it does not survive process restart (not needed: readers start fresh after restart).

## Checkpoint blocking

WAL checkpoint must not overwrite DB-file pages that a snapshot reader still needs. If any read-only transaction is active, `Checkpoint` returns `ErrCheckpointBlockedByReaders`. Callers must retry or skip the checkpoint.

## Rules

- Never read `commitSeq` outside `tm.mu` except via `atomic` helpers provided.
- `SnapshotSeq` on a transaction is immutable after `BeginReadOnlyTransaction` — never modify it.
- Read-only transactions skip OCC read-set tracking entirely (no `ReadSet` population).
- Snapshot history entries are `*Page` pointers — do not mutate a page after storing it in history; always copy before modifying.
