---
name: Optimistic Concurrency Control (OCC)
description: Transaction ReadSet/WriteSet lifecycle; transaction must travel via context, never as a parameter
type: standard
---

# Optimistic Concurrency Control (OCC)

All reads and writes go through a `Transaction` that is stored in the `context.Context`.

## Transaction lifecycle

1. **Begin** — `TransactionManager` creates a `Transaction` with a unique ID and start time. Injected into ctx via `WithTransaction(ctx, tx)`.
2. **Read** — `TrackRead(pageIdx, version)` records the page version in `ReadSet`.
3. **Write** — `TrackWrite(pageIdx, clonedPage, table, index)` stores a modified page copy in `WriteSet`. Original live page is unchanged.
4. **Commit** — Validation: check no page in `ReadSet` was modified by a concurrent committed tx since the recorded version. If ok, flush `WriteSet` to the pager + journal, mark `TxCommitted`.
5. **Abort** — `Abort()` discards `WriteSet` and `DbHeaderWrite`; no disk I/O needed.

## Context convention

- **Always** retrieve tx via `TxFromContext(ctx)` or `MustTxFromContext(ctx)`.
- **Never** pass `*Transaction` as a function parameter — it must travel via context.
- `MustTxFromContext` panics if no tx is present; only use it in code paths that are guaranteed to run inside a transaction.
