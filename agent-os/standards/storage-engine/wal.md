---
name: Write-Ahead Log (WAL)
description: WAL commit protocol, in-memory WAL index, crash recovery, checkpoint, and totalPages initialisation rule
type: standard
---

# Write-Ahead Log (WAL)

MiniSQL uses WAL-only mode: every committed transaction appends frames to `{dbpath}-wal` and the main database file (`{dbpath}`) is **never written during a commit**. The DB file is updated only by a checkpoint.

## Files

| File | Purpose |
|---|---|
| `wal.go` | `WAL` struct — append frames, read frames, checkpoint, open/replay |
| `wal_index.go` | `WALIndex` struct — in-memory map of `PageIndex → latest committed bytes` |
| `pager.go` | `pagerImpl.SetWALIndex` — wires WAL index into cache miss path |
| `transaction_manager.go` | `commitWithWAL` — serialises write-set pages as WAL frames on commit |
| `database.go` | `WALConfig` struct + `NewDatabase` WAL wiring — WAL is always configured in production |

## Commit Protocol

1. Serialise each modified page from the transaction write-set into a WAL frame (page index + raw bytes).
2. Call `WAL.AppendTransaction` which writes all frames atomically and `Sync()`s the WAL file.
3. Call `WALIndex.Update(pageIdx, data)` for each frame so in-memory reads see the new pages immediately.
4. Call `pagerImpl.SavePage` for each page to update the in-memory cache.
5. The main DB file is **not touched**.

## Cache-Miss Read Path

`pagerImpl.GetPage` checks the WAL index **before** reading the DB file on every cache miss:

```
cache hit → return cached page
WAL index hit → unmarshal WAL bytes → cache → return
DB file read (fallback for pre-checkpoint pages) → unmarshal → cache → return
```

This guarantees that readers always see the latest committed page even if no checkpoint has run.

## Crash Recovery

On open, `OpenWALAndRebuildIndex` is called:

1. Read all frames from the WAL file.
2. Validate each frame's checksum; stop at the first invalid frame.
3. Group frames by transaction (commit marker); discard any trailing uncommitted frames.
4. Call `WALIndex.Rebuild` with the validated, committed frames.

After rebuild the in-memory WAL index contains the latest committed version of every page — no further action is needed before the database is usable.

## Checkpoint (`PRAGMA wal_checkpoint`)

1. Read every entry from the WAL index.
2. Write each page to its correct offset in the main DB file.
3. `Sync()` the DB file.
4. Truncate the WAL file to its header (32 bytes).
5. Call `WALIndex.Reset()` to clear the in-memory index.

Checkpoints are triggered in three ways:

| Trigger | Mechanism |
|---|---|
| Explicit | `PRAGMA wal_checkpoint` |
| Auto-threshold | `commitWithWAL` when `wal.FrameCount() >= checkpointThreshold` (default 1000; 0 = disabled via `wal_checkpoint_threshold` connection string parameter) |
| On close | `Database.Close()` runs a passive checkpoint if `wal.FrameCount() > 0` (mirrors SQLite behaviour) |

The on-close checkpoint keeps the DB file as a complete snapshot between sessions, limiting WAL growth across restarts and making crash recovery faster (less to replay). Errors during the on-close checkpoint are logged but not fatal — the WAL is still valid and will be replayed on the next open.

## `totalPages` Initialisation Rule

The main DB file is empty in WAL-only mode (0 bytes). `pagerImpl.TotalPages()` is used by `GetFreePage` to allocate new pages. If `totalPages` were left at 0 (the DB-file value), new-page allocation would clobber existing WAL pages.

**Rule:** `SetWALIndex` calls `WALIndex.MaxPageIndex()` and sets `p.totalPages = maxIdx + 1` whenever `p.totalPages == 0` and the WAL index is non-empty. This is the only place where `totalPages` is initialised from the WAL.

## `init()` Empty-Database Guard

`Database.init()` calls `initEmptyDatabase` only when **both** conditions hold:

1. `d.saver.TotalPages() == 0` (DB file has no pages), **and**
2. `d.walIndex == nil || d.walIndex.Size() == 0` (WAL index is also empty).

After a normal close the DB file is fully populated (checkpoint-on-close ran) and the WAL index is empty on reopen — the guard passes naturally. In a crash scenario the DB file may be partially written or empty while the WAL index has the data; the guard still prevents `initEmptyDatabase` from inserting a duplicate schema row.

## Rules

- Never write modified pages directly to the DB file during a commit; always go through the WAL.
- `WALConfig` is wired by `NewDatabase` before `init()` runs, so `SetWALIndex` and the WAL fields on `TransactionManager` are always set before any page access.
- Unit tests that do not need WAL infrastructure pass `nil` for `walCfg`; those commits fall back to `commitDirect`. Production code always passes a real `*WALConfig`.
- If the WAL protocol changes (frame format, checksum, commit marker), update `wal.go`, README, this standard, and the WAL e2e tests together.
- `context.Background()` (not the request context) must be used when creating temporary databases for operations like VACUUM to avoid OCC contamination from the calling transaction.
