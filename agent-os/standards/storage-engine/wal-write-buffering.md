---
name: WAL Write Buffering
description: pendingBuf accumulation, flush triggers, flushThreshold=0 for tests, and flush-before-checkpoint rule
type: standard
---

# WAL Write Buffering

WAL frames are accumulated in `pendingBuf` and flushed to disk in one `WriteAt`
call, reducing syscall overhead for high-frequency single-row-per-transaction
workloads.

## Key fields (`WAL`)

| Field            | Purpose                                               |
|------------------|-------------------------------------------------------|
| `pendingBuf`     | Byte buffer holding serialised frames not yet on disk |
| `pendingLen`     | Number of valid bytes in `pendingBuf`                 |
| `flushThreshold` | Flush when `pendingLen >= flushThreshold` (bytes)     |

## Flush triggers

A flush happens when **any** of these is true:
- `flushThreshold == 0` — flush every commit (test / synchronous-off mode)
- `pendingLen >= flushThreshold` — buffer full
- `SynchronousFull` — always flush + fsync per commit

## Default values

- Production default (`DefaultWALWriteBufferSize`): **64 KiB** — balances syscall
  savings against bounded data-loss exposure on unclean shutdown.
- `CreateWAL` default: **0** (flush every commit) — preserves existing unit test
  behavior without modification.
- Configure via connection string: `./my.db?wal_write_buffer_size=131072`

## Critical rule: flush before Checkpoint

Always call `w.flush()` before any checkpoint or truncation. If `pendingBuf`
contains frames that are indexed but not yet written to the WAL file, a
checkpoint will try to copy missing frames into the DB file — causing
corruption on restart.

```go
// Correct pattern (already in Checkpoint/Truncate/Close)
func (w *WAL) Checkpoint(...) error {
    if err := w.flush(); err != nil { return err }
    // ... checkpoint logic
}
```

## Test setup

Unit tests for WAL use `flushThreshold = 0` intentionally — this is correct,
not a mistake. It means every `AppendTransaction` call flushes immediately,
making test assertions about WAL file content deterministic.

## Rules

- Never add writes directly to the WAL file bypassing `pendingBuf` — that
  skips the offset tracking and corrupts `nextOffset`.
- `SetWriteBufferSize` must be called before the first `AppendTransaction`;
  changing it mid-stream with data in `pendingBuf` is undefined behaviour.
- `FrameCount()` includes buffered-but-not-flushed frames — it reflects logical
  state, not physical file state.
