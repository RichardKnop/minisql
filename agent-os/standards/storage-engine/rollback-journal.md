---
name: Rollback Journal (removed)
description: The rollback journal was replaced by WAL in Phase 7. This file is kept as a tombstone.
type: standard
---

# Rollback Journal (Removed)

The rollback journal (`{dbpath}-journal`) has been **removed**. MiniSQL now uses a Write-Ahead Log exclusively.

See [`wal.md`](./wal.md) for the current crash-recovery and commit protocol.
