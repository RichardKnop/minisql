# Online Backup

MiniSQL supports online backup — creating a consistent, point-in-time copy of the database while it continues to serve reads and writes.

---

## How it works

The algorithm mirrors SQLite's WAL-mode online backup:

1. **Begin a read-only snapshot transaction** — `CheckpointWAL` checks for active snapshot readers before acquiring `walWriteMu`. Any checkpoint that starts after this point is turned away with `ErrCheckpointBlockedByReaders`, freezing the main DB file for the duration of the backup.
2. **Snapshot the WAL index** — `walWriteMu` is held for the minimum time needed to deep-copy the WAL index (page index → raw committed bytes) and record the page count. Write transactions are blocked only during this window, typically **microseconds**.
3. **Release `walWriteMu`** — writers immediately resume.
4. **Copy pages** — for each page 0..N-1:
   - If the page has an entry in the WAL snapshot (committed but not yet checkpointed), those bytes are written to the destination.
   - Otherwise the page is read directly from the main DB file.
5. **Release the snapshot transaction** — checkpoints may proceed again.

**Why the read-only snapshot is necessary:** WAL commits write to the WAL file only; the main DB file is updated exclusively by checkpoints. Without the snapshot transaction blocking checkpoints, a post-snapshot write transaction could commit, trigger an auto-checkpoint, and flush its pages into the DB file before the backup loop reads them — producing a backup that mixes pre- and post-snapshot data. The read-only snapshot prevents this entirely: any checkpoint attempt during the copy phase is rejected, so every `ReadAt` call sees the DB file at its pre-snapshot state.

The destination is a self-contained database file with no associated WAL file.

---

## Usage

```go
import (
    "context"
    "database/sql"
    "github.com/RichardKnop/minisql"
    _ "github.com/RichardKnop/minisql"
)

db, err := sql.Open("minisql", "./production.db")
// ...
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)

if err := minisql.Backup(ctx, db, "./backup.db"); err != nil {
    log.Fatal(err)
}
```

The backup file can be opened immediately:

```go
backup, err := sql.Open("minisql", "./backup.db")
if err != nil {
    log.Fatal(err)
}
backup.SetMaxOpenConns(1)
backup.SetMaxIdleConns(1)
defer backup.Close()

rows, err := backup.QueryContext(ctx, `select count(*) from "orders"`)
```

---

## Behaviour

| Property | Detail |
|----------|--------|
| Writer blocking | Microseconds (WAL index snapshot only) |
| Reader blocking | None |
| Consistency point | State at the moment the WAL snapshot is taken |
| WAL file created | No — destination is a clean standalone file |
| Encryption | Backup is encrypted with the same key as the source |
| Constraint | Must not be called inside an explicit `BEGIN` transaction |

---

## Periodic backups

```go
func scheduleBackups(ctx context.Context, db *sql.DB, dir string, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case t := <-ticker.C:
            path := filepath.Join(dir, fmt.Sprintf("backup-%s.db", t.Format("20060102-150405")))
            if err := minisql.Backup(ctx, db, path); err != nil {
                log.Printf("backup failed: %v", err)
            } else {
                log.Printf("backup written to %s", path)
            }
        case <-ctx.Done():
            return
        }
    }
}
```

---

## Comparison with VACUUM

| | `Backup` | `VACUUM` |
|-|----------|---------|
| Purpose | Off-site copy | Compaction in-place |
| Writers blocked | ~microseconds | Full duration |
| Readers blocked | None | Full duration |
| Output | New file at `destPath` | Replaces current DB file |
| Preserves source | Yes | Yes (atomic swap) |
