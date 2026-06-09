# Architecture

## Storage

### Pages

The database file is divided into fixed-size **4 096-byte pages**. Every page ends with a 4-byte CRC32-IEEE checksum that is verified on every read. A checksum mismatch returns an error immediately — corrupted pages are never silently accepted.

```
Page layout (4 096 bytes)
├── [page 0 only] Database header — bytes 0–99 (100 bytes, always plaintext)
├── Page type header — 7 bytes (base) + 8 bytes (leaf/internal node)
├── Null bitmask — 8 bytes per row (max 64 columns)
├── Cell data — rows / index entries
└── CRC32 checksum — last 4 bytes
```

Usable inline cell space per non-root page is **~4 061 bytes**. Large values (TEXT/JSON > 512 bytes, all VECTOR data) spill onto **overflow pages** chained via next-page pointers, bypassing the per-page limit.

### B+ Tree

Every table and every index is stored as an independent **B+ tree**:

- **Leaf nodes** hold the actual row cells (table scans) or index entries (index scans).
- **Internal nodes** hold routing keys and child page references.
- Leaf nodes at the same level are **linked** in a doubly-linked list, enabling efficient range scans without descending the tree.

### Free page list

Deleted pages are tracked in a **free-page linked list** anchored in the database header. The VACUUM command compacts the file by copying live data into a fresh database, reclaiming all free pages.

### Page cache

An **LRU page cache** keeps recently accessed pages in memory (default 2 000 pages ≈ 8 MB). The cache is shared across all transactions on a connection. Each write transaction also maintains a **write-set** of pages modified in the current transaction.

---

## Write-Ahead Log (WAL)

All commits write modified pages to the WAL file (`{dbpath}-wal`) **before** updating the main database file. The main file is updated only during a **checkpoint**.

### Commit protocol

1. Serialise all modified pages as WAL frames and write them to the WAL file.
2. Optionally `fsync()` the WAL file (controlled by `PRAGMA synchronous`).
3. Update the in-memory WAL index so subsequent reads see the new pages immediately.
4. The main database file is **not** touched during a commit.

### Checkpoint

1. Copy every WAL page into the main database file.
2. `sync()` the main file (skipped in `synchronous=off`).
3. Truncate the WAL file to its header (32 bytes).
4. Reset the in-memory WAL index.

An automatic checkpoint runs after `wal_checkpoint_threshold` WAL frames (default 1 000). Set `wal_checkpoint_threshold=0` to disable automatic checkpointing and run `PRAGMA wal_checkpoint` manually.

Checkpoint is blocked while any snapshot read transaction is active (`ErrCheckpointBlockedByReaders`). The checkpoint resumes once all readers finish.

### Crash recovery

On startup MiniSQL checks for an existing WAL file and replays all valid committed frames into the in-memory WAL index. Partially written frames (uncommitted) are discarded. The database is always consistent after recovery.

---

## Concurrency

### Write transactions — Optimistic Concurrency Control (OCC)

MiniSQL allows **one write transaction at a time**. A second concurrent write transaction blocks until the first finishes.

Within a write transaction:

- Page versions are recorded in a **read-set** when each page is first accessed.
- At commit, the engine checks that no page in the read-set has been modified by a concurrent transaction since it was first read.
- If a conflict is detected, `ErrTxConflict` is returned and the transaction can be retried.
- Conflicts can also be detected early in `ModifyPage` to fail fast.

```go
for {
    tx, _ := db.Begin()
    _, err = tx.Exec("update accounts set balance = balance - 100 where id = 1")
    if err := tx.Commit(); err != nil {
        if errors.Is(err, minisqlErrors.ErrTxConflict) {
            tx.Rollback()
            continue // retry
        }
        return err
    }
    break
}
```

### Read-only transactions — MVCC Snapshot Isolation

Every `SELECT` statement runs inside a **read-only transaction** with snapshot isolation:

1. At `BeginReadOnlyTransaction`, the current **commit sequence number** (`commitSeq`) is captured.
2. All page reads check whether the page was modified after the snapshot. If so, the engine serves the **historical version** from `pageVersionHistory`.
3. The reader sees a fully consistent snapshot of the database at the moment the transaction began, regardless of concurrent writers.
4. Multiple readers run concurrently without blocking each other or writers.

**Result:** No dirty reads, no non-repeatable reads, no phantom reads within a snapshot. Writers never block readers; readers never block writers.

---

## System diagram

```
┌─────────────────────────────────────────────────────┐
│                  Go application                     │
│               database/sql interface                │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│               MiniSQL driver layer                  │
│  Parser → Planner → Executor → Transaction Manager  │
└──────────┬──────────────────────────┬───────────────┘
           │                          │
┌──────────▼──────────┐  ┌────────────▼───────────────┐
│    Page cache (LRU) │  │   WAL (write-ahead log)    │
│    In-memory        │  │   {dbpath}-wal             │
└──────────┬──────────┘  └────────────┬───────────────┘
           │                          │
┌──────────▼──────────────────────────▼───────────────┐
│              Main database file                     │
│         B+ trees — 4 096-byte pages                 │
│         CRC32 on every page                         │
│         Optional AES-256-CTR encryption             │
└─────────────────────────────────────────────────────┘
```

---

## Query planner

MiniSQL includes a cost-based query planner that:

- Selects the best index for each table access (B-tree, fulltext, inverted, HNSW).
- Performs **predicate pushdown** — filters are applied as early as possible.
- Considers **index-only scans** (covering indexes) to avoid main-table page reads.
- Uses table statistics (row count, column cardinality, histograms, most-common values) populated by `ANALYZE`.
- Rewrites joins using **semi-join** and **anti-semi-join** for IN/NOT IN subqueries.
- Reorders join tables for star-schema queries.

Use `EXPLAIN` and `EXPLAIN ANALYZE` to inspect the chosen plan. See [EXPLAIN](sql/explain.md).
