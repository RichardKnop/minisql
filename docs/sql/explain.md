# EXPLAIN, ANALYZE, VACUUM & PRAGMA

## EXPLAIN

`EXPLAIN` shows the query plan the engine will use without executing the query:

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
EXPLAIN SELECT * FROM users WHERE email = 'alice@example.com';
EXPLAIN SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name;
```

The output includes:

| Column | Description |
|--------|-------------|
| `op` | Operator name (e.g., `SeqScan`, `IndexScan`, `HashJoin`) |
| `table` | Table being accessed |
| `index` | Index used, if any |
| `filter` | Predicate applied at this node |
| `est_rows` | Estimated output row count |
| `est_cost` | Estimated cost |

---

## EXPLAIN ANALYZE

`EXPLAIN ANALYZE` executes the query and augments the plan with actual runtime statistics:

```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1;
EXPLAIN ANALYZE SELECT * FROM articles WHERE MATCH(body, 'database storage');
```

Additional columns in the output:

| Column | Description |
|--------|-------------|
| `actual_rows` | Rows actually produced |
| `actual_time_ms` | Time spent in this node (milliseconds) |
| `loops` | Number of times this node was invoked |

---

## ANALYZE

`ANALYZE` collects table statistics that the query planner uses to estimate row counts, select indexes, and order joins:

```sql
-- Analyze one table
ANALYZE users;

-- Analyze multiple tables
ANALYZE users, orders, products;
```

Statistics collected:

- Row count
- Column cardinality (distinct value count)
- Histograms of value distribution
- Most-common values (MCV) per column

Run `ANALYZE` after large bulk inserts or updates to keep the planner's estimates accurate.

---

## VACUUM

`VACUUM` compacts the database file by rewriting all live data into a fresh file, reclaiming space from deleted rows, dropped columns, and freed pages:

```sql
VACUUM;
```

What VACUUM does:

1. Creates a new database file alongside the original.
2. Copies all live B-tree pages (tables and indexes) into the new file.
3. Resets the free-page list.
4. Replaces the original file with the compacted copy.
5. Carries encryption through — if the database is encrypted, the new file is encrypted with the same key.

!!! note
    VACUUM requires exclusive access and blocks other connections for its duration. It is safe to run at any time and is fully crash-safe; an interrupted VACUUM leaves the original file intact.

---

## PRAGMA

PRAGMAs control database engine settings at runtime.

### `PRAGMA synchronous`

Controls WAL fsync behaviour:

```sql
PRAGMA synchronous;           -- read current mode (returns 0, 1, or 2)
PRAGMA synchronous = off;     -- no fsync (0) — fastest, least durable
PRAGMA synchronous = normal;  -- periodic fsync (1) — default
PRAGMA synchronous = full;    -- fsync on every commit (2) — safest
```

| Mode | Value | Behaviour |
|------|-------|-----------|
| `off` | 0 | No `fsync` calls. Best throughput; data may be lost on power failure. |
| `normal` | 1 | `fsync` during checkpoint. Default. Safe under most failure scenarios. |
| `full` | 2 | `fsync` after every commit. Maximum durability, lowest throughput. |

### `PRAGMA foreign_keys`

Enable or disable foreign-key constraint enforcement (enabled by default):

```sql
PRAGMA foreign_keys;          -- read current state (1 = on, 0 = off)
PRAGMA foreign_keys = on;
PRAGMA foreign_keys = off;
```

### `PRAGMA parallel_scan`

Enable parallel table scans using multiple goroutines:

```sql
PRAGMA parallel_scan;         -- read current state (1 = on, 0 = off)
PRAGMA parallel_scan = on;
PRAGMA parallel_scan = off;   -- default
```

Parallel scan uses a goroutine pool to read pages concurrently. Beneficial for large full-table scans on multi-core machines. Can also be set via the DSN connection string parameter `parallel_scan=true`.

### `PRAGMA wal_checkpoint`

Flush all WAL frames to the main database file and truncate the WAL:

```sql
PRAGMA wal_checkpoint;
```

Checkpoint blocks if any read-only snapshot transaction is active; it resumes automatically once all readers finish. See [Architecture](../architecture.md) for WAL details.

### `PRAGMA integrity_check`

Full integrity check of all B-tree pages, CRC32 checksums, cell ordering, overflow page chains, and index consistency:

```sql
PRAGMA integrity_check;
```

Returns one row per issue found, or a single `ok` row if the database is healthy.

### `PRAGMA quick_check`

Faster subset of `integrity_check` — checks page structure and checksums but skips cross-index consistency:

```sql
PRAGMA quick_check;
```
