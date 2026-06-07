# Connection

## Connection string format

```
/path/to/database.db?param1=value1&param2=value2
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `wal_checkpoint_threshold` | `1000` | Auto-checkpoint after N WAL frames. Set to `0` to disable automatic checkpointing. |
| `wal_write_buffer_size` | `65536` | WAL write-buffer size in bytes. Set to `0` to flush every commit (lowest throughput, lowest exposure). |
| `log_level` | `warn` | Log verbosity: `debug`, `info`, `warn`, `error` |
| `max_cached_pages` | `2000` | Maximum pages to keep in the in-memory LRU page cache. Each page is 4 096 bytes; default ≈ 8 MB. |
| `slow_query_threshold` | `0` (disabled) | Log queries at WARN level when elapsed time meets or exceeds this value. Accepts Go duration strings: `50ms`, `2s`. |
| `synchronous` | `normal` | WAL fsync mode. See [WAL durability modes](#wal-durability-modes). |
| `parallel_scan` | `off` | Enable concurrent leaf-page scanning for full table scans. See [Parallel scan](#parallel-scan). |
| `sort_mem_limit` | `4194304` | Maximum bytes of row data held in memory before an `ORDER BY` sort spills to disk. Set to `0` to disable disk spill (all sorted rows stay in memory). See [Disk-backed sort](#disk-backed-sort). |
| `encryption_key` | _(none)_ | Hex-encoded AES-256-CTR encryption key. See [Encryption](encryption.md). |

## Examples

```go
// Enable debug logging
db, err := sql.Open("minisql", "./my.db?log_level=debug")

// Larger page cache (16 000 pages ≈ 64 MB)
db, err := sql.Open("minisql", "./my.db?max_cached_pages=16000")

// Disable auto-checkpoint (run PRAGMA wal_checkpoint manually)
db, err := sql.Open("minisql", "./my.db?wal_checkpoint_threshold=0")

// Maximum write durability (fsync after every commit)
db, err := sql.Open("minisql", "./my.db?synchronous=full")

// Log queries taking more than 50 ms
db, err := sql.Open("minisql", "./my.db?slow_query_threshold=50ms")

// Enable parallel full table scans
db, err := sql.Open("minisql", "./my.db?parallel_scan=on")

// Raise ORDER BY sort memory to 64 MiB before spilling to disk
db, err := sql.Open("minisql", "./my.db?sort_mem_limit=67108864")

// Disable disk spill entirely (all sorted rows stay in memory)
db, err := sql.Open("minisql", "./my.db?sort_mem_limit=0")

// Encrypted database
import "encoding/hex"
key := []byte("my-32-byte-secret-key")
db, err := sql.Open("minisql", "./my.db?encryption_key="+hex.EncodeToString(key))

// Multiple parameters
db, err := sql.Open("minisql", "./my.db?log_level=info&max_cached_pages=4000&synchronous=full")
```

## Connection pooling

!!! warning "Single connection required"
    MiniSQL requires exactly **one open connection** per database file. Multiple connections to the same file do **not** share page cache, WAL index, or transaction state, and will produce inconsistent results or corrupt the database.

Always configure the pool to use exactly one connection:

```go
db, err := sql.Open("minisql", "./my.db")
if err != nil {
    log.Fatal(err)
}
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
```

**Read concurrency is still handled within the single connection.** Concurrent goroutines running `SELECT` through the same `*sql.DB` each get their own MVCC snapshot and execute without blocking each other. The single-connection constraint does not reduce read throughput.

## WAL durability modes

The `synchronous` parameter controls when `fsync()` is called on the WAL file, trading durability for write performance.

| Mode | Parameter | Description |
|------|-----------|-------------|
| `normal` | `synchronous=normal` | **Default.** No fsync per commit. fsync only at checkpoint. Matches SQLite WAL default. Data loss possible only during OS crash/power failure in the narrow window between commit and next checkpoint. |
| `full` | `synchronous=full` | fsync after every WAL commit. Survives OS crash between commits. Significantly slower for write-heavy workloads. |
| `off` | `synchronous=off` | No fsyncs at all. Fastest, but uncommitted data may be lost on OS crash or power failure. |

Read and change the current mode at runtime with PRAGMA:

```sql
PRAGMA synchronous;           -- returns 0 (off), 1 (normal), 2 (full)
PRAGMA synchronous = full;
PRAGMA synchronous = normal;
PRAGMA synchronous = off;
```

## Parallel scan

When enabled, full table scans split the leaf-page chain across `runtime.NumCPU()` goroutines so multiple pages are decoded and filtered concurrently.

```go
db, err := sql.Open("minisql", "./my.db?parallel_scan=on")
```

Or toggle at runtime:

```sql
PRAGMA parallel_scan = on;
PRAGMA parallel_scan;         -- returns 0 (off) or 1 (on)
PRAGMA parallel_scan = off;
```

!!! note
    Parallel scan does **not** guarantee row-ID ordering. If your query depends on insertion order, always add an explicit `ORDER BY`.

## Disk-backed sort

`ORDER BY` queries accumulate matching rows in memory to sort them. When the total size of those rows exceeds `sort_mem_limit` bytes (default 4 MiB), MiniSQL flushes the current sorted batch to a temporary file and continues accumulating. At the end of the scan all temp files are merged with a min-heap into a single sorted stream.

```go
// Raise the threshold to 64 MiB for analytics workloads with large result sets
db, err := sql.Open("minisql", "./my.db?sort_mem_limit=67108864")

// Disable disk spill entirely (use when you know result sets are small)
db, err := sql.Open("minisql", "./my.db?sort_mem_limit=0")
```

Or adjust at runtime per-session with PRAGMA:

```sql
PRAGMA sort_mem_limit;              -- returns current limit in bytes
PRAGMA sort_mem_limit = 67108864;   -- set to 64 MiB
PRAGMA sort_mem_limit = 0;          -- disable disk spill
```

!!! note
    Disk spill only applies to the `selectWithSortRowView` fast path: sequential scans with no joins, no `LIMIT`, and no aggregate/GROUP BY. Queries that do not match this path sort in memory as before, regardless of `sort_mem_limit`.

Parallel scan is most beneficial for large tables on multi-core machines running filter-heavy queries. For small tables or single-CPU environments the overhead typically outweighs the benefit.
