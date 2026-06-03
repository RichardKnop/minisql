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

Parallel scan is most beneficial for large tables on multi-core machines running filter-heavy queries. For small tables or single-CPU environments the overhead typically outweighs the benefit.
