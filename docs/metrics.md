# Metrics

MiniSQL exposes a native Go metrics API that returns a point-in-time snapshot of engine statistics. All counters are cumulative since the database was opened; gauges reflect instantaneous state.

---

## Reading metrics

```go
import (
    "context"
    "database/sql"
    "github.com/RichardKnop/minisql"
    _ "github.com/RichardKnop/minisql"
)

db, err := sql.Open("minisql", "./my.db")
// ...
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)

m, err := minisql.ReadMetrics(context.Background(), db)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("cache hit rate: %.2f%%\n",
    100*float64(m.PageCacheHits)/float64(m.PageCacheHits+m.PageCacheMisses))
fmt.Printf("tx commits: %d  rollbacks: %d\n", m.TxCommits, m.TxRollbacks)
fmt.Printf("queries total: %d  slow: %d\n", m.QueriesTotal, m.QueriesSlow)
```

`ReadMetrics` acquires the engine's connection briefly to copy the counters; it does not block query execution.

---

## Fields

### Page cache

| Field | Kind | Description |
|-------|------|-------------|
| `PageCacheHits` | counter | Requests served from the in-memory LRU cache |
| `PageCacheMisses` | counter | Requests that required a WAL or disk read |
| `PageCacheEvictions` | counter | Pages removed to make room for incoming pages |
| `PageCacheSize` | gauge | Pages currently held in the cache |
| `PageCacheCapacity` | gauge | Maximum pages the cache can hold (`max_cached_pages` setting) |

The cache hit rate is `PageCacheHits / (PageCacheHits + PageCacheMisses)`. A rate below ~95% on a read-heavy workload is a signal to raise `max_cached_pages`.

### WAL

| Field | Kind | Description |
|-------|------|-------------|
| `WALFramesWritten` | counter | Cumulative frames written to the WAL since open |
| `WALCheckpoints` | counter | Cumulative checkpoint runs (automatic + `PRAGMA wal_checkpoint`) |
| `WALCurrentFrames` | gauge | Frames currently in the WAL; resets to 0 after each checkpoint and truncate |

`WALCurrentFrames` approaching the `wal_checkpoint_threshold` (default 1000) means a checkpoint will run soon.

### Transactions

| Field | Kind | Description |
|-------|------|-------------|
| `TxCommits` | counter | Write transactions successfully committed |
| `TxRollbacks` | counter | Transactions rolled back (explicit or due to error) |

Read-only transactions are not counted — they produce no WAL frames and have zero commit overhead.

### Queries

| Field | Kind | Description |
|-------|------|-------------|
| `QueriesTotal` | counter | Total calls to `ExecContext` and `QueryContext` since open |
| `QueriesSlow` | counter | Calls that met or exceeded `slow_query_threshold` |

`QueriesSlow` is only meaningful when `slow_query_threshold` is set in the DSN. See [Connection](connection.md#parameters).

### Sort

| Field | Kind | Description |
|-------|------|-------------|
| `SortsInMemory` | counter | `ORDER BY` queries completed entirely in memory |
| `SortSpillRuns` | counter | Sorted run files written to disk for external merge sort |
| `SortSpillBytes` | counter | Cumulative bytes written to disk run files |

`SortSpillRuns > 0` means at least one `ORDER BY` query exceeded `sort_mem_limit`. Raise the limit or investigate query result sizes. See [Disk-backed sort](connection.md#disk-backed-sort).

---

## Periodic polling

Metrics are in-process counters — there is no background thread accumulating them. Poll on your own schedule:

```go
func reportMetrics(ctx context.Context, db *sql.DB, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            m, err := minisql.ReadMetrics(ctx, db)
            if err != nil {
                log.Printf("metrics error: %v", err)
                continue
            }
            log.Printf("cache=%.1f%% wal_frames=%d commits=%d slow_queries=%d spill_runs=%d",
                100*float64(m.PageCacheHits)/float64(max(m.PageCacheHits+m.PageCacheMisses, 1)),
                m.WALCurrentFrames,
                m.TxCommits,
                m.QueriesSlow,
                m.SortSpillRuns,
            )
        case <-ctx.Done():
            return
        }
    }
}
```

---

## Prometheus integration

There is no built-in Prometheus exporter. To expose metrics via Prometheus, wrap `ReadMetrics` in a custom collector:

```go
type miniSQLCollector struct {
    db           *sql.DB
    cacheHits    *prometheus.Desc
    cacheMisses  *prometheus.Desc
    txCommits    *prometheus.Desc
    // ... one Desc per field
}

func (c *miniSQLCollector) Collect(ch chan<- prometheus.Metric) {
    m, err := minisql.ReadMetrics(context.Background(), c.db)
    if err != nil {
        return
    }
    ch <- prometheus.MustNewConstMetric(c.cacheHits, prometheus.CounterValue, float64(m.PageCacheHits))
    ch <- prometheus.MustNewConstMetric(c.cacheMisses, prometheus.CounterValue, float64(m.PageCacheMisses))
    ch <- prometheus.MustNewConstMetric(c.txCommits, prometheus.CounterValue, float64(m.TxCommits))
    // ...
}
```
