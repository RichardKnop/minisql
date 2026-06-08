package minisql

import (
	"context"
	"database/sql"
	"fmt"
)

// Metrics is a point-in-time snapshot of MiniSQL engine statistics.
// Counter fields are cumulative since the database was opened.
// Gauge fields (Size, CurrentFrames) reflect the instantaneous state.
type Metrics struct {
	// PageCache reflects the LRU page cache (controlled by max_cached_pages).
	// HitRate = PageCacheHits / (PageCacheHits + PageCacheMisses).
	PageCacheHits      int64 // requests served from cache
	PageCacheMisses    int64 // requests that required a WAL or disk read
	PageCacheEvictions int64 // pages removed to make room for new ones
	PageCacheSize      int64 // pages currently held in cache
	PageCacheCapacity  int64 // configured maximum (max_cached_pages × PageSize bytes each)

	// WAL reflects the Write-Ahead Log.
	WALFramesWritten int64 // cumulative frames written since open
	WALCheckpoints   int64 // cumulative checkpoint runs since open
	WALCurrentFrames int64 // frames currently in the WAL (resets to 0 after each checkpoint+truncate)

	// Tx reflects write transaction lifecycle.
	// Read-only transactions are not counted (they produce no WAL frames).
	TxCommits   int64 // write transactions successfully committed
	TxRollbacks int64 // transactions rolled back (explicit or due to error)

	// Queries reflects overall query volume across ExecContext and QueryContext.
	QueriesTotal int64 // cumulative calls since open
	QueriesSlow  int64 // calls that exceeded slow_query_threshold

	// Sort reflects ORDER BY behaviour.
	SortsInMemory  int64 // ORDER BY completed entirely in memory
	SortSpillRuns  int64 // cumulative run files written to disk for external merge sort
	SortSpillBytes int64 // cumulative bytes written to run files
}

// ReadMetrics returns a point-in-time snapshot of engine statistics for db.
// db must have been opened with sql.Open("minisql", dsn).
//
// Example:
//
//	m, err := minisql.ReadMetrics(ctx, db)
//	if err != nil { ... }
//	hitRate := float64(m.PageCacheHits) / float64(m.PageCacheHits + m.PageCacheMisses)
func ReadMetrics(ctx context.Context, db *sql.DB) (Metrics, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return Metrics{}, fmt.Errorf("minisql: ReadMetrics: acquire connection: %w", err)
	}
	defer conn.Close()

	var m Metrics
	err = conn.Raw(func(c any) error {
		mc, ok := c.(*Conn)
		if !ok {
			return fmt.Errorf("minisql: ReadMetrics: unexpected connection type %T", c)
		}
		m = mc.readMetrics()
		return nil
	})
	return m, err
}

// readMetrics takes a snapshot from the engine metrics store.
func (c *Conn) readMetrics() Metrics {
	s := c.db.ReadEngineMetrics()
	return Metrics{
		PageCacheHits:      s.PageCacheHits,
		PageCacheMisses:    s.PageCacheMisses,
		PageCacheEvictions: s.PageCacheEvictions,
		PageCacheSize:      s.PageCacheSize,
		PageCacheCapacity:  s.PageCacheCapacity,
		WALFramesWritten:   s.WALFramesWritten,
		WALCheckpoints:     s.WALCheckpoints,
		WALCurrentFrames:   s.WALCurrentFrames,
		TxCommits:          s.TxCommits,
		TxRollbacks:        s.TxRollbacks,
		QueriesTotal:       s.QueriesTotal,
		QueriesSlow:        s.QueriesSlow,
		SortsInMemory:      s.SortsInMemory,
		SortSpillRuns:      s.SortSpillRuns,
		SortSpillBytes:     s.SortSpillBytes,
	}
}
