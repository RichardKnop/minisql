package minisql

import "sync/atomic"

// EngineSnapshot is a point-in-time copy of all engine-level counters and gauges.
// Counter fields are cumulative since the database was opened.
// Gauge fields (PageCacheSize, WALCurrentFrames) reflect instantaneous state.
type EngineSnapshot struct {
	PageCacheHits      int64
	PageCacheMisses    int64
	PageCacheEvictions int64
	PageCacheSize      int64
	PageCacheCapacity  int64
	WALFramesWritten   int64
	WALCheckpoints     int64
	WALCurrentFrames   int64
	TxCommits          int64
	TxRollbacks        int64
	QueriesTotal       int64
	QueriesSlow        int64
	SortsInMemory      int64
	SortSpillRuns      int64
	SortSpillBytes     int64
}

// engineMetrics holds all engine-level performance counters and gauges.
// Every field is updated atomically so the struct is safe to read and write
// from concurrent goroutines without additional locking.
type engineMetrics struct {
	// Page cache
	pageCacheHits      atomic.Int64
	pageCacheMisses    atomic.Int64
	pageCacheEvictions atomic.Int64
	pageCacheSize      atomic.Int64 // gauge: pages currently held in LRU cache
	pageCacheCapacity  int64        // const after init: max_cached_pages setting

	// WAL
	walFramesWritten atomic.Int64 // cumulative frames written by AppendTransaction
	walCheckpoints   atomic.Int64 // cumulative Checkpoint calls
	walCurrentFrames atomic.Int64 // gauge: frames currently in the WAL

	// Transactions
	txCommits   atomic.Int64 // write transactions successfully committed
	txRollbacks atomic.Int64 // transactions rolled back

	// Queries
	queriesTotal atomic.Int64
	queriesSlow  atomic.Int64

	// Sort
	sortsInMemory  atomic.Int64 // ORDER BY completed without spilling to disk
	sortSpillRuns  atomic.Int64 // cumulative run files written to disk
	sortSpillBytes atomic.Int64 // cumulative bytes written to run files
}

func (m *engineMetrics) recordQuery(slow bool) {
	m.queriesTotal.Add(1)
	if slow {
		m.queriesSlow.Add(1)
	}
}

func (m *engineMetrics) snapshot() EngineSnapshot {
	return EngineSnapshot{
		PageCacheHits:      m.pageCacheHits.Load(),
		PageCacheMisses:    m.pageCacheMisses.Load(),
		PageCacheEvictions: m.pageCacheEvictions.Load(),
		PageCacheSize:      m.pageCacheSize.Load(),
		PageCacheCapacity:  m.pageCacheCapacity,
		WALFramesWritten:   m.walFramesWritten.Load(),
		WALCheckpoints:     m.walCheckpoints.Load(),
		WALCurrentFrames:   m.walCurrentFrames.Load(),
		TxCommits:          m.txCommits.Load(),
		TxRollbacks:        m.txRollbacks.Load(),
		QueriesTotal:       m.queriesTotal.Load(),
		QueriesSlow:        m.queriesSlow.Load(),
		SortsInMemory:      m.sortsInMemory.Load(),
		SortSpillRuns:      m.sortSpillRuns.Load(),
		SortSpillBytes:     m.sortSpillBytes.Load(),
	}
}
