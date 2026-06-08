package e2etests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql"
)

func openMetricsDB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "minisql_metrics_*.db")
	require.NoError(t, err)
	dbPath := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
	})
	db, err := sql.Open("minisql", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// TestReadMetrics_Basic verifies that ReadMetrics returns a valid snapshot
// and that the counters reflect observable activity.
func TestReadMetrics_Basic(t *testing.T) {
	ctx := context.Background()
	db := openMetricsDB(t)

	// Schema + data setup.
	_, err := db.ExecContext(ctx, `create table "things" (id int8 primary key autoincrement, name varchar(255))`)
	require.NoError(t, err)

	const rowCount = 50
	for i := 0; i < rowCount; i++ {
		_, err = db.ExecContext(ctx, `insert into "things" (name) values (?)`, fmt.Sprintf("thing_%03d", i))
		require.NoError(t, err)
	}

	// Run a SELECT to produce cache activity and a sort.
	rows, err := db.QueryContext(ctx, `select id, name from "things" order by name asc`)
	require.NoError(t, err)
	n := 0
	for rows.Next() {
		var id int64
		var name string
		require.NoError(t, rows.Scan(&id, &name))
		n++
	}
	require.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, rowCount, n)

	m, err := minisql.ReadMetrics(ctx, db)
	require.NoError(t, err)

	// Page cache: at least some hits and misses since we read pages.
	assert.Positive(t, m.PageCacheHits+m.PageCacheMisses, "expected some page cache activity")
	assert.Positive(t, m.PageCacheCapacity, "capacity should reflect max_cached_pages")

	// WAL: at least one frame per write transaction (CREATE TABLE + inserts).
	assert.Positive(t, m.WALFramesWritten, "expected WAL frames from writes")

	// Transactions: CREATE TABLE + each INSERT = at least rowCount+1 commits.
	assert.GreaterOrEqual(t, m.TxCommits, int64(rowCount+1))
	assert.Zero(t, m.TxRollbacks, "no rollbacks expected")

	// Queries: CREATE TABLE + inserts + the SELECT above = at least rowCount+2.
	assert.GreaterOrEqual(t, m.QueriesTotal, int64(rowCount+2))

	// Sort: the ORDER BY select should have produced an in-memory sort (50 rows << 4MB limit).
	assert.Positive(t, m.SortsInMemory, "expected at least one in-memory sort")
	assert.Zero(t, m.SortSpillRuns, "no disk spill expected for 50 rows")
}

// TestReadMetrics_SpillCounters verifies that SortSpillRuns and SortSpillBytes
// are incremented when an ORDER BY query exceeds sort_mem_limit.
func TestReadMetrics_SpillCounters(t *testing.T) {
	ctx := context.Background()

	// Use a very low sort_mem_limit so even a handful of rows trigger a spill.
	f, err := os.CreateTemp("", "minisql_metrics_spill_*.db")
	require.NoError(t, err)
	dbPath := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
	})

	db, err := sql.Open("minisql", fmt.Sprintf("%s?sort_mem_limit=4096", dbPath))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.ExecContext(ctx, `create table "vals" (id int8 primary key autoincrement, v varchar(255))`)
	require.NoError(t, err)

	const n = 200
	for i := n; i >= 1; i-- {
		_, err = db.ExecContext(ctx, `insert into "vals" (v) values (?)`, fmt.Sprintf("val_%05d", i))
		require.NoError(t, err)
	}

	rows, err := db.QueryContext(ctx, `select v from "vals" order by v asc`)
	require.NoError(t, err)
	for rows.Next() {
	}
	rows.Close()
	require.NoError(t, rows.Err())

	m, err := minisql.ReadMetrics(ctx, db)
	require.NoError(t, err)

	assert.Positive(t, m.SortSpillRuns, "expected at least one spill run")
	assert.Positive(t, m.SortSpillBytes, "expected non-zero spill bytes")
	assert.Zero(t, m.SortsInMemory, "all sorts should have spilled")
}

// TestReadMetrics_WALCheckpoint verifies that WALCheckpoints is incremented
// after an explicit PRAGMA wal_checkpoint.
func TestReadMetrics_WALCheckpoint(t *testing.T) {
	ctx := context.Background()
	db := openMetricsDB(t)

	_, err := db.ExecContext(ctx, `create table "cp" (id int8 primary key)`)
	require.NoError(t, err)

	before, err := minisql.ReadMetrics(ctx, db)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `PRAGMA wal_checkpoint`)
	require.NoError(t, err)

	after, err := minisql.ReadMetrics(ctx, db)
	require.NoError(t, err)

	assert.Greater(t, after.WALCheckpoints, before.WALCheckpoints,
		"WALCheckpoints should increase after explicit checkpoint")
	assert.GreaterOrEqual(t, after.QueriesTotal, before.QueriesTotal+1)
}
