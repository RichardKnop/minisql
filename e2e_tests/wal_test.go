package e2etests

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/RichardKnop/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openWALDB opens (or creates) a WAL-mode database at dbPath.
// WAL is the only supported mode; no connection string parameter is required.
func openWALDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("minisql", dbPath)
	require.NoError(t, err)
	return db
}

func TestWAL_BasicReadWrite(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", "wal_basic_")
	require.NoError(t, err)
	dbPath := dbFile.Name()
	require.NoError(t, dbFile.Close())
	t.Cleanup(func() {
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
	})

	db := openWALDB(t, dbPath)
	defer func() { require.NoError(t, db.Close()) }()

	_, err = db.Exec(createUsersTableSQL)
	require.NoError(t, err)

	// Insert rows using a prepared statement (the driver requires Prepare for args).
	const wantRows = 10
	stmt, err := db.Prepare(`insert into "users" (email, name) values (?, ?);`)
	require.NoError(t, err)
	defer func() { require.NoError(t, stmt.Close()) }()

	g := newDataGen(42)
	for _, u := range g.Users(wantRows) {
		_, err = stmt.Exec(u.Email.String, u.Name.String)
		require.NoError(t, err)
	}

	// Reads must see all committed rows through the in-memory WAL index.
	var count int
	require.NoError(t, db.QueryRow(`select count(*) from "users";`).Scan(&count))
	assert.Equal(t, wantRows, count)
}

func TestWAL_DataDurability(t *testing.T) {
	// Verifies that data written before Close is fully durable across a
	// close/reopen cycle.  On close a passive checkpoint flushes all committed
	// WAL frames to the DB file (SQLite-style), so the WAL is truncated and the
	// DB file is a complete snapshot.  The second open must see all rows via the
	// DB file (WAL index is empty after the checkpoint).
	t.Parallel()

	dbFile, err := os.CreateTemp("", "wal_recovery_")
	require.NoError(t, err)
	dbPath := dbFile.Name()
	require.NoError(t, dbFile.Close())
	t.Cleanup(func() {
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
	})

	const wantRows = 20

	// --- First open: write data and close ---
	{
		db := openWALDB(t, dbPath)

		_, err = db.Exec(createUsersTableSQL)
		require.NoError(t, err)

		stmt, err := db.Prepare(`insert into "users" (email, name) values (?, ?);`)
		require.NoError(t, err)

		g := newDataGen(99)
		for _, u := range g.Users(wantRows) {
			_, err = stmt.Exec(u.Email.String, u.Name.String)
			require.NoError(t, err)
		}
		require.NoError(t, stmt.Close())

		// Verify before close.
		var count int
		require.NoError(t, db.QueryRow(`select count(*) from "users";`).Scan(&count))
		assert.Equal(t, wantRows, count)

		// Close triggers a passive checkpoint — WAL is truncated to header only.
		require.NoError(t, db.Close())
	}

	// WAL must be truncated to header-only after the checkpoint-on-close.
	info, err := os.Stat(dbPath + "-wal")
	require.NoError(t, err, "WAL file should still exist (header preserved)")
	assert.LessOrEqual(t, info.Size(), int64(32),
		"WAL file should be truncated to header-only size after checkpoint-on-close")

	// --- Second open: data must be fully visible from the DB file ---
	{
		db := openWALDB(t, dbPath)
		defer func() { require.NoError(t, db.Close()) }()

		var count int
		require.NoError(t, db.QueryRow(`select count(*) from "users";`).Scan(&count))
		assert.Equal(t, wantRows, count,
			"all rows written before close must be visible after reopen")
	}
}

func TestWAL_Checkpoint(t *testing.T) {
	// Verifies that PRAGMA wal_checkpoint:
	//   1. Copies all WAL frames into the main DB file.
	//   2. Truncates the WAL file (frames = 0 after checkpoint).
	//   3. Data remains fully visible after the checkpoint.
	t.Parallel()

	dbFile, err := os.CreateTemp("", "wal_checkpoint_")
	require.NoError(t, err)
	dbPath := dbFile.Name()
	require.NoError(t, dbFile.Close())
	t.Cleanup(func() {
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
	})

	db := openWALDB(t, dbPath)
	defer func() { require.NoError(t, db.Close()) }()

	_, err = db.Exec(createUsersTableSQL)
	require.NoError(t, err)

	const wantRows = 15
	for i := range wantRows {
		_, err = db.Exec(fmt.Sprintf(
			`insert into "users" (email, name) values ('chk%d@example.com', 'Chk %d');`, i, i,
		))
		require.NoError(t, err)
	}

	// WAL file must exist before checkpoint.
	walPath := dbPath + "-wal"
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "WAL file should be non-empty before checkpoint")

	// Run the pragma checkpoint.
	rows, err := db.Query(`PRAGMA wal_checkpoint;`)
	require.NoError(t, err)
	require.True(t, rows.Next())
	var status string
	require.NoError(t, rows.Scan(&status))
	assert.Equal(t, "ok", status)
	require.NoError(t, rows.Close())

	// After checkpoint the WAL must be empty (truncated to header only).
	info, err = os.Stat(walPath)
	require.NoError(t, err)
	assert.LessOrEqual(t, info.Size(), int64(32),
		"WAL file should be truncated to header-only size after checkpoint")

	// Data must still be fully readable.
	var count int
	require.NoError(t, db.QueryRow(`select count(*) from "users";`).Scan(&count))
	assert.Equal(t, wantRows, count)
}

func TestWAL_CheckpointThenWrite(t *testing.T) {
	// After a checkpoint, new writes go to a fresh WAL and remain visible.
	t.Parallel()

	dbFile, err := os.CreateTemp("", "wal_chk_then_write_")
	require.NoError(t, err)
	dbPath := dbFile.Name()
	require.NoError(t, dbFile.Close())
	t.Cleanup(func() {
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
	})

	db := openWALDB(t, dbPath)
	defer func() { require.NoError(t, db.Close()) }()

	_, err = db.Exec(createUsersTableSQL)
	require.NoError(t, err)

	// First batch before checkpoint.
	const firstBatch = 5
	for i := range firstBatch {
		_, err = db.Exec(fmt.Sprintf(
			`insert into "users" (email, name) values ('pre%d@example.com', 'Pre %d');`, i, i,
		))
		require.NoError(t, err)
	}

	// Checkpoint.
	rows, err := db.Query(`PRAGMA wal_checkpoint;`)
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	// Second batch after checkpoint.
	const secondBatch = 3
	for i := range secondBatch {
		_, err = db.Exec(fmt.Sprintf(
			`insert into "users" (email, name) values ('post%d@example.com', 'Post %d');`, i, i,
		))
		require.NoError(t, err)
	}

	var count int
	require.NoError(t, db.QueryRow(`select count(*) from "users";`).Scan(&count))
	assert.Equal(t, firstBatch+secondBatch, count)
}
