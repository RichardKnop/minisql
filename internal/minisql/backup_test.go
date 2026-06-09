package minisql

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// backupColumns is a minimal two-column schema for backup tests.
var backupColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Int8, Size: 8, Name: "v"},
}

// newBackupTestDB creates a WAL-enabled temp database and returns the
// Database, its file path, and the WAL file path (for cleanup).
func newBackupTestDB(t *testing.T) (*Database, string) {
	t.Helper()

	dbFile, err := os.CreateTemp("", "backup_test_*.db")
	require.NoError(t, err)
	dbPath := dbFile.Name()

	walIndex := NewWALIndex()
	wal, _, err := OpenWALAndRebuildIndex(dbPath, PageSize, walIndex)
	require.NoError(t, err)

	pager, err := NewPager(dbFile, PageSize, PageCacheSize)
	require.NoError(t, err)

	db, err := NewDatabase(
		context.Background(), testLogger, dbPath, nil, pager, pager,
		&WALConfig{
			WAL:                 wal,
			Index:               walIndex,
			DBFile:              dbFile,
			CheckpointThreshold: 10000, // high threshold: no auto-checkpoint
		},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
	})

	return db, dbPath
}

// insertBackupRows inserts n rows with v = startVal, startVal+1, ... into tbl.
func insertBackupRows(t *testing.T, db *Database, tbl *Table, n, startVal int) {
	t.Helper()
	ctx := context.Background()
	fields := fieldsFromColumns(tbl.Columns...)
	err := db.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		for i := 0; i < n; i++ {
			row := []OptionalValue{
				{Value: int64(startVal + i)},
				{Value: int64(startVal + i)},
			}
			if _, err := tbl.Insert(txCtx, Statement{
				Kind:    Insert,
				Fields:  fields,
				Inserts: [][]OptionalValue{row},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// countBackupRows returns the row count in tbl via a direct table scan.
func countBackupRows(t *testing.T, db *Database, tbl *Table) int {
	t.Helper()
	ctx := context.Background()
	count := 0
	err := db.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		result, err := tbl.Select(txCtx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(tbl.Columns...),
		})
		if err != nil {
			return err
		}
		for result.Rows.Next(txCtx) {
			count++
		}
		return result.Rows.Err()
	})
	require.NoError(t, err)
	return count
}

// TestBackup_CheckpointBlockedDuringCopy is the targeted test for the
// correctness edge case: a write transaction that commits after the WAL
// snapshot must not appear in the backup, even when it triggers a checkpoint
// that would otherwise flush its pages into the main DB file.
//
// The backupHook fires after the WAL snapshot is taken and walWriteMu is
// released — exactly the window where a checkpoint could contaminate the DB
// file if the backup did not hold a read-only snapshot transaction.
//
// The test verifies two things:
//  1. The checkpoint attempt INSIDE the hook is rejected with
//     ErrCheckpointBlockedByReaders — proving the backup's snapshot
//     transaction is live and correctly blocking checkpoints.
//  2. The backup file contains only the data committed before the snapshot,
//     not the data written by the hook.
func TestBackup_CheckpointBlockedDuringCopy(t *testing.T) {
	ctx := context.Background()
	db, dbPath := newBackupTestDB(t)

	// Create the test table.
	createStmt := Statement{
		Kind:      CreateTable,
		TableName: "items",
		Columns:   append([]Column{}, backupColumns...),
	}
	err := db.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		_, err := db.ExecuteStatement(txCtx, createStmt)
		return err
	})
	require.NoError(t, err)

	tbl, ok := db.tables["items"]
	require.True(t, ok)

	// Write 10 pre-snapshot rows. These must appear in the backup.
	insertBackupRows(t, db, tbl, 10, 1)

	// Force a checkpoint so the pre-snapshot rows are in the DB file and the
	// WAL is empty before we take the backup snapshot.
	require.NoError(t, db.Checkpoint(ctx))

	// Write 5 more rows to the WAL (not yet checkpointed).
	// These are also pre-snapshot and must appear in the backup.
	insertBackupRows(t, db, tbl, 5, 100)

	// Verify the live database has 15 rows before backup.
	assert.Equal(t, 15, countBackupRows(t, db, tbl))

	// Install the hook. It fires after the backup has taken its WAL snapshot
	// and released walWriteMu but before the page-copy loop starts.
	var (
		hookFired          bool
		checkpointBlocked  bool
	)
	db.backupHook = func() {
		hookFired = true

		// Write 5 post-snapshot rows. These must NOT appear in the backup.
		insertBackupRows(t, db, tbl, 5, 200)

		// Attempt a manual checkpoint. The backup holds a read-only snapshot
		// transaction, so this must be rejected.
		checkpointErr := db.Checkpoint(ctx)
		checkpointBlocked = errors.Is(checkpointErr, ErrCheckpointBlockedByReaders)
	}

	destPath := dbPath + ".backup"
	t.Cleanup(func() {
		os.Remove(destPath)
		os.Remove(destPath + "-wal")
	})

	require.NoError(t, db.Backup(ctx, destPath))

	// The hook must have fired and the checkpoint must have been blocked.
	assert.True(t, hookFired, "backupHook did not fire")
	assert.True(t, checkpointBlocked,
		"checkpoint should have been blocked by the backup's snapshot transaction")

	// Open the backup as a fresh database and count its rows.
	// MockParser is required so initTable can parse the stored DDL for "items".
	backupParser := new(MockParser)
	backupParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil)

	backupFile, err := os.OpenFile(destPath, os.O_RDWR, 0o600)
	require.NoError(t, err)

	backupWALIndex := NewWALIndex()
	backupWAL, _, err := OpenWALAndRebuildIndex(destPath, PageSize, backupWALIndex)
	require.NoError(t, err)

	backupPager, err := NewPager(backupFile, PageSize, PageCacheSize)
	require.NoError(t, err)

	backupDB, err := NewDatabase(
		ctx, testLogger, destPath, backupParser, backupPager, backupPager,
		&WALConfig{WAL: backupWAL, Index: backupWALIndex, DBFile: backupFile},
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, backupDB.Close())
		os.Remove(destPath + "-wal")
	}()

	backupTbl, ok := backupDB.tables["items"]
	require.True(t, ok, "backup database must have the 'items' table")

	backupCount := countBackupRows(t, backupDB, backupTbl)

	// Backup must contain the 10 DB-file rows + 5 WAL-snapshot rows = 15.
	// The 5 rows written by the hook (post-snapshot) must NOT be present.
	assert.Equal(t, 15, backupCount,
		"backup should contain exactly 15 rows (10 checkpointed + 5 from WAL snapshot), not the 5 post-snapshot rows written during the hook")
}
