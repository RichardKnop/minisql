package e2etests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql"
)

func openBackupDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	f, err := os.CreateTemp("", "minisql_backup_src_*.db")
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
	return db, dbPath
}

func tempBackupPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "minisql_backup_dst_*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	os.Remove(path) // Backup creates the file itself
	t.Cleanup(func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	})
	return path
}

func openReadOnly(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("minisql", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// TestBackup_Basic verifies that Backup produces a complete, readable copy
// of the database with all rows intact.
func TestBackup_Basic(t *testing.T) {
	ctx := context.Background()
	src, _ := openBackupDB(t)
	destPath := tempBackupPath(t)

	_, err := src.ExecContext(ctx, `create table "items" (id int8 primary key autoincrement, name varchar(255))`)
	require.NoError(t, err)

	const rowCount = 100
	for i := 0; i < rowCount; i++ {
		_, err = src.ExecContext(ctx, `insert into "items" (name) values (?)`, fmt.Sprintf("item_%04d", i))
		require.NoError(t, err)
	}

	require.NoError(t, minisql.Backup(ctx, src, destPath))

	dst := openReadOnly(t, destPath)
	rows, err := dst.QueryContext(ctx, `select id, name from "items" order by id asc`)
	require.NoError(t, err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var id int64
		var name string
		require.NoError(t, rows.Scan(&id, &name))
		got = append(got, name)
	}
	require.NoError(t, rows.Err())
	assert.Len(t, got, rowCount)
	assert.Equal(t, "item_0000", got[0])
	assert.Equal(t, fmt.Sprintf("item_%04d", rowCount-1), got[len(got)-1])
}

// TestBackup_WithPendingWALFrames verifies that Backup captures committed WAL
// frames that have not yet been checkpointed to the main DB file.
func TestBackup_WithPendingWALFrames(t *testing.T) {
	ctx := context.Background()
	// Use a high checkpoint threshold so no automatic checkpoint fires.
	f, err := os.CreateTemp("", "minisql_backup_wal_*.db")
	require.NoError(t, err)
	srcPath := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(srcPath)
		os.Remove(srcPath + "-wal")
	})
	src, err := sql.Open("minisql", fmt.Sprintf("%s?wal_checkpoint_threshold=100000", srcPath))
	require.NoError(t, err)
	src.SetMaxOpenConns(1)
	src.SetMaxIdleConns(1)
	t.Cleanup(func() { src.Close() })

	destPath := tempBackupPath(t)

	_, err = src.ExecContext(ctx, `create table "vals" (id int8 primary key autoincrement, v int8)`)
	require.NoError(t, err)

	const n = 50
	for i := 0; i < n; i++ {
		_, err = src.ExecContext(ctx, `insert into "vals" (v) values (?)`, i*2)
		require.NoError(t, err)
	}

	// Verify WAL has frames before backup (checkpoint threshold is very high).
	m, err := minisql.ReadMetrics(ctx, src)
	require.NoError(t, err)
	assert.Positive(t, m.WALCurrentFrames, "expected pending WAL frames before backup")

	require.NoError(t, minisql.Backup(ctx, src, destPath))

	dst := openReadOnly(t, destPath)
	var count int64
	require.NoError(t, dst.QueryRowContext(ctx, `select count(*) from "vals"`).Scan(&count))
	assert.Equal(t, int64(n), count)

	var sum int64
	require.NoError(t, dst.QueryRowContext(ctx, `select sum(v) from "vals"`).Scan(&sum))
	// sum of 0,2,4,...,98 = n*(n-1) = 50*49 = 2450 ... actually 2*sum(0..49) = 2*(49*50/2) = 2450
	assert.Equal(t, int64(n*(n-1)), sum)
}

// TestBackup_MultipleTables verifies that Backup captures all tables and their
// indexes correctly.
func TestBackup_MultipleTables(t *testing.T) {
	ctx := context.Background()
	src, _ := openBackupDB(t)
	destPath := tempBackupPath(t)

	_, err := src.ExecContext(ctx, `create table "users" (id int8 primary key autoincrement, email varchar(255) unique)`)
	require.NoError(t, err)
	_, err = src.ExecContext(ctx, `create table "posts" (id int8 primary key autoincrement, user_id int8, title varchar(255))`)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		_, err = src.ExecContext(ctx, `insert into "users" (email) values (?)`, fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}
	for i := 0; i < 40; i++ {
		_, err = src.ExecContext(ctx, `insert into "posts" (user_id, title) values (?, ?)`, (i%20)+1, fmt.Sprintf("Post %d", i))
		require.NoError(t, err)
	}

	require.NoError(t, minisql.Backup(ctx, src, destPath))

	dst := openReadOnly(t, destPath)

	var userCount, postCount int64
	require.NoError(t, dst.QueryRowContext(ctx, `select count(*) from "users"`).Scan(&userCount))
	require.NoError(t, dst.QueryRowContext(ctx, `select count(*) from "posts"`).Scan(&postCount))
	assert.Equal(t, int64(20), userCount)
	assert.Equal(t, int64(40), postCount)
}

// TestBackup_ConcurrentWrites verifies that Backup produces a consistent
// snapshot even when write transactions are committing concurrently.
func TestBackup_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	src, _ := openBackupDB(t)
	destPath := tempBackupPath(t)

	_, err := src.ExecContext(ctx, `create table "counters" (id int8 primary key autoincrement, v int8)`)
	require.NoError(t, err)

	// Seed some initial rows so the backup has something to copy.
	const seedRows = 30
	for i := 0; i < seedRows; i++ {
		_, err = src.ExecContext(ctx, `insert into "counters" (v) values (?)`, i)
		require.NoError(t, err)
	}

	// Run concurrent inserts while the backup executes.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				// Best-effort inserts; ignore errors (driver may queue behind backup).
				_, _ = src.ExecContext(ctx, `insert into "counters" (v) values (1)`)
			}
		}
	}()

	require.NoError(t, minisql.Backup(ctx, src, destPath))
	close(stop)
	wg.Wait()

	// The backup must be a valid, openable database with at least the seed rows.
	dst := openReadOnly(t, destPath)
	var count int64
	require.NoError(t, dst.QueryRowContext(ctx, `select count(*) from "counters"`).Scan(&count))
	assert.GreaterOrEqual(t, count, int64(seedRows), "backup must contain at least the seeded rows")
}

// TestBackup_IsIndependent verifies that writes to the source after the backup
// do not affect the backup file.
func TestBackup_IsIndependent(t *testing.T) {
	ctx := context.Background()
	src, _ := openBackupDB(t)
	destPath := tempBackupPath(t)

	_, err := src.ExecContext(ctx, `create table "data" (id int8 primary key autoincrement, x int8)`)
	require.NoError(t, err)
	_, err = src.ExecContext(ctx, `insert into "data" (x) values (1), (2), (3)`)
	require.NoError(t, err)

	require.NoError(t, minisql.Backup(ctx, src, destPath))

	// Mutate the source after backup.
	_, err = src.ExecContext(ctx, `insert into "data" (x) values (4), (5)`)
	require.NoError(t, err)
	_, err = src.ExecContext(ctx, `delete from "data" where x = 1`)
	require.NoError(t, err)

	// Source now has rows 2,3,4,5 (4 rows). Backup must still have 1,2,3 (3 rows).
	dst := openReadOnly(t, destPath)
	var count int64
	require.NoError(t, dst.QueryRowContext(ctx, `select count(*) from "data"`).Scan(&count))
	assert.Equal(t, int64(3), count)

	var minX, maxX int64
	require.NoError(t, dst.QueryRowContext(ctx, `select min(x), max(x) from "data"`).Scan(&minX, &maxX))
	assert.Equal(t, int64(1), minX)
	assert.Equal(t, int64(3), maxX)
}
