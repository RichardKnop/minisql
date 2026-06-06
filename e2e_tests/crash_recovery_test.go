package e2etests

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	_ "github.com/RichardKnop/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --------------------------------------------------------------------------
// Crash-scenario subprocess infrastructure
//
// Go tests run in a single process, so we cannot simulate a hard crash (one
// that bypasses all defer/cleanup) from inside a test. Instead each crash
// scenario is executed by re-running the test binary as a subprocess; the
// subprocess calls os.Exit(1) at the right moment, leaving the database files
// in the exact state they would be in after a real process crash.
//
// TestMain is the entry point. When MINISQL_CRASH_SCENARIO is set, TestMain
// runs the requested scenario (which always calls os.Exit(1)) and never calls
// m.Run(). All other tests run normally when the env var is absent.
// --------------------------------------------------------------------------

const (
	crashScenarioEnv = "MINISQL_CRASH_SCENARIO"
	crashDBEnv       = "MINISQL_CRASH_DB"
)

// TestMain enables the crash-scenario worker mode used by crash recovery tests.
func TestMain(m *testing.M) {
	if scenario := os.Getenv(crashScenarioEnv); scenario != "" {
		dbPath := os.Getenv(crashDBEnv)
		if dbPath == "" {
			fmt.Fprintln(os.Stderr, "MINISQL_CRASH_DB not set")
			os.Exit(2)
		}
		runCrashScenario(scenario, dbPath)
		os.Exit(0) // unreachable: runCrashScenario always calls os.Exit(1)
	}
	os.Exit(m.Run())
}

// runCrashScenario performs the requested database operations and then calls
// os.Exit(1), leaving the database in a crashed (not checkpointed) state.
// The db.Close() is intentionally never called: this is the crash simulation.
//
// wal_write_buffer_size=0 disables write buffering so every committed
// transaction is immediately written to the WAL file.  Without this, the
// default 64 KiB write buffer holds the last few transactions in Go heap
// memory; os.Exit(1) terminates the process before the buffer is flushed,
// and those transactions are silently lost — making crash recovery untestable.
func runCrashScenario(scenario, dbPath string) {
	db, err := sql.Open("minisql", dbPath+"?wal_write_buffer_size=0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "crash scenario %q: open DB: %v\n", scenario, err)
		os.Exit(2)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	switch scenario {
	case "single_txn":
		// CREATE TABLE + 10 single-row inserts (each auto-committed separately).
		crashMustExec(db, createUsersTableSQL)
		for i := range 10 {
			crashMustExec(db, fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('user%d@example.com', 'User %d')`,
				i, i,
			))
		}

	case "multi_txn":
		// Two batches of inserts across separate auto-commit transactions.
		crashMustExec(db, createUsersTableSQL)
		for i := range 5 {
			crashMustExec(db, fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('a%d@example.com', 'A %d')`,
				i, i,
			))
		}
		for i := 5; i < 10; i++ {
			crashMustExec(db, fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('b%d@example.com', 'B %d')`,
				i, i,
			))
		}

	case "with_deletes":
		// Insert 10 rows then delete 3 — verifies DELETE pages are replayed.
		crashMustExec(db, createUsersTableSQL)
		for i := range 10 {
			crashMustExec(db, fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('u%d@example.com', 'U %d')`,
				i, i,
			))
		}
		crashMustExec(db, `DELETE FROM "users" WHERE id <= 3`)

	case "with_index":
		// CREATE TABLE + CREATE INDEX + 10 inserts — verifies index pages are replayed.
		// Index on "created" (timestamp): text columns are unsupported for B-tree indexes;
		// email already has an implicit unique index so a second one would fail.
		crashMustExec(db, createUsersTableSQL)
		crashMustExec(db, `CREATE INDEX idx_created ON "users" (created)`)
		for i := range 10 {
			crashMustExec(db, fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('i%d@example.com', 'I %d')`,
				i, i,
			))
		}

	case "large_wal":
		// 200 rows spread across many auto-commit transactions — stress test.
		crashMustExec(db, createUsersTableSQL)
		for i := range 200 {
			crashMustExec(db, fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('u%d@example.com', 'U %d')`,
				i, i,
			))
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown crash scenario: %q\n", scenario)
		os.Exit(2)
	}

	// Simulate crash: exit without db.Close() — no checkpoint, no WAL truncation.
	os.Exit(1)
}

func crashMustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		fmt.Fprintf(os.Stderr, "crashMustExec %q: %v\n", query, err)
		os.Exit(2)
	}
}

// spawnAndCrash re-runs the test binary as a subprocess with the requested
// crash scenario. The subprocess is expected to exit with code 1.
func spawnAndCrash(t *testing.T, dbPath, scenario string) {
	t.Helper()
	// -test.run=^$ matches no test functions; TestMain handles the scenario.
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		crashScenarioEnv+"="+scenario,
		crashDBEnv+"="+dbPath,
	)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		require.Equal(t, 1, exitErr.ExitCode(),
			"crash subprocess must exit with code 1 (simulated crash)")
		return
	}
	t.Fatalf("crash subprocess did not exit with code 1: %v", err)
}

// openCrashedDB opens a previously crashed database for recovery verification.
// The connection has the standard single-connection settings and is closed by
// t.Cleanup.
func openCrashedDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("minisql", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// --------------------------------------------------------------------------
// Crash recovery tests
// --------------------------------------------------------------------------

func TestCrashRecovery_AfterSingleTransaction(t *testing.T) {
	// Crash after 10 committed inserts, before any checkpoint.
	// The WAL contains committed frames; on reopen they are replayed from
	// the WAL index so all 10 rows are immediately visible.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "single_txn")

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	assert.Equal(t, 10, count, "all 10 rows must survive crash recovery via WAL replay")
}

func TestCrashRecovery_AfterMultipleTransactions(t *testing.T) {
	// Crash after two batches of inserts (5 rows each) committed separately.
	// Both transaction groups must be visible after WAL replay.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "multi_txn")

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	assert.Equal(t, 10, count, "both transaction batches must be visible after recovery")
}

func TestCrashRecovery_WithDeletesBeforeCrash(t *testing.T) {
	// Crash after 10 inserts + 3 deletes. DELETE page changes must be replayed
	// so the recovered row count is 7, not 10.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "with_deletes")

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	// 10 inserted, IDs 1-3 deleted (autoincrement starts at 1).
	assert.Equal(t, 7, count, "committed DELETEs must be reflected after WAL replay")
}

func TestCrashRecovery_WithIndexBeforeCrash(t *testing.T) {
	// Crash after CREATE INDEX + 10 inserts. Index pages committed to the WAL
	// must be replayed so the index is functional after recovery.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "with_index")

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	assert.Equal(t, 10, count, "rows must be visible after crash recovery including index pages")

	// Secondary index must be functional: email lookup via the unique index
	// (which is also recovered from WAL) must resolve correctly.
	var email string
	require.NoError(t, db.QueryRow(`SELECT email FROM "users" WHERE email = 'i5@example.com'`).Scan(&email))
	assert.Equal(t, "i5@example.com", email, "secondary indexes must work correctly after WAL recovery")
}

func TestCrashRecovery_LargeWAL(t *testing.T) {
	// Stress test: 200 rows committed across many auto-commit transactions.
	// Recovery must replay all frames correctly.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "large_wal")

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	assert.Equal(t, 200, count, "all 200 rows must survive crash recovery from a large WAL")
}

func TestCrashRecovery_WritesAfterRecovery(t *testing.T) {
	// After WAL-based crash recovery the database must remain writable.
	// Pattern: crash → recover (verify 10 rows) → write 5 more → close →
	// reopen (verify 15 rows).
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "single_txn")

	// Recovery phase.
	{
		db := openCrashedDB(t, dbPath)

		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
		require.Equal(t, 10, count, "10 rows must be visible immediately after recovery")

		for i := 10; i < 15; i++ {
			_, err := db.Exec(fmt.Sprintf(
				`INSERT INTO "users" (email, name) VALUES ('new%d@example.com', 'New %d')`,
				i, i,
			))
			require.NoError(t, err)
		}
		// Clean close checkpoints the WAL.
		require.NoError(t, db.Close())
	}

	// Second open: all 15 rows must survive the checkpoint-on-close.
	{
		db, err := sql.Open("minisql", dbPath)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer func() { _ = db.Close() }()

		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
		assert.Equal(t, 15, count,
			"15 rows (10 recovered + 5 new) must be visible after second reopen")
	}
}

func TestCrashRecovery_GarbageAppendedToWAL(t *testing.T) {
	// Simulates a crash that occurred while writing a second transaction:
	// garbage bytes are appended to the WAL after the committed frames.
	// ReadAllFrames stops at the first frame that fails the salt/CRC check,
	// so all previously committed data survives.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "single_txn")

	// Append bytes that will not match the WAL salt — any random data works
	// because the probability of accidentally matching two random 32-bit salts
	// is negligible (2^-64).
	walPath := dbPath + "-wal"
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8,
		0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	assert.Equal(t, 10, count, "committed rows must survive WAL with appended garbage bytes")
}

func TestCrashRecovery_TruncatedWALFrame(t *testing.T) {
	// Simulates a crash that truncated the last byte of the last WAL frame
	// (i.e., the process died while the OS was flushing the final page of the
	// last INSERT). ReadAllFrames must stop at the incomplete frame and discard
	// only the last transaction; all earlier committed transactions survive.
	t.Parallel()

	dbPath := t.TempDir() + "/crash.db"
	t.Cleanup(func() { _ = os.Remove(dbPath + "-wal") })
	spawnAndCrash(t, dbPath, "single_txn") // commits 11 transactions: CREATE + 10 INSERTs

	walPath := dbPath + "-wal"
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(33),
		"WAL must contain at least one frame beyond the header for this test to be meaningful")

	// Truncate the final byte of the last frame's page data.
	// ReadAllFrames will get io.ErrUnexpectedEOF reading that page, break out of
	// the loop, and discard only the last (truncated) transaction — the 10th INSERT.
	require.NoError(t, os.Truncate(walPath, info.Size()-1))

	db := openCrashedDB(t, dbPath)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count))
	// The last INSERT is lost (its commit frame was truncated); the 9 earlier
	// INSERTs and the CREATE TABLE all survive.
	assert.Equal(t, 9, count,
		"only the last (truncated) insert must be lost; the 9 earlier inserts must survive")
}
