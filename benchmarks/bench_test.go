//go:build bench

package benchmarks

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/RichardKnop/minisql"
	_ "modernc.org/sqlite"
)

// dbDriver groups the driver name, DSN builder, and schema variants for each
// database under test.
type dbDriver struct {
	name          string
	driverName    string
	dsn           func(path string) string
	afterOpen     func(db *sql.DB) error
	createTable   string
	insertRow     string // uses ? placeholders; col order: (name, age, email)
	insertRowNoID string // insert without providing id (single row)
	// insertMultiRows(n) returns an INSERT with n value tuples, each (name, age, email).
	insertMultiRows func(n int) string
}

// buildMultiValueInsert constructs an INSERT statement with n value tuples
// each containing 3 ? placeholders, using the provided header
// (everything up to and including "values").
func buildMultiValueInsert(header string, n int) string {
	var b strings.Builder
	b.WriteString(header)
	b.WriteString(" values ")
	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?, ?, ?)")
	}
	return b.String()
}

var drivers = []dbDriver{
	{
		name:       "minisql",
		driverName: "minisql",
		dsn:        func(path string) string { return path },
		afterOpen:  nil,
		createTable: `create table "bench_rows" (
			id    int8 primary key autoincrement,
			name  varchar(255),
			age   int4,
			email varchar(255)
		)`,
		insertRow:     `insert into "bench_rows" (name, age, email) values (?, ?, ?)`,
		insertRowNoID: `insert into "bench_rows" (name, age, email) values (?, ?, ?)`,
		insertMultiRows: func(n int) string {
			return buildMultiValueInsert(`insert into "bench_rows" (name, age, email)`, n)
		},
	},
	{
		name:       "sqlite",
		driverName: "sqlite",
		dsn:        func(path string) string { return path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)" },
		afterOpen:  nil,
		createTable: `CREATE TABLE IF NOT EXISTS bench_rows (
			id    INTEGER PRIMARY KEY AUTOINCREMENT,
			name  TEXT,
			age   INTEGER,
			email TEXT
		)`,
		insertRow:     `INSERT INTO bench_rows (name, age, email) VALUES (?, ?, ?)`,
		insertRowNoID: `INSERT INTO bench_rows (name, age, email) VALUES (?, ?, ?)`,
		insertMultiRows: func(n int) string {
			return buildMultiValueInsert(`INSERT INTO bench_rows (name, age, email)`, n)
		},
	},
}

// openDB opens a temporary database for the given driver, executes afterOpen
// (e.g. PRAGMAs), creates the bench_rows table, and returns the db plus a
// cleanup function that removes all temp files.
func openDB(t testing.TB, d dbDriver) (*sql.DB, func()) {
	t.Helper()

	f, err := os.CreateTemp("", "bench_"+d.name+"_")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	db, err := sql.Open(d.driverName, d.dsn(path))
	if err != nil {
		t.Fatalf("open db (%s): %v", d.name, err)
	}
	// Single connection — removes connection-pool variance from measurements.
	db.SetMaxOpenConns(1)

	if d.afterOpen != nil {
		if err := d.afterOpen(db); err != nil {
			t.Fatalf("afterOpen (%s): %v", d.name, err)
		}
	}

	mustExec(t, db, d.createTable)

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(path + "-wal")
		_ = os.Remove(path + "-shm")
	}
	return db, cleanup
}

// mustExec runs a statement and fails the benchmark immediately on error.
// When args are provided the query is executed via a prepared statement so that
// drivers (like minisql) that require Prepare for placeholder substitution work
// correctly.
func mustExec(t testing.TB, db *sql.DB, query string, args ...any) sql.Result {
	t.Helper()
	if len(args) == 0 {
		res, err := db.Exec(query)
		if err != nil {
			t.Fatalf("exec %q: %v", query, err)
		}
		return res
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatalf("prepare %q: %v", query, err)
	}
	defer stmt.Close()
	res, err := stmt.Exec(args...)
	if err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
	return res
}

// seedRows inserts n rows into bench_rows using the driver's insertRowNoID
// template. Row values are deterministic so they compress/cache uniformly.
func seedRows(t testing.TB, db *sql.DB, d dbDriver, n int) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare(d.insertRowNoID)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("prepare insert: %v", err)
	}
	defer stmt.Close()
	for i := range n {
		name := fmt.Sprintf("user-%06d", i)
		email := fmt.Sprintf("user%06d@example.com", i)
		if _, err := stmt.Exec(name, i%100, email); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
}
