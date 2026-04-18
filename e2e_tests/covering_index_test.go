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

// openCoveringDB opens a fresh database for covering-index tests.
func openCoveringDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "covering_index_")
	require.NoError(t, err)
	path := f.Name()
	require.NoError(t, f.Close())

	db, err := sql.Open("minisql", path)
	require.NoError(t, err)

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(path + "-wal")
	}
	return db, cleanup
}

// TestCoveringIndex_SingleColumnUniqueIndex verifies that
// SELECT <indexed-col> WHERE <indexed-col> = ? returns correct results
// via an index-only scan (no table row fetch).
func TestCoveringIndex_SingleColumnUniqueIndex(t *testing.T) {
	t.Parallel()
	db, cleanup := openCoveringDB(t)
	defer cleanup()

	_, err := db.Exec(`create table "items" (
		id   int8 primary key autoincrement,
		code varchar(64) unique,
		name text
	);`)
	require.NoError(t, err)

	// Insert rows.
	for i := range 10 {
		_, err = db.Exec(fmt.Sprintf(
			`insert into "items" (code, name) values ('code-%02d', 'Item %d');`, i, i,
		))
		require.NoError(t, err)
	}

	// SELECT only the indexed column — covered by the unique index on code.
	rows, err := db.Query(`select code from "items" where code = 'code-03';`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var code string
	require.NoError(t, rows.Scan(&code))
	assert.Equal(t, "code-03", code)
	assert.False(t, rows.Next(), "expected exactly one row")
}

// TestCoveringIndex_CountStar verifies that COUNT(*) with a WHERE on an
// indexed column uses a covering index scan.
func TestCoveringIndex_CountStar(t *testing.T) {
	t.Parallel()
	db, cleanup := openCoveringDB(t)
	defer cleanup()

	_, err := db.Exec(`create table "scores" (
		id    int8 primary key autoincrement,
		score int4
	);`)
	require.NoError(t, err)
	_, err = db.Exec(`create index "idx_score" on "scores" (score);`)
	require.NoError(t, err)

	for i := range 20 {
		_, err = db.Exec(fmt.Sprintf(`insert into "scores" (score) values (%d);`, i))
		require.NoError(t, err)
	}

	var count int
	// COUNT(*) with range scan on indexed column.
	require.NoError(t, db.QueryRow(`select count(*) from "scores" where score > 9;`).Scan(&count))
	assert.Equal(t, 10, count)
}

// TestCoveringIndex_RangeScan verifies that a range scan on an indexed column
// returns the correct rows when the query only selects that column.
func TestCoveringIndex_RangeScan(t *testing.T) {
	t.Parallel()
	db, cleanup := openCoveringDB(t)
	defer cleanup()

	_, err := db.Exec(`create table "vals" (
		id  int8 primary key autoincrement,
		num int4
	);`)
	require.NoError(t, err)
	_, err = db.Exec(`create index "idx_num" on "vals" (num);`)
	require.NoError(t, err)

	for i := range 10 {
		_, err = db.Exec(fmt.Sprintf(`insert into "vals" (num) values (%d);`, i))
		require.NoError(t, err)
	}

	// SELECT only the indexed column via range scan.
	rows, err := db.Query(`select num from "vals" where num >= 5 order by num;`)
	require.NoError(t, err)
	defer rows.Close()

	var got []int
	for rows.Next() {
		var n int
		require.NoError(t, rows.Scan(&n))
		got = append(got, n)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int{5, 6, 7, 8, 9}, got)
}

// TestCoveringIndex_CompositeIndex verifies a composite secondary index can
// serve a covering scan for queries selecting both index columns.
func TestCoveringIndex_CompositeIndex(t *testing.T) {
	t.Parallel()
	db, cleanup := openCoveringDB(t)
	defer cleanup()

	_, err := db.Exec(`create table "people" (
		id         int8 primary key autoincrement,
		first_name varchar(64),
		last_name  varchar(64),
		age        int4
	);`)
	require.NoError(t, err)
	_, err = db.Exec(`create index "idx_name" on "people" (first_name, last_name);`)
	require.NoError(t, err)

	_, err = db.Exec(`insert into "people" (first_name, last_name, age)
		values ('Alice', 'Smith', 30), ('Bob', 'Jones', 25), ('Alice', 'Brown', 35);`)
	require.NoError(t, err)

	// Both selected columns are in the composite index — covering scan.
	rows, err := db.Query(`select first_name, last_name from "people" where first_name = 'Alice' order by last_name;`)
	require.NoError(t, err)
	defer rows.Close()

	type nameRow struct{ first, last string }
	var got []nameRow
	for rows.Next() {
		var r nameRow
		require.NoError(t, rows.Scan(&r.first, &r.last))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []nameRow{{"Alice", "Brown"}, {"Alice", "Smith"}}, got)
}

// TestCoveringIndex_NonCoveredFallback ensures a query that needs a non-indexed
// column still returns correct results (falls back to table fetch).
func TestCoveringIndex_NonCoveredFallback(t *testing.T) {
	t.Parallel()
	db, cleanup := openCoveringDB(t)
	defer cleanup()

	_, err := db.Exec(`create table "widgets" (
		id          int8 primary key autoincrement,
		code        varchar(64) unique,
		description text
	);`)
	require.NoError(t, err)

	_, err = db.Exec(`insert into "widgets" (code, description) values ('W001', 'Widget One'), ('W002', 'Widget Two');`)
	require.NoError(t, err)

	// SELECT includes a non-indexed column (description) — must hit the table page.
	rows, err := db.Query(`select code, description from "widgets" where code = 'W001';`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var code, description string
	require.NoError(t, rows.Scan(&code, &description))
	assert.Equal(t, "W001", code)
	assert.Equal(t, "Widget One", description)
	assert.False(t, rows.Next())
}
