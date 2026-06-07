package e2etests

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/RichardKnop/minisql"
)

// openSpillDB opens a temporary MiniSQL database with sort_mem_limit set to
// limit bytes, registers cleanup, and returns the *sql.DB.
func openSpillDB(t *testing.T, limit int) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "minisql_sort_spill_*.db")
	require.NoError(t, err)
	dbPath := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
	})

	dsn := fmt.Sprintf("%s?sort_mem_limit=%d", dbPath, limit)
	db, err := sql.Open("minisql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// TestOrderBy_DiskSpill_DSN opens a fresh database with sort_mem_limit set low
// enough that a 1 000-row ORDER BY query crosses the threshold and spills sorted
// runs to disk. It verifies that the merged result is correctly ordered.
func TestOrderBy_DiskSpill_DSN(t *testing.T) {
	// 64 KB limit — 1 000 rows × ~150 bytes each ≈ 150 KB, forcing at least two spills.
	db := openSpillDB(t, 65536)

	_, err := db.Exec(`create table "items" (
		id   int8 primary key autoincrement,
		name varchar(255)
	)`)
	require.NoError(t, err)

	const rowCount = 1000
	// Insert in reverse-name order to ensure a naïve scan gives wrong results.
	for i := rowCount; i >= 1; i-- {
		name := fmt.Sprintf("item_%06d", i)
		_, err = db.Exec(`insert into "items" (name) values (?)`, name)
		require.NoError(t, err)
	}

	rows, err := db.Query(`select id, name from "items" order by name asc`)
	require.NoError(t, err)
	t.Cleanup(func() { rows.Close() })

	var names []string
	for rows.Next() {
		var id int64
		var name string
		require.NoError(t, rows.Scan(&id, &name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.Len(t, names, rowCount)

	// Verify that names are in ascending alphabetical order.
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i],
			"row %d: %q should come before %q", i, names[i-1], names[i])
	}

	// Spot-check: first and last names.
	assert.Equal(t, "item_000001", names[0])
	assert.Equal(t, fmt.Sprintf("item_%06d", rowCount), names[rowCount-1])

	// Also verify the PRAGMA reflects the DSN-supplied limit.
	var limit int64
	require.NoError(t, db.QueryRow(`PRAGMA sort_mem_limit`).Scan(&limit))
	assert.Equal(t, int64(65536), limit)

	// Verify DESC order too.
	descRows, err := db.Query(`select name from "items" order by name desc`)
	require.NoError(t, err)
	t.Cleanup(func() { descRows.Close() })

	var descNames []string
	for descRows.Next() {
		var name string
		require.NoError(t, descRows.Scan(&name))
		descNames = append(descNames, name)
	}
	require.NoError(t, descRows.Err())
	require.Len(t, descNames, rowCount)

	for i := 1; i < len(descNames); i++ {
		assert.GreaterOrEqual(t, descNames[i-1], descNames[i],
			"row %d: %q should come after %q", i, descNames[i-1], descNames[i])
	}

	// Verify ORDER BY with OFFSET still works correctly after external merge.
	offsetRows, err := db.Query(`select name from "items" order by name asc offset 500`)
	require.NoError(t, err)
	t.Cleanup(func() { offsetRows.Close() })

	var offsetNames []string
	for offsetRows.Next() {
		var name string
		require.NoError(t, offsetRows.Scan(&name))
		offsetNames = append(offsetNames, name)
	}
	require.NoError(t, offsetRows.Err())
	require.Len(t, offsetNames, rowCount-500)
	assert.True(t, strings.HasPrefix(offsetNames[0], "item_"),
		"first offset row should be item_000501, got %q", offsetNames[0])
	assert.Equal(t, "item_000501", offsetNames[0])
}

// TestOrderBy_DiskSpill_Join verifies that JOIN + ORDER BY queries spill to disk
// correctly and produce a properly ordered result. This exercises the universal
// spill path (selectWithSortSpill) rather than the sequential-scan fast path.
func TestOrderBy_DiskSpill_Join(t *testing.T) {
	// 8 KB limit forces spills even for a few hundred rows.
	db := openSpillDB(t, 8192)

	_, err := db.Exec(`create table "depts" (
		id   int8 primary key,
		name varchar(255)
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`create table "emps" (
		id      int8 primary key,
		dept_id int8,
		name    varchar(255),
		salary  int8
	)`)
	require.NoError(t, err)

	// Two departments.
	_, err = db.Exec(`insert into "depts" (id, name) values (1, 'Engineering'), (2, 'Sales')`)
	require.NoError(t, err)

	// 200 employees across two departments, inserted in reverse salary order.
	const n = 200
	for i := n; i >= 1; i-- {
		dept := 1 + (i % 2) // alternates 1 and 2
		name := fmt.Sprintf("emp_%04d", i)
		_, err = db.Exec(`insert into "emps" (id, dept_id, name, salary) values (?, ?, ?, ?)`,
			int64(i), int64(dept), name, int64(i*100))
		require.NoError(t, err)
	}

	// INNER JOIN ordered by salary ASC — all 200 employees have a matching dept.
	rows, err := db.Query(`
		select e.id, e.name, d.name, e.salary
		from "emps" AS e
		inner join "depts" AS d on e.dept_id = d.id
		order by e.salary asc
	`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		empID  int64
		emp    string
		dept   string
		salary int64
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.empID, &r.emp, &r.dept, &r.salary))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, n)

	// Verify ascending salary order.
	for i := 1; i < len(results); i++ {
		assert.LessOrEqual(t, results[i-1].salary, results[i].salary,
			"row %d: salary %d should be <= %d", i, results[i-1].salary, results[i].salary)
	}
	assert.Equal(t, int64(100), results[0].salary)
	assert.Equal(t, int64(n*100), results[n-1].salary)
}

// TestOrderBy_DiskSpill_Distinct verifies that ORDER BY + DISTINCT with disk spill
// produces a deduplicated, correctly ordered result set.
func TestOrderBy_DiskSpill_Distinct(t *testing.T) {
	// 4 KB limit forces spills for even small result sets.
	db := openSpillDB(t, 4096)

	_, err := db.Exec(`create table "scores" (
		id    int8 primary key autoincrement,
		level int4,
		score int4
	)`)
	require.NoError(t, err)

	// Insert 300 rows with only 10 distinct levels (each level repeated 30 times).
	for level := 10; level >= 1; level-- {
		for rep := 0; rep < 30; rep++ {
			_, err = db.Exec(`insert into "scores" (level, score) values (?, ?)`,
				int32(level), int32(level*10+rep))
			require.NoError(t, err)
		}
	}

	// SELECT DISTINCT level ORDER BY level ASC — should yield 10 unique levels.
	rows, err := db.Query(`select distinct level from "scores" order by level asc`)
	require.NoError(t, err)
	defer rows.Close()

	var levels []int32
	for rows.Next() {
		var lvl int32
		require.NoError(t, rows.Scan(&lvl))
		levels = append(levels, lvl)
	}
	require.NoError(t, rows.Err())
	require.Len(t, levels, 10)

	for i, lvl := range levels {
		assert.Equal(t, int32(i+1), lvl, "level at position %d", i)
	}
}

// TestOrderBy_DiskSpill_MultiColumn verifies multi-column ORDER BY with disk spill
// produces results sorted by the primary key then tiebreaker in the correct order.
func TestOrderBy_DiskSpill_MultiColumn(t *testing.T) {
	// 4 KB limit forces spills for even a small table.
	db := openSpillDB(t, 4096)

	_, err := db.Exec(`create table "events" (
		id       int8 primary key autoincrement,
		category varchar(50),
		priority int4,
		label    varchar(255)
	)`)
	require.NoError(t, err)

	// 150 rows with 3 categories and 5 priorities, inserted in scrambled order.
	categories := []string{"C", "B", "A"}
	for _, cat := range categories {
		for p := 5; p >= 1; p-- {
			for seq := 10; seq >= 1; seq-- {
				label := fmt.Sprintf("%s_p%d_s%02d", cat, p, seq)
				_, err = db.Exec(`insert into "events" (category, priority, label) values (?, ?, ?)`,
					cat, int32(p), label)
				require.NoError(t, err)
			}
		}
	}

	// ORDER BY category ASC, priority ASC.
	rows, err := db.Query(`select category, priority from "events" order by category asc, priority asc`)
	require.NoError(t, err)
	defer rows.Close()

	type rec struct {
		cat string
		pri int32
	}
	var results []rec
	for rows.Next() {
		var r rec
		require.NoError(t, rows.Scan(&r.cat, &r.pri))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 150)

	for i := 1; i < len(results); i++ {
		prev, cur := results[i-1], results[i]
		if prev.cat == cur.cat {
			assert.LessOrEqual(t, prev.pri, cur.pri,
				"row %d: priority %d should be <= %d within category %q", i, prev.pri, cur.pri, cur.cat)
		} else {
			assert.LessOrEqual(t, prev.cat, cur.cat,
				"row %d: category %q should come before %q", i, prev.cat, cur.cat)
		}
	}
	// Spot-check: first row must be category A, priority 1.
	assert.Equal(t, "A", results[0].cat)
	assert.Equal(t, int32(1), results[0].pri)
	// Last row must be category C, priority 5.
	assert.Equal(t, "C", results[len(results)-1].cat)
	assert.Equal(t, int32(5), results[len(results)-1].pri)
}

// TestOrderBy_DiskSpill_Join_Offset verifies JOIN + ORDER BY + OFFSET with disk
// spill returns the correct tail of the sorted result.
func TestOrderBy_DiskSpill_Join_Offset(t *testing.T) {
	db := openSpillDB(t, 8192)

	_, err := db.Exec(`create table "tags" (
		id   int8 primary key,
		name varchar(100)
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`create table "items2" (
		id     int8 primary key,
		tag_id int8,
		value  int8
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`insert into "tags" (id, name) values (1, 'alpha'), (2, 'beta')`)
	require.NoError(t, err)

	// 100 items with alternating tags, inserted in reverse value order.
	const n = 100
	for i := n; i >= 1; i-- {
		tag := 1 + (i % 2)
		_, err = db.Exec(`insert into "items2" (id, tag_id, value) values (?, ?, ?)`,
			int64(i), int64(tag), int64(i))
		require.NoError(t, err)
	}

	// JOIN ordered by value ASC with OFFSET 50 — should return items 51..100 sorted.
	rows, err := db.Query(`
		select i.value, t.name
		from "items2" AS i
		inner join "tags" AS t on i.tag_id = t.id
		order by i.value asc
		offset 50
	`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		value int64
		tag   string
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.value, &r.tag))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, n-50)

	// First result after OFFSET 50 should be value 51 (the 51st in ascending order).
	assert.Equal(t, int64(51), results[0].value)
	assert.Equal(t, int64(n), results[len(results)-1].value)

	for i := 1; i < len(results); i++ {
		assert.LessOrEqual(t, results[i-1].value, results[i].value,
			"row %d: value %d should be <= %d", i, results[i-1].value, results[i].value)
	}
}
