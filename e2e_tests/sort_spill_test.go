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

// TestOrderBy_DiskSpill_DSN opens a fresh database with sort_mem_limit set low
// enough that a 1 000-row ORDER BY query crosses the threshold and spills sorted
// runs to disk. It verifies that the merged result is correctly ordered.
func TestOrderBy_DiskSpill_DSN(t *testing.T) {
	f, err := os.CreateTemp("", "minisql_sort_spill_*.db")
	require.NoError(t, err)
	dbPath := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
	})

	// 64 KB limit — 1 000 rows × ~150 bytes each ≈ 150 KB, forcing at least two spills.
	dsn := fmt.Sprintf("%s?sort_mem_limit=65536", dbPath)
	db, err := sql.Open("minisql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`create table "items" (
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
