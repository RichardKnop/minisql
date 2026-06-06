package e2etests

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/RichardKnop/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openUnsupportedIndexDB opens a fresh database for unsupported-index tests.
func openUnsupportedIndexDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "unsupported_index_")
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

// TestCreateIndex_UnsupportedColumnType verifies that CREATE INDEX on TEXT,
// JSON, and VECTOR columns (single and composite) returns an error instead of
// panicking or silently creating a broken index.
func TestCreateIndex_UnsupportedColumnType(t *testing.T) {
	t.Parallel()
	db, cleanup := openUnsupportedIndexDB(t)
	defer cleanup()

	_, err := db.Exec(`CREATE TABLE things (
		id      INT8 PRIMARY KEY AUTOINCREMENT,
		name    TEXT NOT NULL,
		meta    JSON,
		vec     VECTOR(3),
		label   VARCHAR(100)
	)`)
	require.NoError(t, err)

	t.Run("TEXT column single-column index", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_name ON things (name)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "TEXT")
	})

	t.Run("JSON column single-column index", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_meta ON things (meta)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "JSON")
	})

	t.Run("VECTOR column single-column index", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_vec ON things (vec)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "VECTOR")
	})

	t.Run("composite index containing TEXT column", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_label_name ON things (label, name)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "TEXT")
	})

	t.Run("composite index containing JSON column", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_label_meta ON things (label, meta)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "JSON")
	})

	t.Run("composite index containing VECTOR column", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_label_vec ON things (label, vec)`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "VECTOR")
	})

	t.Run("supported column types still work", func(t *testing.T) {
		_, err := db.Exec(`CREATE INDEX idx_things_label ON things (label)`)
		require.NoError(t, err)
	})
}
