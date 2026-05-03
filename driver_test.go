package minisql

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriverPreparedStatementsAndTransactions(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "minisql-driver-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.Remove(tempFile.Name())
		_ = os.Remove(tempFile.Name() + "-wal")
	})

	db, err := sql.Open("minisql", tempFile.Name())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err = db.ExecContext(context.Background(), `create table "users" (
		id int8 primary key autoincrement,
		name text
	);`)
	require.NoError(t, err)

	insertStmt, err := db.PrepareContext(context.Background(), `insert into users("name") values(?);`)
	require.NoError(t, err)
	defer insertStmt.Close()

	result, err := insertStmt.ExecContext(context.Background(), "Alice")
	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	selectStmt, err := db.PrepareContext(context.Background(), `select id, name from users where name = ?;`)
	require.NoError(t, err)
	defer selectStmt.Close()

	rows, err := selectStmt.QueryContext(context.Background(), "Alice")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var (
		id   int64
		name string
	)
	require.NoError(t, rows.Scan(&id, &name))
	assert.Equal(t, int64(1), id)
	assert.Equal(t, "Alice", name)
	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `insert into users("name") values('Bob');`)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	var count int
	require.NoError(t, db.QueryRowContext(context.Background(), `select count(*) from users;`).Scan(&count))
	assert.Equal(t, 1, count)

	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `insert into users("name") values('Cara');`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	require.NoError(t, db.QueryRowContext(context.Background(), `select count(*) from users;`).Scan(&count))
	assert.Equal(t, 2, count)
}
