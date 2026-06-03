package e2etests

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/RichardKnop/minisql"
)

func encryptedDSN(path string, key []byte) string {
	return path + "?encryption_key=" + hex.EncodeToString(key)
}

func openEncryptedDB(t *testing.T, path string, key []byte) *sql.DB {
	t.Helper()
	db, err := sql.Open("minisql", encryptedDSN(path, key))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Ping forces Driver.Open so the DB file is initialised immediately.
	require.NoError(t, db.Ping())
	return db
}

// TestEncryption_WriteAndRead verifies that:
//   - Plaintext values are not visible in the raw file bytes.
//   - After closing and reopening with the same key, all rows are readable.
func TestEncryption_WriteAndRead(t *testing.T) {
	t.Parallel()

	key := []byte("e2e-test-key-32bytes-long-padded")

	f, err := os.CreateTemp("", "minisql-e2e-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// --- Create table and insert rows ---
	{
		db := openEncryptedDB(t, path, key)

		_, err = db.Exec(`create table "secrets" (id int8 primary key autoincrement, value text not null)`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into "secrets" (value) values (?)`, "confidential-alpha")
		require.NoError(t, err)
		_, err = db.Exec(`insert into "secrets" (value) values (?)`, "confidential-beta")
		require.NoError(t, err)

		require.NoError(t, db.Close())
	}

	// --- File must not contain plaintext values ---
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.False(t, bytes.Contains(raw, []byte("confidential-alpha")), "plaintext found in encrypted DB file")
	assert.False(t, bytes.Contains(raw, []byte("confidential-beta")), "plaintext found in encrypted DB file")

	// --- Reopen and verify rows are readable ---
	{
		db := openEncryptedDB(t, path, key)
		defer db.Close()

		rows, err := db.QueryContext(context.Background(), `select value from "secrets" order by id`)
		require.NoError(t, err)
		defer rows.Close()

		var values []string
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			values = append(values, v)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"confidential-alpha", "confidential-beta"}, values)
	}
}

// TestEncryption_WrongKey verifies that opening an encrypted database with an
// incorrect key returns an error during connection setup.
func TestEncryption_WrongKey(t *testing.T) {
	t.Parallel()

	rightKey := []byte("e2e-correct-key-32bytes-long-pad")
	wrongKey := []byte("e2e-wrong---key-32bytes-long-pad")

	f, err := os.CreateTemp("", "minisql-e2e-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Create and close with the right key
	{
		db := openEncryptedDB(t, path, rightKey)
		require.NoError(t, db.Close())
	}

	// Opening with the wrong key should fail — the database/sql driver surfaces
	// NewDatabase errors on the first Ping/query.
	db, err := sql.Open("minisql", encryptedDSN(path, wrongKey))
	require.NoError(t, err) // sql.Open is lazy; error comes on first use
	db.SetMaxOpenConns(1)
	pingErr := db.Ping()
	db.Close()
	assert.Error(t, pingErr, "expected error when opening with wrong key")
}

// TestEncryption_NoKeyForEncryptedDB verifies that opening an encrypted database
// without providing any key returns an error.
func TestEncryption_NoKeyForEncryptedDB(t *testing.T) {
	t.Parallel()

	key := []byte("e2e-secret-key-for-no-key-test!!")

	f, err := os.CreateTemp("", "minisql-e2e-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Create with encryption
	{
		db := openEncryptedDB(t, path, key)
		require.NoError(t, db.Close())
	}

	// Reopen without key — should fail
	db, err := sql.Open("minisql", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	pingErr := db.Ping()
	db.Close()
	require.Error(t, pingErr)
	assert.Contains(t, pingErr.Error(), "encrypted")
}

// TestEncryption_KeyForUnencryptedDB verifies that providing an encryption key
// when opening a plaintext database returns an error.
func TestEncryption_KeyForUnencryptedDB(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "minisql-e2e-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Create without encryption
	{
		db, err := sql.Open("minisql", path)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		require.NoError(t, db.Ping())
		require.NoError(t, db.Close())
	}

	// Reopen with a key — should fail
	key := []byte("e2e-key-for-unencrypted-db-paddd")
	db, err := sql.Open("minisql", encryptedDSN(path, key))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	pingErr := db.Ping()
	db.Close()
	require.Error(t, pingErr)
	assert.Contains(t, pingErr.Error(), "not encrypted")
}

// TestEncryption_VacuumPreservesEncryption verifies that VACUUM on an encrypted
// database produces a compacted file that is still encrypted and readable.
func TestEncryption_VacuumPreservesEncryption(t *testing.T) {
	t.Parallel()

	key := []byte("e2e-vacuum-enc-key-32bytes-paddd")

	f, err := os.CreateTemp("", "minisql-e2e-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	db := openEncryptedDB(t, path, key)

	_, err = db.Exec(`create table "data" (id int8 primary key autoincrement, val text not null)`)
	require.NoError(t, err)

	for i := range 10 {
		_, err = db.Exec(`insert into "data" (val) values (?)`, "secret-value")
		require.NoError(t, err)
		if i%3 == 0 {
			_, err = db.Exec(`delete from "data" where id = ?`, i+1)
			require.NoError(t, err)
		}
	}

	_, err = db.ExecContext(context.Background(), `VACUUM`)
	require.NoError(t, err)

	// Verify the compacted file contains no plaintext
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.False(t, bytes.Contains(raw, []byte("secret-value")), "plaintext found after VACUUM on encrypted DB")

	// Verify data is still queryable after VACUUM
	var count int
	err = db.QueryRow(`select count(*) from "data"`).Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	require.NoError(t, db.Close())
}
