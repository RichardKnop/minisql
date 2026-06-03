package minisql

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var encColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Varchar, Size: MaxInlineVarchar, Name: "name", Nullable: true},
}

// openEncryptedDB creates or opens a file-backed database with encryption.
// Pass a non-nil parser when reopening a database that already contains tables.
func openEncryptedDB(t *testing.T, path string, key []byte, p Parser) *Database {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	require.NoError(t, err)

	pager, err := NewPager(f, PageSize, PageCacheSize)
	require.NoError(t, err)

	db, err := NewDatabase(
		context.Background(), testLogger, path, p, pager, pager, nil,
		WithEncryptionKey(key),
	)
	require.NoError(t, err)
	return db
}

func TestEncryption_WriteAndRead(t *testing.T) {
	t.Parallel()

	key := []byte("super-secret-key-for-testing-123")

	f, err := os.CreateTemp("", "minisql-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	usersStmt := Statement{
		Kind:      CreateTable,
		TableName: "users",
		Columns:   encColumns,
	}

	// --- Create and populate an encrypted database ---
	{
		db := openEncryptedDB(t, path, key, nil)
		ctx := context.Background()

		err = db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, Statement{
				Kind:      CreateTable,
				TableName: "users",
				Columns:   encColumns,
			})
			return err
		})
		require.NoError(t, err)

		err = db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, Statement{
				Kind:      Insert,
				TableName: "users",
				Columns:   encColumns,
				Fields:    fieldsFromColumns(encColumns...),
				Inserts: [][]OptionalValue{
					{{Value: int64(1), Valid: true}, {Value: NewTextPointer([]byte("alice")), Valid: true}},
					{{Value: int64(2), Valid: true}, {Value: NewTextPointer([]byte("bob")), Valid: true}},
				},
			})
			return err
		})
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}

	// --- Verify: file should not contain plaintext user names ---
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.False(t, bytes.Contains(raw, []byte("alice")), "plaintext 'alice' found in encrypted DB file")
	assert.False(t, bytes.Contains(raw, []byte("bob")), "plaintext 'bob' found in encrypted DB file")

	// --- Reopen and query ---
	// A MockParser is required so initTable can parse the stored DDL for "users".
	{
		mp := new(MockParser)
		mp.On("Parse", mock.Anything, usersStmt.DDL()).Return([]Statement{usersStmt}, nil)
		db := openEncryptedDB(t, path, key, mp)
		defer db.Close()

		ctx := context.Background()
		var names []string
		err := db.txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
			result, err := db.ExecuteStatement(ctx, Statement{
				Kind:      Select,
				TableName: "users",
				Columns:   encColumns,
				Fields:    fieldsFromColumns(encColumns...),
			})
			if err != nil {
				return err
			}
			for result.Rows.Next(ctx) {
				row := result.Rows.Row()
				v, ok := row.GetValue("name")
				if ok && v.Valid {
					names = append(names, v.Value.(TextPointer).String())
				}
			}
			return result.Rows.Err()
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"alice", "bob"}, names)
	}
}

func TestEncryption_WrongKey(t *testing.T) {
	t.Parallel()

	rightKey := []byte("correct-key-for-testing-purposes")
	wrongKey := []byte("wrong---key-for-testing-purposes")

	f, err := os.CreateTemp("", "minisql-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	// Create database with the right key
	{
		db := openEncryptedDB(t, path, rightKey, nil)
		require.NoError(t, db.Close())
	}

	// Reopen with the wrong key — decryption produces invalid plaintext so the
	// page checksum verification on page 0 should fail during init.
	f2, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	pager, err := NewPager(f2, PageSize, PageCacheSize)
	require.NoError(t, err)
	_, dbErr := NewDatabase(
		context.Background(), testLogger, path, nil, pager, pager, nil,
		WithEncryptionKey(wrongKey),
	)
	assert.Error(t, dbErr, "expected error when opening with wrong key")
}

func TestEncryption_NoKeyForEncryptedDB(t *testing.T) {
	t.Parallel()

	key := []byte("some-secret-encryption-key-here!")

	f, err := os.CreateTemp("", "minisql-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	// Create with encryption
	{
		db := openEncryptedDB(t, path, key, nil)
		require.NoError(t, db.Close())
	}

	// Reopen without key — setupEncryption should detect the encrypted header
	f2, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	pager, err := NewPager(f2, PageSize, PageCacheSize)
	require.NoError(t, err)
	_, dbErr := NewDatabase(
		context.Background(), testLogger, path, nil, pager, pager, nil,
		// no WithEncryptionKey
	)
	require.Error(t, dbErr)
	assert.Contains(t, dbErr.Error(), "encrypted")
}

func TestEncryption_KeyForUnencryptedDB(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "minisql-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	// Create without encryption
	{
		f2, err := os.OpenFile(path, os.O_RDWR, 0o600)
		require.NoError(t, err)
		pager, err := NewPager(f2, PageSize, PageCacheSize)
		require.NoError(t, err)
		db, err := NewDatabase(
			context.Background(), testLogger, path, nil, pager, pager, nil,
		)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}

	// Reopen with key — setupEncryption should return an error
	key := []byte("some-key-for-an-unencrypted-db!!")
	f2, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	pager, err := NewPager(f2, PageSize, PageCacheSize)
	require.NoError(t, err)
	_, dbErr := NewDatabase(
		context.Background(), testLogger, path, nil, pager, pager, nil,
		WithEncryptionKey(key),
	)
	require.Error(t, dbErr)
	assert.Contains(t, dbErr.Error(), "not encrypted")
}

func TestEncryption_SaltInHeader(t *testing.T) {
	t.Parallel()

	key := []byte("test-key-for-salt-header-check!!")

	f, err := os.CreateTemp("", "minisql-enc-*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	{
		db := openEncryptedDB(t, path, key, nil)
		require.NoError(t, db.Close())
	}

	// Read the raw header bytes (always plaintext, never encrypted)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), RootPageConfigSize)

	var hdr DatabaseHeader
	require.NoError(t, UnmarshalDatabaseHeader(raw[:RootPageConfigSize], &hdr))
	assert.Equal(t, EncryptionModeAES256CTR, hdr.EncryptionMode)
	assert.NotEqual(t, [32]byte{}, hdr.EncryptionSalt, "salt must be non-zero")
}
