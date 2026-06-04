# Transparent Page Encryption

MiniSQL supports transparent AES-256-CTR page encryption. When enabled, every database page (except the plaintext header) is encrypted on write and decrypted on read. Application code does not need to change — encryption is handled entirely by the storage layer.

---

## How it works

- **Algorithm:** AES-256-CTR (stream cipher, no padding, seekable).
- **Key derivation:** HKDF (HMAC-based Key Derivation Function) derives the actual AES key from the user-supplied key and a per-database random salt. This means the on-disk AES key material is unique per database file even when the same passphrase is reused.
- **Plaintext header:** The first 100 bytes of page 0 (the database file header) are always stored in plaintext. This allows MiniSQL to read the salt, detect the encryption mode, and bootstrap key derivation before decrypting any pages.
- **WAL consistency:** Pages written to the WAL file are also encrypted. The WAL and main database file always use the same key.
- **VACUUM aware:** `VACUUM` carries encryption through — the compacted file is encrypted with the same key.
- **Mismatch detection:** Opening an encrypted database without a key, or with the wrong key, returns an error. Corrupted pages are detected by the CRC32 checksum on every page.

---

## Enabling encryption

### Via the Go API

```go
import (
    "database/sql"
    "github.com/RichardKnop/minisql"
    _ "github.com/RichardKnop/minisql"
)

key := []byte("my-32-byte-secret-key-here!!!!!!")  // any length, HKDF handles it

db, err := sql.Open("minisql", "/path/to/db.db?encryption_key=" + hex.EncodeToString(key))
if err != nil {
    log.Fatal(err)
}
defer db.Close()
db.SetMaxOpenConns(1)

// Force connection open and verify key is accepted
if err := db.Ping(); err != nil {
    log.Fatal(err)
}
```

### DSN parameter

The `encryption_key` DSN parameter accepts a **hex-encoded** key:

```
/path/to/db.db?encryption_key=<hex-encoded-key>
```

Example:

```go
import "encoding/hex"

key := make([]byte, 32) // 256-bit key
rand.Read(key)

dsn := "/var/data/app.db?encryption_key=" + hex.EncodeToString(key)
db, err := sql.Open("minisql", dsn)
```

---

## Key requirements

- The key can be any length — HKDF normalises it.
- A 32-byte (256-bit) random key is recommended.
- Store the key securely — losing the key means losing access to the database permanently.
- The key is never written to disk.

---

## Error handling

| Situation | Error |
|-----------|-------|
| Encrypted DB opened without a key | `"database is encrypted (mode 1) but no encryption key was provided"` |
| Encrypted DB opened with wrong key | CRC32 mismatch error on first page read |
| Unencrypted DB opened with a key | Error — key provided for non-encrypted database |

Always call `db.Ping()` after opening to surface any key errors early:

```go
db, _ := sql.Open("minisql", dsn)
if err := db.Ping(); err != nil {
    // wrong key, missing key, or corrupted database
    return err
}
```

---

## Full example

```go
package main

import (
    "crypto/rand"
    "database/sql"
    "encoding/hex"
    "fmt"
    "log"

    _ "github.com/RichardKnop/minisql"
)

func main() {
    // Generate a random 256-bit key
    key := make([]byte, 32)
    if _, err := rand.Read(key); err != nil {
        log.Fatal(err)
    }

    dsn := "/tmp/encrypted.db?encryption_key=" + hex.EncodeToString(key)

    db, err := sql.Open("minisql", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    db.SetMaxOpenConns(1)
    db.SetMaxIdleConns(1)

    if err := db.Ping(); err != nil {
        log.Fatal("failed to open encrypted database:", err)
    }

    db.Exec(`CREATE TABLE IF NOT EXISTS secrets (
        id   INT8 PRIMARY KEY AUTOINCREMENT,
        data TEXT NOT NULL
    )`)

    db.Exec(`INSERT INTO secrets (data) VALUES (?)`, "top secret value")

    var data string
    db.QueryRow(`SELECT data FROM secrets WHERE id = 1`).Scan(&data)
    fmt.Println(data) // "top secret value"
}
```

---

---

## Key rotation

Key rotation re-encrypts the entire database with a new key.  It is implemented
as a crash-safe copy-and-swap (the same mechanism as `VACUUM`) so the original
file is never modified in place.  A `.bak` backup is kept until the swap
succeeds.

### Via SQL (PRAGMA rekey)

```sql
PRAGMA rekey = '<hex-encoded-new-key>';
```

The value must be a hex-encoded key (same encoding as the DSN `encryption_key`
parameter).

```go
import "encoding/hex"

newKey := make([]byte, 32)
rand.Read(newKey)

// Rotate while the connection is still open.
_, err = db.ExecContext(ctx, `PRAGMA rekey = '`+hex.EncodeToString(newKey)+`'`)
if err != nil {
    log.Fatal(err)
}

// After this call the in-process connection uses the new key.
// The next time you open the file you must supply newKey, not the old one.
```

### Via the Go API

```go
import "github.com/RichardKnop/minisql/internal/minisql"

// db is a *minisql.Database (obtained via NewDatabase or cast from driver.Conn).
err := db.ReKey(ctx, newKey)   // key rotation / adding encryption
err  = db.ReKey(ctx, nil)      // remove encryption
```

`ReKey(ctx, nil)` strips encryption — the resulting file is plaintext.
There is no SQL equivalent for removing encryption; use the Go API directly.

### Adding encryption to an existing plaintext database

```sql
-- DB was opened without a key; add encryption in-place.
PRAGMA rekey = '<hex-encoded-new-key>';
```

After this, re-open the database with the key in the DSN:

```go
dsn := "/path/to/db.db?encryption_key=" + hex.EncodeToString(newKey)
```

### What happens during rotation

1. A temporary database file is created and encrypted with the new key (gets a fresh random salt).
2. All schema and rows are copied from the live database (decrypted with the old key → re-encrypted with the new key).
3. The live file is atomically replaced by the temp file.
4. The in-process connection is re-opened and the new cipher is installed.

The operation holds the exclusive write lock for its full duration (same as `VACUUM`).

---

## Notes

- Encryption adds one AES-CTR encrypt/decrypt operation per page read or write. For most workloads the overhead is negligible.
- The database file and WAL file should be treated as equally sensitive — both contain encrypted pages.
- Key rotation always generates a fresh random salt, so the derived AES key changes even if the raw key material is the same.
- `PRAGMA integrity_check` works on encrypted databases — pages are decrypted in-memory before the check.
