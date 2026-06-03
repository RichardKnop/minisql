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

## Notes

- Encryption adds one AES-CTR encrypt/decrypt operation per page read or write. For most workloads the overhead is negligible.
- The database file and WAL file should be treated as equally sensitive — both contain encrypted pages.
- There is no support for key rotation; to change the key, use `VACUUM` after re-opening with the new key (VACUUM rewrites all pages, but key change support is a planned feature).
- `PRAGMA integrity_check` works on encrypted databases — pages are decrypted in-memory before the check.
