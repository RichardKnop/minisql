# MiniSQL

**MiniSQL** is an embedded, single-file SQL database written in pure Go, inspired by SQLite but borrowing ideas from PostgreSQL. It requires zero CGO and zero external dependencies beyond the Go standard library.

## Why MiniSQL?

| Feature | MiniSQL | SQLite |
|---------|---------|--------|
| Language | Pure Go / zero CGO | C |
| Read concurrency | MVCC snapshot isolation | WAL reader/writer lock |
| Parallel scan | ✅ | ❌ |
| Native JSON type | ✅ | Extension only |
| Native UUID type | ✅ | ❌ |
| Built-in full-text search | ✅ (BM25) | Extension only |
| JSON inverted index | ✅ | ❌ |
| Vector similarity search | ✅ (HNSW ANN) | Extension only |
| Transparent page encryption | ✅ (AES-256-CTR) | Extension only |

## Quick start

```go
import (
    "database/sql"
    _ "github.com/RichardKnop/minisql"
)

db, err := sql.Open("minisql", "./my.db")
if err != nil {
    log.Fatal(err)
}
// MiniSQL requires exactly one open connection
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
defer db.Close()

_, err = db.Exec(`create table "users" (
    id    int8 primary key autoincrement,
    email varchar(255) unique,
    name  text,
    created timestamp default now()
)`)

_, err = db.Exec(
    `insert into "users" (email, name) values (?, ?)`,
    "alice@example.com", "Alice",
)

rows, err := db.Query(`select id, name from "users" order by id`)
defer rows.Close()
for rows.Next() {
    var id int64
    var name string
    rows.Scan(&id, &name)
    fmt.Printf("%d  %s\n", id, name)
}
```

## Key characteristics

- **Single file** — the entire database is one `*.db` file plus a `*-wal` WAL file.
- **Write-Ahead Log** — all-or-nothing commits; safe crash recovery; automatic checkpoint.
- **OCC writes + MVCC reads** — one writer at a time; unlimited concurrent readers with snapshot isolation.
- **B+ tree storage** — fixed 4 096-byte pages with CRC32 integrity checking on every page.
- **Max 64 columns per table** — tracked via a 64-bit NULL bitmask.
- **`database/sql` compatible** — works with any Go code that uses the standard `database/sql` interface.

## Navigation

| Section | What you'll find |
|---------|-----------------|
| [Getting Started](getting-started.md) | Installation, connection, first queries |
| [CLI Shell](cli.md) | Interactive shell, `-c` flag, dot commands |
| [Connection](connection.md) | DSN parameters, connection pooling |
| [Architecture](architecture.md) | Storage, WAL, concurrency model |
| [Data Types](data-types.md) | All supported column types |
| [SQL Reference](sql/create-table.md) | Full SQL syntax reference |
| [Indexes](indexes/overview.md) | B-tree, full-text, JSON, HNSW |
| [Functions](functions/string.md) | String, numeric, date/time, UUID, aggregate, window, JSON functions |
| [JSON](json.md) | JSON type, operators, inverted index |
| [Vector Search](vector-search.md) | VECTOR type and HNSW ANN search |
| [Encryption](encryption.md) | Transparent AES-256-CTR encryption |
| [Metrics](metrics.md) | Native engine statistics API |
| [Backup](backup.md) | Online backup while the database is live |
| [Constraints](constraints.md) | CHECK, FK, NOT NULL, UNIQUE |
| [System Table](system-table.md) | `minisql_schema` structure |
| [Limitations](limitations.md) | Known limitations and workarounds |
