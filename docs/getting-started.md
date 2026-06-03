# Getting Started

## Installation

```bash
go get github.com/RichardKnop/minisql
```

Import the driver (blank import registers it with `database/sql`):

```go
import (
    "database/sql"
    _ "github.com/RichardKnop/minisql"
)
```

## Opening a database

```go
db, err := sql.Open("minisql", "./my.db")
if err != nil {
    log.Fatal(err)
}

// Required: MiniSQL supports only one open connection per file
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
defer db.Close()
```

The database file is created automatically if it does not exist.

## Connection string

The first argument to `sql.Open` is a file path with optional query parameters:

```go
// Simple path
db, err := sql.Open("minisql", "./my.db")

// With parameters
db, err := sql.Open("minisql", "./my.db?log_level=debug&max_cached_pages=1000")

// With encryption
db, err := sql.Open("minisql", "./my.db?encryption_key="+hex.EncodeToString(key))
```

See [Connection](connection.md) for the full parameter reference.

## Creating a table

```go
_, err = db.Exec(`create table "users" (
    id      int8      primary key autoincrement,
    email   varchar(255) not null unique,
    name    text,
    active  boolean   not null default true,
    created timestamp default now()
)`)
if err != nil {
    log.Fatal(err)
}
```

## Inserting rows

```go
// Single row
_, err = db.Exec(
    `insert into "users" (email, name) values (?, ?)`,
    "alice@example.com", "Alice",
)

// Multi-row
_, err = db.Exec(
    `insert into "users" (email, name) values (?, ?), (?, ?)`,
    "bob@example.com", "Bob",
    "carol@example.com", "Carol",
)

// Prepared statement (recommended for repeated inserts)
stmt, err := db.Prepare(`insert into "users" (email, name) values (?, ?)`)
defer stmt.Close()
for _, u := range users {
    _, err = stmt.Exec(u.Email, u.Name)
}
```

## Querying rows

```go
rows, err := db.Query(`select id, name, email from "users" where active = ? order by id`, true)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int64
    var name, email string
    if err := rows.Scan(&id, &name, &email); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%d  %-20s  %s\n", id, name, email)
}
if err := rows.Err(); err != nil {
    log.Fatal(err)
}
```

## Scanning a single row

```go
var count int64
err = db.QueryRow(`select count(*) from "users"`).Scan(&count)
```

## Updating rows

```go
result, err := db.Exec(
    `update "users" set active = ? where id = ?`,
    false, 42,
)
n, _ := result.RowsAffected()
fmt.Printf("%d row(s) updated\n", n)
```

## Deleting rows

```go
_, err = db.Exec(`delete from "users" where id = ?`, 42)
```

!!! note
    DELETE always requires a WHERE clause. Use `WHERE 1=1` to delete all rows.

## Transactions

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

_, err = tx.Exec(`insert into "users" (email, name) values (?, ?)`, "dave@example.com", "Dave")
_, err = tx.Exec(`update "accounts" set balance = balance - ? where user_id = ?`, 100, 1)

if err != nil {
    tx.Rollback()
    return err
}
if err := tx.Commit(); err != nil {
    return err
}
```

## Using RETURNING

```go
var newID int64
err = db.QueryRow(
    `insert into "users" (email, name) values (?, ?) returning id`,
    "eve@example.com", "Eve",
).Scan(&newID)
```

## Next steps

- [Connection parameters](connection.md) — tune cache size, WAL behaviour, logging
- [SQL Reference](sql/create-table.md) — complete SQL syntax
- [Data Types](data-types.md) — all supported column types
- [Indexes](indexes/overview.md) — add indexes to speed up queries
