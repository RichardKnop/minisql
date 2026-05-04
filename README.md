# minisql

[![CI Status](https://github.com/RichardKnop/minisql/actions/workflows/go.yml/badge.svg)](https://github.com/RichardKnop/minisql/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/RichardKnop/minisql)](https://goreportcard.com/report/github.com/RichardKnop/minisql)

`MiniSQL` is an embedded single file database written in Golang, inspired by `SQLite`. It is not a clone of `SQLite` in Go, it is an alternative solution which borrows ideas from other databases as well. It can differentiate itself from `SQLite` in several areas: 

1. **Pure Go, zero CGO** — already a differentiator.
2. **MVCC snapshot isolation** — true MVCC for reads already implemented.
3. **Parallel query execution** — SQLite is single-threaded; MiniSQL can parallelize full table scans.
4. **Modern API surface** — idiomatic Go, context-aware, `database/sql` compatible.
5. **JSON as a first-class type** — native JSONB (not an extension) - not implemented yet.

This is an early stage project and it might contain bugs and is not battle tested. Please employ caution when using this database.

[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://richardknop.github.io/donate/)

To use minisql in your Go code, import the driver:

```go
import (
  _ "github.com/RichardKnop/minisql"
)
```

And create a database instance:

```go
// Simple path
db, err := sql.Open("minisql", "./my.db")

// With connection parameters
db, err := sql.Open("minisql", "./my.db?log_level=debug")

// Multiple parameters
db, err := sql.Open("minisql", "./my.db?log_level=debug&max_cached_pages=500")
```

## Connection Pooling

**MiniSQL is an embedded, single-file database (similar to SQLite).** However, it can support multiple connections for reads so it is not necessary to set max connections to 1, it depends on your workloads:

- Write-heavy workloads: SetMaxOpenConns(1) still makes sense — writes serialize internally on dbLock anyway, multiple connections just add OCC conflict noise without throughput gain.                                                                                              
 - Read-heavy or mixed workloads: multiple connections are beneficial — read-only transactions run concurrently via MVCC snapshot isolation without holding the database lock.

## Connection String Parameters

MiniSQL supports optional connection string parameters:

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `wal_checkpoint_threshold` | non-negative integer | `1000` | Auto-checkpoint after N WAL frames (0 = disabled) |
| `log_level` | `debug`, `info`, `warn`, `error` | `warn` | Set logging verbosity level |
| `max_cached_pages` | positive integer | `2000` | Maximum number of pages to keep in memory cache |
| `slow_query_threshold` | Go duration, e.g. `50ms`, `2s` | `0` | Log queries taking at least this long at WARN level (0 = disabled) |
| `synchronous` | `off`, `normal`, `full` | `normal` | WAL fsync mode (see [WAL durability](#wal-durability-modes) below) |

**Examples:**
```go
// Enable debug logging
db, err := sql.Open("minisql", "./my.db?log_level=debug")

// Set cache size to 500 pages (~2MB memory)
db, err := sql.Open("minisql", "./my.db?max_cached_pages=500")

// Disable auto-checkpoint (manual checkpoint only)
db, err := sql.Open("minisql", "./my.db?wal_checkpoint_threshold=0")

// Maximum write durability (fsync after every commit)
db, err := sql.Open("minisql", "./my.db?synchronous=full")

// Log queries that take at least 50ms
db, err := sql.Open("minisql", "./my.db?slow_query_threshold=50ms")

// Combine multiple parameters
db, err := sql.Open("minisql", "/path/to/db.db?log_level=info&max_cached_pages=2000")
```

## Write-Ahead Log (WAL)

MiniSQL uses a Write-Ahead Log (`{dbpath}-wal`) for crash recovery and atomic commits. All page modifications are appended to the WAL before the main database file is updated.

Commit protocol:

1. Serialise all modified pages as WAL frames and write them to the WAL file.
2. Optionally `fsync()` the WAL file (controlled by the `synchronous` setting).
3. The in-memory WAL index is updated so subsequent reads see the new pages immediately.
4. The main database file is **not written** during a commit — it is updated only during a checkpoint.

On startup, if a WAL file exists, MiniSQL replays all valid committed frames into the in-memory WAL index so the data is visible immediately without a checkpoint.

Checkpoint (`PRAGMA wal_checkpoint`):

1. Copies every WAL page into the main database file.
2. `Sync()`s the database file (skipped in `synchronous=off`).
3. Truncates the WAL file to its header (32 bytes).
4. Resets the in-memory WAL index.

An automatic checkpoint is triggered after `wal_checkpoint_threshold` WAL frames (default 1000). Set `wal_checkpoint_threshold=0` to disable auto-checkpoint and run `PRAGMA wal_checkpoint` manually.

### WAL Durability Modes

The `synchronous` setting controls when `fsync()` is called, trading durability for write performance. This matches SQLite's `PRAGMA synchronous` for WAL mode.

| Mode | Connection string | PRAGMA | Description |
|------|------------------|--------|-------------|
| `normal` | `synchronous=normal` | `PRAGMA synchronous = normal` | **Default.** No fsync per commit. fsync only at checkpoint. Matches SQLite WAL default. |
| `full` | `synchronous=full` | `PRAGMA synchronous = full` | fsync after every WAL commit. Maximum durability — survives an OS crash between commits. |
| `off` | `synchronous=off` | `PRAGMA synchronous = off` | No fsyncs at all. Fastest, but uncommitted data may be lost on OS crash or power failure. |

The default (`normal`) matches SQLite's WAL default behaviour. In practice, data committed under `normal` mode survives application crashes and most OS crashes — the only scenario where data is lost is a power failure or kernel panic occurring in the narrow window after a commit write but before the next checkpoint fsync.

You can read the current mode at runtime:

```sql
PRAGMA synchronous;   -- returns 0 (off), 1 (normal), or 2 (full)
```

And change it for the current connection:

```sql
PRAGMA synchronous = full;
PRAGMA synchronous = normal;
PRAGMA synchronous = off;
```

## Storage

Each page size is `4096 bytes`. Rows larger than page size are not supported. Therefore, the largest allowed inline row size is `4065 bytes` (with exception of root page 0 which has first 100 bytes reserved for config). Variable text colums can use overflow pages and are not limited by page size.

```
4096 (page size) 
- 7 (base header size) 
- 8 (internal / leaf node header size) 
- 8 (null bit mask) 
- 8 (internal row ID / key) 
= 4065
```

All tables are kept track of via a system table `minisql_schema` which contains table name, `CREATE TABLE` SQL to document table structure and a root page index indicating which page contains root node of the table B+ Tree.

Each row has an internal row ID which is an unsigned 64 bit integer starting at 0. These are used as keys in B+ Tree data structure. 

Moreover, each row starts with 64 bit null mask which determines which values are NULL. Because of the NULL bit mask being an unsigned 64 bit integer, there is a limit of `maximum 64 columns per table`.

### Database Header Format

The first `100` bytes of page `0` are reserved for the MiniSQL database header. This is part of the on-disk file format.

Current header fields:

| Offset | Size | Field | Description |
|---|---:|---|---|
| `0` | `8` | magic | `minisql\0` file signature |
| `8` | `4` | format version | Current value: `1` |
| `12` | `4` | page size | Current value: `4096` |
| `16` | `4` | first free page | Head of the free-page linked list |
| `20` | `4` | free page count | Number of free pages currently tracked |
| `24` | `76` | reserved | Reserved for future file-format metadata |

Notes:

- MiniSQL now requires the header magic/version/page size to be present when opening a database file.
- The remaining bytes are reserved so the header can grow without immediately changing the page layout again.
- The rest of page `0` after the first `100` bytes is used as a normal root B+ tree page.

## Concurrency

MiniSQL implements two complementary concurrency control mechanisms:

### Write Transactions — Optimistic Concurrency Control (OCC)

Write transactions use `Optimistic Concurrency Control`. The transaction manager follows a simple process:

1. Track read versions — Record the page version at the time each page is first read (captured before the LRU cache read to avoid TOCTOU races with concurrent commits).
2. Check at commit time — Verify no pages were modified between the first read and the commit.
3. Abort on conflict — If any tracked page has a newer version at commit time, abort with `ErrTxConflict`.

You can use `ErrTxConflict` to decide whether to retry or surface the error to the caller.

### Read-Only Transactions — Snapshot Isolation (MVCC)

Read-only transactions use in-memory `MVCC` (`Multi-Version Concurrency Control`) to provide snapshot isolation: a reader sees the database exactly as it was at the moment `BeginReadOnlyTransaction` was called, regardless of writes that commit afterward.

This is similar to how [SQLite handles isolation](https://sqlite.org/isolation.html). Under the hood:

- A monotonically increasing `commitSeq` counter is incremented on every write commit.
- Each read-only transaction captures the current `commitSeq` as its `SnapshotSeq` at start time.
- At write commit time, the pre-modification copy of each modified page is saved in an in-memory version history (`pageVersionHistory`).
- When a snapshot reader accesses a page whose cached version is newer than its `SnapshotSeq`, it retrieves the appropriate historical version from the version history.
- Historical versions are garbage-collected once all snapshot readers that needed them have committed.

```
Time 0: Read TX1 starts — SnapshotSeq = 1
Time 1: Write TX2 modifies page, commits — commitSeq advances to 2; old page saved in version history
Time 2: TX1 reads the page → sees the historical version at seq 1, not TX2's change
Time 3: TX1 commits; version history for seq 1 is GC'd
```

Checkpoint (WAL truncation) is blocked while any snapshot reader is active, since old page versions are held only in the in-memory version history rather than the WAL.

## System Table

All tables and indexes are tracked in the system table `minisql_schema`. For empty database, it would contain only its own reference:

```sh
 type   | name               | table_name         | root_page   | sql                                                
--------+--------------------+--------------------+-------------+----------------------------------------
 1      | minisql_schema     |                    | 0           | create table "minisql_schema" (        
        |                    |                    |             | 	type int4 not null,                  
        |                    |                    |             | 	name varchar(255) not null,          
        |                    |                    |             | 	table_name varchar(255),             
        |                    |                    |             | 	root_page int4,                      
        |                    |                    |             | 	sql text                             
        |                    |                    |             | )                                      
```

Let's say you create a table such as:

```sql
create table "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	age int4,
	created timestamp default now()
);
create index "idx_created" on "users" (
	created
);
```

It will be added to the system table as well as its primary key and any unique or secondary indexes. Secondary index on `created TIMESTAMP` column created separately will also be added to the system table.

You can check current objects in the `minisql_schema` system table by a simple `SELECT` query.

```go
// type schema struct {
// 	Type      int
// 	Name      string
// 	TableName *string
// 	RootPage  int
// 	Sql       *string
// }

rows, err := db.QueryContext(context.Background(), `select * from minisql_schema;`)
if err != nil {
	return err
}
defer rows.Close()

var schemas []schema
for rows.Next() {
	var aSchema schema
	if err := rows.Scan(&aSchema.Type, &aSchema.Name, &aSchema.TableName, &aSchema.RootPage, &aSchema.SQL); err != nil {
		return err
	}
	schemas = append(schemas, aSchema)
}
if err := rows.Err(); err != nil {
	return err
}
```

```sh
 type   | name               | table_name         | root_page   | sql                                                
--------+--------------------+--------------------+-------------+----------------------------------------
 1      | minisql_schema     |                    | 0           | create table "minisql_schema" (        
        |                    |                    |             | 	type int4 not null,                  
        |                    |                    |             | 	name varchar(255) not null,          
        |                    |                    |             | 	table_name varchar(255),             
        |                    |                    |             | 	root_page int4,                      
        |                    |                    |             | 	sql text                             
        |                    |                    |             | )                                      
 1      | users              |                    | 1           | create table "users" (                 
        |                    |                    |             | 	id int8 primary key autoincrement,   
        |                    |                    |             | 	email varchar(255) unique,           
        |                    |                    |             | 	name text,                           
        |                    |                    |             | 	age int4,                            
        |                    |                    |             | 	created timestamp default now()      
        |                    |                    |             | );                                     
 2      | pkey__users        | users              | 2           | NULL                                   
 3      | key__users_email   | users              | 3           | NULL                                   
 4      | idx_users          | users              | 4           | create index "idx_created" on "users" (             
        |                    |                    |             | 	created,                             
        |                    |                    |             | );                                     
```

## Data Types And Storage

| Data type    | Description |
|--------------|-------------|
| `BOOLEAN`    | 1-byte boolean value (true/false). |
| `INT4`       | 4-byte signed integer (-2,147,483,648 to 2,147,483,647). |
| `INT8`       | 8-byte signed integer (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807). |
| `REAL`       | 4-byte single-precision floating-point number. |
| `DOUBLE`     | 8-byte double-precision floating-point number. |
| `TEXT`       | Variable-length text. If length is <= 255, the text is stored inline, otherwise text is stored in overflow pages (with UTF-8 encoding). |
| `VARCHAR(n)` | Storage works the same way as `TEXT` but allows limiting length of inserted/updated text to max value. |
| `TIMESTAMP`  | 8-byte signed integer representing number of microseconds from `2000-01-01 00:00:00 UTC` (`Postgres epoch`). Supported range is from `4713 BC` to `294276 AD` inclusive. |

## TIMESTAMP Spec

MiniSQL `TIMESTAMP` is a timestamp-without-time-zone type. It stores a calendar date and wall-clock time with microsecond precision, but it does not store or interpret any timezone offset.

- Storage format: signed 64-bit integer counting microseconds since `2000-01-01 00:00:00 UTC` (the PostgreSQL epoch).
- Precision: microseconds. Fractional seconds from 1 to 6 digits are accepted and are scaled to microseconds.
- Calendar model: proleptic Gregorian calendar for the full supported range.
- Supported range: `4713-01-01 00:00:00 BC` through `294276-12-31 23:59:59.999999`.
- BC handling: input and output use PostgreSQL-style ` BC` suffix. Internally, astronomical year numbering is used (`1 BC` = year `0`, `2 BC` = year `-1`).
- `NOW()`: evaluated in UTC and stored as a timezone-naive timestamp value.

Accepted literal forms:

- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DD HH:MM:SS.f`
- `YYYY-MM-DD HH:MM:SS.ff`
- `YYYY-MM-DD HH:MM:SS.ffffff`
- Any of the above with trailing ` BC`

Examples:

```sql
'2024-03-15 10:30:45'
'2024-03-15 10:30:45.1'
'2024-03-15 10:30:45.123456'
'0001-12-31 23:59:59.999999 BC'
```

Important behavior and current non-goals:

- Timezone-qualified values are rejected. Examples: `Z`, `UTC`, `GMT`, `+01:00`, `-05:30`.
- Leap seconds are not supported. Seconds must be in the range `00` to `59`.
- Year `0000` is rejected in input. Use `0001 ... BC` for 1 BC.
- MiniSQL does not currently support `TIMESTAMP WITH TIME ZONE`.
- String formatting normalizes fractional precision to either no fractional part or exactly 6 fractional digits.

## SQL Features

| Feature | Notes |
|---------|-------|
| `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS` | |
| `PRIMARY KEY` | Single column only; no composite primary keys |
| `AUTOINCREMENT` | Primary key must be of type `INT8` |
| `UNIQUE` | Can be specified when creating a table |
| `CHECK` | Constraints to test values whenever they are inserted or updated in a column |
| Composite primary key or unique constraint | As part of `CREATE TABLE` |
| `NULL` and `NOT NULL` | Via null bit mask included in each row/cell |
| `DEFAULT` | Supported for all columns, including `NOW()` for `TIMESTAMP` |
| `DROP TABLE` | |
| `CREATE INDEX`, `DROP INDEX` | Secondary non-unique indexes only; primary and unique indexes are declared as part of `CREATE TABLE` |
| `INSERT` | Single row or multiple rows via a tuple of values separated by commas |
| `ON CONFLICT` | Both `DO NOTHING` and `DO UPDATE` supported (with `EXCLUDED` pseudo table syntax for updating) |
| `SELECT` | All fields with `*`, specific fields, or row count with `COUNT(*)`, |
| `SELECT DISTINCT` | |
| `EXPLAIN`, `EXPLAIN ANALYZE` | Query plan inspection for `SELECT` statements. `EXPLAIN ANALYZE` also executes the query and returns actual row counts and timing |
| `JOIN` | `INNER`, `LEFT` and `RIGHT` joins supported |
| `UPDATE` | |
| `DELETE` | |
| `RETURNING` | Can be used to return columns from `INSERT` or `DELETE` queries, common use case is to return auto incremented primary key |
| `WHERE` | Operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, `BETWEEN`, support for non-correlated scalar subqueries |
| `LIKE`, `NOT LIKE` | `%` matches any sequence of zero or more characters; `_` matches any single character |
| `LIMIT` and `OFFSET` | Basic pagination |
| `ORDER BY` | Single column only |
| `GROUP BY` and `HAVING` | Aggregate functions: `COUNT`, `MAX`, `MIN`, `SUM`, `AVG` |
| Arithmetic expressions | `+`, `-`, `*`, `/` in `SELECT` and `UPDATE SET` (e.g. `price * 1.1`, `count + 1`) |
| Scalar functions | `COALESCE(a, b, ...)` returns first non-NULL argument; `NULLIF(a, b)` returns NULL when `a = b`, else `a`. Both usable in `SELECT`, `UPDATE SET`, and nested inside arithmetic |
| String functions | `UPPER(s)`, `LOWER(s)` — case conversion; `TRIM(s[, chars])`, `LTRIM(s[, chars])`, `RTRIM(s[, chars])` — strip whitespace or custom characters; `LENGTH(s)` — byte length; `SUBSTR(s, start[, len])` — 1-based substring; `REPLACE(s, from, to)` — replace all occurrences; `CONCAT(a, b, ...)` — concatenate (NULLs skipped). All usable in `SELECT`, `UPDATE SET`, and composable with each other and arithmetic |
| Numeric functions | `ABS(n)` — absolute value (preserves input type); `FLOOR(n)`, `CEIL(n)` — floor/ceiling; `ROUND(n[, d])` — round to `d` decimal places (default 0); `MOD(a, b)` — modulo (integer or float). All usable in `SELECT`, `UPDATE SET`, composable with each other and arithmetic |
| Date/time functions | `NOW()` — current UTC timestamp; `DATE_TRUNC('unit', ts)` — truncate to `year`/`month`/`week`/`day`/`hour`/`minute`/`second`; `EXTRACT('field', ts)` / `DATE_PART('field', ts)` — extract numeric field (`year`, `month`, `day`, `hour`, `minute`, `second`, `dow`); `TO_TIMESTAMP('str')` — parse timestamp string into a TIMESTAMP value. All usable in `SELECT`, `UPDATE SET`, composable with other expressions |
| `CASE WHEN` | Searched form: `CASE WHEN cond THEN result … ELSE default END`; simple form: `CASE expr WHEN val THEN result … ELSE default END`. Multiple WHEN clauses, optional ELSE (omitting returns NULL). Usable in `SELECT` (including nested in arithmetic), `UPDATE SET`, supports `IS NULL` / `IS NOT NULL` / all comparison operators in conditions |
| `UNION` / `UNION ALL` | Combine results of two or more `SELECT` statements. `UNION ALL` concatenates all rows (duplicates kept); `UNION` deduplicates the combined result. Chains of three or more branches supported (e.g. `SELECT … UNION ALL SELECT … UNION SELECT …`). Each branch may have its own `WHERE` clause. |
| `CAST(expr AS type)` | Standard SQL type coercion. Supported target types: `BOOLEAN`, `INT4`, `INT8`, `REAL`, `DOUBLE`, `TEXT`, `VARCHAR(n)`, `TIMESTAMP`. Follows SQLite semantics: float→int truncates toward zero; text→int/float parses leading digits (non-numeric input → 0). NULL propagates. Usable anywhere an expression is valid (e.g. `SELECT CAST(price AS INT8)`, `SELECT CAST(n AS TEXT) AS label`). |
| `INTERVAL` arithmetic | PostgreSQL-style interval expressions. Supported units: `year`, `month`, `week`, `day`, `hour`, `minute`, `second`, `microsecond` (singular or plural). Supports compound intervals (`'1 year 3 months'`) and negative values (`'-2 days'`). Operations: `timestamp + interval → timestamp`, `timestamp - interval → timestamp`, `interval + interval → interval`, `interval - interval → interval`, `timestamp - timestamp → interval`. Month arithmetic is calendar-aware — adding 1 month to Jan 31 yields the last day of February. Usable in `SELECT` and `UPDATE SET`, composable with `AS` aliases. Examples: `SELECT created_at + INTERVAL '7 days' AS expires_at`, `SELECT ts - INTERVAL '1 year 6 months'`. |
| `VACUUM` | Rebuilds the database file, repacking it into a minimal amount of disk space (similar to SQLite) |
| `PRAGMA quick_check` | A cheap structural health check of the open database. |
| `PRAGMA integrity_check` | A deeper structural and logical check: page graph, overflow chains, and table/index consistency. Prefer offline use for large databases |
| `PRAGMA wal_checkpoint` | Manually flush WAL frames to the main database file and truncate the WAL. |
| `PRAGMA synchronous` | Read current WAL fsync mode (returns 0/1/2). |
| `PRAGMA synchronous = off\|normal\|full` | Set WAL fsync mode for the current connection. See [WAL Durability Modes](#wal-durability-modes). |


Prepared statements are supported using `?` as a placeholder. For example:

```sql
insert into users("name", "email") values(?, ?), (?, ?);
```

## DDL SQL Commands

### CREATE TABLE

Let's start by creating your first table:

```go
_, err := db.Exec(`create table "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	age int4,
	created timestamp default now()
);`)
```

### DROP TABLE

```go
_, err := db.Exec(`drop table "users";`)
```

### CREATE INDEX

Currently you can only create secondary non unique indexes. Unique and primary index can be created as part of `CREATE TABLE`.

```go
_, err := db.Exec(`create index "idx_created" on "users" (created);`)
```

### DROP INDEX

Currently you can only drop secondary non unique indexes.

```go
_, err := db.Exec(`drop index "idx_created";`)
```

## DML Commands

### INSERT

Insert test rows:

```go
tx, err := s.db.Begin()
if err != nil {
	return err
}
aResult, err := tx.ExecContext(context.Background(), `insert into users("email", "name", "age") values('Danny_Mason2966@xqj6f.tech', 'Danny Mason', 35),
('Johnathan_Walker250@ptr6k.page', 'Johnathan Walker', 32),
('Tyson_Weldon2108@zynuu.video', 'Tyson Weldon', 27),
('Mason_Callan9524@bu2lo.edu', 'Mason Callan', 19),
('Logan_Flynn9019@xtwt3.pro', 'Logan Flynn', 42),
('Beatrice_Uttley1670@1wa8o.org', 'Beatrice Uttley', 32),
('Harry_Johnson5515@jcf8v.video', 'Harry Johnson', 25),
('Carl_Thomson4218@kyb7t.host', 'Carl Thomson', 53),
('Kaylee_Johnson8112@c2nyu.design', 'Kaylee Johnson', 48),
('Cristal_Duvall6639@yvu30.press', 'Cristal Duvall', 27);`)
if err != nil {
	return err
}
rowsAffected, err = aResult.RowsAffected()
if err != nil {
	return err
}
// rowsAffected = 10
if err := tx.Commit(); err != nil {
	if errors.Is(err, minisql.ErrTxConflict) {
		// transaction conflict, you might want to retry here
	}
	return err
}
```

When trying to insert a duplicate primary key, you will get an error:

```go
_, err := db.ExecContext(context.Background(), `insert into users("id", "name", "email", "age") values(1, 'Danny Mason', 'Danny_Mason2966@xqj6f.tech', 35);`)
if err != nil {
	if errors.Is(err, minisql.ErrDuplicateKey) {
		// handle duplicate primary key
	}
	return err
}
```

### SELECT

Selecting from the table:

```go
// type user struct {
// 	ID      int64
// 	Email   string
// 	Name    string
// 	Created time.Time
// }

rows, err := db.QueryContext(context.Background(), `select * from users;`)
if err != nil {
	return err
}
defer rows.Close()
var users []user
for rows.Next() {
	var aUser user
	err := rows.Scan(&aUser.ID, &aUser.Name, &aUser.Email, &aUser.Created)
	if err != nil {
		return err
	}
	users = append(users, aUser)
}
if err := rows.Err(); err != nil {
	return err
}
// continue
```

Table should have 10 rows now:

```sh
 id     | email                            | name                    | age    | created                       
--------+----------------------------------+-------------------------+--------+-------------------------------
 1      | Danny_Mason2966@xqj6f.tech       | Danny Mason             | 35     | 2025-12-21 22:31:35.514831    
 2      | Johnathan_Walker250@ptr6k.page   | Johnathan Walker        | 32     | 2025-12-21 22:31:35.514831    
 3      | Tyson_Weldon2108@zynuu.video     | Tyson Weldon            | 27     | 2025-12-21 22:31:35.514831    
 4      | Mason_Callan9524@bu2lo.edu       | Mason Callan.           | 19     | 2025-12-21 22:31:35.514831    
 5      | Logan_Flynn9019@xtwt3.pro        | Logan Flynn             | 42     | 2025-12-21 22:31:35.514831    
 6      | Beatrice_Uttley1670@1wa8o.org    | Beatrice Uttley         | 32     | 2025-12-21 22:31:35.514831    
 7      | Harry_Johnson5515@jcf8v.video    | Harry Johnson.          | 25     | 2025-12-21 22:31:35.514831    
 8      | Carl_Thomson4218@kyb7t.host      | Carl Thomson            | 53     | 2025-12-21 22:31:35.514831    
 9      | Kaylee_Johnson8112@c2nyu.design  | Kaylee Johnson.         | 48     | 2025-12-21 22:31:35.514831    
 10     | Cristal_Duvall6639@yvu30.press   | Cristal Duvall.         | 27     | 2025-12-21 22:31:35.514831    
```

You can also count rows in a table:

```go
var count int
if err := db.QueryRow(`select count(*) from users;`).Scan(&count); err != nil {
	return err
}
```

You can inspect the query plan for a `SELECT` with `EXPLAIN`. The result columns are `step`, `operation`, `detail`, `rows_estimated`, `rows_actual`, and `duration_us`. For plain `EXPLAIN`, actual rows and duration are `NULL`; `EXPLAIN ANALYZE` executes the query and fills those fields.

```go
type explainStep struct {
	Step          int64
	Operation     string
	Detail        string
	RowsEstimated sql.NullInt64
	RowsActual    sql.NullInt64
	DurationUS    sql.NullInt64
}

rows, err := db.QueryContext(context.Background(), `
	EXPLAIN ANALYZE
	SELECT * FROM users WHERE age >= 30 ORDER BY created DESC;
`)
if err != nil {
	return err
}
defer rows.Close()

var plan []explainStep
for rows.Next() {
	var step explainStep
	if err := rows.Scan(
		&step.Step,
		&step.Operation,
		&step.Detail,
		&step.RowsEstimated,
		&step.RowsActual,
		&step.DurationUS,
	); err != nil {
		return err
	}
	plan = append(plan, step)
}
if err := rows.Err(); err != nil {
	return err
}
```

#### INNER JOIN (star schema)

There is an experimental support for `INNER JOIN`, however it only supports star schema joins, i.e. one or more tables joined with the base table. Nested joins and other types of joins such as `LEFT`, `RIGHT` are not supported yet.

### UPDATE

Let's try using a prepared statement to update a row:

```go
stmt, err := db.Prepare(`update users set age = ? where id = ?;`)
if err != nil {
	return err
}
aResult, err := stmt.Exec(int64(36), int64(1))
if err != nil {
	return err
}
rowsAffected, err = aResult.RowsAffected()
if err != nil {
	return err
}
// rowsAffected = 1
```

Select to verify update:

```sh
 id     | email                            | name                    | age    | created                       
--------+----------------------------------+-------------------------+--------+-------------------------------
 1      | Danny_Mason2966@xqj6f.tech       | Danny Mason             | 36     | 2025-12-21 22:31:35.514831    
```

### DELETE

You can also delete rows:

```go
_, err := db.ExecContext(context.Background(), `delete from users;`)
if err != nil {
	return err
}
rowsAffected, err = aResult.RowsAffected()
if err != nil {
	return err
}
```

## Development 

MiniSQL uses [mockery](https://github.com/vektra/mockery) to generate mocks for interfaces. Install mockery:

```sh
go install github.com/vektra/mockery/v3@v3.6.1
```

Then to generate mocks:

```sh
mockery
```

To run unit tests:

```sh
LOG_LEVEL=info go test ./... -count=1
```

Setting the `LOG_LEVEL` to `info` makes sure to supress debug logs and makes potential error messages in tests easier to read and debug.

### Benchmarking & Profiling

See the [benchmarks/README.md](https://github.com/RichardKnop/minisql/blob/main/benchmarks/README.md) and [benchmarks/RESULTS.md](https://github.com/RichardKnop/minisql/blob/main/benchmarks/RESULTS.md).

## Acknowledgements 

Shout out to some great repos and other resources that were invaluable while figuring out how to get this all working together:
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part1.html)
- [go-sqldb](https://github.com/auxten/go-sqldb)
- [sqlparser](https://github.com/marianogappa/sqlparser)
- [sqlite docs](https://www.sqlite.org/fileformat2.html) (section about file format has been especially useful)
- [C++ implementation of B+ tree](https://github.com/sayef/bplus-tree)
