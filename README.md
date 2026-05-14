# minisql

[![CI Status](https://github.com/RichardKnop/minisql/actions/workflows/go.yml/badge.svg)](https://github.com/RichardKnop/minisql/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/RichardKnop/minisql)](https://goreportcard.com/report/github.com/RichardKnop/minisql)

`MiniSQL` is an embedded single file database written in Golang, inspired by `SQLite`. It is not a clone of `SQLite` in Go, but rather an alternative database which borrows ideas from other databases as well (like Postgres). It can differentiate itself from `SQLite` in several areas: 

1. **Pure Go, zero CGO** — already a differentiator.
2. **MVCC snapshot isolation** — true MVCC for reads already implemented.
3. **Parallel query execution** — SQLite is single-threaded; MiniSQL can parallelize full table scans.
4. **Modern API surface** — idiomatic Go, context-aware, `database/sql` compatible.
5. **JSON as a first-class type** — native `json` column type with path operators and functions (not an extension).
6. **UUID as a first-class type** — native `uuid` column type with 16-byte binary storage, canonical lowercase formatting, and full index support.

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
| `parallel_scan` | `on`, `off` | `off` | Enable concurrent leaf-page scanning for full table scans (see [Parallel Full Table Scan](#parallel-full-table-scan) below) |

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

// Enable parallel full table scans
db, err := sql.Open("minisql", "./my.db?parallel_scan=on")

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

## Parallel Full Table Scan

When a query requires a full table scan (no usable index, or explicit sequential scan), MiniSQL normally reads leaf pages one at a time in a single goroutine. **Parallel scan** splits the leaf-page chain across up to `runtime.NumCPU()` goroutines so that multiple pages are decoded and filtered concurrently.

Parallel scan is **off by default** because it adds overhead for small tables and single-CPU environments. It is most beneficial for large tables on multi-core machines running filter-heavy queries that touch many pages.

**Note:** Parallel scan does **not** guarantee row-ID ordering. Queries that rely on insertion order without an explicit `ORDER BY` may observe a different row sequence.

Enable at connection open time via the connection string:

```go
db, err := sql.Open("minisql", "./my.db?parallel_scan=on")
```

Or toggle at runtime with PRAGMA (affects all existing tables on the connection immediately):

```sql
PRAGMA parallel_scan = on;
PRAGMA parallel_scan;     -- returns 0 (off) or 1 (on)
PRAGMA parallel_scan = off;
```

| Mode | Connection string | PRAGMA | Description |
|------|------------------|--------|-------------|
| off | _(default)_ | `PRAGMA parallel_scan = off` | Single-goroutine sequential leaf scan. Best for small tables or single-CPU environments. |
| on | `parallel_scan=on` | `PRAGMA parallel_scan = on` | Leaf pages partitioned across `runtime.NumCPU()` goroutines. Rows delivered in arrival order (not row-ID order). |

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

### Storage Data Structures

MiniSQL currently uses a few related page-backed trees:

- Tables use a B+ tree keyed by MiniSQL's internal row ID. Leaf pages store rows; internal pages store routing keys and child page references.
- Primary, unique and secondary indexes use the existing B-tree-style index pages. Secondary index keys can point to multiple row IDs.
- Full-text and JSON inverted indexes use dedicated inverted-index pages. An entry tree maps each generated term, such as a text token or JSON key/value term, to postings. Small posting lists are stored inline in the entry leaf. Larger posting lists are promoted to compressed posting leaf pages, with internal posting-tree routing pages keyed by row-id ranges.

The inverted index is therefore not just a regular secondary index with larger value lists. It has two levels of structure: term lookup in the entry tree, then posting lookup/iteration in a posting tree. This keeps high-frequency terms from forcing huge values into entry pages and gives the storage layer room for future optimisations such as better posting compression, posting-tree skipping, and eventually pending-list style batched updates.

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
| `JSON`       | Variable-length JSON document. Stored as compact text (whitespace stripped on write). Validated on insert/update — invalid JSON is rejected. Supports path extraction via `->` / `->>` operators and `JSON_*` functions. See [JSON Type](#json-type). |
| `UUID`       | Fixed 16-byte binary UUID stored inline in B-tree pages. Accepts the standard hyphenated form `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. Upper-case input is normalised to lowercase on write. Invalid values are rejected at insert/update time. Returned as a lowercase hyphenated string. See [UUID Type](#uuid-type). |

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

## JSON Type

The `json` column type stores any valid JSON document — object, array, string, number, boolean, or `null`. Values are validated and compacted on write (whitespace stripped, key insertion order preserved). Invalid JSON is rejected at insert/update time.

```sql
CREATE TABLE events (
    id      int8 primary key autoincrement,
    name    varchar(100) not null,
    payload json
);

INSERT INTO events (name, payload) VALUES ('login', '{"user":"alice","uid":42}');
INSERT INTO events (name, payload) VALUES ('tags',  '["go","sql","json"]');
```

### Path Operators

| Operator | Returns | Description |
|----------|---------|-------------|
| `col -> 'key'` | JSON fragment | Extracts a field and returns it as a JSON-encoded string (the value is still quoted/wrapped). |
| `col ->> 'key'` | SQL scalar | Extracts a field and returns it as a plain SQL value (string unquoted, number as integer or float). |
| `col -> 0` | JSON fragment | Indexes into a JSON array by position (0-based). |
| `col ->> 0` | SQL scalar | Same as above but as a scalar. |

```sql
-- Returns the JSON fragment: "alice"  (quoted)
SELECT payload -> 'user' FROM events WHERE name = 'login';

-- Returns the scalar string: alice  (unquoted)
SELECT payload ->> 'user' FROM events WHERE name = 'login';

-- Returns integer: 42
SELECT payload ->> 'uid' FROM events WHERE name = 'login';

-- Array index: returns "go"
SELECT payload ->> 0 FROM events WHERE name = 'tags';

-- Filter by JSON field value
SELECT name FROM events WHERE payload ->> 'user' = 'alice';
SELECT name FROM events WHERE payload ->> 'uid' = 42;
```

### JSON Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `JSON_EXTRACT(doc, path)` | scalar | Extracts the value at a JSONPath expression as a SQL scalar. Equivalent to `doc ->> path`. Path syntax: `$` (root), `$.key`, `$['key']`, `$[n]`, chainable. |
| `JSON_VALID(val)` | `1` or `0` | Returns `1` if `val` is syntactically valid JSON, `0` otherwise. Useful for validating text columns. |
| `JSON_TYPE(doc[, path])` | text | Returns the JSON type name of the document root, or of the value at `path`. Values: `object`, `array`, `text`, `integer`, `real`, `true`, `false`, `null`. |
| `JSON_ARRAY_LENGTH(doc)` | integer | Returns the number of elements in a JSON array. Returns `NULL` if the document is not an array. |
| `JSON_CONTAINS(doc, query)` | boolean | Returns `true` when `doc` contains `query` as a JSON subset. Object keys are matched recursively, arrays use element containment, and scalar values compare by JSON type/value. |

```sql
-- Extract with JSONPath
SELECT JSON_EXTRACT(payload, '$.user') FROM events WHERE name = 'login';
-- Returns: alice

-- Type inspection
SELECT JSON_TYPE(payload) FROM events WHERE name = 'login';  -- object
SELECT JSON_TYPE(payload) FROM events WHERE name = 'tags';   -- array

SELECT JSON_TYPE(payload, '$.uid') FROM events WHERE name = 'login'; -- integer

-- Array length
SELECT JSON_ARRAY_LENGTH(payload) FROM events WHERE name = 'tags'; -- 3

-- Validate arbitrary text
SELECT JSON_VALID('{"x":1}');  -- 1
SELECT JSON_VALID('bad json'); -- 0

-- JSON containment
SELECT name FROM events WHERE JSON_CONTAINS(payload, '{"user":"alice"}');
```

### JSON Inverted Indexes

MiniSQL supports a v1 JSON inverted index for accelerating literal `JSON_CONTAINS` predicates on one `json` column:

```sql
CREATE INVERTED INDEX idx_events_payload
ON events (payload);

SELECT name
FROM events
WHERE JSON_CONTAINS(payload, '{"type":"click","tags":["web"]}');
```

The v1 index stores generated JSON terms in MiniSQL's dedicated inverted-index storage. Terms include key existence (`k:user.id`) and scalar key/value entries (`kv:type:s:"click"`, `kv:tags[]:s:"web"`), with each term pointing at row-id postings. Small posting lists are stored inline; larger posting lists are promoted to compressed posting pages with internal posting-tree routing pages. Generated terms longer than the current 255-byte index-key limit are skipped; indexed queries are always rechecked against the full row, and queries that cannot produce any indexable terms fall back to sequential evaluation. It does not support path-specific operators or dynamic query expressions yet.

### CAST AS JSON

`CAST(expr AS JSON)` validates and compacts an expression as JSON. Useful for casting a text column or literal to a JSON value.

```sql
SELECT CAST('{"a": 1}' AS JSON);  -- Returns: {"a":1}  (compacted)
```

### Null and Missing Keys

- Inserting `NULL` into a `json` column is allowed (the column stores SQL `NULL`, not the JSON string `"null"`).
- Extracting a key that does not exist returns SQL `NULL`.
- Applying `->` or `->>` to a SQL `NULL` returns `NULL`.

## UUID Type

The `uuid` column type stores a standard UUID in fixed 16-byte binary form, inline in the B-tree page. No overflow pages are used.

- Input is accepted in the standard hyphenated form: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
- Upper-case hex digits are normalised to lower-case on write.
- Invalid UUID strings are rejected at insert/update time with an error.
- Values are returned as lowercase hyphenated strings via the `database/sql` driver.
- UUID columns can be used as primary keys, unique indexes, and secondary indexes.

```sql
CREATE TABLE widgets (
    id    uuid primary key,
    name  varchar(100) not null,
    owner uuid
);
```

### Inserting UUIDs

Pass UUID values as strings via prepared statements:

```go
const uuid1 = "550e8400-e29b-41d4-a716-446655440000"

_, err := db.Exec(
    `INSERT INTO widgets (id, name) VALUES (?, ?)`,
    uuid1, "Widget Alpha",
)
```

Upper-case input is accepted and silently normalised:

```go
_, err := db.Exec(
    `INSERT INTO widgets (id, name) VALUES (?, ?)`,
    "6BA7B810-9DAD-11D1-80B4-00C04FD430C8", "Widget Beta",
)
// Stored and returned as: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

### Querying UUIDs

```go
rows, err := db.Query(`SELECT id, name FROM widgets WHERE id = ?`, uuid1)
// ...
var gotID, gotName string
rows.Scan(&gotID, &gotName)
// gotID == "550e8400-e29b-41d4-a716-446655440000"
```

### CAST with UUID

```sql
-- Parse a text literal as UUID (validates and stores in binary form)
SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);

-- Format a UUID column back to text
SELECT CAST(id AS TEXT) FROM widgets WHERE name = 'Widget Alpha';
```

### Nullable UUID columns

```go
var owner *string
rows.Scan(&owner)
// owner == nil when the column value is NULL
```

## SQL Features

| Feature | Notes |
|---------|-------|
| `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS` | |
| `PRIMARY KEY` | Single column only; no composite primary keys |
| `AUTOINCREMENT` | Primary key must be of type `INT8` |
| `UNIQUE` | Can be specified when creating a table |
| `CHECK` | Constraints to test values whenever they are inserted or updated in a column |
| `FOREIGN KEY` | Single-column FK constraints with `RESTRICT` / `NO ACTION`; declared inside `CREATE TABLE`. `PRAGMA foreign_keys = on\|off` (default on). See [Foreign Keys](#foreign-keys). |
| Composite primary key or unique constraint | As part of `CREATE TABLE` |
| `NULL` and `NOT NULL` | Via null bit mask included in each row/cell |
| `DEFAULT` | Supported for all columns, including `NOW()` for `TIMESTAMP` |
| `DROP TABLE` | |
| `CREATE INDEX`, `DROP INDEX` | Secondary non-unique indexes; primary and unique indexes are declared as part of `CREATE TABLE`. Supports composite (multi-column), partial (`WHERE` clause), and expression indexes. See [Indexes](#indexes). |
| `INSERT` | Single row or multiple rows via a tuple of values separated by commas |
| `ON CONFLICT` | Both `DO NOTHING` and `DO UPDATE` supported (with `EXCLUDED` pseudo table syntax for updating) |
| `SELECT` | All fields with `*`, specific fields, or row count with `COUNT(*)`, derived tables support |
| `SELECT DISTINCT` | |
| `WITH` | Basic support for `CTEs`, SELECT only currently |
| `EXPLAIN`, `EXPLAIN ANALYZE` | Query plan inspection for `SELECT` statements. `EXPLAIN ANALYZE` also executes the query and returns actual row counts and timing |
| `JOIN` | `INNER`, `LEFT` and `RIGHT` joins supported |
| `UPDATE` | Standard `UPDATE t SET col = val WHERE …` |
| `UPDATE … FROM` | PostgreSQL-style multi-table update: `UPDATE t1 [AS alias] SET col = t2.val FROM t2 [AS alias] WHERE join_condition`. The `FROM` source can be a table name or a subquery (`FROM (SELECT …) AS alias`). Each target row may match at most one FROM row; zero matches leaves the row unchanged. SET expressions can reference columns from both tables (e.g. `SET salary = dept.budget / 10`). |
| `DELETE` | |
| `RETURNING` | Can be used to return columns from `INSERT` or `DELETE` queries, common use case is to return auto incremented primary key |
| `WHERE` | Operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, `BETWEEN`, support for SELECT only non-correlated scalar subqueries |
| `LIKE`, `NOT LIKE` | `%` matches any sequence of zero or more characters; `_` matches any single character |
| `LIMIT` and `OFFSET` | Basic pagination |
| `ORDER BY` | Single column only |
| `GROUP BY` and `HAVING` | Aggregate functions: `COUNT`, `MAX`, `MIN`, `SUM`, `AVG` |
| Arithmetic expressions | `+`, `-`, `*`, `/` in `SELECT` and `UPDATE SET` (e.g. `price * 1.1`, `count + 1`) |
| Scalar functions | `COALESCE(a, b, ...)` returns first non-NULL argument; `NULLIF(a, b)` returns NULL when `a = b`, else `a`. Both usable in `SELECT`, `UPDATE SET`, and nested inside arithmetic |
| String functions | `UPPER(s)`, `LOWER(s)` — case conversion; `TRIM(s[, chars])`, `LTRIM(s[, chars])`, `RTRIM(s[, chars])` — strip whitespace or custom characters; `LENGTH(s)` — byte length; `SUBSTR(s, start[, len])` — 1-based substring; `REPLACE(s, from, to)` — replace all occurrences; `CONCAT(a, b, ...)` — concatenate (NULLs skipped). All usable in `SELECT`, `UPDATE SET`, and composable with each other and arithmetic |
| Numeric functions | `ABS(n)` — absolute value (preserves input type); `FLOOR(n)`, `CEIL(n)` — floor/ceiling; `ROUND(n[, d])` — round to `d` decimal places (default 0); `MOD(a, b)` — modulo (integer or float). All usable in `SELECT`, `UPDATE SET`, composable with each other and arithmetic |
| Date/time functions | `NOW()` — current UTC timestamp; `DATE_TRUNC('unit', ts)` — truncate to `year`/`month`/`week`/`day`/`hour`/`minute`/`second`; `EXTRACT('field', ts)` / `DATE_PART('field', ts)` — extract numeric field (`year`, `month`, `day`, `hour`, `minute`, `second`, `dow`); `TO_TIMESTAMP('str')` — parse timestamp string into a TIMESTAMP value. All usable in `SELECT`, `UPDATE SET`, composable with other expressions |
| Full-text search functions | `MATCH(doc, query)` and `TS_RANK(doc, query)` provide initial full-text semantics. `CREATE FULLTEXT INDEX` can accelerate literal `MATCH` predicates on one `TEXT`/`VARCHAR` column. |
| `CASE WHEN` | Searched form: `CASE WHEN cond THEN result … ELSE default END`; simple form: `CASE expr WHEN val THEN result … ELSE default END`. Multiple WHEN clauses, optional ELSE (omitting returns NULL). Usable in `SELECT` (including nested in arithmetic), `UPDATE SET`, supports `IS NULL` / `IS NOT NULL` / all comparison operators in conditions |
| `UNION` / `UNION ALL` | Combine results of two or more `SELECT` statements. `UNION ALL` concatenates all rows (duplicates kept); `UNION` deduplicates the combined result. Chains of three or more branches supported (e.g. `SELECT … UNION ALL SELECT … UNION SELECT …`). Each branch may have its own `WHERE` clause. |
| `CAST(expr AS type)` | Standard SQL type coercion. Supported target types: `BOOLEAN`, `INT4`, `INT8`, `REAL`, `DOUBLE`, `TEXT`, `VARCHAR(n)`, `TIMESTAMP`, `JSON`, `UUID`. Follows SQLite semantics: float→int truncates toward zero; text→int/float parses leading digits (non-numeric input → 0). `CAST(x AS JSON)` validates and compacts the value. `CAST(x AS UUID)` parses a UUID string and stores it in binary form. `CAST(uuid_col AS TEXT)` formats the 16-byte value back to a hyphenated lowercase string. NULL propagates. Usable anywhere an expression is valid (e.g. `SELECT CAST(price AS INT8)`, `SELECT CAST(n AS TEXT) AS label`, `SELECT CAST(id AS TEXT) FROM widgets`). |
| JSON operators | `col -> 'key'` — extract a JSON field and return it as a JSON fragment (quoted string, array, object). `col ->> 'key'` — extract a JSON field and return it as a SQL scalar (unquoted string, integer, or float). Integer keys index into arrays (e.g. `col -> 0`). Both operators work in `SELECT` and `WHERE`. See [JSON Type](#json-type). |
| JSON functions | `JSON_EXTRACT(doc, path)` — extract value at JSON path as a scalar (equivalent to `->>`). `JSON_VALID(val)` — returns `1` if the value is valid JSON, `0` otherwise. `JSON_TYPE(doc[, path])` — returns the JSON type name. `JSON_ARRAY_LENGTH(doc)` — returns the number of elements in a JSON array. `JSON_CONTAINS(doc, query)` — tests JSON subset containment and can use `CREATE INVERTED INDEX` for literal predicates. See [JSON Type](#json-type). |
| `INTERVAL` arithmetic | PostgreSQL-style interval expressions. Supported units: `year`, `month`, `week`, `day`, `hour`, `minute`, `second`, `microsecond` (singular or plural). Supports compound intervals (`'1 year 3 months'`) and negative values (`'-2 days'`). Operations: `timestamp + interval → timestamp`, `timestamp - interval → timestamp`, `interval + interval → interval`, `interval - interval → interval`, `timestamp - timestamp → interval`. Month arithmetic is calendar-aware — adding 1 month to Jan 31 yields the last day of February. Usable in `SELECT` and `UPDATE SET`, composable with `AS` aliases. Examples: `SELECT created_at + INTERVAL '7 days' AS expires_at`, `SELECT ts - INTERVAL '1 year 6 months'`. |
| `VACUUM` | Rebuilds the database file, repacking it into a minimal amount of disk space (similar to SQLite) |
| `PRAGMA quick_check` | A cheap structural health check of the open database. |
| `PRAGMA integrity_check` | A deeper structural and logical check: page graph, overflow chains, and table/index consistency. Prefer offline use for large databases |
| `PRAGMA wal_checkpoint` | Manually flush WAL frames to the main database file and truncate the WAL. |
| `PRAGMA synchronous` | Read current WAL fsync mode (returns 0/1/2). |
| `PRAGMA synchronous = off\|normal\|full` | Set WAL fsync mode for the current connection. See [WAL Durability Modes](#wal-durability-modes). |
| `PRAGMA parallel_scan` | Read current parallel scan state (returns 0 = off, 1 = on). |
| `PRAGMA parallel_scan = on\|off` | Enable or disable concurrent leaf-page scanning for full table scans. See [Parallel Full Table Scan](#parallel-full-table-scan). |


### Scalar Functions Reference

#### String Functions

| Function | Description |
|----------|-------------|
| `UPPER(s)` | Convert string to upper case. |
| `LOWER(s)` | Convert string to lower case. |
| `TRIM(s[, chars])` | Strip leading and trailing whitespace, or the given characters. |
| `LTRIM(s[, chars])` | Strip leading whitespace or characters. |
| `RTRIM(s[, chars])` | Strip trailing whitespace or characters. |
| `LENGTH(s)` | Byte length of the string. |
| `SUBSTR(s, start[, len])` | 1-based substring extraction. |
| `REPLACE(s, from, to)` | Replace all occurrences of `from` with `to`. |
| `CONCAT(a, b, ...)` | Concatenate arguments, skipping NULLs. |

#### Numeric Functions

| Function | Description |
|----------|-------------|
| `ABS(n)` | Absolute value; preserves input type (`INT8` or `DOUBLE`). |
| `FLOOR(n)` | Largest integer not greater than `n`. |
| `CEIL(n)` | Smallest integer not less than `n`. |
| `ROUND(n[, d])` | Round to `d` decimal places (default 0). |
| `MOD(a, b)` | Modulo; integer or float depending on inputs. |

#### Date / Time Functions

| Function | Description |
|----------|-------------|
| `NOW()` | Current UTC timestamp. |
| `DATE_TRUNC('unit', ts)` | Truncate timestamp to `year`, `month`, `week`, `day`, `hour`, `minute`, or `second`. |
| `EXTRACT('field', ts)` | Extract numeric field from timestamp: `year`, `month`, `day`, `hour`, `minute`, `second`, `dow`. |
| `DATE_PART('field', ts)` | Alias for `EXTRACT`. |
| `TO_TIMESTAMP('str')` | Parse a timestamp string into a `TIMESTAMP` value. |

#### Conditional Functions

| Function | Description |
|----------|-------------|
| `COALESCE(a, b, ...)` | Return the first non-NULL argument. |
| `NULLIF(a, b)` | Return `NULL` when `a = b`, otherwise return `a`. |

#### JSON Functions

| Function | Description |
|----------|-------------|
| `JSON_EXTRACT(doc, path)` | Extract value at JSONPath as a SQL scalar. |
| `JSON_VALID(val)` | `1` if `val` is valid JSON, `0` otherwise. |
| `JSON_TYPE(doc[, path])` | JSON type name of the value (`object`, `array`, `text`, `integer`, `real`, `true`, `false`, `null`). |
| `JSON_ARRAY_LENGTH(doc)` | Number of elements in a JSON array. |
| `JSON_CONTAINS(doc, query)` | Boolean JSON subset containment, indexable with `CREATE INVERTED INDEX` when the query JSON is a literal. |

#### Full-Text Search Functions

MiniSQL supports initial full-text search semantics with an optional v1 full-text index. Without an index, `MATCH` scans candidate rows, tokenizes the document and query in memory, and evaluates the match during the normal `WHERE` filter.

```sql
CREATE FULLTEXT INDEX idx_articles_body
ON articles (body)
WITH (tokenizer = 'simple');
```

The v1 index uses MiniSQL's dedicated inverted-index storage: an entry tree maps each unique token to ordered positional postings `(row ID, token position)`. Small posting lists are stored inline in the entry leaf; larger posting lists are promoted to compressed posting pages with internal posting-tree routing pages. Literal `MATCH(body, 'mini database')` predicates can use the index by intersecting posting rows for all query tokens; quoted phrases such as `MATCH(body, '"database pages"')` additionally require adjacent token positions. Dynamic query expressions and queries containing tokens longer than the current 255-byte index-key limit fall back to the sequential semantics.

| Function | Description |
|----------|-------------|
| `MATCH(doc, query)` | Returns `true` when every non-stop-word query token appears in `doc`. Double-quoted phrases require adjacent indexed token positions, e.g. `WHERE MATCH(body, '"mini database"')`. |
| `TS_RANK(doc, query)` | Returns a relevance score that combines saturated term frequency, query coverage, mild document-length normalization, exact phrase boosts, and token-proximity boosts. |

Tokenizer v1 lowercases text, splits on non-letter/non-digit boundaries, removes a small built-in English stop-word list, and does not perform stemming. For example, `database` and `databases` are different tokens.

```sql
SELECT id, TS_RANK(body, 'mini database') AS score
FROM articles
WHERE MATCH(body, 'mini database')
ORDER BY score DESC;

SELECT id
FROM articles
WHERE MATCH(body, 'mini "database pages"');
```

### Operators Reference

#### Comparison Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal. |
| `!=` | Not equal. |
| `>` | Greater than. |
| `>=` | Greater than or equal. |
| `<` | Less than. |
| `<=` | Less than or equal. |
| `IS NULL` | Value is NULL. |
| `IS NOT NULL` | Value is not NULL. |
| `IN (...)` | Value is in a list or subquery result. |
| `NOT IN (...)` | Value is not in a list or subquery result. |
| `BETWEEN a AND b` | Value is between `a` and `b` inclusive. |
| `NOT BETWEEN a AND b` | Value is outside `a` and `b`. |
| `LIKE pattern` | String matches pattern (`%` = any sequence, `_` = single char, case-sensitive). |
| `NOT LIKE pattern` | String does not match pattern. |

#### Arithmetic Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition (numeric or `timestamp + interval`). |
| `-` | Subtraction (numeric, `timestamp - interval`, or `timestamp - timestamp → interval`). |
| `*` | Multiplication. |
| `/` | Division (always returns `DOUBLE` when either side is fractional). |

#### JSON Path Operators

| Operator | Returns | Description |
|----------|---------|-------------|
| `col -> key` | JSON fragment | Extract field or array element; result is JSON-encoded. |
| `col ->> key` | SQL scalar | Extract field or array element; result is a plain SQL value. |

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

### Indexes

MiniSQL supports several index types, each suited to a different access pattern. The query planner picks the best available index automatically using cost estimation based on statistics collected by `ANALYZE`.

#### Index Types at a Glance

| Type | Where declared | Example |
|------|---------------|---------|
| Primary key | `CREATE TABLE` column definition | `id INT8 PRIMARY KEY AUTOINCREMENT` |
| Unique (single column) | `CREATE TABLE` column definition | `email VARCHAR(255) UNIQUE` |
| Unique (composite) | `CREATE TABLE` table constraint | `UNIQUE (first_name, last_name)` |
| Secondary | `CREATE INDEX` | `CREATE INDEX idx ON t (col)` |
| Composite | `CREATE INDEX` | `CREATE INDEX idx ON t (col1, col2)` |
| Partial | `CREATE INDEX … WHERE` | `CREATE INDEX idx ON t (col) WHERE active = true` |
| Expression | `CREATE INDEX` with expression | `CREATE INDEX idx ON t (LOWER(col))` |
| Full-text | `CREATE FULLTEXT INDEX` | `CREATE FULLTEXT INDEX idx ON articles (body) WITH (tokenizer = 'simple')` |
| JSON inverted | `CREATE INVERTED INDEX` | `CREATE INVERTED INDEX idx ON events (payload)` |

#### Primary Key Index

Declared inline as part of `CREATE TABLE`. Only a single-column primary key is supported. Using `AUTOINCREMENT` requires the type to be `INT8`.

```sql
CREATE TABLE users (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255)
);
```

The primary key index is always a unique B+ tree keyed by the row ID.

#### Unique Index

A unique constraint creates a B+ tree index that rejects duplicate values. It can be declared inline on a single column or as a table-level constraint for multi-column uniqueness:

```sql
-- Inline single-column unique
CREATE TABLE users (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) UNIQUE
);

-- Table-level composite unique constraint
CREATE TABLE memberships (
    user_id INT8  NOT NULL,
    org_id  INT8  NOT NULL,
    UNIQUE (user_id, org_id)
);
```

Attempting to insert or update a row that would violate a unique constraint returns `ErrDuplicateKey`.

#### Secondary Index (Non-Unique)

A plain secondary index speeds up equality and range lookups on a column without enforcing uniqueness.

```sql
CREATE INDEX idx_users_created ON users (created);
DROP INDEX idx_users_created;
```

```go
_, err := db.Exec(`CREATE INDEX "idx_users_created" ON "users" (created);`)
```

Use `ANALYZE` after bulk inserts to update the row-count and cardinality statistics that the planner relies on when choosing between a sequential scan and an index scan.

#### Composite Index

A composite index covers multiple columns. The planner can use it for:

- Equality or range filters on any **prefix** of the index columns.
- `ORDER BY` — when the query orders by exactly the same columns in the same sequence and direction as the index, the planner uses the index to read rows in order and skips the in-memory sort.

```sql
-- Index supporting WHERE last_name = ? AND first_name = ?
-- and ORDER BY last_name, first_name
CREATE INDEX idx_users_name ON users (last_name, first_name);
```

Mixed `ASC`/`DESC` on different columns still falls back to an in-memory sort. All columns must share the same direction for the ORDER BY optimisation to apply.

#### Partial Index

A partial index only stores entries for rows that satisfy a `WHERE` predicate. This makes the index smaller and faster when the interesting subset of rows is much smaller than the full table.

```sql
-- Only index active users — WHERE queries that include active = true can use it
CREATE INDEX idx_active_users ON users (email) WHERE active = true;

-- Only index high-value orders
CREATE INDEX idx_large_orders ON orders (amount DESC) WHERE amount > 1000;

-- Compound predicate
CREATE INDEX idx_pending_recent ON orders (created DESC)
    WHERE status = 'pending' AND amount > 0;
```

The planner uses a partial index when every term in the index's `WHERE` clause also appears verbatim in the query's `WHERE` clause. This check is conservative (syntactic containment), so complex rewrites or equivalent but differently structured conditions will not trigger the optimisation; in those cases the planner falls back to a sequential scan or a full secondary index.

```sql
-- Uses idx_active_users ✓
SELECT email FROM users WHERE active = true AND email LIKE 'a%';

-- Falls back to sequential scan — predicate is not a superset of the index predicate
SELECT email FROM users WHERE email LIKE 'a%';
```

Rows that do not satisfy the partial index predicate are never stored in the index. `INSERT`, `UPDATE`, and `DELETE` automatically maintain the index for qualifying rows only.

#### Expression Index

An expression index keys the B+ tree on the *result* of evaluating a SQL expression rather than a raw column value. The most common use case is case-insensitive search:

```sql
-- Create the index on the lower-cased name
CREATE INDEX idx_users_lower_name ON users (LOWER(name));

-- The planner automatically uses the index for this query
SELECT * FROM users WHERE LOWER(name) = 'alice';
```

The planner uses an expression index when the expression in the `WHERE` clause is structurally identical to the indexed expression — the function name, arguments, and any operators must match exactly.

**Supported expression forms:**

| Expression | Example | Key type |
|-----------|---------|----------|
| String functions | `LOWER(col)`, `UPPER(col)`, `TRIM(col)`, `SUBSTR(col, 1, 3)`, `REPLACE(col, 'a', 'b')`, `CONCAT(a, b)` | `VARCHAR` |
| Numeric functions | `ABS(col)`, `FLOOR(col)`, `CEIL(col)`, `ROUND(col, 2)`, `MOD(col, 10)`, `LENGTH(col)` | `INT8` / `DOUBLE` |
| Date/time functions | `DATE_TRUNC('month', ts)`, `TO_TIMESTAMP(col)` | `TIMESTAMP` |
| Date extraction | `EXTRACT(year FROM ts)`, `DATE_PART('month', ts)` | `INT8` |
| Arithmetic | `price * quantity`, `score + bonus`, `cost / 100` | `INT8` / `DOUBLE` |
| JSON path | `payload ->> 'status'`, `data -> 'meta' ->> 'id'` | `VARCHAR` |
| Type cast | `CAST(col AS INT8)`, `CAST(col AS TEXT)` | target type |
| Chained functions | `LOWER(TRIM(col))`, `ABS(price * discount)` | inferred |

```sql
-- Arithmetic expression index (e.g. for computed total)
CREATE INDEX idx_line_total ON order_lines (price * quantity);
SELECT * FROM order_lines WHERE price * quantity > 500;

-- JSON field expression index
CREATE INDEX idx_event_type ON events (payload ->> 'type');
SELECT * FROM events WHERE payload ->> 'type' = 'login';

-- Date truncation index (monthly bucketing)
CREATE INDEX idx_orders_month ON orders (DATE_TRUNC('month', created));
SELECT * FROM orders WHERE DATE_TRUNC('month', created) = '2024-01-01 00:00:00';

-- Year extraction
CREATE INDEX idx_orders_year ON orders (EXTRACT(year FROM created));
SELECT * FROM orders WHERE EXTRACT(year FROM created) = 2024;

-- Chained functions
CREATE INDEX idx_norm_email ON users (LOWER(TRIM(email)));
SELECT * FROM users WHERE LOWER(TRIM(email)) = 'alice@example.com';
```

Expression indexes only store entries for rows where the expression evaluates to a non-NULL result. `INSERT`, `UPDATE`, and `DELETE` evaluate the expression automatically to keep the index up to date.

`NOW()` and other non-deterministic functions are rejected at `CREATE INDEX` time.

#### Covering Index (Index-Only Scan)

When all columns referenced by a query are present in the index, MiniSQL performs an **index-only scan** — it reads the result entirely from the index pages without touching the main table. This avoids the extra I/O of looking up each row by its row ID.

```sql
-- Index covers both the filter column and the selected column
CREATE INDEX idx_users_email_name ON users (email, name);

-- Index-only scan: no table pages read
SELECT name FROM users WHERE email = 'alice@example.com';
```

The planner picks index-only scans automatically when the covering condition is satisfied.

#### Updating Statistics for the Planner

The query planner uses per-table and per-index row-count estimates collected by `ANALYZE`. After bulk inserts or significant data changes, run `ANALYZE` to refresh statistics so the planner can make accurate cost comparisons:

```sql
ANALYZE;              -- analyze all tables
ANALYZE users;        -- analyze one table
```

```go
_, err := db.Exec(`ANALYZE;`)
```

Without up-to-date statistics the planner may over- or under-estimate the selectivity of an index and choose a sequential scan instead.

### DROP INDEX

```go
_, err := db.Exec(`DROP INDEX "idx_created";`)
```

### Foreign Keys

MiniSQL supports single-column foreign key constraints declared inside `CREATE TABLE`. There is no `ALTER TABLE ADD CONSTRAINT` — FKs must be defined at table-creation time, following the same approach as SQLite.

Three equivalent syntax forms are accepted:

```sql
-- 1. Inline REFERENCES on the column definition
CREATE TABLE orders (
    id      int8 primary key autoincrement,
    user_id int8 not null references "users" (id)
);

-- 2. Table-level FOREIGN KEY clause
CREATE TABLE orders (
    id      int8 primary key autoincrement,
    user_id int8 not null,
    foreign key (user_id) references "users" (id)
);

-- 3. Named constraint (CONSTRAINT … FOREIGN KEY)
CREATE TABLE orders (
    id      int8 primary key autoincrement,
    user_id int8 not null,
    constraint fk_orders_users foreign key (user_id) references "users" (id)
);
```

**Rules:**
- The referenced column must be a `PRIMARY KEY` or `UNIQUE` column in the parent table.
- A `NULL` value in the FK column bypasses the check (the row is accepted without a matching parent row).
- Dropping a parent table while a child FK still references it is blocked. Drop the child table first.

**Referential actions** (specified via `ON DELETE` / `ON UPDATE`):

| Action | Syntax | Behaviour in MiniSQL |
|--------|--------|----------------------|
| `RESTRICT` | `ON DELETE RESTRICT` | **Default.** Immediately rejects any `DELETE` or `UPDATE` on the parent row if a matching child row exists. |
| `NO ACTION` | `ON DELETE NO ACTION` | Identical to `RESTRICT` in the current implementation. In the SQL standard the check can be deferred to end-of-statement; MiniSQL always checks immediately because deferred constraints are not yet supported. |
| `CASCADE` | `ON DELETE CASCADE` | Parsed but **not yet implemented** — returns an error at `CREATE TABLE` time. |
| `SET NULL` | `ON DELETE SET NULL` | Parsed but **not yet implemented** — returns an error at `CREATE TABLE` time. |

When `ON DELETE` or `ON UPDATE` is omitted, `RESTRICT` is used.

FK enforcement can be toggled at runtime:

```sql
PRAGMA foreign_keys;           -- returns 1 (on) or 0 (off)
PRAGMA foreign_keys = off;     -- disable FK checks for this connection
PRAGMA foreign_keys = on;      -- re-enable FK checks
```

FK checks are **on by default**. The pragma state is per-connection and is not persisted.

```go
// Example: insert a child row with a valid parent
_, err = db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)

// Example: this fails with ErrForeignKeyViolation — user_id 99 does not exist
_, err = db.Exec(`insert into "orders" (user_id, amount) values (99, 100)`)
if err != nil {
    var fkErr minisqlErrors.ErrForeignKeyViolation
    if errors.As(err, &fkErr) {
        fmt.Printf("FK violation: %s.%s → %s.%s\n",
            fkErr.ChildTable, fkErr.ChildColumn,
            fkErr.ParentTable, fkErr.ParentColumn)
    }
}

// Example: deleting a parent row that still has children fails with ErrForeignKeyParentViolation
_, err = db.Exec(`delete from "users" where id = 1`)
if err != nil {
    var fkErr minisqlErrors.ErrForeignKeyParentViolation
    if errors.As(err, &fkErr) {
        fmt.Printf("parent FK violation: %s.%s referenced by %s.%s\n",
            fkErr.ParentTable, fkErr.ParentColumn,
            fkErr.ChildTable, fkErr.ChildColumn)
    }
}
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

#### JOINs

`INNER JOIN`, `LEFT JOIN`, and `RIGHT JOIN` are supported. Arbitrary chain topologies work — three or more tables can be joined in sequence, not just star-schema patterns.

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

### UPDATE FROM

PostgreSQL-style `UPDATE … FROM` lets you set column values based on data from a second table (or subquery). The target table can have an optional alias; the `FROM` source can be a table name or a derived subquery.

```go
// Set each employee's salary to the budget of their department divided by 10.
_, err := db.ExecContext(context.Background(), `
    update employees e
    set salary = d.budget / 10
    from departments d
    where e.dept_id = d.id
`)
if err != nil {
    return err
}

// Same query using explicit AS aliases.
_, err = db.ExecContext(context.Background(), `
    update employees as emp
    set salary = dept.budget / 10
    from departments as dept
    where emp.dept_id = dept.id
`)

// FROM can also be a subquery.
_, err = db.ExecContext(context.Background(), `
    update employees e
    set salary = d.budget
    from (select id, budget from departments where id = 1) as d
    where e.dept_id = d.id
`)
```

Each target row may match **at most one** FROM row; zero matches leaves the row unchanged. If more than one FROM row matches a single target row, the statement returns an error.

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
