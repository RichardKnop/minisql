# minisql

[![CI Status](https://github.com/RichardKnop/minisql/actions/workflows/go.yml/badge.svg)](https://github.com/RichardKnop/minisql/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/RichardKnop/minisql)](https://goreportcard.com/report/github.com/RichardKnop/minisql)

`MiniSQL` is an embedded single file database written in Golang, inspired by `SQLite` (however borrowing some features from other databases as well). It originally started as a research project aimed at learning about internals of relational databases. Over time it has progressed and grown to its current form. It is a very early stage project and it might contain bugs and is not battle tested. Please employ caution when using this database.

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
db, err := sql.Open("minisql", "./my.db?journal=false")

// Multiple parameters
db, err := sql.Open("minisql", "./my.db?journal=true&log_level=debug")
```

## Connection Pooling

**MiniSQL is an embedded, single-file database (like SQLite).** Always configure your connection pool to use a single connection and serialize all writes through it.

```go
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
```

**Why?** Multiple connections to the same file cause:
- Lock contention at the OS level
- Connection pool overhead (97% lock time in benchmarks)
- No performance benefit (writes are serialized anyway)

This is the same recommendation as SQLite.

## Connection String Parameters

MiniSQL supports optional connection string parameters:

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `journal` | `true`, `false` | `true` | Enable/disable rollback journal for crash recovery |
| `log_level` | `debug`, `info`, `warn`, `error` | `warn` | Set logging verbosity level |
| `max_cached_pages` | positive integer | `2000` | Maximum number of pages to keep in memory cache |

**Examples:**
```go
// Disable journaling for better performance (no crash recovery)
db, err := sql.Open("minisql", "./my.db?journal=false")

// Enable debug logging
db, err := sql.Open("minisql", "./my.db?log_level=debug")

// Set cache size to 500 pages (~2MB memory)
db, err := sql.Open("minisql", "./my.db?max_cached_pages=500")

// Combine multiple parameters
db, err := sql.Open("minisql", "/path/to/db.db?journal=true&log_level=info&max_cached_pages=2000")
```

**Note:** Disabling journaling (`journal=false`) improves performance but removes crash recovery protection. If the application crashes during a transaction commit, the database may become corrupted.

##  Write-Ahead Rollback Journal

MiniSQL uses a [rollback journal](https://sqlite.org/lockingv3.html#rollback) to achieve atomic commit and rollback. Before committing a transaction, a journal file with `-journal` suffix is created in the same directory as the database file. It contains the original state of modified pages, and the original database header if the transaction changes it.

Current recovery protocol:

1. Write original page/header bytes into `{dbpath}-journal`.
2. Finalize the journal header with the final page count and `Sync()` it to disk.
3. Only then flush modified database pages to disk.
4. Delete the journal after a clean commit.

On startup, if a journal file exists, MiniSQL treats the finalized journal header as the completeness signal for recovery. Recovery only proceeds when:

- the journal header magic/version/checksum are valid
- the journal page size matches the database page size
- the journal body is exactly as long as the finalized header says it should be

If the journal is truncated, corrupt, or contains trailing bytes beyond the finalized contents, recovery fails closed and the journal file is left in place for inspection.

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

MiniSQL uses `Optimistic Concurrency Control` or `OCC`. It is close to PostgreSQL's [SERIALIZABLE isolation](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-SERIALIZABLE) Transaction manager follows a simple process:

1. Track read versions - Record which version of each page was read
2. Check at commit time - Verify no pages were modified during the transaction
3. Abort on conflict - If a page changed, abort with ErrTxConflict

You can use `ErrTxConflict` to control whether to retry because of a tx serialization error or to return error.

SQlite uses a `snapshot isolation` with `MVCC` (`Multi-Version Concurrency Control`). Read how [SQLite handles isolation](https://sqlite.org/isolation.html). I have chosen a basic OCC model for now for its simplicity.

Example of a snapshot isolation:

```
Time 0: Read TX1 starts, sees version V1
Time 1: Write TX2 modifies page and commits → creates version V2
Time 2: TX1 continues reading, still sees V1 (not V2!)
Time 3: TX1 completes successfully
```

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
| Composite primary key or unique constraint | As part of `CREATE TABLE` |
| `NULL` and `NOT NULL` | Via null bit mask included in each row/cell |
| `DEFAULT` | Supported for all columns, including `NOW()` for `TIMESTAMP` |
| `DROP TABLE` | |
| `CREATE INDEX`, `DROP INDEX` | Secondary non-unique indexes only; primary and unique indexes are declared as part of `CREATE TABLE` |
| `INSERT` | Single row or multiple rows via a tuple of values separated by commas |
| `ON CONFLICT` | Both `DO NOTHING` and `DO UPDATE` supported (with `EXCLUDED` pseudo table syntax for updating) |
| `SELECT` | All fields with `*`, specific fields, or row count with `COUNT(*)` |
| `SELECT DISTINCT` | |
| `JOIN` | `INNER`, `LEFT` and `RIGHT` joins supported. Star schema only — one or more tables joined with the base table |
| `UPDATE` | |
| `DELETE` | |
| `WHERE` | Operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, `BETWEEN` |
| `LIKE`, `NOT LIKE` | `%` matches any sequence of zero or more characters; `_` matches any single character |
| `LIMIT` and `OFFSET` | Basic pagination |
| `ORDER BY` | Single column only |
| `GROUP BY` and `HAVING` | Aggregate functions: `COUNT`, `MAX`, `MIN`, `SUM`, `AVG` |
| `VACUUM` | Rebuilds the database file, repacking it into a minimal amount of disk space (similar to SQLite) |
| `PRAGMA quick_check` | A cheap structural health check of the open database. |
| `PRAGMA integrity_check` | A deeper structural and logical check: page graph, overflow chains, and table/index consistency. Prefer offline use for large databases |


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

### Benchmarking


Some benchmarking commands I have used just for my own reference.

```sh
go test -bench=BenchmarkPageAccess -benchtime=100000x ./internal/minisql 2>&1 | grep -A 20 "Benchmark"
go test -bench=BenchmarkRow -benchmem ./internal/minisql 2>&1 | grep -E "(Benchmark|B/op)"
go test -bench=BenchmarkFlush -benchmem -benchtime=5s ./internal/minisql 2>&1 | grep -E "(Benchmark|B/op)"
go test -bench=BenchmarkFlush -benchmem -benchtime=3s ./internal/minisql 2>&1 | grep -E "(Benchmark|B/op)"
go test -bench=BenchmarkPageAccess -benchmem ./internal/minisql 2>&1 | grep -E "Benchmark|alloc"

# CPU profile concurrent workload
go test -cpuprofile=cpu.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -top cpu.prof | head -30

# When CPU profile is dominated by runtime scheduling overhead, look at specific database operations
go tool pprof -cum -top cpu_reads.prof | grep "minisql" | head -20

# Memory profile
go test -memprofile=mem.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests  
go tool pprof -alloc_space -top mem.prof | head -30

# Mutex contention
go test -mutexprofile=mutex.prof -bench=BenchmarkConcurrent -benchtime=10s ./e2e_tests
go tool pprof -top mutex.prof | head -25

```

## Acknowledgements 

Shout out to some great repos and other resources that were invaluable while figuring out how to get this all working together:
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part1.html)
- [go-sqldb](https://github.com/auxten/go-sqldb)
- [sqlparser](https://github.com/marianogappa/sqlparser)
- [sqlite docs](https://www.sqlite.org/fileformat2.html) (section about file format has been especially useful)
- [C++ implementation of B+ tree](https://github.com/sayef/bplus-tree)
