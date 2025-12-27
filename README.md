# minisql

`MiniSQL` is an embedded single file database written in Golang, inspired by `SQLite` (however borrowing some features from other databases as well). It originally started as a research project aimed at learning about internals of relational databases. Over time it has progressed and grown to its current form. It is a very early stage project and it might contain bugs and is not battle tested. Please employ caution when using this database.

[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://richardknop.github.io/donate/)

Shout out to some great repos and other resources that were invaluable while figuring out how to get this all working together:
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part1.html)
- [go-sqldb](https://github.com/auxten/go-sqldb)
- [sqlparser](https://github.com/marianogappa/sqlparser)
- [sqlite docs](https://www.sqlite.org/fileformat2.html) (section about file format has been especially useful)
- [C++ implementation of B+ tree](https://github.com/sayef/bplus-tree)

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

## Connection String Parameters

MiniSQL supports optional connection string parameters:

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `journal` | `true`, `false` | `true` | Enable/disable rollback journal for crash recovery |
| `log_level` | `debug`, `info`, `warn`, `error` | `warn` | Set logging verbosity level |
| `max_cached_pages` | positive integer | `1000` | Maximum number of pages to keep in memory cache |

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

MiniSQL uses a write-ahead rollback journal by default. Every write transaction, before comitting first writes original version of all modified pages (and database header if applicable) to the journal file (a file with `-journal` suffix created in the same directory as the database file). In case of an error encountered during flushing of pages changed by the transaction to the disk, the database quits and recovers to original state before the transaction from the journal file.

You can disable this behaviour by appending `?journal=false` to the connection string if you value performance over being able to recover from a crash. This might be useful for databases which are not the main source of data and are created each time from the main source when an ephemeral service starts, for example in event sourcing systems.

## Storage

Each page size is `4096 bytes`. Rows larger than page size are not supported. Therefore, the largest allowed row size is `4066 bytes`. Only exception is root page 0 has first 100 bytes reserved for config.

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
	if err := rows.Scan(&aSchema.Type, &aSchema.Name, &aSchema.RootPage, &aSchema.SQL); err != nil {
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

## Supported SQL Features

- `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS`
- `PRIMARY KEY` support, only single column, no composite primary keys
- `AUTOINCREMENT` support, primary key must be of type `INT8` for autoincrement
- `UNIQUE` index can be specified when creating a table
- `NULL` and `NOT NULL` support (via null bit mask included in each row/cell)
- `DEFAULT` support for all columns including `NOW()` for `TIMESTAMP`
- `DROP TABLE`
- `CREATE INDEX`, `DROP INDEX` - only for secondary non unique indexes (primary and unique can be declared as part of `CREATE TABLE`)
- `INSERT` (single row or multi rows via tuple of values separated by comma)
- `SELECT` (all fields with `*`, only specific fields or count rows with `COUNT(*)`)
- `UPDATE`
- `DELETE`
- simple `WHERE` conditions with `AND` and `OR`, no support for more complex nested conditions using parenthesis
- supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`
- `LIMIT` and `OFFSET` clauses for basic pagination

For `WHERE` clauses, currently supported is maximum one level of nesting. You can define multiple groups where each group item is joined with `AND` and groups themselves are joined by `OR`. For example, you could create two condition groups such as:

```sh
(a = 1 and b = 'foo') or (c is null and d in ('bar', 'qux'))
```

Prepared statements are supported using `?` as a placeholder. For example:

```sql
insert into users("name", "email") values(?, ?), (?, ?);
```

## Planned features:

- composite unique index
- date/time functions to make working with `TIMESTAMP` type easier
- joins such as `INNER`, `LEFT`, `RIGHT`
- foreign keys
- support `GROUP BY` and aggregation functions such as `MAX`, `MIN`, `SUM`
- UPDATE from a SELECT
- upsert (insert on conflict)
- rollback journal file
- more complex WHERE clauses
- support altering tables, creating and dropping of indexes outside of create table query
- more sophisticated query planner
- vacuuming
- benchmarks

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
