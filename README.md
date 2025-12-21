# minisql

`MiniSQL` is a research project aimed at implementing a simple relational database in Golang. This project exists mostly for myself as a way to learn principles and design of relational databases. It is not meant to be used as a real database.

[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://richardknop.github.io/donate/)

Shout out to some great repos and other resources that were invaluable while figuring out how to get this all working together:
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part1.html)
- [go-sqldb](https://github.com/auxten/go-sqldb)
- [sqlparser](https://github.com/marianogappa/sqlparser)
- [sqlite docs](https://www.sqlite.org/fileformat2.html) (section about file format has been especially useful)
- [C++ implementation of B+ tree](https://github.com/sayef/bplus-tree)

Run `minisql` in your command line:

```sh
go run cmd/minisql/main.go
minisql>
```

## Current Features

I plan to implement more features of traditional relational databases in the future as part of this project simply to learn and discovery how various features I have grown acustomed to over the years are implemented under the hood. However, currently only a very small number of features are implemented:

- simple SQL parser with partial support for basic queries: 
  - `CREATE TABLE`
  - `DROP TABLE`
  - `INSERT`
  - `SELECT`
  - `UPDATE`
  - `DELETE`
- only tables and primary keys supported, more index support to be implemented in the future
- `BOOLEAN`, `INT4`, `INT8`, `REAL`, `DOUBLE`, `TEXT`, `VARCHAR`, `TIMESTAMP` data types supported
- `PRIMARY KEY` support, only single column, no composite primary keys
- `AUTOINCREMENT` support, primary key must be of type `INT8` for autoincrement
- `NULL` and `NOT NULL` support (via null bit mask included in each row/cell)
- `DEFAULT` support for all columns including `NOW()` for `TIMESTAMP`
- each statement is wrapped in a single statement transaction unless you control transaction context manually with `BEGIN`, `COMMIT`, `ROLLBACK` keywords
- page size is `4096 bytes`, rows cannot exceed page size (minus required headers etc)
- first 100 bytes of the root page are reserved for config
- maximum number of columns for each table is `64`
- basic page recycling (when nodes are merged, the node that no longer exists in the tree is added to free pages linked list in the config and can be later reused as a new page)
- simple `WHERE` conditions with `AND` and `OR`, no support for more complex nested conditions using parenthesis
- supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`
- `LIMIT` and `OFFSET` clauses for basic pagination

### Data Types And Storage

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

Each page size is `4096 bytes`. Rows larger than page size are not supported. Therefor, the largest allowed row size is `4066 bytes`.

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

Moreover, each row starts with 64 bit null mask which determines which values are NULL. Because of the NULL bit mask being an unsigned 64 bit integer, tables are limited to `maximum of 64 columns`.

## Planned features:

- build on existing primary key support, add unique and non unique index support
- date/time functions to make working with `TIMESTAMP` type easier
- joins such as `INNER`, `LEFT`, `RIGHT`
- support `ORDER BY`, `GROUP BY`
- UPDATE from a SELECT
- upsert (insert on conflict)
- more complex WHERE clauses
- support altering tables
- more sophisticated query planner
- vacuuming
- benchmarks

## Meta Commands

You can use meta commands, type `.help` to see available commands or `.exit` to quit minisql:

```sh
minisql> .help
.help    - Show available commands
.exit    - Closes program
.tables  - List all tables in the current database
```

## Examples

Start the database:

```sh
go run cmd/minisql/main.go
```

It will start a TCP server listening on port 8080.

Use client to connect to the database:

```sh
go run cmd/client/main.go
```

When creating a new MiniSQL database, it is initialised with `minisql_schema` system table which holds schema of all tables within the database:

```sh
minisql> select * from minisql_schema;
 type                 | name                                               | root_page            | sql                                                
----------------------+----------------------------------------------------+----------------------+----------------------------------------------------
 1                    | minisql_schema                                     | 0                    | create table "minisql_schema" (                    
                      |                                                    |                      | 	type int4 not null,                            
                      |                                                    |                      | 	table_name varchar(255) not null,              
                      |                                                    |                      | 	root_page int4,                                
                      |                                                    |                      | 	sql text                              
                      |                                                    |                      | )
minisql>
```

You can create your own non-system table now:

```sh
minisql> create table users(id int8 primary key autoincrement, name varchar(255), email text, age int4);
Table 'users' created successfully
minisql>
```

You can now check a new table has been added:

```sh
minisql> .tables
minisql_schema
users
```

Insert test rows:

```sh
minisql> insert into users("name", "email", "age") values('Danny Mason', 'Danny_Mason2966@xqj6f.tech', 35),
('Johnathan Walker', 'Johnathan_Walker250@ptr6k.page', 32),
('Tyson Weldon', 'Tyson_Weldon2108@zynuu.video', 27),
('Mason Callan', 'Mason_Callan9524@bu2lo.edu', 19),
('Logan Flynn', 'Logan_Flynn9019@xtwt3.pro', 42),
('Beatrice Uttley', 'Beatrice_Uttley1670@1wa8o.org', 32),
('Harry Johnson', 'Harry_Johnson5515@jcf8v.video', 25),
('Carl Thomson', 'Carl_Thomson4218@kyb7t.host', 53),
('Kaylee Johnson', 'Kaylee_Johnson8112@c2nyu.design', 48),
('Cristal Duvall', 'Cristal_Duvall6639@yvu30.press', 27);
Rows affected: 10
minisql>
```

When trying to insert a duplicate primary key, you will get an error:

```sh
minisql> insert into users("id", "name", "email", "age") values(1, 'Danny Mason', 'Danny_Mason2966@xqj6f.tech', 35);
Error: failed to insert primary key pk_users: duplicate key
minisql>
```

Select from table:

```sh
minisql> select * from users;
 id                   | name                                               | email                                              | age                  
----------------------+----------------------------------------------------+----------------------------------------------------+----------------------
 1                    | Danny Mason                                        | Danny_Mason2966@xqj6f.tech                         | 35                   
 2                    | Johnathan Walker                                   | Johnathan_Walker250@ptr6k.page                     | 32                   
 3                    | Tyson Weldon                                       | Tyson_Weldon2108@zynuu.video                       | 27                   
 4                    | Mason Callan                                       | Mason_Callan9524@bu2lo.edu                         | 19                   
 5                    | Logan Flynn                                        | Logan_Flynn9019@xtwt3.pro                          | 42                   
 6                    | Beatrice Uttley                                    | Beatrice_Uttley1670@1wa8o.org                      | 32                   
 7                    | Harry Johnson                                      | Harry_Johnson5515@jcf8v.video                      | 25                   
 8                    | Carl Thomson                                       | Carl_Thomson4218@kyb7t.host                        | 53                   
 9                    | Kaylee Johnson                                     | Kaylee_Johnson8112@c2nyu.design                    | 48                   
 10                   | Cristal Duvall                                     | Cristal_Duvall6639@yvu30.press                     | 27                   
minisql>
```

Update rows:

```sh
minisql> update users set age = 36 where id = 1;
Rows affected: 1
minisql>
```

Select to verify update:

```sh
minisql> select * from users where id=1;
 id                   | name                                               | email                                              | age                  
----------------------+----------------------------------------------------+----------------------------------------------------+----------------------
 1                    | Danny Mason                                        | Danny_Mason2966@xqj6f.tech                         | 36                                
minisql>
```

You can also delete rows:

```sh
minisql> delete from users;
Rows affected: 10
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