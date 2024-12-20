# minisql

`MiniSQL` is a research project aimed at implementing a simple relational database in Golang. This project exists mostly for myself as a way to learn principles and design of relational databases. It is not meant to be used as a real database.

Shout out to some great repos and other resources that were invaluable while figuring out how to get this all working together:
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part1.html)
- [go-sqldb](https://github.com/auxten/go-sqldb)
- [sqlparser](https://github.com/marianogappa/sqlparser)
- [sqlite docs](https://www.sqlite.org/fileformat2.html) (section about file format has been especially useful)

Run `minisql` in your command line:

```sh
go run cmd/minisql/main.go
minisql>
```

## Current Features

I plan to implement more features of traditional relational databases in the future as part of this project simply to learn and discovery how various features I have grown acustomed to over the years are implemented under the hood. However, currently only a very small number of features are implemented:

- simple SQL parser (partial support for `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE` queries)
- only tables supported, no indexes (this means all selects are scanning whole tables for now)
- only `int4`, `int8` and `varchar` columns supported
- no primary key support (tables internally use row ID as key in B tree data structure)
- no joins
- only simple `WHERE` conditions with `AND` and `OR`, no support for more complex nested conditions using parenthesis
- no transaction support
- no page overflow support, entire rows must fit within a 4096 byte page

### Planned features:

- support additional basic query types such as `DELETE`, `DROP TABLE`
- support `NULL` values
- B+ tree and support indexes (starting with unique and primary)
- more column types starting with simpler ones such as `bool` and `timestamp`
- support bigger column types such as `text` that can overflow to more pages via linked list data structure
- joins such as `INNER`, `LEFT`, `RIGHT`
- support `ORDER BY`, `LIMIT`, `GROUP BY`
- dedicate first 100B of root page for config similar to how sqlite does it
- support altering tables
- transactions
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

Create a table:

```sh
minisql> create table foo(id int4, email varchar(255), age int4)
Rows affected: 0
minisql>
```

Insert a row:

```sh
minisql> insert into foo(id, email, age) values(1, 'john@example.com', 35)
Rows affected: 1
minisql>
```

Insert multiple rows:

```sh
minisql> insert into foo(id, email, age) values(2, 'jane@example.com', 32), (3, 'jack@example.com', 27)
Rows affected: 2
minisql>
```

Insert more than a single page worth of data:

```sh
minisql> insert into foo(id, email, age) values(1, 'john@example.com', 35), (2, 'jane@example.com', 32), (3, 'jack@example.com', 27), (4, 'jane@example.com', 32), (5, 'jack@example.com', 27), (6, 'jane@example.com', 32), (7, 'jack@example.com', 27),  (8, 'jane@example.com', 32), (9, 'jack@example.com', 27),  (10, 'jane@example.com', 32), (11, 'jack@example.com', 27),  (12, 'jane@example.com', 32), (13, 'jack@example.com', 27), (14, 'jack@example.com', 27), (15, 'jack@example.com', 27)
Rows affected: 15
minisql>
```

Select from table:

```sh
minisql> select * from foo
 id                   | email                                    | age
----------------------+------------------------------------------+----------------------
 1                    | john@example.com                         | 35
 2                    | jane@example.com                         | 32
 3                    | jack@example.com                         | 27
minisql>
```

Update rows:

```sh
minisql> update foo set id = 45 where id = 75
Rows affected: 0
minisql>
```

