# minisql

`MiniSQL` is a research project aimed at implementing a simple relational database in Golang. This project exists mostly for myself as a way to learn principles and design of relational databases. It is not meant to be used as a real database.

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
- simple `WHERE` conditions with `AND` and `OR`, no support for more complex nested conditions using parenthesis
- only tables supported, no indexes yet (this means all selects are scanning whole tables for now)
- `BOOLEAN`, `INT4`, `INT8`, `REAL`, `DOUBLE` and `VARCHAR` data types supported
- `NULL` and `NOT NULL` support (via null bit mask included in each row/cell)
- page size is `4096 bytes`, rows cannot exceed page size (minus required headers etc)
- first 100 bytes of the root page are reserved for config
- maximum number of columns for each table is `64`
- basic page recycling (when nodes are merged, the node that no longer exists in the tree is added to free pages linked list in the config and can be later reused as a new page)

### Data Types And Storage

| Data type    | Description |
|--------------|-------------|
| `BOOLEAN`    | 1-byte boolean value (true/false). |
| `INT4`       | 4-byte signed integer (-2,147,483,648 to 2,147,483,647). |
| `INT8`       | 8-byte signed integer (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807). |
| `REAL`       | 4-byte single-precision floating-point number. |
| `DOUBLE`     | 8-byte double-precision floating-point number. |
| `VARCHAR(n)` | Variable-length string with maximum length of n bytes. It is stored in row as text with UTF-8 encoding and cannot exceed page size. |

Each page size is `4096 bytes`. Rows larger than page size are not supported. Therefor, the largest allowed row size is `4066 bytes`.

```
4096 (page size) 
- 6 (base header size) 
- 8 (internal / leaf node header size) 
- 8 (null bit mask) 
- 8 (internal row ID / key) 
= 4066 
```

All tables are kept tract of via a system table `minisql_schema` which contains table name, `CREATE TABLE` SQL to document table structure and a root page index indicating which page contains root node of the table B+ Tree.

`CREATE TABLE` SQL definition cannot exceed `3703 bytes` to fit into a single page.

Each row has an internal row ID which is an unsigned 64 bit integer starting at 0. These are used as keys in B+ Tree data structure. 

Moreover, each row starts with 64 bit null mask which determines which values are NULL. Because of the NULL bit mask being an unsigned 64 bit integer, tables are limited to `maximum of 64 columns`.

## Planned features:

- B tree indexes (starting with unique and primary)
- autoincrementing primary keys
- `timestamp` column and basic date/time functions
- support bigger column types such as `text` that can overflow to more pages via linked list data structure
- joins such as `INNER`, `LEFT`, `RIGHT`
- support `ORDER BY`, `LIMIT`, `GROUP BY`
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
                      |                                                    |                      | 	sql varchar(2056)                              
                      |                                                    |                      | )                                                  
minisql>
```

You can create your own non-system table now:

```sh
minisql> create table users(id int4, name varchar(255), email varchar(255), age int4);
Rows affected: 0
minisql>
```

You can now check a new table has been added:

```sh
minisql> .tables
minisql_schema
users
```

Insert a row:

```sh
minisql> insert into users(id, name, email, age) values(1, 'John Doe', 'john@example.com', 35);
insert into users(id, name, email, age) values(2, 'Jane Doe', 'jane@example.com', 32);
insert into users(id, name, email, age) values(3, 'Jack Doe', 'jack@example.com', 27);
Rows affected: 1
minisql>
```

Insert multiple rows:

```sh
minisql> insert into users(id, name, email, age) values(2, 'Jane Doe', 'jane@example.com', 32), (3, 'Jack Doe', 'jack@example.com', 27);
Rows affected: 2
minisql>
```

Select from table:

```sh
minisql> select * from users;
 id                   | name                                               | email                                              | age                  
----------------------+----------------------------------------------------+----------------------------------------------------+----------------------
 1                    | john doe                                           | john@example.com                                   | 35                   
 2                    | jane doe                                           | jane@example.com                                   | 32                   
 3                    | jack doe                                           | jack@example.com                                   | 27   
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
minisql> select * from users;
 id                   | name                                               | email                                              | age                  
----------------------+----------------------------------------------------+----------------------------------------------------+----------------------
 1                    | john doe                                           | john@example.com                                   | 36                   
 2                    | jane doe                                           | jane@example.com                                   | 32                   
 3                    | jack doe                                           | jack@example.com                                   | 27     
minisql>
```

You can also delete rows:

```sh
minisql> delete from users;
Rows affected: 3
```


echo "insert into users(id, name, email, age) values(1, 'John Doe', 'john@example.com', 35);
insert into users(id, name, email, age) values(2, 'Jane Doe', 'jane@example.com', 32);
insert into users(id, name, email, age) values(3, 'Jack Doe', 'jack@example.com', 27);" | nc localhost 8080


printf '%s\n' '
insert into users(id, name, email, age) values(1, 'John Doe', 'john@example.com', 35);
insert into users(id, name, email, age) values(2, 'Jane Doe', 'jane@example.com', 32);
insert into users(id, name, email, age) values(3, 'Jack Doe', 'jack@example.com', 27);
' | nc localhost 8080