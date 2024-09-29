# minisql

`MiniSQL` is a research project aimed at implementing a simple relational database in Golang. This project exists mostly for myself as a way to learn principles and design of relational databases. It is not meant to be used as a real database.

Run `minisql` in your command line:

```sh
go run cmd/main/main.go
minisql>
```

## Meta Commands

You can use meta commands, type `.help` to see available commands or `.exit` to quit minisql:

```sh
minisql> .help
.help    - Show available commands
.exit    - Closes program
.tables  - List all tables in the current database
```

### SQL Queries

Create a table:

```sh
minisql> create table foo(id int4, email varchar(255), age int4)
Rows affected: 0
```