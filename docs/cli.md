# CLI Shell

MiniSQL ships with a standalone command-line shell (`minisql`) that lets you open, query, and manage a database without writing Go code — similar to the `sqlite3` CLI.

## Installation

```bash
go install github.com/RichardKnop/minisql/cmd/minisql@latest
```

Or build from source:

```bash
git clone https://github.com/RichardKnop/minisql
cd minisql
go build -o minisql ./cmd/minisql/
```

## Usage

```
minisql [options] <database-file>
```

The database file is created automatically if it does not exist.

### Options

| Flag | Description |
|------|-------------|
| `-c <query>` | Execute a SQL statement and exit (may be repeated). |
| `-csv` | Set output format to CSV (default: table). |
| `-h` / `--help` | Print usage. |

## Interactive shell

Running without `-c` opens the interactive REPL:

```
$ minisql my.db
MiniSQL — my.db
Enter ".help" for usage hints.
minisql> create table "users" (
      ->     id   int8 primary key autoincrement,
      ->     name varchar(255),
      ->     age  int4
      -> );
minisql> insert into "users" (name, age) values ('alice', 30), ('bob', 25);
minisql> select * from "users";
id  name   age
--  -----  ---
1   alice  30
2   bob    25
minisql> .quit
```

- Statements span multiple lines and are executed when a `;` is reached.
- `Ctrl-D` (EOF) exits the shell after flushing any buffered input.
- When stdin is not a terminal (pipe or redirect), prompts are suppressed.

## Non-interactive mode (`-c`)

Execute one or more SQL statements and exit — useful for scripts, CI, or quick one-liners:

```bash
# Single query
minisql -c 'select * from "users"' my.db

# Multiple statements in sequence
minisql \
  -c 'create table "events" (id int8 primary key autoincrement, name varchar(255))' \
  -c 'insert into "events" (name) values ('"'"'signup'"'"')' \
  -c 'select * from "events"' \
  my.db

# Pipe into another tool
minisql -csv -c 'select * from "users"' my.db | cut -d, -f2
```

Output mode flags apply to `-c` as well — combine `-csv` with `-c` for machine-readable output.

## Dot commands

Dot commands control the shell itself and are always single-line.

| Command | Description |
|---------|-------------|
| `.help` | Show dot command reference. |
| `.tables` | List all user tables. |
| `.schema [table]` | Print `CREATE` statement(s). Omit `[table]` to show all. |
| `.mode table` | Aligned table output (default). |
| `.mode csv` | CSV output (RFC 4180). |
| `.timer on\|off` | Toggle per-query timing. |
| `.quit` / `.exit` | Exit the shell. |

### `.tables`

```
minisql> .tables
name
----
events
users
```

### `.schema`

```
minisql> .schema users
create table "users" (
    id   int8 primary key autoincrement,
    name varchar(255),
    age  int4
);
```

### Output modes

```
minisql> .mode table
minisql> select id, name from "users";
id  name
--  -----
1   alice
2   bob

minisql> .mode csv
minisql> select id, name from "users";
id,name
1,alice
2,bob
```

### Query timing

```
minisql> .timer on
minisql> select count(*) from "users";
COUNT(*)
--------
2
Time: 0.001s
```

## Scripting via stdin

Pipe a SQL script into the shell for batch operations:

```bash
minisql my.db <<'EOF'
create table "logs" (id int8 primary key autoincrement, msg text);
insert into "logs" (msg) values ('hello');
select * from "logs";
EOF
```
