# System Tables

MiniSQL maintains internal tables that store schema metadata and query planner statistics. These tables are read-only from SQL and are automatically maintained by the engine.

---

## minisql_schema

`minisql_schema` stores the schema definition for every user table and index. It is the single source of truth for the database schema — analogous to `sqlite_schema` in SQLite or `information_schema` in PostgreSQL.

### Structure

| Column | Type | Description |
|--------|------|-------------|
| `type` | `INT4` | Object kind: `1` = table, `2` = primary key, `3` = unique index, `4` = secondary index, `5` = foreign key |
| `name` | `VARCHAR(255)` | Object name (table name, index name, or FK constraint name) |
| `tbl_name` | `VARCHAR(255)` | Parent table name (for indexes and foreign keys); NULL for tables |
| `root_page` | `INT4` | B-tree root page number for this object |
| `sql` | `TEXT` | DDL statement that created this object |

### Querying the schema

```sql
-- List all user tables
SELECT name, sql FROM minisql_schema WHERE type = 1;

-- List all indexes on a specific table
SELECT name, type, sql
FROM minisql_schema
WHERE tbl_name = 'users' AND type IN (2, 3, 4);

-- List all foreign keys
SELECT name, tbl_name, sql FROM minisql_schema WHERE type = 5;

-- Show DDL for a specific table
SELECT sql FROM minisql_schema WHERE type = 1 AND name = 'users';
```

### Type values

| Value | Constant | Meaning |
|-------|----------|---------|
| 1 | `SchemaTable` | User table |
| 2 | `SchemaPrimaryKey` | Primary key index |
| 3 | `SchemaUniqueIndex` | Unique index |
| 4 | `SchemaSecondaryIndex` | Secondary / fulltext / inverted / HNSW index |
| 5 | `SchemaForeignKey` | Foreign key constraint |

---

## minisql_stats

`minisql_stats` stores column statistics collected by the `ANALYZE` command. The query planner reads these statistics to estimate row counts, choose indexes, and order joins.

Statistics are populated by:

```sql
ANALYZE table_name;
```

### What ANALYZE collects

- Row count per table
- Distinct value count (cardinality) per column
- Value distribution histograms per column
- Most-common values (MCV) per column

### Usage

```sql
-- Check that stats exist for a table
SELECT * FROM minisql_stats WHERE tbl_name = 'orders';
```

---

## Notes

- Both system tables are read-only from SQL — writing to them directly is rejected.
- `minisql_schema` is always consistent with the current schema; DDL operations (CREATE TABLE, DROP TABLE, CREATE INDEX, etc.) update it atomically within the same transaction.
- `minisql_stats` is stale until `ANALYZE` is run; the planner falls back to default estimates for tables without statistics.
- System tables do not appear in `SELECT * FROM minisql_schema WHERE type = 1` because the filter on `type = 1` (user tables) excludes them. They are visible if you query without a type filter.
