# B-tree Indexes

Every table's primary key is a B-tree. Additional B-tree indexes can be created on most column types — see [Supported column types](#supported-column-types) for the full list.

---

## Primary key index

Defined in `CREATE TABLE`:

```sql
CREATE TABLE users (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) NOT NULL UNIQUE,
    name  TEXT
);
```

Primary key lookups are O(log N):

```sql
SELECT * FROM users WHERE id = 42;
```

---

## Secondary indexes

```sql
-- Single-column secondary index
CREATE INDEX idx_users_created ON users (created);

-- Unique secondary index
CREATE UNIQUE INDEX idx_users_email ON users (email);
```

Secondary indexes support:

- **Equality** — `WHERE email = 'alice@example.com'`
- **Range** — `WHERE created > '2024-01-01 00:00:00'`
- **ORDER BY** — `ORDER BY created` (avoids sort)
- **Covering** (see below)

---

## Composite indexes

Index multiple columns together:

```sql
CREATE INDEX idx_orders_user_status ON orders (user_id, status);
CREATE INDEX idx_events_type_created ON events (event_type, created_at);
```

The planner can use a composite index when the query predicates match from the **leftmost column**:

```sql
-- Uses idx_orders_user_status (both columns covered)
SELECT * FROM orders WHERE user_id = 1 AND status = 'pending';

-- Uses idx_orders_user_status (leading column only)
SELECT * FROM orders WHERE user_id = 1;

-- Does NOT use idx_orders_user_status (skips user_id)
SELECT * FROM orders WHERE status = 'pending';
```

---

## Partial indexes

A partial index includes only rows that satisfy a `WHERE` condition. This keeps the index smaller and faster for selective queries:

```sql
-- Only index active users
CREATE INDEX idx_active_users_email ON users (email) WHERE active = true;

-- Only index non-cancelled orders
CREATE INDEX idx_open_orders_user ON orders (user_id) WHERE status <> 'cancelled';
```

The planner uses a partial index only when the query predicate implies the index's WHERE condition.

---

## Expression indexes

Index the result of an expression rather than a raw column value:

```sql
-- Case-insensitive name search
CREATE INDEX idx_lower_name ON users (LOWER(name));

-- Index a specific JSON field
CREATE INDEX idx_payload_uid ON events (payload ->> 'uid');

-- Index a date-truncated timestamp
CREATE INDEX idx_orders_day ON orders (DATE_TRUNC('day', created_at));
```

Queries that use the same expression in their predicate benefit automatically:

```sql
-- Uses idx_lower_name
SELECT * FROM users WHERE LOWER(name) = 'alice';

-- Uses idx_payload_uid
SELECT * FROM events WHERE payload ->> 'uid' = '42';
```

---

## Covering indexes (index-only scans)

When all columns referenced by a query are present in the index, MiniSQL performs an **index-only scan** — it reads only the index pages and skips main-table pages entirely.

```sql
-- Include extra columns in the index for covering queries
CREATE INDEX idx_orders_user_amount ON orders (user_id, amount);

-- Index-only scan: only user_id and amount are needed
SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
```

Use `EXPLAIN` to verify:

```sql
EXPLAIN SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
-- op: IndexOnlyScan  index: idx_orders_user_amount
```

---

## Dropping B-tree indexes

```sql
DROP INDEX idx_users_created;
DROP INDEX idx_orders_user_status;
```

Dropping a primary key index requires dropping the table.

---

## Supported column types

B-tree secondary indexes support the following column types:

| Column type | Notes |
|-------------|-------|
| `BOOLEAN` | |
| `INT4` | |
| `INT8` | |
| `REAL` | |
| `DOUBLE` | |
| `TIMESTAMP` | |
| `UUID` | |
| `VARCHAR(n)` | Keys are stored inline up to 255 bytes; longer values are rejected |
| Composite | Any combination of the above in a multi-column index |

### Types that do not support B-tree indexes

| Column type | Alternative |
|-------------|-------------|
| `TEXT` | Use `CREATE FULLTEXT INDEX` for token search, or an expression index on `SUBSTR(col, 1, 255)` for prefix matching |
| `JSON` | Use `CREATE INVERTED INDEX` for `JSON_CONTAINS` queries |
| `VECTOR(n)` | Use `CREATE HNSW INDEX` for approximate nearest-neighbour search |

`TEXT` and `JSON` columns are designed for unbounded content. A B-tree index requires keys that fit within a single 4 KiB page (255 bytes for `VARCHAR`); text that overflows to linked overflow pages cannot be used as a B-tree key without truncation, which would silently break equality and uniqueness semantics. The dedicated index types (`FULLTEXT`, `INVERTED`, `HNSW`) are purpose-built for each data shape.

---

## Notes

- B-tree indexes are stored as independent B+ trees with doubly-linked leaf pages.
- The planner uses table statistics from `ANALYZE` to choose between indexes.
- `CREATE UNIQUE INDEX` enforces uniqueness at the storage level.
- Composite primary keys (`PRIMARY KEY (col1, col2)`) use a composite B-tree key.
