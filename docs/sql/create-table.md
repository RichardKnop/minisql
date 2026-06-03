# CREATE / ALTER / DROP

## CREATE TABLE

```sql
CREATE TABLE table_name (
    column_name data_type [column_constraint ...],
    ...
    [table_constraint, ...]
);
```

### Column constraints

| Constraint | Description |
|-----------|-------------|
| `PRIMARY KEY` | Unique B-tree key for the table. One per table. |
| `PRIMARY KEY AUTOINCREMENT` | Auto-incrementing primary key. Requires `INT8`. |
| `NOT NULL` | Rejects NULL on insert/update. |
| `NULL` | Explicitly marks column as nullable (default). |
| `UNIQUE` | Creates a unique index on this column. |
| `DEFAULT value` | Default value when column is omitted from INSERT. |
| `DEFAULT NOW()` | Default current UTC timestamp for TIMESTAMP columns. |
| `CHECK (expr)` | Rejects rows where expression is false. |
| `REFERENCES table (col)` | Inline foreign key. |

### Table constraints

| Constraint | Description |
|-----------|-------------|
| `PRIMARY KEY (col, ...)` | Composite primary key. |
| `UNIQUE (col, ...)` | Composite unique constraint. |
| `FOREIGN KEY (col) REFERENCES table (col)` | Table-level foreign key. |
| `CONSTRAINT name FOREIGN KEY ...` | Named foreign key. |

### Examples

**Simple table with autoincrement primary key:**

```sql
CREATE TABLE users (
    id      INT8         PRIMARY KEY AUTOINCREMENT,
    email   VARCHAR(255) NOT NULL UNIQUE,
    name    TEXT,
    active  BOOLEAN      NOT NULL DEFAULT true,
    created TIMESTAMP    DEFAULT NOW()
);
```

**Table with CHECK constraint:**

```sql
CREATE TABLE products (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL,
    price INT8 NOT NULL CHECK (price > 0),
    qty   INT8 NOT NULL DEFAULT 0 CHECK (qty >= 0)
);
```

**Table with composite primary key:**

```sql
CREATE TABLE memberships (
    user_id   INT8 NOT NULL,
    org_id    INT8 NOT NULL,
    role      VARCHAR(50),
    joined_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, org_id)
);
```

**Table with composite UNIQUE constraint:**

```sql
CREATE TABLE tags (
    id       INT8         PRIMARY KEY AUTOINCREMENT,
    post_id  INT8         NOT NULL,
    tag_name VARCHAR(100) NOT NULL,
    UNIQUE (post_id, tag_name)
);
```

**Table with foreign key:**

```sql
CREATE TABLE orders (
    id         INT8 PRIMARY KEY AUTOINCREMENT,
    user_id    INT8 NOT NULL REFERENCES users (id),
    product_id INT8 NOT NULL,
    amount     INT8 NOT NULL CHECK (amount > 0),
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Table-level named foreign key:**

```sql
CREATE TABLE orders (
    id      INT8 PRIMARY KEY AUTOINCREMENT,
    user_id INT8 NOT NULL,
    amount  INT8 NOT NULL,
    CONSTRAINT fk_orders_users FOREIGN KEY (user_id) REFERENCES users (id)
);
```

**Table with JSON, UUID, and VECTOR columns:**

```sql
CREATE TABLE documents (
    id        INT8      PRIMARY KEY AUTOINCREMENT,
    ref_id    UUID,
    metadata  JSON,
    body      TEXT      NOT NULL,
    embedding VECTOR(768) NOT NULL
);
```

## CREATE TABLE IF NOT EXISTS

```sql
CREATE TABLE IF NOT EXISTS users (
    id    INT8         PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) UNIQUE,
    name  TEXT,
    created TIMESTAMP DEFAULT NOW()
);
```

Silently does nothing if the table already exists. Useful for idempotent schema initialisation.

---

## DROP TABLE

```sql
DROP TABLE table_name;
```

Removes the table and all its indexes. Fails if the table has child rows referencing it via a foreign key (when `PRAGMA foreign_keys = on`, which is the default).

---

## ALTER TABLE

### ADD COLUMN

```sql
ALTER TABLE users ADD COLUMN score INT4;
ALTER TABLE users ADD COLUMN nickname VARCHAR(64);
ALTER TABLE users ADD COLUMN active BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE users ADD COLUMN metadata JSON;
```

!!! note
    The new column is added as nullable or with the specified DEFAULT. Existing rows get the DEFAULT value (or NULL if no default).

### DROP COLUMN

```sql
ALTER TABLE users DROP COLUMN internal_note;
```

Marks the column as **dropped** in the DDL (tombstone). Existing rows retain the bytes on disk but the column is invisible to SQL. The space is reclaimed by `VACUUM`.

### RENAME COLUMN

```sql
ALTER TABLE users RENAME COLUMN nm TO full_name;
ALTER TABLE users RENAME COLUMN ts TO created_at;
```

### RENAME TABLE

```sql
ALTER TABLE users RENAME TO members;
```

Renames the table and updates all schema references. Indexes are updated automatically.

---

## CREATE INDEX

See [Indexes](../indexes/overview.md) for the full index reference.

```sql
-- B-tree secondary index
CREATE INDEX idx_users_created ON users (created);

-- Fulltext index
CREATE FULLTEXT INDEX idx_articles_body ON articles (body)
    WITH (tokenizer = 'simple');

-- JSON inverted index
CREATE INVERTED INDEX idx_events_payload ON events (payload);

-- HNSW vector index
CREATE HNSW INDEX idx_embedding ON documents (embedding)
    WITH (m = 16, ef_construction = 200);
```

## DROP INDEX

```sql
DROP INDEX index_name;
```
