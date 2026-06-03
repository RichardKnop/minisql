# Constraints

Constraints enforce data integrity rules at the storage layer, rejecting invalid inserts and updates before they reach the database.

---

## PRIMARY KEY

Every table has at most one primary key. The primary key uniquely identifies each row and is stored as a B-tree index.

```sql
-- Single-column primary key
CREATE TABLE users (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) NOT NULL
);

-- Composite primary key (table constraint)
CREATE TABLE memberships (
    user_id INT8 NOT NULL,
    org_id  INT8 NOT NULL,
    role    VARCHAR(50),
    PRIMARY KEY (user_id, org_id)
);
```

`AUTOINCREMENT` requires `INT8` and generates sequential IDs automatically.

---

## NOT NULL

Rejects `NULL` on insert and update:

```sql
CREATE TABLE users (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) NOT NULL,
    name  TEXT NOT NULL
);
```

Columns declared without `NOT NULL` accept `NULL` by default.

---

## UNIQUE

Creates a unique B-tree index that rejects duplicate values:

```sql
-- Column-level UNIQUE
CREATE TABLE users (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) NOT NULL UNIQUE
);

-- Composite UNIQUE (table constraint)
CREATE TABLE tags (
    id       INT8         PRIMARY KEY AUTOINCREMENT,
    post_id  INT8         NOT NULL,
    tag_name VARCHAR(100) NOT NULL,
    UNIQUE (post_id, tag_name)
);

-- Explicit UNIQUE index (equivalent)
CREATE UNIQUE INDEX idx_users_email ON users (email);
```

Inserting a duplicate value returns an error, unless `ON CONFLICT DO NOTHING` or `ON CONFLICT DO UPDATE` is used.

---

## CHECK

Rejects rows where an expression evaluates to false:

```sql
CREATE TABLE products (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL,
    price INT8 NOT NULL CHECK (price > 0),
    stock INT8 NOT NULL DEFAULT 0 CHECK (stock >= 0)
);

CREATE TABLE accounts (
    id      INT8   PRIMARY KEY AUTOINCREMENT,
    balance INT8   NOT NULL CHECK (balance >= 0),
    status  VARCHAR(20) NOT NULL CHECK (status IN ('active', 'frozen', 'closed'))
);
```

CHECK constraints are evaluated on every `INSERT` and `UPDATE`.

---

## FOREIGN KEY

Foreign keys enforce referential integrity between tables:

### Inline (column-level)

```sql
CREATE TABLE orders (
    id      INT8 PRIMARY KEY AUTOINCREMENT,
    user_id INT8 NOT NULL REFERENCES users (id),
    amount  INT8 NOT NULL
);
```

### Table-level

```sql
CREATE TABLE orders (
    id      INT8 PRIMARY KEY AUTOINCREMENT,
    user_id INT8 NOT NULL,
    amount  INT8 NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id)
);
```

### Named foreign key

```sql
CREATE TABLE orders (
    id      INT8 PRIMARY KEY AUTOINCREMENT,
    user_id INT8 NOT NULL,
    CONSTRAINT fk_orders_users FOREIGN KEY (user_id) REFERENCES users (id)
);
```

Foreign key enforcement is controlled by `PRAGMA foreign_keys` (default: on):

```sql
PRAGMA foreign_keys;        -- 1 (enabled)
PRAGMA foreign_keys = off;  -- disable for bulk load
PRAGMA foreign_keys = on;   -- re-enable
```

When enabled:

- **INSERT / UPDATE:** the referenced row must exist in the parent table.
- **DELETE:** deleting a parent row that has child rows referencing it returns an error.

---

## DEFAULT values

Default values are applied when a column is omitted from an `INSERT`:

```sql
CREATE TABLE users (
    id      INT8      PRIMARY KEY AUTOINCREMENT,
    active  BOOLEAN   NOT NULL DEFAULT true,
    created TIMESTAMP DEFAULT NOW(),
    score   INT8      DEFAULT 0
);

-- active, created, and score get their defaults
INSERT INTO users (email) VALUES ('alice@example.com');
```

---

## Constraint violations

All constraint violations return an error. When using `ON CONFLICT`:

```sql
-- Silently skip on unique constraint violation
INSERT INTO users (email, name)
VALUES ('alice@example.com', 'Duplicate')
ON CONFLICT DO NOTHING;

-- Update on unique constraint violation
INSERT INTO users (email, name)
VALUES ('alice@example.com', 'Alice V2')
ON CONFLICT DO UPDATE SET name = EXCLUDED.name;
```

See [INSERT](sql/insert.md) for full `ON CONFLICT` reference.
