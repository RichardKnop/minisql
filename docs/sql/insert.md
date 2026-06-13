# INSERT

## Basic INSERT

```sql
INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
```

### Single row

```sql
INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');
```

### Multi-row

```sql
INSERT INTO users (email, name) VALUES
    ('bob@example.com', 'Bob'),
    ('carol@example.com', 'Carol'),
    ('dave@example.com', 'Dave');
```

### Using DEFAULT values

Omit columns that have defaults — they are filled in automatically:

```sql
-- active defaults to true, created defaults to NOW(), id defaults to GEN_RANDOM_UUID()
INSERT INTO users (email, name) VALUES ('eve@example.com', 'Eve');

-- Explicit NOW() or GEN_RANDOM_UUID() in VALUES
INSERT INTO events (name, created) VALUES ('login', NOW());
INSERT INTO sessions (id, user_id) VALUES (GEN_RANDOM_UUID(), 42);
```

### Bind parameters

Always use `?` placeholders for user-supplied values:

```go
_, err = db.Exec(
    `INSERT INTO users (email, name) VALUES (?, ?)`,
    "frank@example.com", "Frank",
)
```

### Prepared statements

```go
stmt, err := db.Prepare(`INSERT INTO users (email, name) VALUES (?, ?)`)
defer stmt.Close()

for _, u := range users {
    _, err = stmt.Exec(u.Email, u.Name)
}
```

---

## INSERT INTO … SELECT

Populate a table from a query result instead of a literal `VALUES` list.

```sql
INSERT INTO table_name (col1, col2, ...)
SELECT expr1, expr2, ...
FROM source_table
[WHERE condition];
```

The column list is required. The number of SELECT output columns must match the number of target columns.

### Copy all rows

```sql
INSERT INTO archived_users (email, name)
SELECT email, name FROM users;
```

### Copy a filtered subset

```sql
INSERT INTO archived_users (email, name)
SELECT email, name FROM users
WHERE created < '2024-01-01';
```

### Copy a transformed subset

```sql
INSERT INTO audit_log (user_id, action, ts)
SELECT id, 'signup', created FROM users
WHERE created > '2025-01-01';
```

### Bulk move between tables

```sql
INSERT INTO orders_archive (order_id, user_id, total_paid)
SELECT order_id, user_id, total_paid FROM orders
WHERE created < '2024-01-01';

DELETE FROM orders WHERE created < '2024-01-01';
```

### With ON CONFLICT

```sql
INSERT INTO archived_users (email, name)
SELECT email, name FROM users
ON CONFLICT DO NOTHING;
```

### With RETURNING

```go
rows, err := db.Query(`
    INSERT INTO archived_users (email, name)
    SELECT email, name FROM users WHERE id = ?
    RETURNING id, email
`, userID)
defer rows.Close()
for rows.Next() {
    var id int64
    var email string
    rows.Scan(&id, &email)
}
```

### Seeding from a subquery

```sql
INSERT INTO product_stats (product_id, total_orders)
SELECT product_id, COUNT(*) FROM orders
GROUP BY product_id;
```

---

## ON CONFLICT

### DO NOTHING

Silently skip the row if a unique or primary-key constraint is violated:

```sql
INSERT INTO users (email, name)
VALUES ('alice@example.com', 'Alice Duplicate')
ON CONFLICT DO NOTHING;
```

### DO UPDATE (upsert)

Update the existing row when there is a conflict:

```sql
INSERT INTO users (email, name)
VALUES ('alice@example.com', 'Alice Updated')
ON CONFLICT DO UPDATE SET name = 'Alice Updated';
```

Use the `EXCLUDED` pseudo-table to reference the values that were proposed for insertion:

```sql
INSERT INTO users (email, name)
VALUES ('alice@example.com', 'Alice V2')
ON CONFLICT DO UPDATE SET name = EXCLUDED.name;
```

Multi-row upsert with `EXCLUDED`:

```sql
INSERT INTO users (email, name) VALUES
    ('alice@example.com', 'Alice V3'),
    ('bob@example.com',   'Bob V3')
ON CONFLICT DO UPDATE SET name = EXCLUDED.name;
```

Update multiple columns:

```sql
INSERT INTO products (sku, name, price, stock)
VALUES ('ABC-1', 'Widget', 999, 100)
ON CONFLICT DO UPDATE
    SET name  = EXCLUDED.name,
        price = EXCLUDED.price,
        stock = stock + EXCLUDED.stock;
```

---

## RETURNING

`RETURNING` makes INSERT behave like a query — it returns rows from the newly inserted data.

### Return the generated primary key

```go
var newID int64
err = db.QueryRow(
    `INSERT INTO users (email, name) VALUES (?, ?) RETURNING id`,
    "alice@example.com", "Alice",
).Scan(&newID)
```

### Return multiple columns

```sql
INSERT INTO users (email, name)
VALUES ('bob@example.com', 'Bob')
RETURNING id, name, email, created;
```

### Multi-row RETURNING

```go
rows, err := db.Query(`
    INSERT INTO users (email, name) VALUES
        ('carol@example.com', 'Carol'),
        ('dave@example.com',  'Dave')
    RETURNING id, name
`)
defer rows.Close()
for rows.Next() {
    var id int64
    var name string
    rows.Scan(&id, &name)
}
```

### ON CONFLICT … RETURNING

```sql
INSERT INTO users (email, name)
VALUES ('alice@example.com', 'Alice')
ON CONFLICT DO UPDATE SET name = EXCLUDED.name
RETURNING id, name;
```

---

## Inserting JSON

```sql
INSERT INTO events (name, payload)
VALUES ('login', '{"user":"alice","uid":42}');

INSERT INTO events (name, payload)
VALUES ('tags', '["go","sql","json"]');
```

## Inserting VECTOR data

```go
// String literal
_, err = db.Exec(
    `INSERT INTO documents (body, embedding) VALUES (?, ?)`,
    "hello world", "[0.1, 0.2, 0.3]",
)

// []float32 bind parameter
vec := []float32{0.1, 0.2, 0.3}
_, err = db.Exec(
    `INSERT INTO documents (body, embedding) VALUES (?, ?)`,
    "hello world", vec,
)
```

## Inserting UUID

**Auto-generate with `DEFAULT GEN_RANDOM_UUID()`** (recommended):

```sql
CREATE TABLE users (
    id    UUID NOT NULL DEFAULT GEN_RANDOM_UUID(),
    email VARCHAR(255) NOT NULL
);

-- id is generated automatically — omit it from the column list
INSERT INTO users (email) VALUES ('alice@example.com');
```

**Explicit `GEN_RANDOM_UUID()` call in VALUES:**

```sql
INSERT INTO sessions (id, user_id) VALUES (GEN_RANDOM_UUID(), 1);
```

**Explicit UUID string literal:**

```sql
INSERT INTO sessions (id, user_id)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 1);
```

Or use `CAST`:

```sql
INSERT INTO sessions (id, user_id)
VALUES (CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID), 1);
```
