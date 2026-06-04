# Operators & WHERE

## Comparison operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `WHERE id = 1` |
| `<>` / `!=` | Not equal | `WHERE status <> 'active'` |
| `<` | Less than | `WHERE price < 100` |
| `<=` | Less than or equal | `WHERE age <= 18` |
| `>` | Greater than | `WHERE score > 80` |
| `>=` | Greater than or equal | `WHERE score >= 90` |

---

## Logical operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Both conditions true | `WHERE active = true AND age >= 18` |
| `OR` | Either condition true | `WHERE role = 'admin' OR role = 'owner'` |
| `NOT` | Negates a condition | `WHERE NOT active` |

`AND` binds more tightly than `OR`. Use parentheses to control grouping:

```sql
WHERE (status = 'pending' OR status = 'review') AND created > '2024-01-01 00:00:00'
```

---

## Arithmetic operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `price + tax` |
| `-` | Subtraction | `balance - amount` |
| `*` | Multiplication | `price * quantity` |
| `/` | Division | `total / count` |
| `%` | Modulo | `id % 2` |

```sql
SELECT id, price * quantity AS total FROM order_lines;
SELECT * FROM orders WHERE amount * 1.2 > 1000;
```

!!! warning "No negative integer literals"
    The parser does not accept negative integer literals directly. Use a bind parameter instead:

    ```go
    db.Exec("SELECT * FROM t WHERE n > ?", int64(-1)) // ✅
    // db.Exec("SELECT * FROM t WHERE n > -1")        // ❌ parse error
    ```

---

## LIKE and ILIKE

Pattern matching with `%` (any sequence) and `_` (single character):

```sql
SELECT * FROM users WHERE email LIKE '%@example.com';
SELECT * FROM users WHERE name  LIKE 'Al%';
SELECT * FROM users WHERE code  LIKE 'A_C';
```

`ILIKE` is case-insensitive:

```sql
SELECT * FROM users WHERE name ILIKE 'alice%';
```

---

## BETWEEN … AND

```sql
SELECT * FROM users  WHERE score BETWEEN 80 AND 100;
SELECT * FROM events WHERE created BETWEEN '2024-01-01 00:00:00' AND '2024-12-31 23:59:59';
```

`BETWEEN` is inclusive on both ends (equivalent to `>= low AND <= high`).

---

## IN and NOT IN

Match against a literal list:

```sql
SELECT * FROM users WHERE id IN (1, 2, 3);
SELECT * FROM orders WHERE status NOT IN ('cancelled', 'refunded');
```

Match against a subquery:

```sql
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);
SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users);
```

### Bind parameters with IN

Use one `?` placeholder per value — this is the standard behaviour across all `database/sql` drivers (SQLite, MySQL, PostgreSQL included):

```go
// ✅ one placeholder per value
rows, err := db.Query(
    `SELECT * FROM users WHERE id IN (?, ?, ?)`,
    1, 2, 3,
)
```

To build the placeholder list dynamically from a Go slice, use [`sqlx.In`](https://jmoiron.github.io/sqlx/#inQueries) which rewrites the query before it reaches the driver:

```go
import "github.com/jmoiron/sqlx"

ids := []int64{1, 2, 3}
query, args, err := sqlx.In(`SELECT * FROM users WHERE id IN (?)`, ids)
// query is now "SELECT * FROM users WHERE id IN (?, ?, ?)"
rows, err := db.Query(query, args...)
```

---

## IS NULL and IS NOT NULL

```sql
SELECT * FROM users WHERE nickname IS NULL;
SELECT * FROM users WHERE email   IS NOT NULL;
```

---

## String concatenation: `||`

```sql
SELECT first_name || ' ' || last_name AS full_name FROM users;
```

---

## JSON operators

| Operator | Returns | Example |
|----------|---------|---------|
| `->` key | JSON fragment (string) | `payload -> 'user'` |
| `->>` key | SQL scalar (string/number) | `payload ->> 'uid'` |
| `->` index | JSON array element | `tags -> 0` |
| `->>` index | Array element as scalar | `tags ->> 0` |

Path navigation:

```sql
-- Navigate nested object
SELECT payload -> 'address' -> 'city' FROM users;

-- Extract scalar
SELECT payload ->> 'name' FROM events;

-- Filter by JSON field
SELECT * FROM events WHERE payload ->> 'type' = 'login';
SELECT * FROM events WHERE (payload -> 'score')::INT8 > 90;
```

See [JSON](../json.md) for the full JSON reference.

---

## WHERE clause examples

```sql
-- Simple equality
SELECT * FROM users WHERE id = 1;

-- Multiple conditions
SELECT * FROM users WHERE age >= 18 AND active = true;

-- OR conditions
SELECT * FROM orders WHERE status = 'pending' OR status = 'processing';

-- LIKE
SELECT * FROM users WHERE email LIKE '%@example.com';

-- IN list
SELECT * FROM users WHERE id IN (1, 2, 3);

-- BETWEEN
SELECT * FROM users WHERE score BETWEEN 80 AND 100;

-- IS NULL
SELECT * FROM users WHERE nickname IS NULL;

-- Subquery
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- JSON field filter
SELECT * FROM events WHERE payload ->> 'action' = 'signup';

-- Combined
SELECT * FROM products
WHERE category = 'electronics'
  AND price BETWEEN 100 AND 500
  AND stock > 0
  AND name LIKE '%phone%';
```
