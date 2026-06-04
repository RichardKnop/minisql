# DELETE

## Basic syntax

```sql
DELETE FROM table_name
[WHERE condition]
[RETURNING column_list]
```

---

## Basic DELETE

```sql
-- Delete one row by primary key
DELETE FROM users WHERE id = 1;

-- Delete by condition
DELETE FROM sessions WHERE expires < '2024-01-01 00:00:00';

-- Delete with multiple conditions
DELETE FROM orders WHERE user_id = 5 AND status = 'cancelled';

-- Delete with bind parameters
DELETE FROM tokens WHERE user_id = ? AND token = ?;
```

---

## DELETE all rows

Omit `WHERE` to remove every row in the table:

```sql
DELETE FROM sessions;
```

!!! warning
    There is no confirmation prompt. A DELETE without a WHERE clause removes all rows immediately and cannot be undone without a rollback. Use `TRUNCATE TABLE` for the same effect with identical semantics.

---

## DELETE with RETURNING

`RETURNING` makes DELETE behave like a query — it returns the deleted rows.

```sql
-- Return the deleted row's id
DELETE FROM users WHERE id = 1 RETURNING id;

-- Return multiple columns
DELETE FROM sessions WHERE expires < NOW() RETURNING id, user_id;
```

In Go:

```go
rows, err := db.Query(
    `DELETE FROM sessions WHERE expires < ? RETURNING id, user_id`,
    time.Now(),
)
defer rows.Close()
for rows.Next() {
    var id, userID int64
    rows.Scan(&id, &userID)
    // handle deleted session
}
```

---

## DELETE with subquery

```sql
-- Delete orders for banned users
DELETE FROM orders
WHERE user_id IN (SELECT id FROM users WHERE banned = true);

-- Delete the oldest sessions, keeping only the 10 most recent per user
DELETE FROM sessions
WHERE id NOT IN (
    SELECT id FROM sessions
    ORDER BY created_at DESC
    LIMIT 10
);
```

---

## TRUNCATE TABLE

`TRUNCATE TABLE` is shorthand for a full-table delete with no WHERE clause. It removes every row and maintains all indexes and foreign-key constraints exactly as a plain DELETE would.

```sql
TRUNCATE TABLE table_name;
```

It is semantically equivalent to:

```sql
DELETE FROM table_name;
```

Use `TRUNCATE` when intent matters for readability — it makes it explicit that you mean to empty the whole table, not that you forgot a WHERE clause.

```sql
-- Clear a cache table between test runs
TRUNCATE TABLE session_cache;

-- Reset a staging table before a bulk load
TRUNCATE TABLE import_staging;
```

After a TRUNCATE, unique constraint slots are freed so previously-inserted values can be reinserted:

```sql
INSERT INTO users (email) VALUES ('alice@example.com');
TRUNCATE TABLE users;
INSERT INTO users (email) VALUES ('alice@example.com'); -- succeeds
```

---

## Notes

- Foreign-key constraints are checked on delete when `PRAGMA foreign_keys = on` (the default). Deleting a parent row that has child rows referencing it returns an error.
- `RETURNING` returns the row values *before* deletion — useful for audit logging or cascading application logic.
- Omitting `WHERE` deletes all rows. Use `TRUNCATE TABLE` for identical semantics with a clearer intent.
