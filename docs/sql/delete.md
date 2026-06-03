# DELETE

## Basic syntax

```sql
DELETE FROM table_name
WHERE condition
[RETURNING column_list]
```

!!! warning
    MiniSQL requires a `WHERE` clause on every `DELETE`. A DELETE without a WHERE clause is a parse error. To remove all rows, use `DELETE FROM table_name WHERE true` or `TRUNCATE` (not yet implemented).

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

## Notes

- Foreign-key constraints are checked on delete when `PRAGMA foreign_keys = on` (the default). Deleting a parent row that has child rows referencing it returns an error.
- `RETURNING` returns the row values *before* deletion — useful for audit logging or cascading application logic.
- To delete all rows efficiently, use `DELETE FROM table_name WHERE true`.
