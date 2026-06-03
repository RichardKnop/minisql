# UPDATE

## Basic syntax

```sql
UPDATE table_name
SET col1 = expr1 [, col2 = expr2 ...]
[FROM other_table]
[WHERE condition]
[RETURNING column_list]
```

---

## Simple UPDATE

```sql
-- Update one column
UPDATE users SET name = 'Alice Smith' WHERE id = 1;

-- Update multiple columns
UPDATE users
SET name = 'Bob', active = false
WHERE id = 2;

-- Update with expression
UPDATE products SET price = price * 2 WHERE category = 'rare';

-- Update with bind parameters
UPDATE users SET email = ? WHERE id = ?;
```

---

## UPDATE with RETURNING

`RETURNING` makes UPDATE behave like a query — it returns the modified rows after the update is applied.

```sql
-- Return updated row
UPDATE users SET name = 'Carol' WHERE id = 3 RETURNING id, name;

-- Return all columns
UPDATE accounts SET balance = balance - 100 WHERE id = 1 RETURNING *;
```

In Go:

```go
var newBalance int64
err = db.QueryRow(
    `UPDATE accounts SET balance = balance - ? WHERE id = ? RETURNING balance`,
    100, accountID,
).Scan(&newBalance)
```

---

## UPDATE FROM

`UPDATE … FROM` joins a second table to compute the new values:

```sql
-- Apply a discount from the discount table
UPDATE products p
SET price = p.price * (1 - d.pct)
FROM discounts d
WHERE d.product_id = p.id AND d.active = true;

-- Copy a field from another table
UPDATE orders o
SET status = s.state
FROM order_states s
WHERE s.order_id = o.id;
```

---

## UPDATE all rows

Omit `WHERE` to update every row in the table:

```sql
UPDATE settings SET value = 'default';
```

!!! warning
    There is no confirmation prompt. An UPDATE without a WHERE clause modifies all rows immediately and cannot be undone without a rollback.

---

## CASE WHEN in SET

```sql
UPDATE employees
SET salary = CASE
    WHEN department = 'eng'   THEN salary * 1.10
    WHEN department = 'sales' THEN salary * 1.05
    ELSE salary
END;
```

---

## Subquery in SET

```sql
UPDATE orders o
SET amount = (SELECT SUM(price * qty) FROM order_lines WHERE order_id = o.id);
```

---

## Notes

- `UPDATE` without a `WHERE` clause affects every row in the table.
- Expressions in `SET` are evaluated against the *original* row values, not intermediate results; updating column `a` and using `a` in a second `SET` clause sees the old value of `a`.
- Foreign-key constraints are checked after every update when `PRAGMA foreign_keys = on` (the default).
- CHECK constraints are re-evaluated on the updated values.
