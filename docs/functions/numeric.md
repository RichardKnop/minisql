# Numeric Functions

## ABS(n)

Returns the absolute value of a number.

```sql
SELECT ABS(-42);         -- 42
SELECT ABS(-3.14);       -- 3.14
SELECT ABS(score) FROM measurements;
```

Works with `INT4`, `INT8`, `REAL`, and `DOUBLE`. Returns `NULL` for `NULL` input.

## FLOOR(n)

Rounds down to the nearest integer.

```sql
SELECT FLOOR(3.7);       -- 3.0
SELECT FLOOR(-2.3);      -- -3.0
SELECT FLOOR(amount / 100.0) FROM orders;
```

Integer inputs are returned unchanged. Returns `NULL` for `NULL` input.

## CEIL(n)

Rounds up to the nearest integer.

```sql
SELECT CEIL(3.2);        -- 4.0
SELECT CEIL(-2.7);       -- -2.0
```

Integer inputs are returned unchanged. Returns `NULL` for `NULL` input.

## ROUND(n [, digits])

Rounds to `digits` decimal places (default 0 — nearest integer).

```sql
SELECT ROUND(3.567);         -- 4.0
SELECT ROUND(3.567, 2);      -- 3.57
SELECT ROUND(3.567, 1);      -- 3.6
SELECT ROUND(amount, 2) FROM transactions;
```

Integer inputs are returned unchanged regardless of `digits`. Returns `NULL` for `NULL` input.

## MOD(a, b)

Returns the remainder of `a / b`.

```sql
SELECT MOD(10, 3);       -- 1
SELECT MOD(10.5, 3.0);   -- 1.5
SELECT * FROM rows WHERE MOD(id, 2) = 0;   -- even IDs
```

Returns `NULL` for `NULL` input. Raises an error on division by zero.

---

## Arithmetic operators

Arithmetic can also be expressed inline:

```sql
SELECT price * quantity           AS total   FROM order_lines;
SELECT balance - debit            AS new_bal FROM accounts;
SELECT total / CAST(count AS DOUBLE) AS avg  FROM summaries;
SELECT id % 2                     AS parity  FROM rows;
```

!!! warning "No negative integer literals"
    The SQL parser rejects negative integer literals. Use bind parameters instead:

    ```go
    db.Exec("UPDATE t SET n = n + ?", int64(-5)) // ✅
    // db.Exec("UPDATE t SET n = n + (-5)")       // ❌ parse error
    ```

---

## Type promotion

When both operands are integers, arithmetic returns an integer. When one is a float, the result is a float:

```sql
SELECT 10 / 3;           -- 3   (integer division)
SELECT 10 / 3.0;         -- 3.333…  (float)
SELECT CAST(10 AS DOUBLE) / 3;  -- 3.333…
```
