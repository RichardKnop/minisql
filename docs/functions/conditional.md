# Conditional Functions

## COALESCE(val1, val2, …)

Returns the first non-NULL argument. Useful for providing fallback values.

```sql
SELECT COALESCE(nickname, name, 'Anonymous') FROM users;
SELECT COALESCE(discount, 0)                 FROM products;
SELECT COALESCE(phone, email)                FROM contacts;
```

At least one argument is required. Returns `NULL` only if all arguments are `NULL`.

## NULLIF(a, b)

Returns `NULL` if `a = b`; otherwise returns `a`. Useful for avoiding division-by-zero and replacing sentinel values with `NULL`.

```sql
-- Avoid division by zero
SELECT total / NULLIF(count, 0) AS avg FROM summaries;

-- Replace empty string with NULL
SELECT NULLIF(name, '') FROM users;

-- Replace a sentinel value
SELECT NULLIF(score, -1) AS score FROM results;
```

---

## CASE WHEN

`CASE WHEN` is the general conditional expression.

### Searched form

```sql
SELECT id, name,
    CASE
        WHEN score >= 90 THEN 'A'
        WHEN score >= 80 THEN 'B'
        WHEN score >= 70 THEN 'C'
        ELSE 'F'
    END AS grade
FROM students;
```

### Simple form (equality test)

```sql
SELECT id,
    CASE status
        WHEN 'active'   THEN 'Active user'
        WHEN 'inactive' THEN 'Inactive user'
        ELSE 'Unknown'
    END AS status_label
FROM accounts;
```

### In WHERE

```sql
SELECT * FROM orders
WHERE
    CASE type
        WHEN 'premium' THEN amount > 0
        WHEN 'trial'   THEN amount = 0
        ELSE true
    END;
```

### In UPDATE SET

```sql
UPDATE employees
SET salary = CASE
    WHEN department = 'eng'   THEN salary * 1.10
    WHEN department = 'sales' THEN salary * 1.05
    ELSE salary
END;
```

---

## IS NULL / IS NOT NULL

Not a function, but commonly used for null handling:

```sql
SELECT * FROM users WHERE nickname IS NULL;
SELECT * FROM users WHERE email    IS NOT NULL;

SELECT COALESCE(nickname, name) AS display
FROM users
WHERE nickname IS NOT NULL;
```
