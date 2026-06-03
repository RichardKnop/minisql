# SELECT

## Basic syntax

```sql
SELECT [DISTINCT] column_list
FROM   table_name [AS alias]
[JOIN  ...]
[WHERE condition]
[GROUP BY column_list]
[HAVING condition]
[ORDER BY column_list [ASC|DESC]]
[LIMIT n]
[OFFSET m]
```

---

## Column selection

```sql
-- All columns
SELECT * FROM users;

-- Named columns
SELECT id, name, email FROM users;

-- Aliases
SELECT id, UPPER(name) AS display_name FROM users;

-- Expressions
SELECT id, price * quantity AS total FROM order_lines;
```

---

## WHERE

See [Operators & WHERE](operators.md) for the full operator reference.

```sql
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE age >= 18 AND active = true;
SELECT * FROM users WHERE email LIKE '%@example.com';
SELECT * FROM users WHERE id IN (1, 2, 3);
SELECT * FROM users WHERE score BETWEEN 80 AND 100;
SELECT * FROM users WHERE name IS NOT NULL;
```

---

## DISTINCT

Remove duplicate rows from the result:

```sql
SELECT DISTINCT department FROM employees;
SELECT DISTINCT user_id, product_id FROM orders;
```

---

## ORDER BY

```sql
-- Single column ascending (default)
SELECT * FROM users ORDER BY created;

-- Descending
SELECT * FROM users ORDER BY created DESC;

-- Multiple columns
SELECT * FROM users ORDER BY department ASC, salary DESC;
```

---

## LIMIT and OFFSET

```sql
-- First 10 rows
SELECT * FROM users LIMIT 10;

-- Pagination: rows 21–30
SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20;
```

---

## GROUP BY and HAVING

```sql
-- Count per department
SELECT department, COUNT(*) AS cnt
FROM employees
GROUP BY department;

-- Only departments with more than 5 members
SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_sal
FROM employees
GROUP BY department
HAVING COUNT(*) > 5;

-- Sum per user with filter
SELECT user_id, SUM(amount) AS total
FROM orders
GROUP BY user_id
HAVING SUM(amount) > 1000;
```

---

## Aggregate functions

```sql
SELECT COUNT(*)                    FROM orders;
SELECT COUNT(DISTINCT user_id)     FROM orders;
SELECT SUM(amount)                 FROM orders;
SELECT AVG(amount)                 FROM orders;
SELECT MIN(amount), MAX(amount)    FROM orders;
```

See [Aggregate Functions](../functions/aggregate.md).

---

## JOINs

### INNER JOIN

Returns rows matching in both tables:

```sql
SELECT u.id, u.name, o.amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id;
```

### LEFT JOIN

All rows from the left table; NULL for unmatched right rows:

```sql
SELECT u.id, u.name, COALESCE(o.amount, 0) AS total
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;
```

### RIGHT JOIN

All rows from the right table; NULL for unmatched left rows:

```sql
SELECT u.id, u.name, o.amount
FROM users u
RIGHT JOIN orders o ON u.id = o.user_id;
```

### Multi-table chain

```sql
SELECT u.name, p.name AS product, o.amount
FROM users u
INNER JOIN orders o   ON u.id = o.user_id
INNER JOIN products p ON o.product_id = p.id
LEFT JOIN  reviews r  ON p.id = r.product_id;
```

---

## Subqueries

### Scalar subquery in SELECT

```sql
SELECT id, name,
       (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count
FROM users;
```

### IN with subquery

```sql
SELECT * FROM users
WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

SELECT * FROM users
WHERE id NOT IN (SELECT user_id FROM banned_users);
```

### Derived table in FROM

```sql
SELECT * FROM (
    SELECT id, name FROM users WHERE score > 80
) AS active_users;
```

---

## Common Table Expressions (CTEs)

```sql
-- Single CTE
WITH active_users AS (
    SELECT id, name FROM users WHERE score > 80
)
SELECT * FROM active_users;

-- Multiple CTEs
WITH
    high_scorers AS (SELECT id FROM users WHERE score > 80),
    user_orders  AS (SELECT user_id, amount FROM orders)
SELECT uo.amount
FROM user_orders uo
WHERE uo.user_id IN (SELECT id FROM high_scorers);

-- CTE joined with real table
WITH active AS (SELECT id FROM users WHERE score > 80)
SELECT u.name
FROM users AS u
INNER JOIN active AS a ON u.id = a.id;

-- CTE with aggregation
WITH totals AS (
    SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
    FROM orders
    GROUP BY user_id
)
SELECT * FROM totals WHERE total > 500;
```

!!! note
    Recursive CTEs (`WITH RECURSIVE`) are not yet implemented.

---

## UNION and UNION ALL

```sql
-- UNION deduplicates rows
SELECT id, name FROM active_users
UNION
SELECT id, name FROM archived_users;

-- UNION ALL keeps duplicates (faster)
SELECT id, name FROM users
UNION ALL
SELECT id, name FROM former_users;

-- Chain of three
SELECT id FROM users
UNION ALL
SELECT id FROM archived_users
UNION
SELECT id FROM deleted_users;
```

---

## CASE WHEN

Searched form (arbitrary conditions):

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

Simple form (equality on one expression):

```sql
SELECT id,
    CASE status
        WHEN 'active'   THEN 'Active'
        WHEN 'inactive' THEN 'Inactive'
        ELSE 'Unknown'
    END AS status_label
FROM accounts;
```

---

## CAST

```sql
SELECT CAST(price AS INT8)   FROM products;
SELECT CAST(id AS TEXT)      FROM users;
SELECT CAST(n AS DOUBLE)     FROM numbers;
SELECT CAST(name AS VARCHAR(50)) FROM users;
SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP);
SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);
SELECT CAST('{"a": 1}' AS JSON);
```

---

## Window functions

```sql
-- Row number
SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM students;

-- Partition by department
SELECT name, dept,
       ROW_NUMBER()  OVER (PARTITION BY dept ORDER BY score DESC) AS dept_rank,
       DENSE_RANK()  OVER (PARTITION BY dept ORDER BY score DESC) AS dense_rk,
       SUM(score)    OVER (PARTITION BY dept) AS dept_total,
       AVG(score)    OVER (PARTITION BY dept) AS dept_avg
FROM employees;

-- Running total
SELECT name, score,
       SUM(score) OVER (ORDER BY score) AS running_sum
FROM students;

-- LAG / LEAD
SELECT name, score,
       LAG(score,  1) OVER (ORDER BY score) AS prev_score,
       LEAD(score, 1) OVER (ORDER BY score) AS next_score
FROM students;
```

See [Window Functions](../functions/window.md).

---

## Full-text search

```sql
SELECT id, TS_RANK(body, 'database storage') AS score
FROM articles
WHERE MATCH(body, 'database storage')
ORDER BY score DESC
LIMIT 10;
```

See [Full-text Search](../indexes/fulltext.md).

---

## Vector nearest-neighbour search

```sql
SELECT id, body, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist
LIMIT 5;
```

See [Vector Search](../vector-search.md).
