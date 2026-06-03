# Aggregate Functions

Aggregate functions compute a single value from a set of rows. They are used with `SELECT` and, optionally, `GROUP BY` and `HAVING`.

---

## COUNT

```sql
-- Count all rows
SELECT COUNT(*) FROM orders;

-- Count non-NULL values in a column
SELECT COUNT(user_id) FROM orders;

-- Count distinct values
SELECT COUNT(DISTINCT user_id) FROM orders;
```

## SUM

```sql
SELECT SUM(amount)        FROM orders;
SELECT SUM(price * qty)   FROM order_lines;
```

`SUM` ignores `NULL` values. Returns `NULL` if all values are `NULL`.

## AVG

```sql
SELECT AVG(amount)  FROM orders;
SELECT AVG(salary)  FROM employees;
```

`AVG` ignores `NULL` values. Returns `NULL` if all values are `NULL`.

## MIN and MAX

```sql
SELECT MIN(amount), MAX(amount) FROM orders;
SELECT MIN(created), MAX(created) FROM events;
SELECT MIN(name) FROM users;   -- lexicographic min
```

`MIN` / `MAX` ignore `NULL` values. Return `NULL` if all values are `NULL`.

---

## With GROUP BY

```sql
-- Total orders per user
SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total
FROM orders
GROUP BY user_id;

-- Department statistics
SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
ORDER BY headcount DESC;
```

## With HAVING

Filter groups after aggregation:

```sql
-- Only users with more than 5 orders
SELECT user_id, COUNT(*) AS cnt
FROM orders
GROUP BY user_id
HAVING COUNT(*) > 5;

-- Departments with average salary above threshold
SELECT department, AVG(salary) AS avg_sal
FROM employees
GROUP BY department
HAVING AVG(salary) > 50000;

-- Users whose total spend exceeds 1000
SELECT user_id, SUM(amount) AS total
FROM orders
GROUP BY user_id
HAVING SUM(amount) > 1000;
```

!!! note
    `HAVING` does not support `?` bind parameters. Use literal values in HAVING conditions.

## COUNT(DISTINCT …)

```sql
-- Number of unique products ordered
SELECT COUNT(DISTINCT product_id) FROM order_lines;

-- Unique users per day
SELECT DATE_TRUNC('day', created) AS day, COUNT(DISTINCT user_id) AS dau
FROM events
GROUP BY DATE_TRUNC('day', created);
```

---

## In subqueries

```sql
-- Users who have placed at least one order over 100
SELECT * FROM users
WHERE id IN (
    SELECT user_id FROM orders WHERE amount > 100
);

-- CTE with aggregation
WITH totals AS (
    SELECT user_id, SUM(amount) AS total
    FROM orders
    GROUP BY user_id
)
SELECT u.name, t.total
FROM users u
JOIN totals t ON u.id = t.user_id
WHERE t.total > 500;
```
