# Window Functions

Window functions compute a value for each row based on a related set of rows (the *window*), without collapsing the result into a single row. They require an `OVER` clause.

## Syntax

```sql
function_name([args]) OVER (
    [PARTITION BY column_list]
    [ORDER BY column_list [ASC|DESC]]
)
```

---

## ROW_NUMBER()

Assigns a unique sequential integer to each row within the window, starting at 1.

```sql
-- Global ranking by score
SELECT name, score,
       ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM students;

-- Rank within each department
SELECT name, dept, score,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS dept_rank
FROM employees;
```

---

## RANK()

Like `ROW_NUMBER()`, but rows with equal values receive the same rank, and the next rank skips (1, 2, 2, 4, …).

```sql
SELECT name, score,
       RANK() OVER (ORDER BY score DESC) AS rank
FROM students;
```

---

## DENSE_RANK()

Like `RANK()`, but without gaps — tied rows share a rank and the next rank is consecutive (1, 2, 2, 3, …).

```sql
SELECT name, dept, score,
       DENSE_RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS dense_rank
FROM employees;
```

---

## SUM, AVG, MIN, MAX over a window

Aggregate functions can be used as window functions to compute running totals, partition totals, and partition averages.

```sql
-- Running sum of scores
SELECT name, score,
       SUM(score) OVER (ORDER BY score) AS running_sum
FROM students;

-- Department totals alongside individual rows
SELECT name, dept, salary,
       SUM(salary)  OVER (PARTITION BY dept) AS dept_total,
       AVG(salary)  OVER (PARTITION BY dept) AS dept_avg,
       MIN(salary)  OVER (PARTITION BY dept) AS dept_min,
       MAX(salary)  OVER (PARTITION BY dept) AS dept_max
FROM employees;
```

---

## LAG(col, offset)

Returns the value of `col` from `offset` rows before the current row within the window. Returns `NULL` for the first `offset` rows.

```sql
SELECT name, score,
       LAG(score, 1) OVER (ORDER BY score) AS prev_score
FROM students;
```

---

## LEAD(col, offset)

Returns the value of `col` from `offset` rows after the current row. Returns `NULL` for the last `offset` rows.

```sql
SELECT name, score,
       LEAD(score, 1) OVER (ORDER BY score) AS next_score
FROM students;
```

---

## LAG and LEAD together

```sql
SELECT name, score,
       LAG(score,  1) OVER (ORDER BY score) AS prev_score,
       LEAD(score, 1) OVER (ORDER BY score) AS next_score
FROM students;
```

---

## Full example

```sql
CREATE TABLE employees (
    id         INT8        PRIMARY KEY AUTOINCREMENT,
    name       VARCHAR(64) NOT NULL,
    dept       VARCHAR(32) NOT NULL,
    salary     INT8        NOT NULL
);

INSERT INTO employees (name, dept, salary) VALUES
    ('Alice', 'eng',   120000),
    ('Bob',   'eng',    95000),
    ('Carol', 'sales',  80000),
    ('Dave',  'sales',  85000),
    ('Eve',   'eng',   110000);

SELECT
    name,
    dept,
    salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank,
    DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dense_rk,
    SUM(salary)  OVER (PARTITION BY dept)                      AS dept_total,
    AVG(salary)  OVER (PARTITION BY dept)                      AS dept_avg
FROM employees
ORDER BY dept, dept_rank;
```

---

## Notes

- Multiple `OVER` clauses in the same `SELECT` are evaluated independently.
- `PARTITION BY` is optional — omitting it treats all rows as a single partition.
- `ORDER BY` inside `OVER` is independent of the query-level `ORDER BY`.
