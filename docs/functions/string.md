# String Functions

## UPPER(str)

Converts a string to uppercase.

```sql
SELECT UPPER('hello');         -- 'HELLO'
SELECT UPPER(name) FROM users;
```

## LOWER(str)

Converts a string to lowercase.

```sql
SELECT LOWER('HELLO');         -- 'hello'
SELECT LOWER(email) FROM users;
```

## LENGTH(str)

Returns the byte length of the string.

```sql
SELECT LENGTH('hello');        -- 5
SELECT LENGTH(name) FROM users;
```

## TRIM([str [, chars]])

Removes leading and trailing characters from a string. Defaults to whitespace.

```sql
SELECT TRIM('  hello  ');         -- 'hello'
SELECT TRIM('xxhelloxx', 'x');    -- 'hello'
SELECT TRIM(name) FROM users;
```

## LTRIM([str [, chars]])

Removes leading characters only.

```sql
SELECT LTRIM('  hello  ');        -- 'hello  '
SELECT LTRIM('xxhello', 'x');     -- 'hello'
```

## RTRIM([str [, chars]])

Removes trailing characters only.

```sql
SELECT RTRIM('  hello  ');        -- '  hello'
SELECT RTRIM('helloxx', 'x');     -- 'hello'
```

## SUBSTR(str, start [, length])

Returns a substring. `start` is 1-based. If `length` is omitted, returns to end of string.

```sql
SELECT SUBSTR('hello world', 7);      -- 'world'
SELECT SUBSTR('hello world', 1, 5);   -- 'hello'
SELECT SUBSTR(body, 1, 100) FROM articles;
```

## REPLACE(str, from, to)

Replaces all occurrences of `from` with `to`.

```sql
SELECT REPLACE('foo bar foo', 'foo', 'baz');  -- 'baz bar baz'
SELECT REPLACE(email, '@old.com', '@new.com') FROM users;
```

## CONCAT(str1, str2, ...)

Concatenates strings. NULL arguments are silently skipped (PostgreSQL semantics).

```sql
SELECT CONCAT('hello', ' ', 'world');   -- 'hello world'
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;
```

Alternatively, use the `||` operator:

```sql
SELECT first_name || ' ' || last_name FROM users;
```

## NATURAL_SORT(str)

Converts a string to a sort key where each run of ASCII digits is zero-padded to 20 characters. Comparing these keys lexicographically gives the same order as natural (human) sort — numeric segments are compared by value, not by byte sequence.

```sql
-- Semantic version ordering
SELECT version FROM releases ORDER BY NATURAL_SORT(version);
-- '1.2.0', '1.9.1', '1.10.2', '2.0.0', '10.0.0'   ✓
-- (plain ORDER BY version gives '1.10.2', '1.2.0', '1.9.1', ...)

-- File names with embedded numbers
SELECT name FROM files ORDER BY NATURAL_SORT(name);
-- 'file1.txt', 'file2.txt', 'file10.txt', 'file20.txt'

-- Descending
SELECT version FROM releases ORDER BY NATURAL_SORT(version) DESC;

-- Inspect the sort key
SELECT NATURAL_SORT('1.10.2');
-- '00000000000000000001.00000000000000000010.00000000000000000002'
```

`NATURAL_SORT` returns `NULL` when its argument is `NULL`.

The key can also be stored in an expression index to accelerate queries that always sort by natural order:

```sql
CREATE INDEX idx_releases_ver ON releases (NATURAL_SORT(version));

SELECT version FROM releases ORDER BY NATURAL_SORT(version) LIMIT 10;
```

## NULL behaviour

All string functions return `NULL` if any non-skippable argument is `NULL`:

```sql
SELECT UPPER(NULL);         -- NULL
SELECT LENGTH(NULL);        -- NULL
SELECT REPLACE(NULL, 'a', 'b'); -- NULL
```

`CONCAT` is an exception — it skips `NULL` arguments and returns the concatenation of non-null values.

---

## Expression index example

String functions can be used in expression indexes to accelerate function-based predicates:

```sql
CREATE INDEX idx_lower_email ON users (LOWER(email));

-- Uses the expression index
SELECT * FROM users WHERE LOWER(email) = 'alice@example.com';
```
