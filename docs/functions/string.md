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
