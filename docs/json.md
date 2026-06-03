# JSON

MiniSQL has a native `JSON` column type that validates and stores JSON data. JSON columns support path navigation with `->` / `->>` operators, containment queries with `JSON_CONTAINS`, and optional inverted index acceleration.

---

## The JSON type

```sql
CREATE TABLE events (
    id      INT8 PRIMARY KEY AUTOINCREMENT,
    created TIMESTAMP DEFAULT NOW(),
    payload JSON NOT NULL
);
```

- Values must be valid JSON; invalid JSON is rejected on insert and update.
- Stored as compact UTF-8 text (whitespace is not preserved).
- Size is unlimited (large values use overflow pages).
- Validated automatically — no application-level validation needed.

---

## Inserting JSON

```sql
-- JSON object
INSERT INTO events (payload) VALUES ('{"action": "login", "user": "alice", "uid": 42}');

-- JSON array
INSERT INTO events (payload) VALUES ('["go", "sql", "database"]');

-- Nested
INSERT INTO events (payload) VALUES ('{"meta": {"version": 2}, "tags": ["a", "b"]}');
```

In Go:

```go
_, err = db.Exec(
    `INSERT INTO events (payload) VALUES (?)`,
    `{"action": "purchase", "amount": 99.99}`,
)
```

---

## Path operators

### `->` — JSON fragment

Returns the value at a key or array index as a JSON string.

```sql
SELECT payload -> 'action'  FROM events;   -- '"login"'
SELECT payload -> 'user'    FROM events;   -- '"alice"'
SELECT payload -> 0         FROM events;   -- first element as JSON
```

### `->>` — SQL scalar

Returns the value as a SQL string or number.

```sql
SELECT payload ->> 'action' FROM events;   -- 'login'
SELECT payload ->> 'uid'    FROM events;   -- '42'
SELECT payload ->> 0        FROM events;   -- first element as string
```

### Chaining

```sql
SELECT payload -> 'meta' -> 'version' FROM events;   -- '2' (JSON)
SELECT payload -> 'meta' ->> 'version' FROM events;  -- '2' (scalar)
```

---

## Filtering on JSON fields

```sql
-- Equality on extracted scalar
SELECT * FROM events WHERE payload ->> 'action' = 'login';

-- Numeric comparison (cast to INT8 first)
SELECT * FROM events WHERE CAST(payload ->> 'uid' AS INT8) > 100;

-- Nested field
SELECT * FROM events WHERE payload -> 'meta' ->> 'version' = '2';

-- Array element
SELECT * FROM events WHERE payload ->> 0 = 'go';
```

---

## JSON functions

| Function | Description |
|----------|-------------|
| `JSON_CONTAINS(json, val)` | True if json contains val as a sub-document or element |
| `JSON_EXTRACT(json, path)` | Extract scalar at path |
| `JSON_TYPE(json [, path])` | Type of the JSON value: `'object'`, `'array'`, `'string'`, `'number'`, `'boolean'`, `'null'` |
| `JSON_ARRAY_LENGTH(json [, path])` | Number of elements in a JSON array |
| `JSON_VALID(json)` | 1 if valid JSON, 0 otherwise |

See [JSON Functions](functions/json.md) for full examples.

---

## JSON inverted index

For fast containment queries, create an inverted index on a JSON column:

```sql
CREATE INVERTED INDEX idx_events_payload ON events (payload);

-- Index-accelerated containment query
SELECT * FROM events WHERE JSON_CONTAINS(payload, '{"action": "login"}');
```

See [JSON Inverted Index](indexes/json-inverted.md) for details.

---

## Examples

```sql
-- Create table with JSON column
CREATE TABLE sessions (
    id      UUID      PRIMARY KEY,
    user_id INT8      NOT NULL,
    meta    JSON
);

INSERT INTO sessions (id, user_id, meta) VALUES
    ('550e8400-e29b-41d4-a716-446655440001', 1, '{"ip": "1.2.3.4", "ua": "Firefox"}'),
    ('550e8400-e29b-41d4-a716-446655440002', 2, '{"ip": "5.6.7.8", "ua": "Chrome"}');

-- Find sessions from a specific IP
SELECT user_id FROM sessions WHERE meta ->> 'ip' = '1.2.3.4';

-- Aggregate by user-agent family
SELECT meta ->> 'ua' AS browser, COUNT(*) AS cnt
FROM sessions
GROUP BY meta ->> 'ua'
ORDER BY cnt DESC;
```
