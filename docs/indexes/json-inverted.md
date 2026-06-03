# JSON Inverted Index

A JSON inverted index accelerates containment queries on `JSON` columns. Instead of deserialising every row to check a JSON predicate, the index maps JSON values to the row IDs that contain them.

---

## Creating an inverted index

```sql
CREATE INVERTED INDEX idx_events_payload ON events (payload);
```

The index is built on a `JSON` column. No additional options are required.

---

## Querying with JSON_CONTAINS

`JSON_CONTAINS(column, value)` returns `true` if the JSON column contains the given value as a sub-document or array element:

```sql
-- Object containment
SELECT * FROM events WHERE JSON_CONTAINS(payload, '{"action": "login"}');

-- Scalar value in any position
SELECT * FROM events WHERE JSON_CONTAINS(payload, '"go"');

-- Nested object
SELECT * FROM events WHERE JSON_CONTAINS(payload, '{"user": {"role": "admin"}}');
```

---

## Index-accelerated queries

When the query planner detects a `JSON_CONTAINS(col, val)` predicate and an inverted index exists on `col`, it uses the index to look up matching row IDs directly:

```sql
EXPLAIN SELECT * FROM events WHERE JSON_CONTAINS(payload, '{"action": "login"}');
-- op: InvertedIndexScan  index: idx_events_payload
```

Without an inverted index, `JSON_CONTAINS` falls back to a full table scan with per-row JSON deserialization.

---

## Combining with other predicates

```sql
-- Inverted index for the JSON filter; B-tree index for created_at range
SELECT * FROM events
WHERE JSON_CONTAINS(payload, '{"type": "purchase"}')
  AND created_at > '2024-01-01 00:00:00';

-- With ORDER BY and LIMIT
SELECT id, payload ->> 'user' AS user
FROM events
WHERE JSON_CONTAINS(payload, '{"action": "signup"}')
ORDER BY created_at DESC
LIMIT 20;
```

---

## Full example

```sql
-- Create table and index
CREATE TABLE events (
    id         INT8 PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT NOW(),
    payload    JSON NOT NULL
);

CREATE INVERTED INDEX idx_events_payload ON events (payload);

-- Insert events
INSERT INTO events (payload) VALUES
    ('{"action": "login",  "user": "alice", "ip": "1.2.3.4"}'),
    ('{"action": "signup", "user": "bob",   "plan": "free"}'),
    ('{"action": "login",  "user": "carol", "ip": "5.6.7.8"}'),
    ('{"tags": ["go", "sql", "database"]}');

-- Find all login events
SELECT id, payload ->> 'user' AS user
FROM events
WHERE JSON_CONTAINS(payload, '{"action": "login"}');

-- Find events tagged "go"
SELECT id FROM events
WHERE JSON_CONTAINS(payload, '"go"');
```

---

## Notes

- The inverted index is updated automatically on every `INSERT`, `UPDATE`, and `DELETE`.
- `JSON_CONTAINS` can be used without an inverted index — it will scan all rows in that case.
- The index handles both JSON objects and JSON arrays.
- Drop the index with `DROP INDEX idx_events_payload`.

See [JSON](../json.md) for the full JSON type and operator reference, including `->`, `->>`, `JSON_EXTRACT`, `JSON_TYPE`, and `JSON_ARRAY_LENGTH`.
