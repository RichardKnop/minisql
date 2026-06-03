# JSON Functions

MiniSQL provides operators and functions for navigating and querying `JSON` columns.

See also [JSON](../json.md) for the data type reference and [JSON Inverted Index](../indexes/json-inverted.md) for indexed JSON queries.

---

## Path operators

### `->` — JSON fragment

Returns the value at a key (object) or index (array) as a JSON string.

```sql
SELECT payload -> 'user'       FROM events;   -- '{"name":"alice"}'
SELECT payload -> 0            FROM events;   -- first array element as JSON
```

### `->>` — SQL scalar

Returns the value as a SQL scalar (string or number), not a JSON fragment.

```sql
SELECT payload ->> 'user'      FROM events;   -- 'alice'
SELECT payload ->> 'uid'       FROM events;   -- '42' (string)
SELECT payload ->> 0           FROM events;   -- first array element as string
```

### Chaining

```sql
SELECT payload -> 'address' ->> 'city' FROM users;
```

---

## JSON_EXTRACT(json, path)

Extracts a value at the given path. Returns a SQL scalar. Returns `NULL` if the path does not exist.

```sql
SELECT JSON_EXTRACT(payload, 'user')        FROM events;
SELECT JSON_EXTRACT(payload, 'score')       FROM events;  -- returned as number
SELECT JSON_EXTRACT(payload, 'tags.0')      FROM events;  -- first element of 'tags' array
```

---

## JSON_CONTAINS(json, value)

Returns `true` if `json` contains `value` as a sub-document or element. Used with the [JSON inverted index](../indexes/json-inverted.md).

```sql
-- Object containment
SELECT * FROM events WHERE JSON_CONTAINS(payload, '{"action": "login"}');

-- Scalar element in array or nested value
SELECT * FROM events WHERE JSON_CONTAINS(payload, '"go"');
```

---

## JSON_TYPE(json [, path])

Returns the JSON type of the value as a string: `'object'`, `'array'`, `'string'`, `'number'`, `'boolean'`, or `'null'`.

```sql
SELECT JSON_TYPE('{"a": 1}');           -- 'object'
SELECT JSON_TYPE('[1, 2, 3]');          -- 'array'
SELECT JSON_TYPE('"hello"');            -- 'string'
SELECT JSON_TYPE('42');                 -- 'number'
SELECT JSON_TYPE('true');              -- 'boolean'

-- At a specific path
SELECT JSON_TYPE(payload, 'tags')      FROM events;  -- 'array'
SELECT JSON_TYPE(payload, 'score')     FROM events;  -- 'number'
```

---

## JSON_ARRAY_LENGTH(json [, path])

Returns the number of elements in a JSON array, or `NULL` if the target is not an array.

```sql
SELECT JSON_ARRAY_LENGTH('[1, 2, 3]');        -- 3
SELECT JSON_ARRAY_LENGTH('[]');               -- 0
SELECT JSON_ARRAY_LENGTH(payload)       FROM events;
SELECT JSON_ARRAY_LENGTH(payload, 'tags') FROM events;  -- length of payload.tags
```

---

## JSON_VALID(json)

Returns `1` if the string is valid JSON, `0` otherwise.

```sql
SELECT JSON_VALID('{"a": 1}');       -- 1
SELECT JSON_VALID('not json');        -- 0
SELECT JSON_VALID(NULL);              -- NULL
```

---

## CAST to JSON

```sql
SELECT CAST('{"a": 1}' AS JSON);
```

Validates and normalises the JSON on insert/update.

---

## Examples

```sql
-- Filter by nested JSON field
SELECT * FROM events WHERE payload ->> 'action' = 'login';

-- Extract a numeric field and compare
SELECT * FROM events WHERE CAST(payload ->> 'score' AS INT8) > 90;

-- Navigate nested structure
SELECT payload -> 'meta' ->> 'version' AS version FROM events;

-- Aggregate over a JSON field
SELECT payload ->> 'country' AS country, COUNT(*) AS cnt
FROM events
WHERE payload ->> 'action' = 'signup'
GROUP BY payload ->> 'country'
ORDER BY cnt DESC;
```
