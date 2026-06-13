# UUID Functions

## GEN_RANDOM_UUID()

Generates a random [UUID version 4](https://www.rfc-editor.org/rfc/rfc9562) value.

```sql
SELECT GEN_RANDOM_UUID() FROM users LIMIT 1;
-- e.g. 'f47ac10b-58cc-4372-a567-0e02b2c3d479'

INSERT INTO sessions (id, user_id) VALUES (GEN_RANDOM_UUID(), 1);

UPDATE tokens SET id = GEN_RANDOM_UUID() WHERE id = ?;
```

### DEFAULT GEN_RANDOM_UUID()

The most common use is as a column default so UUID primary keys are generated automatically on every INSERT:

```sql
CREATE TABLE users (
    id         UUID     NOT NULL DEFAULT GEN_RANDOM_UUID(),
    email      VARCHAR(255) NOT NULL UNIQUE,
    name       TEXT     NOT NULL,
    created_at TIMESTAMP    DEFAULT NOW()
);

-- id is generated automatically — no need to supply it
INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');

SELECT id, email FROM users WHERE email = 'alice@example.com';
-- 'f47ac10b-58cc-4372-a567-0e02b2c3d479'  alice@example.com
```

The constraint order required by the parser is `NOT NULL` before `DEFAULT`:

```sql
-- correct
id UUID NOT NULL DEFAULT GEN_RANDOM_UUID()

-- incorrect — parser does not accept DEFAULT before NOT NULL
id UUID DEFAULT GEN_RANDOM_UUID() NOT NULL
```

### ALTER TABLE ADD COLUMN

```sql
ALTER TABLE documents ADD COLUMN external_id UUID NOT NULL DEFAULT GEN_RANDOM_UUID();
```

Existing rows receive a freshly generated UUID for the new column.

### UUID version

`GEN_RANDOM_UUID()` produces **version 4** UUIDs — 122 bits of cryptographic randomness with 6 fixed version/variant bits. This matches PostgreSQL's `gen_random_uuid()` behaviour.

The alternative is **version 7** (RFC 9562, 2024), which embeds a Unix millisecond timestamp in the high bits, making values monotonically increasing within the same millisecond. This improves B-tree insertion locality (sequential writes avoid random page splits) but leaks creation-time information. MiniSQL currently generates v4; if you need sorted identifiers consider using `INT8 PRIMARY KEY AUTOINCREMENT` or supplying v7 UUIDs from your application layer.

### Return type

Returns `UUID` (16-byte binary, displayed as a lowercase hyphenated string).
