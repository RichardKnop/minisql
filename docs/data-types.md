# Data Types

## Type reference

| SQL Type | Storage | Range / Format | Go type | Notes |
|----------|---------|----------------|---------|-------|
| `BOOLEAN` | 1 byte | `true` / `false` | `bool` | Stored internally as int8 |
| `INT4` | 4 bytes | −2 147 483 648 … 2 147 483 647 | `int32` | 32-bit signed integer |
| `INT8` | 8 bytes | −9 223 372 036 854 775 808 … 9 223 372 036 854 775 807 | `int64` | 64-bit signed integer; required for `AUTOINCREMENT` |
| `REAL` | 4 bytes | IEEE 754 single-precision | `float32` | |
| `DOUBLE` | 8 bytes | IEEE 754 double-precision | `float64` | |
| `VARCHAR(n)` | Variable, ≤ 512 bytes inline | At most *n* bytes | `string` | Inline storage up to 512 bytes; overflow pages for larger values |
| `TEXT` | Variable, ≤ 64 MiB | UTF-8 text | `string` / `io.Reader` | Overflow pages for values > 512 bytes; accepts `io.Reader` for streaming |
| `TIMESTAMP` | 8 bytes | 4713 BC … 294 276 AD | `time.Time` | Microseconds since 2000-01-01 (PostgreSQL epoch); timezone-naive |
| `JSON` | Variable, ≤ 64 MiB | Valid UTF-8 JSON text | `string` / `io.Reader` | Validated on insert/update; accepts `io.Reader` for streaming (validation skipped) |
| `UUID` | 16 bytes (fixed) | Hyphenated string `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | `string` | Stored as inline binary; output is lowercase |
| `VECTOR(n)` | 8 bytes inline + overflow | `[f1, f2, …, fn]` — *n* × float32 | `string` / `[]float32` | *n* fixed at column definition; data always on overflow pages |

## Nullable columns

All types support `NULL` values when the column is declared `NULLABLE` (i.e. without `NOT NULL`). NULL tracking uses a 64-bit bitmask per row, which limits tables to **64 columns maximum**.

## Detailed type notes

### BOOLEAN

```sql
CREATE TABLE flags (id INT8 PRIMARY KEY AUTOINCREMENT, active BOOLEAN NOT NULL DEFAULT true);
INSERT INTO flags (active) VALUES (true), (false);
SELECT * FROM flags WHERE active = true;
```

Use Go `bool` when binding parameters.

### INT4 and INT8

```sql
CREATE TABLE stats (
    id     INT8 PRIMARY KEY AUTOINCREMENT,
    hits   INT8 NOT NULL DEFAULT 0,
    rating INT4
);
```

### REAL and DOUBLE

```sql
CREATE TABLE measurements (
    id    INT8   PRIMARY KEY AUTOINCREMENT,
    temp  REAL,
    score DOUBLE NOT NULL DEFAULT 0.0
);
```

### VARCHAR(n)

```sql
CREATE TABLE users (
    id    INT8         PRIMARY KEY AUTOINCREMENT,
    email VARCHAR(255) NOT NULL UNIQUE,
    code  VARCHAR(10)  NOT NULL
);
```

- Values up to 512 bytes are stored **inline** in the leaf cell.
- Values longer than 512 bytes spill onto overflow pages (VARCHAR effectively becomes TEXT for large values).
- Can be used as primary key or unique key columns (up to 255 bytes).

### TEXT

```sql
CREATE TABLE articles (
    id   INT8 PRIMARY KEY AUTOINCREMENT,
    body TEXT
);
```

- Always uses overflow pages for values exceeding the inline threshold.
- Maximum value size is **64 MiB** per row.
- Cannot be a primary key or unique key column.
- Accepts an `io.Reader` as a bind parameter to stream large values without loading the full content into memory (see [Streaming large TEXT/JSON values](#streaming-large-textjson-values)).

### TIMESTAMP

```sql
CREATE TABLE events (
    id      INT8      PRIMARY KEY AUTOINCREMENT,
    created TIMESTAMP DEFAULT NOW(),
    updated TIMESTAMP
);

INSERT INTO events (updated) VALUES ('2024-06-01 12:00:00');
INSERT INTO events (updated) VALUES ('2024-06-01 12:00:00.123456');
```

Accepted string formats:

| Format | Example |
|--------|---------|
| `YYYY-MM-DD HH:MM:SS` | `2024-06-01 12:00:00` |
| `YYYY-MM-DD HH:MM:SS.f` (1–6 fractional digits) | `2024-06-01 12:00:00.123456` |
| Either format with trailing ` BC` | `0001-01-01 00:00:00 BC` |

Use `NOW()` for the current UTC timestamp. Use `DATE_TRUNC`, `EXTRACT`, and `DATE_PART` functions to manipulate timestamps. See [Date & Time Functions](functions/datetime.md).

### JSON

```sql
CREATE TABLE events (
    id      INT8 PRIMARY KEY AUTOINCREMENT,
    payload JSON
);

INSERT INTO events (payload) VALUES ('{"user":"alice","uid":42}');
INSERT INTO events (payload) VALUES ('["go","sql","json"]');
```

- Validated as legal JSON on every insert and update (validation is skipped when binding an `io.Reader` — the caller must supply valid JSON).
- Stored as compact UTF-8 text (whitespace not preserved).
- Maximum value size is **64 MiB** per row.
- Accepts an `io.Reader` as a bind parameter to stream large JSON values without loading the full content into memory (see [Streaming large TEXT/JSON values](#streaming-large-textjson-values)).
- Use `->` and `->>` path operators and JSON functions for access. See [JSON](json.md).

### UUID

```sql
CREATE TABLE users (
    id      UUID     NOT NULL DEFAULT GEN_RANDOM_UUID(),
    email   VARCHAR(255) NOT NULL UNIQUE,
    name    TEXT     NOT NULL
);

-- id is generated automatically
INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');

-- or supply an explicit value
INSERT INTO sessions (id, user_id)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 1);

SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);
```

- Stored as 16-byte binary (compact, no string overhead).
- Accepted and returned as lowercase hyphenated string.
- Use `DEFAULT GEN_RANDOM_UUID()` to auto-generate a random UUID v4 on INSERT. See [UUID Functions](functions/uuid.md).

### VECTOR(n)

```sql
CREATE TABLE documents (
    id        INT8      PRIMARY KEY AUTOINCREMENT,
    body      TEXT      NOT NULL,
    embedding VECTOR(3) NOT NULL
);

INSERT INTO documents (body, embedding)
VALUES ('hello world', '[0.1, 0.2, 0.3]');
```

- Dimension count *n* is fixed at table creation time.
- All vector data lives on overflow pages; the inline cell stores only the dimension count and the first overflow page index.
- Values are passed as bracket-delimited strings `'[f1, f2, …, fn]'` or `[]float32` bind parameters.
- Used with `VEC_L2` and `VEC_COSINE` distance functions for similarity search. See [Vector Search](vector-search.md).

---

## Streaming large TEXT/JSON values

`TEXT` and `JSON` columns accept an `io.Reader` as a bind parameter. MiniSQL reads the stream in page-sized chunks and writes each chunk directly to overflow pages, keeping peak memory usage at roughly one page (~4 KB) rather than the full value size.

```go
f, err := os.Open("large-document.txt")
if err != nil {
    log.Fatal(err)
}
defer f.Close()

_, err = db.Exec(
    `INSERT INTO articles (title, body) VALUES (?, ?)`,
    "My Article",
    f, // io.Reader — streamed directly to overflow pages
)
```

Works the same for `JSON` columns:

```go
f, err := os.Open("large-payload.json")
if err != nil {
    log.Fatal(err)
}
defer f.Close()

_, err = db.Exec(
    `INSERT INTO events (payload) VALUES (?)`,
    f,
)
```

**Constraints and caveats:**

- Maximum value size is **64 MiB**. Exceeding this returns an error and the transaction is rolled back.
- `io.Reader` is **not** supported for `VARCHAR` columns — the engine cannot validate the declared length limit without consuming the full stream. Use `TEXT` instead.
- When binding an `io.Reader` to a `JSON` column, MiniSQL skips JSON structure validation. The caller is responsible for supplying valid JSON.
- If the reader yields ≤ 512 bytes, MiniSQL falls back to inline storage with no overflow pages allocated.
- The reader is consumed exactly once and is not rewound on retry. Wrap with a resettable source if your transaction may be retried on conflict.
