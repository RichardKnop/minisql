# Date & Time Functions

MiniSQL stores timestamps as microseconds since the PostgreSQL epoch (2000-01-01). All functions operate on the `TIMESTAMP` type.

---

## NOW()

Returns the current UTC timestamp.

```sql
SELECT NOW();

INSERT INTO events (name, created) VALUES ('login', NOW());

CREATE TABLE users (
    id      INT8      PRIMARY KEY AUTOINCREMENT,
    created TIMESTAMP DEFAULT NOW()
);
```

---

## DATE_TRUNC(unit, timestamp)

Truncates a timestamp to the specified precision.

| Unit | Truncates to |
|------|-------------|
| `'year'` | First day of the year, midnight |
| `'month'` | First day of the month, midnight |
| `'day'` | Midnight of the day |
| `'hour'` | Start of the hour |
| `'minute'` | Start of the minute |
| `'second'` | Start of the second |

```sql
SELECT DATE_TRUNC('year',   created) FROM events;  -- 2024-01-01 00:00:00
SELECT DATE_TRUNC('month',  created) FROM events;  -- 2024-06-01 00:00:00
SELECT DATE_TRUNC('day',    created) FROM events;  -- 2024-06-15 00:00:00
SELECT DATE_TRUNC('hour',   created) FROM events;  -- 2024-06-15 14:00:00
SELECT DATE_TRUNC('minute', created) FROM events;  -- 2024-06-15 14:30:00
SELECT DATE_TRUNC('second', created) FROM events;  -- 2024-06-15 14:30:45
```

Group events by day:

```sql
SELECT DATE_TRUNC('day', created) AS day, COUNT(*) AS cnt
FROM events
GROUP BY DATE_TRUNC('day', created)
ORDER BY day;
```

---

## EXTRACT(field, timestamp) / DATE_PART(field, timestamp)

Extracts a single field from a timestamp as `INT8` (or `INT8` epoch seconds for `epoch`).

| Field | Returns |
|-------|---------|
| `'year'` | Year (e.g., 2024) |
| `'month'` | Month (1–12) |
| `'day'` | Day of month (1–31) |
| `'hour'` | Hour (0–23) |
| `'minute'` | Minute (0–59) |
| `'second'` | Second (0–59) |
| `'microsecond'` | Microsecond part (0–999999) |
| `'epoch'` | Seconds since Unix epoch (1970-01-01) |

```sql
SELECT EXTRACT('year',   created) FROM events;
SELECT EXTRACT('month',  created) FROM events;
SELECT EXTRACT('day',    created) FROM events;
SELECT EXTRACT('hour',   created) FROM events;
SELECT EXTRACT('epoch',  created) FROM events;

-- DATE_PART is an alias for EXTRACT
SELECT DATE_PART('year', created) FROM events;
```

Filter by month:

```sql
SELECT * FROM events WHERE EXTRACT('month', created) = 6;
```

---

## TO_TIMESTAMP(str)

Parses a timestamp string. Accepts the same formats as literal timestamp values.

```sql
SELECT TO_TIMESTAMP('2024-06-01 12:00:00');
SELECT TO_TIMESTAMP('2024-06-01 12:00:00.123456');
```

---

## Timestamp literals

Timestamp values can be written as strings in `INSERT` / `WHERE` clauses:

```sql
INSERT INTO events (created) VALUES ('2024-06-01 12:00:00');
SELECT * FROM events WHERE created > '2024-01-01 00:00:00';
SELECT * FROM events WHERE created BETWEEN '2024-01-01 00:00:00' AND '2024-12-31 23:59:59';
```

Accepted formats:

| Format | Example |
|--------|---------|
| `YYYY-MM-DD HH:MM:SS` | `2024-06-01 12:00:00` |
| `YYYY-MM-DD HH:MM:SS.f` (1–6 fractional digits) | `2024-06-01 12:00:00.123456` |
| Either format with ` BC` suffix | `0001-01-01 00:00:00 BC` |

---

## INTERVAL arithmetic

Add or subtract intervals from timestamps:

```sql
SELECT NOW() + INTERVAL '1 day';
SELECT NOW() - INTERVAL '30 days';
SELECT created + INTERVAL '1 hour' FROM events;
```

Supported interval units: `second`, `minute`, `hour`, `day`, `month`, `year`.

---

## Expression index on timestamp

```sql
-- Index by day for fast GROUP BY / WHERE on truncated dates
CREATE INDEX idx_events_day ON events (DATE_TRUNC('day', created));

SELECT DATE_TRUNC('day', created) AS day, COUNT(*) AS cnt
FROM events
GROUP BY DATE_TRUNC('day', created);
-- Uses idx_events_day
```
