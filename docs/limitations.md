# Limitations

MiniSQL is a research and learning project, not yet production-ready. This page lists known limitations and workarounds.

---

## SQL features not yet implemented

| Feature | Notes |
|---------|-------|
| Recursive CTEs (`WITH RECURSIVE`) | Non-recursive CTEs are fully supported |
| Savepoints (`SAVEPOINT`, `RELEASE`, `ROLLBACK TO`) | Full transaction rollback is supported |
| `CREATE TABLE AS SELECT` | Use a regular `CREATE TABLE` followed by `INSERT INTO … SELECT` |
| `MERGE` / `UPSERT` by conflict target | `ON CONFLICT DO NOTHING / DO UPDATE` is supported but without explicit conflict-column targeting |
| `CROSS JOIN` | Not supported |
| Lateral joins | Not supported |

---

## Parser limitations

| Limitation | Workaround |
|-----------|------------|
| Negative integer literals rejected | Use `?` bind parameter with a negative `int64` value: `db.Exec("… WHERE n > ?", int64(-1))` |
| `FROM table alias` (bare alias) | Use `FROM table AS alias` |
| `HAVING` does not accept `?` placeholders | Use literal values in HAVING conditions |

---

## Type system limitations

| Limitation | Notes |
|-----------|-------|
| Maximum 64 columns per table | Enforced by the 64-bit NULL bitmask per row |
| No `INTERVAL` column type | `INTERVAL` literals are supported in arithmetic expressions only |
| No `DECIMAL` / `NUMERIC` types | Use `INT8` for fixed-precision integers or `DOUBLE` for approximation |
| `TEXT` columns cannot be primary keys or unique-index keys | Use `VARCHAR(n)` for indexed string columns |
| No slice expansion in `IN` | `IN (?, ?)` with individual bind args works; passing a `[]T` slice as a single `?` does not. List values as separate `?` placeholders or use a subquery |

---

## Concurrency limitations

| Limitation | Notes |
|-----------|-------|
| Single connection recommended | MiniSQL's OCC write lock and shared pager are not safe with multiple concurrent connections from different `sql.DB` pools. Set `MaxOpenConns(1)`. |
| No WAL group commit | Each write transaction flushes to the WAL individually |
| Checkpoint blocked by active readers | `PRAGMA wal_checkpoint` waits until all snapshot readers finish |

---

## Storage limitations

| Limitation | Notes |
|-----------|-------|
| No connection pooling | `db.SetMaxOpenConns(1)` is required |
| No online backup API | Use `VACUUM` to compact; copy the file while no writes are active |
| No key rotation for encryption | Re-encrypt by opening with old key, then VACUUM (planned feature) |
| `VACUUM` requires exclusive access | Blocks all other connections for its duration |

---

## Not yet implemented

- Metrics / observability API (`db.Stats()` equivalent beyond the standard `database/sql` pool stats)
- `CREATE VIEW`
- `CREATE TRIGGER`
- `ALTER TABLE … MODIFY COLUMN` (type changes)
- `ALTER TABLE … ADD CONSTRAINT` after table creation

---

## Reporting issues

If you encounter a bug or unexpected behaviour, please open an issue at [github.com/RichardKnop/minisql](https://github.com/RichardKnop/minisql/issues).
