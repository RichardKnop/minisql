# Index Overview

MiniSQL supports four index types. The query planner automatically selects the best index for each query.

## Index types

| Type | Best for | SQL |
|------|----------|-----|
| [B-tree](btree.md) | Equality, range, ORDER BY, covering | `CREATE INDEX` |
| [Full-text](fulltext.md) | Token search in TEXT columns | `CREATE FULLTEXT INDEX` |
| [JSON inverted](json-inverted.md) | Containment queries on JSON columns | `CREATE INVERTED INDEX` |
| [HNSW vector](hnsw.md) | Approximate nearest-neighbour on VECTOR columns | `CREATE HNSW INDEX` |

---

## Creating indexes

```sql
-- B-tree secondary index
CREATE INDEX idx_users_email ON users (email);

-- Unique B-tree index
CREATE UNIQUE INDEX idx_users_email ON users (email);

-- Composite B-tree index
CREATE INDEX idx_orders_user_created ON orders (user_id, created_at);

-- Partial index (only indexes matching rows)
CREATE INDEX idx_active_users ON users (email) WHERE active = true;

-- Expression index
CREATE INDEX idx_lower_name ON users (LOWER(name));

-- Full-text index
CREATE FULLTEXT INDEX idx_articles_body ON articles (body)
    WITH (tokenizer = 'simple');

-- JSON inverted index
CREATE INVERTED INDEX idx_events_payload ON events (payload);

-- HNSW vector index
CREATE HNSW INDEX idx_embedding ON documents (embedding)
    WITH (m = 16, ef_construction = 200);
```

---

## Dropping indexes

```sql
DROP INDEX index_name;
```

---

## Index selection by the planner

The planner chooses among all available indexes based on cost estimates:

- **Equality predicate** (`col = ?`) → B-tree index on `col`
- **Range predicate** (`col > ?`, `BETWEEN`) → B-tree range scan
- **Leading column of composite index** → B-tree index scan with trailing filter
- **Covering index** (all needed columns in the index) → index-only scan, no main-table page reads
- **MATCH(col, query)** → fulltext index on `col`
- **JSON_CONTAINS(col, val)** → inverted index on `col`
- **ORDER BY dist LIMIT n** where `dist = VEC_L2(col, ?)` → HNSW ANN search

Use `EXPLAIN` to inspect the chosen plan. Use `ANALYZE table_name` to refresh statistics.

---

## Index maintenance

- Indexes are updated automatically on every `INSERT`, `UPDATE`, and `DELETE`.
- HNSW indexes support online DML — inserts add new nodes; deletes mark nodes as deleted and are lazily reclaimed.
- `DROP INDEX` removes the index immediately; space is reclaimed by `VACUUM`.
- `PRAGMA integrity_check` verifies that every index entry matches the corresponding table row.
