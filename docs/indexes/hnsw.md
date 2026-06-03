# HNSW Vector Index

HNSW (Hierarchical Navigable Small World) is a graph-based algorithm for approximate nearest-neighbour (ANN) search on high-dimensional vectors. It enables sub-linear similarity search over `VECTOR(n)` columns.

---

## Creating an HNSW index

```sql
CREATE HNSW INDEX idx_embedding ON documents (embedding)
    WITH (m = 16, ef_construction = 200);
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `m` | 16 | Maximum bidirectional links per node. Higher values improve recall but increase build time and memory. |
| `ef_construction` | 200 | Beam width during graph construction. Higher values improve graph quality but slow down builds. |

The index can only be created on a `VECTOR(n)` column.

---

## Querying with vector distance

### L2 (Euclidean) distance

```sql
SELECT id, body, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist
LIMIT 5;
```

### Cosine distance

```sql
SELECT id, body, VEC_COSINE(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist
LIMIT 5;
```

When the query planner detects `ORDER BY VEC_L2(col, ?) LIMIT n` or `ORDER BY VEC_COSINE(col, ?) LIMIT n` and an HNSW index exists on `col`, it performs an ANN search through the HNSW graph instead of scanning all rows:

```sql
EXPLAIN
SELECT id, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist LIMIT 5;
-- op: HNSWSearch  index: idx_embedding
```

---

## Full example

```sql
-- Create table
CREATE TABLE documents (
    id        INT8       PRIMARY KEY AUTOINCREMENT,
    body      TEXT       NOT NULL,
    embedding VECTOR(3)  NOT NULL
);

-- Create HNSW index
CREATE HNSW INDEX idx_docs_embedding ON documents (embedding)
    WITH (m = 16, ef_construction = 200);

-- Insert rows
INSERT INTO documents (body, embedding) VALUES
    ('hello world',     '[0.1, 0.2, 0.3]'),
    ('foo bar baz',     '[0.4, 0.5, 0.6]'),
    ('database engine', '[0.7, 0.8, 0.9]');

-- Find 2 nearest neighbours by L2 distance
SELECT id, body, VEC_L2(embedding, '[0.15, 0.25, 0.35]') AS dist
FROM documents
ORDER BY dist
LIMIT 2;
```

---

## Inserting vectors

Vectors are passed as bracket-delimited strings or `[]float32` bind parameters:

```go
// String literal
_, err = db.Exec(
    `INSERT INTO documents (body, embedding) VALUES (?, ?)`,
    "hello world", "[0.1, 0.2, 0.3]",
)

// []float32 bind parameter
vec := []float32{0.1, 0.2, 0.3}
_, err = db.Exec(
    `INSERT INTO documents (body, embedding) VALUES (?, ?)`,
    "hello world", vec,
)
```

---

## Online DML maintenance

The HNSW index is kept up to date automatically:

- **INSERT** — new vectors are added as nodes in the graph.
- **DELETE** — deleted nodes are marked as tombstoned; they are excluded from search results and lazily reclaimed during compaction.
- **UPDATE** — treated as delete + insert.

---

## Dropping an HNSW index

```sql
DROP INDEX idx_docs_embedding;
```

---

## Notes

- The vector dimension `n` is fixed at table creation time. All inserted vectors must have exactly `n` components.
- HNSW search is *approximate* — it may miss some true nearest neighbours in exchange for speed. Increasing `ef_construction` and `m` improves recall at the cost of build time and memory.
- `PRAGMA integrity_check` verifies HNSW page structure and overflow page chains.
- The HNSW graph is persisted across restarts and carried through `VACUUM` and encryption.

See [Vector Search](../vector-search.md) for the complete vector type and distance function reference.
