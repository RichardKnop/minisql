# Vector Search

MiniSQL supports high-dimensional vector storage and approximate nearest-neighbour (ANN) search via the `VECTOR(n)` column type and the HNSW index.

---

## The VECTOR type

```sql
CREATE TABLE documents (
    id        INT8       PRIMARY KEY AUTOINCREMENT,
    body      TEXT       NOT NULL,
    embedding VECTOR(3)  NOT NULL
);
```

- `n` is the vector dimension — fixed at column creation time.
- All inserted vectors must have exactly `n` components.
- Components are `float32` (IEEE 754 single-precision).
- Data is stored on overflow pages; the inline cell holds only the dimension and first overflow page pointer.

---

## Inserting vectors

As a bracket-delimited string:

```sql
INSERT INTO documents (body, embedding)
VALUES ('hello world', '[0.1, 0.2, 0.3]');
```

In Go — string or `[]float32`:

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

## Distance functions

### VEC_L2(a, b) — Euclidean (L2) distance

Returns the L2 distance between two vectors. Smaller = more similar.

```sql
SELECT id, body, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist
LIMIT 5;
```

### VEC_COSINE(a, b) — Cosine distance

Returns the cosine distance (1 − cosine similarity). Smaller = more similar. Best for comparing direction, independent of magnitude.

```sql
SELECT id, body, VEC_COSINE(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist
LIMIT 5;
```

Both functions accept vectors as column references, string literals `'[…]'`, or `[]float32` bind parameters.

---

## HNSW index for fast ANN search

Without an index, every distance query scans all rows. For large tables, create an HNSW (Hierarchical Navigable Small World) index:

```sql
CREATE HNSW INDEX idx_docs_embedding ON documents (embedding)
    WITH (m = 16, ef_construction = 200);
```

The query planner automatically uses the HNSW index for `ORDER BY VEC_L2(col, ?) LIMIT n` and `ORDER BY VEC_COSINE(col, ?) LIMIT n` patterns:

```sql
-- Uses HNSW ANN search
SELECT id, body, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist
LIMIT 10;
```

Verify with `EXPLAIN`:

```sql
EXPLAIN
SELECT id, VEC_L2(embedding, '[0.1, 0.2, 0.3]') AS dist
FROM documents
ORDER BY dist LIMIT 10;
-- op: HNSWSearch  index: idx_docs_embedding
```

See [HNSW Index](indexes/hnsw.md) for parameter reference and full details.

---

## Full example

```go
package main

import (
    "database/sql"
    "fmt"
    _ "github.com/RichardKnop/minisql"
)

func main() {
    db, _ := sql.Open("minisql", "/tmp/vectors.db")
    defer db.Close()
    db.SetMaxOpenConns(1)

    db.Exec(`CREATE TABLE IF NOT EXISTS documents (
        id        INT8      PRIMARY KEY AUTOINCREMENT,
        body      TEXT      NOT NULL,
        embedding VECTOR(3) NOT NULL
    )`)

    db.Exec(`CREATE HNSW INDEX IF NOT EXISTS idx_embedding
        ON documents (embedding)
        WITH (m = 16, ef_construction = 200)`)

    vecs := []struct {
        body string
        vec  []float32
    }{
        {"hello world",     {0.1, 0.2, 0.3}},
        {"foo bar",         {0.4, 0.5, 0.6}},
        {"database engine", {0.7, 0.8, 0.9}},
    }
    for _, v := range vecs {
        db.Exec(`INSERT INTO documents (body, embedding) VALUES (?, ?)`, v.body, v.vec)
    }

    query := []float32{0.15, 0.25, 0.35}
    rows, _ := db.Query(
        `SELECT id, body, VEC_L2(embedding, ?) AS dist
         FROM documents
         ORDER BY dist
         LIMIT 3`,
        query,
    )
    defer rows.Close()
    for rows.Next() {
        var id int64
        var body string
        var dist float64
        rows.Scan(&id, &body, &dist)
        fmt.Printf("id=%d body=%q dist=%.4f\n", id, body, dist)
    }
}
```

---

## Notes

- `VECTOR(n)` columns cannot be primary keys.
- Dimension count is validated on every insert — mismatched dimensions return an error.
- `VEC_L2` and `VEC_COSINE` return `NULL` if either argument is `NULL`.
- The HNSW index is *approximate* — for exact results (all rows), use the distance functions without an index (smaller tables) or increase `ef_construction`.
