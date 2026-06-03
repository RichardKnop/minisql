# Full-text Search

Full-text search lets you find rows where a text column contains specific words or phrases, using an efficient inverted index instead of a slow `LIKE '%…%'` scan.

---

## Creating a full-text index

```sql
CREATE FULLTEXT INDEX idx_articles_body ON articles (body)
    WITH (tokenizer = 'simple');
```

The only supported tokenizer is `simple`:

- Lowercases the input.
- Splits on whitespace and punctuation.
- Does not apply stemming or stop-word removal.

!!! note
    A full-text index can only be created on `TEXT` or `VARCHAR` columns.

---

## Querying with MATCH

`MATCH(column, query)` returns `true` if the column's text contains all query tokens:

```sql
SELECT id, body
FROM articles
WHERE MATCH(body, 'database storage');
```

The query string is tokenized the same way as the indexed documents. All tokens must be present for a row to match.

---

## Ranking with TS_RANK

`TS_RANK(column, query)` returns a relevance score as `DOUBLE`:

```sql
SELECT id, TS_RANK(body, 'database storage') AS score
FROM articles
WHERE MATCH(body, 'database storage')
ORDER BY score DESC
LIMIT 10;
```

The ranking considers:

- Term frequency — how often each query token appears in the document.
- Proximity — tokens that appear close together score higher.

---

## Index-accelerated search

When the query planner detects a `MATCH(col, query)` predicate and a full-text index exists on `col`, it uses the inverted index to find matching row IDs without scanning the table:

```sql
EXPLAIN SELECT * FROM articles WHERE MATCH(body, 'database storage');
-- op: FullTextIndexScan  index: idx_articles_body
```

Without a full-text index, `MATCH` falls back to a full table scan.

---

## Full example

```sql
-- Create table and index
CREATE TABLE articles (
    id    INT8 PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(255) NOT NULL,
    body  TEXT NOT NULL
);

CREATE FULLTEXT INDEX idx_articles_body ON articles (body)
    WITH (tokenizer = 'simple');

-- Insert rows
INSERT INTO articles (title, body) VALUES
    ('Intro to SQL',      'SQL is a language for managing relational databases'),
    ('Storage Engines',   'Database storage engines manage how data is persisted'),
    ('Query Optimisation','The query planner selects the best index for each query');

-- Search
SELECT id, title, TS_RANK(body, 'database storage') AS score
FROM articles
WHERE MATCH(body, 'database storage')
ORDER BY score DESC;
```

---

## Notes

- `MATCH` and `TS_RANK` can be used without a full-text index — they will scan all rows in that case.
- The full-text index is updated automatically on every `INSERT`, `UPDATE`, and `DELETE`.
- Drop the index with `DROP INDEX idx_articles_body`.
- `PRAGMA integrity_check` verifies the consistency of full-text index entries.
