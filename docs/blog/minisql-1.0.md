# Building MiniSQL: A Pure-Go Embedded SQL Database

*June 2026*

When you reach for an embedded database in Go, the answer is usually SQLite — either the CGO-wrapped `mattn/go-sqlite3` or the C-to-Go transpiled `modernc.org/sqlite`. Both are excellent, but they share the same origin: decades-old C code bridged into Go.

MiniSQL started from a simple question: what does an embedded SQL database look like if you build it in Go from scratch — the B+ tree, the WAL, the query planner, the parser — all idiomatic Go, all zero CGO? After roughly a year of evenings and weekends, the answer is [v1.0.0](https://github.com/RichardKnop/minisql). This post covers the architecture decisions, what worked, what didn't, and how it compares to SQLite.

---

## Why Build Another Database?

**Honest answer:** it was interesting. Building a database teaches you things that reading about databases doesn't.

**Practical answer:** a pure-Go embedded database without CGO simplifies cross-compilation, removes the C toolchain dependency, and opens the door to building in features that SQLite treats as optional extensions — native full-text search, first-class `JSON`, a `UUID` type, a `TIMESTAMP` type that behaves like PostgreSQL's, vector similarity search, and transparent page encryption — all without shipping extra `.so` files or loading dynamic extensions.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│            Go application                   │
│          database/sql interface             │
└────────────────────┬────────────────────────┘
                     │
┌────────────────────▼────────────────────────┐
│              MiniSQL driver                 │
│                                             │
│   Parser ──► Planner ──► Executor           │
│                              │              │
│                 Transaction Manager         │
│              OCC writes · MVCC reads        │
└──────────┬───────────────────────┬──────────┘
           │                       │
┌──────────▼──────────┐ ┌──────────▼──────────┐
│   LRU page cache    │ │  Write-ahead log    │
│  2 000 pages / 8 MB │ │    {db}-wal         │
└──────────┬──────────┘ └──────────┬──────────┘
           │                       │
┌──────────▼───────────────────────▼──────────┐
│               Database file                 │
│     B+ trees · 4 KiB pages · CRC32          │
│     Optional AES-256-CTR encryption         │
└─────────────────────────────────────────────┘
```

### Storage: pages and overflow

The database file is divided into fixed-size **4 096-byte pages**, each ending with a 4-byte CRC32-IEEE checksum verified on every read. A bad checksum is an immediate error — corrupted pages are never silently accepted.

Large values — `TEXT` longer than 512 bytes, `JSON`, `VECTOR` — spill onto **overflow pages** chained via next-page pointers. This mirrors SQLite's overflow design rather than PostgreSQL's TOAST: simpler to implement, no separate storage namespace, and the common case of small rows stays fast. The trade-off: reading a single overflowed column still traverses the full chain.

### B+ trees everywhere

Every table and every index is an independent **B+ tree**:

- **Leaf nodes** hold row data (for tables) or index entries (for indexes).
- **Internal nodes** hold routing keys and child page pointers.
- Leaf nodes at the same level are **doubly-linked**, so range scans walk the list without re-descending the tree on each step.

The schema catalog (`minisql_schema`) is itself a B+ tree, so the metadata is stored with the same engine as user data.

### Write-Ahead Log

All commits append modified pages to a WAL file (`{db}-wal`) before touching the main file. The main file is updated only during a **checkpoint**, which fires automatically after 1 000 WAL frames (configurable via `wal_checkpoint_threshold`).

On startup, MiniSQL replays committed-but-uncheckpointed frames — normal WAL behaviour, not crash recovery. Partially written frames are discarded.

### Concurrency: OCC writes, MVCC reads

Two different models coexist, one for each access pattern:

**Writes — Optimistic Concurrency Control.** One writer runs at a time. The engine tracks a read-set of page versions within the transaction. At commit, it checks for conflicts: if another transaction modified any page in the read-set since it was first read, `ErrTxConflict` is returned and the caller can retry. No locks are held during query execution.

**Reads — MVCC Snapshot Isolation.** Each read-only transaction captures the current commit sequence number at `BEGIN`. Subsequent page reads that would see a newer version are served from a per-page version history. The reader sees a fully consistent snapshot regardless of concurrent writers. Writers never block readers; readers never block writers.

---

## SQL Parser

The parser is a **hand-written state machine** — no parser generator, no ANTLR, no yacc. Each SQL keyword is dispatched through a step machine that advances token-by-token and builds a `Statement` struct.

The trade-off vs a generated parser is development speed: new syntax requires writing state transitions by hand. The payoff is complete control over error messages, zero runtime generation dependencies, and straightforward behaviour under partial input — which is what makes the interactive CLI's multi-line continuation prompt work correctly.

---

## Query Planner

The planner is **cost-based**, driven by statistics populated by `ANALYZE`:

- Row counts, column cardinality, min/max histograms, most-common values (MCV).
- For each predicate, the planner scores all candidate indexes (B-tree, full-text, inverted, HNSW) and picks the cheapest access path.
- Predicate pushdown: filters are applied as close to the scan as possible.
- Covering index detection: if an index contains all projected columns, the main table is never touched.
- Join reordering for star-schema queries; semi-join and anti-semi-join rewrites for `IN`/`NOT IN` subqueries.

Use `EXPLAIN` and `EXPLAIN ANALYZE` to inspect the chosen plan.

---

## Index Types

| Index | Backed by | Use case |
|---|---|---|
| B-tree | Separate B+ tree pages | Primary key, unique constraint, secondary index |
| Inverted | Posting lists per token | Full-text search — `MATCH`, `TS_RANK` (BM25) |
| JSON inverted | Posting lists per path/value | `JSON_CONTAINS` queries |
| HNSW | Hierarchical navigable small world graph | Vector similarity — `VEC_L2`, `VEC_COSINE` |

The full-text and JSON inverted indexes are **log-structured**: inserts append new posting entries; deletes mark tombstones; background merges consolidate entries. This keeps insert latency low at the cost of slightly higher merge work at query time.

The HNSW vector index supports both L2 distance and cosine similarity and is integrated into the query planner, so `ORDER BY VEC_L2(embedding, ?) LIMIT 10` automatically uses the ANN index rather than a full sequential scan.

---

## Built-in Features vs SQLite

| Feature | MiniSQL | SQLite |
|---|---|---|
| Pure Go, zero CGO | ✅ | ❌ (CGO or transpiled C) |
| `TIMESTAMP` column type | ✅ PostgreSQL-compatible | ❌ (TEXT / INTEGER / REAL) |
| `UUID` column type | ✅ | ❌ |
| `JSON` column type + operators | ✅ | Partial (json1 extension) |
| Native full-text search | ✅ inverted index + BM25 | ✅ FTS5 extension |
| JSON inverted index | ✅ | ❌ |
| Vector similarity search | ✅ HNSW ANN | ❌ |
| Transparent page encryption | ✅ AES-256-CTR + HKDF | ❌ (paid SEE extension) |
| MVCC snapshot reads | ✅ | ✅ (WAL mode) |
| Parallel full-table scan | ✅ | ❌ |

---

## Benchmarks

All numbers are from an Apple M1 Max running darwin/arm64 and Go 1.26.3. The SQLite comparison is against [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) — a **pure-Go** port, not the CGO driver — so the comparison is on an even footing: no CGO overhead on either side. Times are median of 3 runs. Ratio below 1.0× means MiniSQL is faster.

### Single-row DML

| Operation | MiniSQL | SQLite | Ratio |
|---|---|---|---|
| Insert | 13.2 µs | 103 µs | **0.13×** |
| Update by PK | 8.08 µs | 39.4 µs | **0.20×** |
| Delete by PK | 14.6 µs | 58.1 µs | **0.25×** |
| ON CONFLICT DO UPDATE | 7.69 µs | 56.0 µs | **0.14×** |
| Foreign key insert | 11.0 µs | 46.4 µs | **0.24×** |

Single-row writes are consistently **4–8× faster**. MiniSQL's OCC model takes no locks during execution and the per-transaction commit path is lightweight for single-page mutations. modernc/sqlite carries more per-statement bookkeeping overhead from its SQLite heritage.

### Analytical queries

| Operation | MiniSQL | SQLite | Ratio |
|---|---|---|---|
| Full table scan (10k rows) | 3.75 ms | 5.30 ms | **0.71×** |
| GROUP BY aggregate | 989 µs | 2.46 ms | **0.40×** |
| HAVING filter | 814 µs | 2.26 ms | **0.36×** |
| DISTINCT high cardinality | 3.37 ms | 6.42 ms | **0.52×** |
| Subquery IN list | 3.04 ms | 3.72 ms | **0.82×** |
| Inner join, low selectivity | 113 µs | 765 µs | **0.15×** |

### Full-text search

| Operation | MiniSQL | SQLite FTS5 | Ratio |
|---|---|---|---|
| Common-term search | 4.36 µs | 64.2 µs | **0.07×** |
| Multi-term AND | 15.3 µs | 35.0 µs | **0.44×** |
| Insert with FTS index | 39.1 µs | 94.1 µs | **0.42×** |
| Update with FTS index | 37.2 µs | 113.4 µs | **0.33×** |
| Delete with FTS index | 92.6 µs | 210.5 µs | **0.44×** |

### Where SQLite wins

| Operation | MiniSQL | SQLite | Ratio |
|---|---|---|---|
| Point scan (PK lookup) | 4.33 µs | 3.44 µs | 1.3× |
| Insert batch (100 rows) | 360 µs | 241 µs | 1.49× |
| Insert prepared batch | 358 µs | 249 µs | 1.44× |
| WAL checkpoint | 204 µs | 102 µs | 2.0× |
| VACUUM | 1.66 ms | 307 µs | 5.4× |

Point scans and batch inserts favour SQLite because its page-level data layout and tight column scanning loop have lower per-row overhead than MiniSQL's row materialisation path. The point-scan ratio improved from 1.5× to 1.3× after Tier-1/Tier-2 allocation optimisations. VACUUM is the largest gap: MiniSQL rebuilds the compacted database through normal write paths (page-by-page via the B+ tree write API) whereas SQLite can do a more direct block-level copy. This is a known optimisation target.

---

## Honest Trade-offs

**Use MiniSQL if:**

- You want zero-CGO, single binary cross-compilation with no C toolchain.
- You need native full-text search, JSON querying, or vector similarity without bolting on extensions.
- Single-row write latency matters — 4–5× faster than SQLite in our benchmarks.
- You want `TIMESTAMP` stored as microseconds (not the SQLite triple-type ambiguity), or a real `UUID` column type.
- Transparent encryption at rest is a requirement without paying for a commercial extension.

**Use SQLite if:**

- You need 20+ years of production hardening and a mature ecosystem.
- Your workload is read-heavy with frequent point lookups.
- You run VACUUM frequently and care about its latency.
- SQLite file format compatibility or third-party tool support is required.

---

## What's Next

MiniSQL v1.0 is usable, but several things need work before it deserves to be called production-grade:

- **VACUUM performance** — direct block-level copy rather than the current write-path rebuild.
- **Batch insert** — reduce per-row allocation overhead; bulk-load path for large inserts.
- **Memory** — HNSW index build peaks at 200+ MiB for 10k rows at 768 dimensions; GROUP BY / HAVING use 10–13× more heap than SQLite's C hash table (structural Go-vs-C gap, but a custom open-address hash table would close most of it).
- **Savepoints** — `SAVEPOINT` / `ROLLBACK TO` not yet implemented.

More profiling and optimisation work is planned; the benchmark suite in `benchmarks/` has a baseline file that makes regressions visible before they ship.

---

## Try It

```bash
# Use as a library
go get github.com/RichardKnop/minisql

# Interactive CLI
go install github.com/RichardKnop/minisql/cmd/minisql@latest
# or
brew install minisql
```

Source and full documentation: 

- [github.com/RichardKnop/minisql](https://github.com/RichardKnop/minisql)
- [richardknop.github.io/minisql](https://richardknop.github.io/minisql/)

Feedback, bug reports, and contributions are welcome — the project is in active development.
