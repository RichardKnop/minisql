// Package minisql is an embedded, single-file SQL database written in pure Go.
//
// # Quick start
//
// MiniSQL registers itself as a [database/sql] driver under the name "minisql".
// Import the package for its side effects and open the database with
// [database/sql.Open]:
//
//	import (
//	    "database/sql"
//	    _ "github.com/RichardKnop/minisql"
//	)
//
//	db, err := sql.Open("minisql", "./app.db")
//	if err != nil { log.Fatal(err) }
//
//	// MiniSQL requires exactly one connection per file.
//	db.SetMaxOpenConns(1)
//	db.SetMaxIdleConns(1)
//	defer db.Close()
//
// # Connection string
//
// The data source name is a file path with optional query parameters:
//
//	./app.db
//	./app.db?wal_checkpoint_threshold=500
//	./app.db?log_level=debug&max_cached_pages=4000
//	./app.db?encryption_key=<hex-encoded-32-byte-key>
//
// See [ParseConnectionString] for the full parameter reference.
//
// # Storage model
//
// The database is a sequence of 4 096-byte pages stored in a single file.
// Every page ends with a CRC32-IEEE checksum; a mismatch returns an error
// immediately. Tables and indexes are each stored as independent B+ trees.
// Text and JSON values longer than 512 bytes spill onto overflow pages chained
// by next-page pointers. All vector data lives on overflow pages.
//
// # Write-Ahead Log (WAL)
//
// All commits write modified pages to a WAL file ({dbpath}-wal) before the
// main database file is touched. The WAL is replayed automatically on startup
// after a crash. An automatic checkpoint copies WAL pages back to the main file
// after wal_checkpoint_threshold frames (default 1 000).
//
// # Concurrency
//
// Write transactions use Optimistic Concurrency Control (OCC): page versions
// are recorded at read time and verified at commit. A conflict returns
// [github.com/RichardKnop/minisql/pkg/errors.ErrTxConflict]; callers should
// retry.
//
// Read-only SELECT statements run under MVCC snapshot isolation: each reader
// sees a consistent point-in-time view regardless of concurrent writers.
// Readers never block writers; writers never block readers.
//
// # Supported SQL
//
// SELECT (with WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET,
// DISTINCT, UNION, CTEs, window functions, subqueries), INSERT, UPDATE,
// DELETE, CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX, DROP INDEX,
// EXPLAIN, EXPLAIN ANALYZE, ANALYZE, VACUUM, PRAGMA.
//
// Data types: BOOLEAN, INT4, INT8, REAL, DOUBLE, VARCHAR(n), TEXT, TIMESTAMP,
// JSON, UUID, VECTOR(n).
//
// # Index types
//
// B-tree (primary key, unique, secondary, composite, partial, expression),
// full-text BM25 inverted index, JSON inverted index, HNSW approximate
// nearest-neighbour vector index.
//
// # Encryption
//
// Pass an encryption_key in the connection string (or [ConnectionConfig]) to
// enable transparent AES-256-CTR page encryption with HKDF key derivation.
// Key rotation is available via PRAGMA rekey or the Go API on the internal
// Database type.
//
// # Utilities
//
// [Backup] — online backup while the database remains live.
// [ReadMetrics] — point-in-time engine statistics snapshot.
package minisql
