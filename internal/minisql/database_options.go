package minisql

import (
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

// DatabaseOption is a functional option for configuring a Database.
type DatabaseOption func(*Database)

const (
	defaultMaxCachedStatements = 1000
	defaultMaxCachedPlans      = 1000
	defaultSortMemLimit        = 4 * 1024 * 1024 // 4 MiB
	// defaultHNSWVecCacheSize is the default number of vector entries kept in the
	// per-index LRU cache.  Each entry is one VectorPointer (8 bytes overhead +
	// dims×4 bytes of float32 data).  4096 entries at 128 dims ≈ 2 MiB; at 768
	// dims ≈ 12 MiB.  Raise via WithHNSWVecCacheSize or hnsw_vec_cache_size=N.
	defaultHNSWVecCacheSize = 4096

	// DefaultSortMemLimit is the exported default for use by the root package's
	// connection string parser.
	DefaultSortMemLimit = defaultSortMemLimit
	// DefaultHNSWVecCacheSize is the exported default for use by the root package's
	// connection string parser.
	DefaultHNSWVecCacheSize = defaultHNSWVecCacheSize
)

// WithMaxCachedStatements configures the maximum number of prepared statements to keep in the LRU cache.
func WithMaxCachedStatements(maxStatements int) DatabaseOption {
	return func(d *Database) {
		if maxStatements > 0 {
			d.stmtCache = lrucache.New[string](maxStatements)
		}
	}
}

// WithMaxCachedPlans configures the maximum number of query plans to keep in the plan cache.
// Plans are keyed by the original SQL text (set at PrepareStatement time) and are invalidated
// automatically on CREATE INDEX, DROP INDEX, DROP TABLE, and ANALYZE.
func WithMaxCachedPlans(maxPlans int) DatabaseOption {
	return func(d *Database) {
		if maxPlans > 0 {
			d.planCache = lrucache.New[string](maxPlans)
		}
	}
}

// WithParallelScanEnabled turns on concurrent leaf-page scanning for all user tables.
func WithParallelScanEnabled() DatabaseOption {
	return func(d *Database) {
		d.parallelScan = true
	}
}

// WithSortMemLimit sets the maximum bytes of row data accumulated in memory before
// spilling to a temp file during an ORDER BY sort. 0 disables external sort.
// The default is 4 MiB.
func WithSortMemLimit(n int64) DatabaseOption {
	return func(d *Database) {
		d.sortMemLimit = n
	}
}

// WithHNSWVecCacheSize sets the maximum number of vector entries cached per HNSW
// index. Each entry holds the full float32 slice for one row. Larger values
// reduce overflow-page I/O during ANN search at the cost of more RAM. The
// default is 4096. Setting n ≤ 0 is a no-op (default is preserved).
func WithHNSWVecCacheSize(n int) DatabaseOption {
	return func(d *Database) {
		if n > 0 {
			d.hnswVecCacheSize = n
		}
	}
}

// WithEncryptionKey enables transparent AES-256-CTR page encryption using key.
// The key is never written to disk; a random per-database salt is stored in the
// plaintext database header and used to derive the actual AES key via HKDF.
//
// Rules:
//   - New databases: a fresh salt is generated and encryption is set up.
//   - Existing encrypted databases: the stored salt is read and combined with key.
//   - Mismatched state (key provided for unencrypted DB or vice-versa): error.
//
// The key must not be empty.  Any length is accepted; longer keys provide more
// entropy but the derived key is always 256 bits regardless.
func WithEncryptionKey(key []byte) DatabaseOption {
	return func(d *Database) {
		if len(key) > 0 {
			d.encryptionKey = key
		}
	}
}
