package minisql

import (
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

// DatabaseOption is a functional option for configuring a Database.
type DatabaseOption func(*Database)

const defaultMaxCachedStatements = 1000

// WithMaxCachedStatements configures the maximum number of prepared statements to keep in the LRU cache.
func WithMaxCachedStatements(maxStatements int) DatabaseOption {
	return func(d *Database) {
		if maxStatements > 0 {
			d.stmtCache = lrucache.New[string](maxStatements)
		}
	}
}

// WithParallelScanEnabled turns on concurrent leaf-page scanning for all user tables.
func WithParallelScanEnabled() DatabaseOption {
	return func(d *Database) {
		d.parallelScan = true
	}
}
