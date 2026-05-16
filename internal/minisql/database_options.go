package minisql

import (
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

// DatabaseOption is a functional option for configuring a Database.
type DatabaseOption func(*Database)

const (
	defaultMaxCachedStatements = 1000
	defaultMaxCachedPlans      = 1000
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
