package minisql

import (
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

// DatabaseOption is a functional option for configuring a Database.
type DatabaseOption func(*Database)

// WithJournal enables or disables the rollback journal for crash recovery.
func WithJournal(enabled bool) DatabaseOption {
	return func(d *Database) {
		d.txManager.journalEnabled = enabled
	}
}

const defaultMaxCachedStatements = 1000

// WithMaxCachedStatements configures the maximum number of prepared statements to keep in the LRU cache.
func WithMaxCachedStatements(maxStatements int) DatabaseOption {
	return func(d *Database) {
		if maxStatements > 0 {
			d.stmtCache = lrucache.New[string](maxStatements)
		}
	}
}
