package minisql

import (
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

type DatabaseOption func(*Database)

func WithJournal(enabled bool) DatabaseOption {
	return func(d *Database) {
		d.txManager.journalEnabled = enabled
	}
}

const defaultMaxCachedStatements = 1000

func WithMaxCachedStatements(maxStatements int) DatabaseOption {
	return func(d *Database) {
		if maxStatements > 0 {
			d.stmtCache = lrucache.New[string](maxStatements)
		}
	}
}
