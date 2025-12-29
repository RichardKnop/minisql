package minisql

type DatabaseOption func(*Database)

func WithJournal(enabled bool) DatabaseOption {
	return func(d *Database) {
		d.txManager.journalEnabled = enabled
	}
}

func WithMaxCachedStatements(maxStatements int) DatabaseOption {
	return func(d *Database) {
		if maxStatements > 0 {
			d.stmtCache = newStatementCache(maxStatements)
		}
	}
}
