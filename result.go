package minisql

// Result ...
type Result struct {
	rowsAffected int64
	lastInsertID int64
}

// LastInsertId returns the auto-generated primary key of the last inserted row.
// For tables with a single-column int8 primary key (autoincrement or explicit),
// this is the PK value of the last row written by the INSERT statement.
// Returns 0 for composite primary keys, non-integer keys, or when no row was inserted.
func (r Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
