package minisql

type Result struct {
	rowsAffected int64
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r Result) LastInsertId() (int64, error) {
	// TODO - implement last insert ID tracking
	return 0, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
