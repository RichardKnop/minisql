package minisql

type Result struct {
	rowsAffected int64
}

func (r Result) LastInsertId() (int64, error) {
	// TODO - implement last insert ID tracking
	return 0, nil
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
