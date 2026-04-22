package minisql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// Rows ...
type Rows struct {
	columns []minisql.Column
	iter    minisql.Iterator
	ctx     context.Context
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	return r.iter.Close()
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) error {
	if !r.iter.Next(r.ctx) {
		if err := r.iter.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	row := r.iter.Row()
	if len(row.Values) != len(dest) {
		return fmt.Errorf("expected %d values, got %d", len(dest), len(row.Values))
	}

	for i := range dest {
		if !row.Values[i].Valid {
			dest[i] = nil
			continue
		}

		switch v := row.Values[i].Value.(type) {
		case minisql.TextPointer:
			dest[i] = v.String()
		case minisql.Time:
			dest[i] = v.GoTime()
		default:
			dest[i] = row.Values[i].Value
		}
	}

	return nil
}
