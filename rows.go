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
func (r Rows) Columns() []string {
	names := make([]string, 0, len(r.columns))
	for _, aColumn := range r.columns {
		names = append(names, aColumn.Name)
	}
	return names
}

// Close closes the rows iterator.
func (r Rows) Close() error {
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
func (r Rows) Next(dest []driver.Value) error {
	if !r.iter.Next(r.ctx) {
		if err := r.iter.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	aRow := r.iter.Row()
	if len(aRow.Values) != len(dest) {
		return fmt.Errorf("expected %d values, got %d", len(dest), len(aRow.Values))
	}

	for i := range dest {
		val := aRow.Values[i]
		if !val.IsValid() {
			dest[i] = nil
			continue
		}
		if val.IsTextLike() {
			dest[i] = string(val.AsTextPointer().Data)
			continue
		}
		switch v := val.AsAny().(type) {
		case minisql.TimestampMicros:
			dest[i] = minisql.FromMicroseconds(int64(v)).GoTime()
		case minisql.UUIDValue:
			dest[i] = v.String()
		default:
			dest[i] = v
		}
	}

	return nil
}
