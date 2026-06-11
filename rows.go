package minisql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// Rows is a database/sql/driver.Rows implementation that iterates over the
// results of a SELECT query. It is returned by QueryContext and
// Stmt.QueryContext; callers interact with it through the database/sql API
// rather than directly.
type Rows struct {
	columns             []minisql.Column
	columnNames         []string // precomputed from columns; nil means compute on demand
	rowViewFieldIndexes []int
	iter                minisql.Iterator
	rowViewIter         minisql.RowViewIterator
	rowViewPager        minisql.TxPager
	ctx                 context.Context
	txManager           *minisql.TransactionManager
	tx                  *minisql.Transaction
	useRowViews         bool
	txClosed            bool
}

// buildColumnNames extracts the Name field from each Column into a plain string slice.
func buildColumnNames(cols []minisql.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r Rows) Columns() []string {
	if r.columnNames != nil {
		return r.columnNames
	}
	return buildColumnNames(r.columns)
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	if r.useRowViews {
		return r.closeRowViewIterators(true)
	}
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
	if r.useRowViews {
		return r.nextRowView(dest)
	}

	if !r.iter.Next(r.ctx) {
		if err := r.iter.Err(); err != nil {
			_ = r.closeReadTx(false)
			return err
		}
		if err := r.closeReadTx(true); err != nil {
			return err
		}
		return io.EOF
	}

	aRow := r.iter.Row()
	if len(aRow.Values) != len(dest) {
		return fmt.Errorf("expected %d values, got %d", len(dest), len(aRow.Values))
	}

	for i := range dest {
		if !aRow.Values[i].Valid {
			dest[i] = nil
			continue
		}
		switch v := aRow.Values[i].Value.(type) {
		case minisql.TextPointer:
			dest[i] = string(v.Data)
		case minisql.TimestampMicros:
			dest[i] = minisql.FromMicroseconds(int64(v)).GoTime()
		case minisql.UUIDValue:
			dest[i] = v.String()
		case minisql.VectorPointer:
			dest[i] = minisql.FormatVector(v)
		default:
			dest[i] = aRow.Values[i].Value
		}
	}

	return nil
}

func (r *Rows) nextRowView(dest []driver.Value) error {
	if !r.rowViewIter.Next(r.ctx) {
		if err := r.rowViewIter.Err(); err != nil {
			_ = r.closeRowViewIterators(false)
			return err
		}
		if err := r.closeRowViewIterators(true); err != nil {
			return err
		}
		return io.EOF
	}
	if len(r.rowViewFieldIndexes) != len(dest) {
		return fmt.Errorf("expected %d values, got %d", len(dest), len(r.rowViewFieldIndexes))
	}

	view := r.rowViewIter.RowView()
	for i, fieldIdx := range r.rowViewFieldIndexes {
		value, err := r.rowViewDriverValue(view, i, fieldIdx)
		if err != nil {
			return err
		}
		dest[i] = value
	}

	return nil
}

func (r *Rows) closeRowViewIterators(success bool) error {
	err := r.rowViewIter.Close()
	if fallbackErr := r.iter.Close(); err == nil {
		err = fallbackErr
	}
	if err != nil {
		_ = r.closeReadTx(false)
		return err
	}
	return r.closeReadTx(success)
}

func (r *Rows) rowViewDriverValue(view minisql.RowView, destIdx, fieldIdx int) (driver.Value, error) {
	if fieldIdx < 0 || fieldIdx >= len(view.Columns()) {
		return nil, fmt.Errorf("column index %d out of bounds", fieldIdx)
	}
	isNull, err := view.IsNull(fieldIdx)
	if err != nil || isNull {
		return nil, err
	}
	switch r.columns[destIdx].Kind {
	case minisql.Boolean:
		value, ok, err := view.BoolAt(fieldIdx)
		if err != nil || !ok {
			return nil, err
		}
		return value, nil
	case minisql.Int4, minisql.Int8:
		value, ok, err := view.Int64At(fieldIdx)
		if err != nil || !ok {
			return nil, err
		}
		return value, nil
	case minisql.Real, minisql.Double:
		value, ok, err := view.Float64At(fieldIdx)
		if err != nil || !ok {
			return nil, err
		}
		return value, nil
	case minisql.Varchar, minisql.Text, minisql.JSON:
		value, err := view.TextAtWithOverflow(r.ctx, r.rowViewPager, fieldIdx)
		if err != nil {
			return nil, err
		}
		return string(value.Data), nil
	case minisql.Timestamp:
		value, ok, err := view.Int64At(fieldIdx)
		if err != nil || !ok {
			return nil, err
		}
		return minisql.FromMicroseconds(value).GoTime(), nil
	case minisql.UUID:
		value, ok, err := view.UUIDAt(fieldIdx)
		if err != nil || !ok {
			return nil, err
		}
		return value.String(), nil
	case minisql.Vector:
		value, err := view.ValueAtWithOverflow(r.ctx, r.rowViewPager, fieldIdx)
		if err != nil {
			return nil, err
		}
		if !value.Valid {
			return nil, nil
		}
		vp, ok := value.Value.(minisql.VectorPointer)
		if !ok {
			return nil, fmt.Errorf("expected VectorPointer for vector column")
		}
		return minisql.FormatVector(vp), nil
	default:
		value, err := view.ValueAt(fieldIdx)
		if err != nil {
			return nil, err
		}
		if !value.Valid {
			return nil, nil
		}
		return value.Value, nil
	}
}

func (r *Rows) closeReadTx(success bool) error {
	if r.txManager == nil || r.tx == nil || r.txClosed {
		return nil
	}
	r.txClosed = true
	tx := r.tx
	if !success {
		r.txManager.RollbackTransaction(r.ctx, tx)
		r.txManager.ReleaseReadOnlyTransaction(tx)
		return nil
	}
	if err := r.txManager.CommitTransaction(r.ctx, tx); err != nil {
		r.txManager.RollbackTransaction(r.ctx, tx)
		r.txManager.ReleaseReadOnlyTransaction(tx)
		return err
	}
	r.txManager.ReleaseReadOnlyTransaction(tx)
	return nil
}
