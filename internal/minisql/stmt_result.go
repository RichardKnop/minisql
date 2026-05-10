package minisql

import (
	"context"
	"errors"
)

// Iterator is a pull-based row cursor returned as part of a StatementResult.
// Call Next to advance, Row to read the current row, and Err after the loop.
type Iterator struct {
	err     error
	rowFunc func(ctx context.Context) (Row, error)
	nextRow Row
	end     bool
}

// NewIterator wraps a row-producing function into an Iterator. The function
// should return ErrNoMoreRows to signal end-of-stream; any other error is
// surfaced via Err() after Next() returns false.
func NewIterator(rowFunc func(ctx context.Context) (Row, error)) Iterator {
	return Iterator{
		rowFunc: rowFunc,
	}
}

// NewSliceIterator returns an Iterator that yields rows from rows in order.
func NewSliceIterator(rows []Row) Iterator {
	idx := 0
	return NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(rows) {
			return Row{}, ErrNoMoreRows
		}
		row := rows[idx]
		idx += 1
		return row, nil
	})
}

// NewSingleRowIterator returns an Iterator that yields exactly one row then signals end-of-stream.
func NewSingleRowIterator(row Row) Iterator {
	i := Iterator{}
	end := false
	i.rowFunc = func(ctx context.Context) (Row, error) {
		if end {
			return Row{}, ErrNoMoreRows
		}
		end = true
		return row, nil
	}
	return i
}

// Row returns the most-recently-fetched row. Only valid after a successful Next call.
func (i *Iterator) Row() Row {
	return i.nextRow
}

// Next advances the iterator to the next row. Returns true if a row is
// available, false when the stream is exhausted or an error occurred.
func (i *Iterator) Next(ctx context.Context) bool {
	if i.err != nil {
		return false
	}
	if i.end {
		return false
	}
	row, err := i.rowFunc(ctx)
	if err != nil {
		if errors.Is(err, ErrNoMoreRows) {
			i.end = true
			return false
		}
		i.err = err
		return false
	}
	i.nextRow = row
	return true
}

// Close marks the iterator as exhausted so subsequent Next calls return false.
func (i *Iterator) Close() error {
	i.end = true
	return nil
}

// Err returns the first non-ErrNoMoreRows error encountered during iteration, or nil.
func (i *Iterator) Err() error {
	return i.err
}

// StatementResult is the value returned by every DML/DDL execution. Rows is a
// lazy iterator over result rows (non-nil even for INSERT/UPDATE/DELETE when a
// RETURNING clause was present). RowsAffected and LastInsertId follow
// database/sql conventions.
type StatementResult struct {
	Rows         Iterator
	Columns      []Column
	RowsAffected int
	LastInsertId int64
}
