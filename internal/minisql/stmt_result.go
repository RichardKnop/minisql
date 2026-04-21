package minisql

import (
	"context"
	"errors"
)

// Iterator ...
type Iterator struct {
	rowFunc func(ctx context.Context) (Row, error)
	nextRow Row
	end     bool
	err     error
}

// NewIterator ...
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

// NewSingleRowIterator ...
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

// Row ...
func (i *Iterator) Row() Row {
	return i.nextRow
}

// Next ...
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

// Close ...
func (i *Iterator) Close() error {
	i.end = true
	return nil
}

// Err ...
func (i *Iterator) Err() error {
	return i.err
}

// StatementResult ...
type StatementResult struct {
	Columns      []Column
	Rows         Iterator
	RowsAffected int
	LastInsertId int64
}
