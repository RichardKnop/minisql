package minisql

import (
	"context"
	"errors"
)

// Iterator is a pull-based row cursor returned as part of a StatementResult.
// Call Next to advance, Row to read the current row, and Err after the loop.
type Iterator struct {
	err       error
	rowFunc   func(ctx context.Context) (Row, error)
	closeFunc func() error
	nextRow   Row
	end       bool
}

// RowViewIterator is a pull-based cursor over lazy RowView results.
type RowViewIterator struct {
	err       error
	rowFunc   func(ctx context.Context) (RowView, error)
	closeFunc func() error
	nextView  RowView
	end       bool
}

// NewRowViewIterator wraps a row-view-producing function into an iterator.
func NewRowViewIterator(rowFunc func(ctx context.Context) (RowView, error)) RowViewIterator {
	return RowViewIterator{rowFunc: rowFunc}
}

func newRowViewIteratorWithClose(rowFunc func(ctx context.Context) (RowView, error), closeFunc func() error) RowViewIterator {
	return RowViewIterator{
		rowFunc:   rowFunc,
		closeFunc: closeFunc,
	}
}

// NewSliceRowViewIterator returns an iterator that yields row views from rows.
func NewSliceRowViewIterator(rows []RowView) RowViewIterator {
	idx := 0
	return NewRowViewIterator(func(ctx context.Context) (RowView, error) {
		if idx >= len(rows) {
			return RowView{}, ErrNoMoreRows
		}
		row := rows[idx]
		idx += 1
		return row, nil
	})
}

// RowView returns the most-recently-fetched view.
func (i *RowViewIterator) RowView() RowView {
	return i.nextView
}

// Next advances the iterator to the next row view.
func (i *RowViewIterator) Next(ctx context.Context) bool {
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
	i.nextView = row
	return true
}

// Close marks the iterator as exhausted.
func (i *RowViewIterator) Close() error {
	i.end = true
	if i.closeFunc != nil {
		return i.closeFunc()
	}
	return nil
}

// Err returns the first non-ErrNoMoreRows error encountered by the iterator.
func (i *RowViewIterator) Err() error {
	return i.err
}

// NewIterator wraps a row-producing function into an Iterator. The function
// should return ErrNoMoreRows to signal end-of-stream; any other error is
// surfaced via Err() after Next() returns false.
func NewIterator(rowFunc func(ctx context.Context) (Row, error)) Iterator {
	return Iterator{
		rowFunc: rowFunc,
	}
}

func newIteratorWithClose(rowFunc func(ctx context.Context) (Row, error), closeFunc func() error) Iterator {
	return Iterator{
		rowFunc:   rowFunc,
		closeFunc: closeFunc,
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

func materializeResultRows(ctx context.Context, result StatementResult) ([]Row, error) {
	if result.rawRows != nil {
		return result.rawRows, nil
	}

	if result.RowViews.rowFunc != nil {
		defer func() {
			_ = result.Rows.Close()
		}()
		defer func() {
			_ = result.RowViews.Close()
		}()

		var rows []Row
		for result.RowViews.Next(ctx) {
			row, err := projectRowView(ctx, result.RowViewPager, result.RowViews.RowView(), result.RowViewFieldIndexes, result.Columns)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		if err := result.RowViews.Err(); err != nil {
			return nil, err
		}
		return rows, nil
	}

	defer func() {
		_ = result.Rows.Close()
	}()
	var rows []Row
	for result.Rows.Next(ctx) {
		rows = append(rows, result.Rows.Row())
	}
	if err := result.Rows.Err(); err != nil {
		return nil, err
	}
	return rows, nil
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
	if i.end || i.rowFunc == nil {
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
	if i.closeFunc != nil {
		return i.closeFunc()
	}
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
//
// rawRows, when non-nil, holds the same projected rows that back the Rows
// iterator. Callers that need to materialise all rows (e.g. CTE body
// collection) can steal this slice directly instead of draining the iterator,
// avoiding a second heap allocation of the same data.
type StatementResult struct {
	Rows                Iterator
	RowViews            RowViewIterator
	RowViewPager        TxPager
	rawRows             []Row // non-nil when produced by selectStreamingDirect
	Columns             []Column
	RowViewFieldIndexes []int
	RowsAffected        int
	LastInsertID        int64
}
