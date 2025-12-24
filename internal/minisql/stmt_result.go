package minisql

import (
	"context"
	"errors"
)

type Iterator struct {
	rowFunc func(ctx context.Context) (Row, error)
	nextRow Row
	end     bool
	err     error
}

func NewIterator(rowFunc func(ctx context.Context) (Row, error)) Iterator {
	return Iterator{
		rowFunc: rowFunc,
	}
}

func NewSingleRowIterator(aRow Row) Iterator {
	i := Iterator{}
	end := false
	i.rowFunc = func(ctx context.Context) (Row, error) {
		if end {
			return Row{}, ErrNoMoreRows
		}
		end = true
		return aRow, nil
	}
	return i
}

func (i *Iterator) Row() Row {
	return i.nextRow
}

func (i *Iterator) Next(ctx context.Context) bool {
	if i.err != nil {
		return false
	}
	if i.end {
		return false
	}
	aRow, err := i.rowFunc(ctx)
	if err != nil {
		if errors.Is(err, ErrNoMoreRows) {
			i.end = true
			return false
		}
		i.err = err
		return false
	}
	i.nextRow = aRow
	return true
}

func (i *Iterator) Err() error {
	return i.err
}

type StatementResult struct {
	Columns      []Column
	Rows         Iterator
	RowsAffected int
}
