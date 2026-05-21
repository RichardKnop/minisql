package minisql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializeResultRowsUsesRowViews(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Key = 42
	data, err := row.Marshal()
	require.NoError(t, err)

	usedRowsIterator := false
	closedRowsIterator := false
	result := StatementResult{
		Rows: newIteratorWithClose(func(ctx context.Context) (Row, error) {
			usedRowsIterator = true
			return Row{}, errors.New("rows iterator should not be used")
		}, func() error {
			closedRowsIterator = true
			return nil
		}),
		RowViews: NewSliceRowViewIterator([]RowView{
			NewRowView(row.Columns, Cell{Key: row.Key, Value: data}),
		}),
		Columns:             []Column{row.Columns[0], row.Columns[2]},
		RowViewFieldIndexes: []int{0, 2},
	}

	rows, err := materializeResultRows(context.Background(), result)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, RowID(42), rows[0].Key)
	assert.Equal(t, []Column{row.Columns[0], row.Columns[2]}, rows[0].Columns)
	assert.Equal(t, row.Values[0], rows[0].Values[0])
	assert.Equal(t, row.Values[2], rows[0].Values[1])
	assert.False(t, usedRowsIterator)
	assert.True(t, closedRowsIterator)
}

func TestLazyRowViewMaterializingIteratorOpensOnDemand(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Key = 7
	data, err := row.Marshal()
	require.NoError(t, err)

	openCount := 0
	closeCount := 0
	newIter := func() RowViewIterator {
		openCount += 1
		return newRowViewIteratorWithClose(func(ctx context.Context) (RowView, error) {
			return NewRowView(row.Columns, Cell{
				Key:         row.Key,
				Value:       data,
				NullBitmask: row.NullBitmask(),
			}), nil
		}, func() error {
			closeCount += 1
			return nil
		})
	}

	iter := lazyRowViewMaterializingIterator(nil, newIter, []int{0, 2}, []Column{row.Columns[0], row.Columns[2]})
	require.NoError(t, iter.Close())
	assert.Zero(t, openCount)
	assert.Zero(t, closeCount)

	iter = lazyRowViewMaterializingIterator(nil, newIter, []int{0, 2}, []Column{row.Columns[0], row.Columns[2]})
	require.True(t, iter.Next(context.Background()))
	got := iter.Row()
	assert.Equal(t, []Column{row.Columns[0], row.Columns[2]}, got.Columns)
	assert.Equal(t, row.Values[0], got.Values[0])
	assert.Equal(t, row.Values[2], got.Values[1])
	assert.Equal(t, 1, openCount)

	require.NoError(t, iter.Close())
	assert.Equal(t, 1, closeCount)
}
