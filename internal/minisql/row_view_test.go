package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowView_ValueAt(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Key = 42

	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, Cell{
		Key:         row.Key,
		NullBitmask: row.NullBitmask(),
		Value:       data,
	})

	assert.Equal(t, row.Key, view.Key())
	assert.Equal(t, row.Columns, view.Columns())

	for i := range row.Columns {
		got, err := view.ValueAt(i)
		require.NoError(t, err)
		assert.Equal(t, row.Values[i], got)
	}
}

func TestRowView_TypedAccessors(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, Cell{
		NullBitmask: row.NullBitmask(),
		Value:       data,
	})

	gotID, ok, err := view.Int64At(0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, row.Values[0].Value.(int64), gotID)

	gotText, err := view.TextAt(1)
	require.NoError(t, err)
	assert.True(t, row.Values[1].Value.(TextPointer).IsEqual(gotText))

	gotAge, ok, err := view.Int64At(2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(row.Values[2].Value.(int32)), gotAge)

	gotBool, ok, err := view.BoolAt(3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, row.Values[3].Value.(bool), gotBool)

	gotReal, ok, err := view.Float64At(4)
	require.NoError(t, err)
	require.True(t, ok)
	assert.InDelta(t, float64(row.Values[4].Value.(float32)), gotReal, 0.0001)
}

func TestRowView_Materialize(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Values[2] = OptionalValue{}

	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, Cell{
		Key:         7,
		NullBitmask: row.NullBitmask(),
		Value:       data,
	})
	mask := selectedColumnsMask(row.Columns, []Field{
		{Name: row.Columns[0].Name},
		{Name: row.Columns[1].Name},
		{Name: row.Columns[2].Name},
	})

	got, err := view.Materialize(mask)
	require.NoError(t, err)

	assert.Equal(t, RowID(7), got.Key)
	assert.Equal(t, row.Values[0], got.Values[0])
	assert.Equal(t, row.Values[1], got.Values[1])
	assert.False(t, got.Values[2].Valid)
	assert.False(t, got.Values[3].Valid)
	assert.False(t, got.Values[4].Valid)
	assert.False(t, got.Values[5].Valid)
}
