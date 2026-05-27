package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTestCell builds a Cell with the correct TypeCodes for test use.
// Production code uses cursor.saveToCell; this helper keeps tests concise.
func makeTestCell(key RowID, nullBitmask uint64, value []byte, columns []Column) Cell {
	return Cell{
		Key:         key,
		NullBitmask: nullBitmask,
		Value:       value,
		TypeCodes:   TypeCodesFromColumns(columns),
		ColumnCount: uint8(len(columns)),
	}
}

func TestRowView_ValueAt(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Key = 42

	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(row.Key, row.NullBitmask(), data, row.Columns))

	assert.Equal(t, row.Key, view.Key())
	assert.Equal(t, row.Columns, view.Columns())

	for i := range row.Columns {
		got, err := view.ValueAt(i)
		require.NoError(t, err)
		assert.Equal(t, row.Values[i], got)
	}
}

func TestRowView_NamedAndNullAccessors(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Values[2] = OptionalValue{}
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))

	isNull, err := view.IsNull(2)
	require.NoError(t, err)
	assert.True(t, isNull)

	value, found, err := view.ValueByName("email")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, row.Values[1], value)

	_, found, err = view.ValueByName("missing")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestRowView_TypedAccessors(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))

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

func TestRowView_UUIDAt(t *testing.T) {
	t.Parallel()

	uuidValue, err := ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	row := NewRowWithValues(
		[]Column{{Name: "uuid", Kind: UUID, Size: 16}},
		[]OptionalValue{{Valid: true, Value: uuidValue}},
	)
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))

	got, ok, err := view.UUIDAt(0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uuidValue, got)
}

func TestRowView_OverflowAwareAccessors(t *testing.T) {
	t.Parallel()

	row := NewRowWithValues(
		[]Column{{Name: "body", Kind: Text, Size: MaxInlineVarchar}},
		[]OptionalValue{{Valid: true, Value: NewTextPointer([]byte("inline body"))}},
	)
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))

	value, err := view.ValueAtWithOverflow(context.Background(), nil, 0)
	require.NoError(t, err)
	require.True(t, value.Valid)
	assert.Equal(t, "inline body", value.Value.(TextPointer).String())

	textValue, err := view.TextAtWithOverflow(context.Background(), nil, 0)
	require.NoError(t, err)
	assert.Equal(t, "inline body", textValue.String())

	materialized, err := view.MaterializeWithOverflow(context.Background(), nil, []bool{true})
	require.NoError(t, err)
	assert.Equal(t, "inline body", materialized.Values[0].Value.(TextPointer).String())
}

func TestRowView_OverflowAwareAccessorsRequirePagerForOverflow(t *testing.T) {
	t.Parallel()

	longText := make([]byte, MaxInlineVarchar+1)
	for i := range longText {
		longText[i] = 'x'
	}
	row := NewRowWithValues(
		[]Column{{Name: "body", Kind: Text, Size: MaxInlineVarchar}},
		[]OptionalValue{{Valid: true, Value: NewTextPointer(longText)}},
	)
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))

	_, err = view.ValueAtWithOverflow(context.Background(), nil, 0)
	require.Error(t, err)

	_, err = view.TextAtWithOverflow(context.Background(), nil, 0)
	require.Error(t, err)

	_, err = view.MaterializeWithOverflow(context.Background(), nil, []bool{true})
	require.Error(t, err)
}

func TestRowView_Materialize(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Values[2] = OptionalValue{}

	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(7, row.NullBitmask(), data, row.Columns))
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

func TestRowView_CheckOneOrMoreWithColumnIndexes(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))
	columnIndexes := make(map[string]int, len(row.Columns))
	for i, col := range row.Columns {
		columnIndexes[col.Name] = i
	}

	id := row.Values[0].Value.(int64)
	age := int64(row.Values[2].Value.(int32))
	conditions := NewOneOrMore(Conditions{
		FieldIsGreaterOrEqual(Field{Name: "id"}, OperandInteger, id),
		FieldIsLessOrEqual(Field{Name: "age"}, OperandInteger, age),
	})

	ok, err := view.CheckOneOrMoreWithColumnIndexes(context.Background(), nil, conditions, columnIndexes)
	require.NoError(t, err)
	assert.True(t, ok)

	conditions = NewOneOrMore(Conditions{
		FieldIsGreater(Field{Name: "age"}, OperandInteger, age),
	})

	ok, err = view.CheckOneOrMoreWithColumnIndexes(context.Background(), nil, conditions, columnIndexes)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRowView_CheckOneOrMoreWithColumnIndexes_Null(t *testing.T) {
	t.Parallel()

	row := gen.Row()
	row.Values[2] = OptionalValue{}
	data, err := row.Marshal()
	require.NoError(t, err)

	view := NewRowView(row.Columns, makeTestCell(0, row.NullBitmask(), data, row.Columns))
	columnIndexes := map[string]int{"age": 2}

	ok, err := view.CheckOneOrMoreWithColumnIndexes(
		context.Background(),
		nil,
		NewOneOrMore(Conditions{FieldIsNull(Field{Name: "age"})}),
		columnIndexes,
	)
	require.NoError(t, err)
	assert.True(t, ok)
}
