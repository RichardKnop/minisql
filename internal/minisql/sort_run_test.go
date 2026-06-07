package minisql

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTripColumns covers every supported kind so that UnmarshalRow is exercised
// for each type code in a single test.
var roundTripColumns = []Column{
	{Name: "b", Kind: Boolean, Size: 1},
	{Name: "i4", Kind: Int4, Size: 4},
	{Name: "i8", Kind: Int8, Size: 8},
	{Name: "r", Kind: Real, Size: 4},
	{Name: "d", Kind: Double, Size: 8},
	{Name: "s", Kind: Varchar, Size: 255},
	{Name: "ts", Kind: Timestamp, Size: 8},
}

func makeRoundTripRow(key RowID) Row {
	return Row{
		Key:     key,
		Columns: roundTripColumns,
		Values: []OptionalValue{
			{Value: true, Valid: true},
			{Value: int32(42), Valid: true},
			{Value: int64(9876543210), Valid: true},
			{Value: float32(1.5), Valid: true},
			{Value: float64(3.14159), Valid: true},
			{Value: NewTextPointer([]byte("hello")), Valid: true},
			{Value: TimestampMicros(1_000_000), Valid: true},
		},
	}
}

func TestRunWriterReader_RoundTrip(t *testing.T) {
	row := makeRoundTripRow(RowID(7))

	w, err := newRunWriter()
	require.NoError(t, err)
	require.NoError(t, w.writeRow(row))
	path := w.filePath()
	require.NoError(t, w.close())
	defer os.Remove(path)

	rr, err := newRunReader(path, roundTripColumns)
	require.NoError(t, err)
	require.False(t, rr.Done())

	got := rr.Row()
	assert.Equal(t, row.Key, got.Key)
	assert.Len(t, got.Values, len(row.Values))
	for i, want := range row.Values {
		g := got.Values[i]
		assert.Equal(t, want.Valid, g.Valid, "column %d valid", i)
		if !want.Valid {
			continue
		}
		switch v := want.Value.(type) {
		case TextPointer:
			tp, ok := g.Value.(TextPointer)
			require.True(t, ok, "column %d: expected TextPointer", i)
			assert.Equal(t, string(v.Data), string(tp.Data))
		default:
			assert.Equal(t, want.Value, g.Value, "column %d", i)
		}
	}

	rr.Next()
	assert.True(t, rr.Done())
	assert.NoError(t, rr.Err())
	require.NoError(t, rr.close())
}

func TestRunWriterReader_MultipleRows(t *testing.T) {
	const n = 50
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Key:     RowID(i + 1),
			Columns: []Column{{Name: "id", Kind: Int8, Size: 8}},
			Values:  []OptionalValue{{Value: int64(i + 1), Valid: true}},
		}
	}

	w, err := newRunWriter()
	require.NoError(t, err)
	for _, r := range rows {
		require.NoError(t, w.writeRow(r))
	}
	path := w.filePath()
	require.NoError(t, w.close())
	defer os.Remove(path)

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	rr, err := newRunReader(path, cols)
	require.NoError(t, err)

	var got []Row
	for !rr.Done() {
		got = append(got, rr.Row())
		rr.Next()
	}
	require.NoError(t, rr.Err())
	_ = rr.close()

	require.Len(t, got, n)
	for i, r := range got {
		assert.Equal(t, RowID(i+1), r.Key)
		assert.Equal(t, int64(i+1), r.Values[0].Value)
	}
}

func TestRunWriterReader_NullValues(t *testing.T) {
	cols := []Column{
		{Name: "a", Kind: Int8, Size: 8},
		{Name: "b", Kind: Varchar, Size: 255},
	}
	row := Row{
		Key:     RowID(1),
		Columns: cols,
		Values: []OptionalValue{
			{Value: int64(99), Valid: true},
			{Valid: false}, // NULL
		},
	}

	w, err := newRunWriter()
	require.NoError(t, err)
	require.NoError(t, w.writeRow(row))
	path := w.filePath()
	require.NoError(t, w.close())
	defer os.Remove(path)

	rr, err := newRunReader(path, cols)
	require.NoError(t, err)
	got := rr.Row()
	assert.True(t, got.Values[0].Valid)
	assert.Equal(t, int64(99), got.Values[0].Value)
	assert.False(t, got.Values[1].Valid)
	_ = rr.close()
}

func TestUnmarshalRow_RoundTrip(t *testing.T) {
	row := makeRoundTripRow(RowID(42))
	buf, err := row.Marshal()
	require.NoError(t, err)

	got, err := UnmarshalRow(buf, roundTripColumns, row.Key, row.NullBitmask())
	require.NoError(t, err)
	assert.Equal(t, row.Key, got.Key)
	for i, want := range row.Values {
		g := got.Values[i]
		assert.Equal(t, want.Valid, g.Valid, "column %d valid", i)
		if !want.Valid {
			continue
		}
		switch v := want.Value.(type) {
		case TextPointer:
			tp, _ := g.Value.(TextPointer)
			assert.Equal(t, string(v.Data), string(tp.Data))
		default:
			assert.Equal(t, want.Value, g.Value, "column %d", i)
		}
	}
}
