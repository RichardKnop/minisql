package minisql

import (
	"testing"
)

func BenchmarkRow_Marshal(b *testing.B) {
	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 255},
		{Name: "age", Kind: Int4, Size: 4},
		{Name: "score", Kind: Double, Size: 8},
		{Name: "active", Kind: Boolean, Size: 1},
	}

	values := []OptionalValue{
		MakeInt8(int64(123)),
		MakeVarchar(TextPointer{Data: []byte("John Doe"), Length: 8}),
		MakeInt4(int32(30)),
		MakeDouble(float64(95.5)),
		MakeBool(true),
	}

	row := NewRowWithValues(columns, values)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := row.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRow_MarshalWithNulls(b *testing.B) {
	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 255},
		{Name: "age", Kind: Int4, Size: 4},
		{Name: "score", Kind: Double, Size: 8},
		{Name: "active", Kind: Boolean, Size: 1},
	}

	values := []OptionalValue{
		MakeInt8(int64(123)),
		MakeNull(), // NULL
		MakeInt4(int32(30)),
		MakeNull(), // NULL
		MakeBool(true),
	}

	row := NewRowWithValues(columns, values)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := row.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRow_Unmarshal(b *testing.B) {
	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 255},
		{Name: "age", Kind: Int4, Size: 4},
		{Name: "score", Kind: Double, Size: 8},
		{Name: "active", Kind: Boolean, Size: 1},
	}

	values := []OptionalValue{
		MakeInt8(int64(123)),
		MakeVarchar(TextPointer{Data: []byte("John Doe"), Length: 8}),
		MakeInt4(int32(30)),
		MakeDouble(float64(95.5)),
		MakeBool(true),
	}

	row := NewRowWithValues(columns, values)
	buf, err := row.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	cell := Cell{
		NullBitmask: 0,
		Key:         123,
		Value:       buf,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		r := NewRow(columns)
		_, err := r.Unmarshal(cell)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRow_OnlyFields(b *testing.B) {
	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 255},
		{Name: "age", Kind: Int4, Size: 4},
		{Name: "score", Kind: Double, Size: 8},
		{Name: "active", Kind: Boolean, Size: 1},
		{Name: "created_at", Kind: Timestamp, Size: 8},
	}

	values := []OptionalValue{
		MakeInt8(int64(123)),
		MakeVarchar(TextPointer{Data: []byte("John Doe"), Length: 8}),
		MakeInt4(int32(30)),
		MakeDouble(float64(95.5)),
		MakeBool(true),
		MakeTimestamp(MustParseTimestampMicros("2024-01-01 00:00:00")),
	}

	row := NewRowWithValues(columns, values)

	fields := []Field{
		{Name: "id"},
		{Name: "name"},
		{Name: "age"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = row.OnlyFields(fields...)
	}
}
