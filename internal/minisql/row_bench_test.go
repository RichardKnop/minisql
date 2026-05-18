package minisql

import (
	"testing"
)

// BenchmarkRowClone measures the performance of row cloning
func BenchmarkRowClone(b *testing.B) {
	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 100},
		{Name: "email", Kind: Varchar, Size: 100},
		{Name: "age", Kind: Int4, Size: 4},
		{Name: "created", Kind: Timestamp, Size: 8},
	}

	row := NewRowWithValues(columns, []OptionalValue{
		MakeInt8(int64(123)),
		MakeVarchar(NewTextPointer([]byte("John Doe"))),
		MakeVarchar(NewTextPointer([]byte("john@example.com"))),
		MakeInt4(int32(30)),
		MakeTimestamp(MustParseTimestampMicros("2024-01-01 00:00:00")),
	})
	row.Key = RowID(123)

	b.ResetTimer()

	for b.Loop() {
		_ = row.Clone()
	}
}

// BenchmarkRowOnlyFields measures filtering performance
func BenchmarkRowOnlyFields(b *testing.B) {
	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 100},
		{Name: "email", Kind: Varchar, Size: 100},
		{Name: "age", Kind: Int4, Size: 4},
		{Name: "created", Kind: Timestamp, Size: 8},
		{Name: "updated", Kind: Timestamp, Size: 8},
		{Name: "status", Kind: Varchar, Size: 50},
		{Name: "score", Kind: Double, Size: 8},
	}

	row := NewRowWithValues(columns, []OptionalValue{
		MakeInt8(int64(123)),
		MakeVarchar(NewTextPointer([]byte("John Doe"))),
		MakeVarchar(NewTextPointer([]byte("john@example.com"))),
		MakeInt4(int32(30)),
		MakeTimestamp(MustParseTimestampMicros("2024-01-01 00:00:00")),
		MakeTimestamp(MustParseTimestampMicros("2024-01-02 00:00:00")),
		MakeVarchar(NewTextPointer([]byte("active"))),
		MakeDouble(float64(95.5)),
	})
	row.Key = RowID(123)

	fields := []Field{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	}

	b.ResetTimer()
	for b.Loop() {
		_ = row.OnlyFields(fields...)
	}
}

// BenchmarkRowOnlyFieldsMany measures filtering with more fields
func BenchmarkRowOnlyFieldsMany(b *testing.B) {
	// Create a row with 20 columns
	columns := make([]Column, 20)
	values := make([]OptionalValue, 20)
	for i := range 20 {
		columns[i] = Column{Name: string(rune('a' + i)), Kind: Int8, Size: 8}
		values[i] = MakeInt8(int64(i))
	}

	row := NewRowWithValues(columns, values)
	row.Key = RowID(123)

	// Filter to first 10 fields
	fields := make([]Field, 10)
	for i := range 10 {
		fields[i] = Field{Name: string(rune('a' + i))}
	}

	b.ResetTimer()
	for b.Loop() {
		_ = row.OnlyFields(fields...)
	}
}
