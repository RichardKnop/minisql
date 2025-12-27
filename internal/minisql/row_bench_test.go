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

	row := Row{
		Key:     RowID(123),
		Columns: columns,
		Values: []OptionalValue{
			{Value: int64(123), Valid: true},
			{Value: "John Doe", Valid: true},
			{Value: "john@example.com", Valid: true},
			{Value: int32(30), Valid: true},
			{Value: Time{Year: 2024, Month: 1, Day: 1}, Valid: true},
		},
	}

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

	row := Row{
		Key:     RowID(123),
		Columns: columns,
		Values: []OptionalValue{
			{Value: int64(123), Valid: true},
			{Value: "John Doe", Valid: true},
			{Value: "john@example.com", Valid: true},
			{Value: int32(30), Valid: true},
			{Value: Time{Year: 2024, Month: 1, Day: 1}, Valid: true},
			{Value: Time{Year: 2024, Month: 1, Day: 2}, Valid: true},
			{Value: "active", Valid: true},
			{Value: float64(95.5), Valid: true},
		},
	}

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
		values[i] = OptionalValue{Value: int64(i), Valid: true}
	}

	row := Row{
		Key:     RowID(123),
		Columns: columns,
		Values:  values,
	}

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
