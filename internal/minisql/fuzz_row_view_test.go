package minisql

import (
	"testing"
)

// FuzzRowView verifies that RowView.ValueAt never panics on arbitrary cell
// value bytes. The only invariant enforced is no-panic: every read must return
// either a value or an error, never crash.
//
// Run for a fixed time during development:
//
//	go test -fuzz=FuzzRowView -fuzztime=60s ./internal/minisql/
//
// Seeds are run as ordinary unit tests on every `go test` invocation.
func FuzzRowView(f *testing.F) {
	// Seed: single INT8 column, all non-NULL (8 bytes).
	f.Add(makeSeedInt8(), uint64(0))

	// Seed: fixed-size mix (Boolean + INT4 + INT8 + Real + Double + Timestamp + UUID).
	f.Add(makeSeedFixed(), uint64(0))

	// Seed: text (Varchar) column (4-byte length prefix + data).
	f.Add(makeSeedText(), uint64(0))

	// Seed: mixed schema (INT8 + Varchar + INT4 + Boolean).
	f.Add(makeSeedMixed(), uint64(0))

	// Seed: all NULL — value area is empty regardless of schema.
	f.Add([]byte{}, uint64(^uint64(0)))

	// Seed: zero-length buffer, all non-NULL.
	f.Add([]byte{}, uint64(0))

	f.Fuzz(func(t *testing.T, valueBytes []byte, nullBitmask uint64) {
		// Drive every schema × every column index through ValueAt.
		// None may panic regardless of the value bytes or null bitmask.
		for _, schema := range fuzzRowSchemas {
			cell := Cell{
				ColumnCount: uint8(len(schema)),
				TypeCodes:   TypeCodesFromColumns(schema),
				NullBitmask: nullBitmask,
				Value:       valueBytes,
			}
			rv := NewRowView(schema, cell)
			for i := range schema {
				_, _ = rv.ValueAt(i)
			}
		}
	})
}

// fuzzRowSchemas is the set of column layouts exercised by FuzzRowView.
// Each schema exercises different TypeCode paths in offsetOf / ValueAt.
var fuzzRowSchemas = [][]Column{
	// Single fixed-size types
	{{Name: "v", Kind: Boolean, Size: 1}},
	{{Name: "v", Kind: Int4, Size: 4}},
	{{Name: "v", Kind: Int8, Size: 8}},
	{{Name: "v", Kind: Real, Size: 4}},
	{{Name: "v", Kind: Double, Size: 8}},
	{{Name: "v", Kind: Timestamp, Size: 8}},
	{{Name: "v", Kind: UUID, Size: 16}},

	// Variable-length text types
	{{Name: "v", Kind: Varchar, Size: 255}},
	{{Name: "v", Kind: Text, Size: 255}},
	{{Name: "v", Kind: JSON, Size: 255}},

	// All fixed-size types together (exercises cumulative offset computation)
	{
		{Name: "b", Kind: Boolean, Size: 1},
		{Name: "i4", Kind: Int4, Size: 4},
		{Name: "i8", Kind: Int8, Size: 8},
		{Name: "r", Kind: Real, Size: 4},
		{Name: "d", Kind: Double, Size: 8},
		{Name: "ts", Kind: Timestamp, Size: 8},
		{Name: "u", Kind: UUID, Size: 16},
	},

	// Mixed: mirrors testColumns used throughout the unit test suite
	{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "email", Kind: Varchar, Size: 255, Nullable: true},
		{Name: "age", Kind: Int4, Size: 4, Nullable: true},
		{Name: "ok", Kind: Boolean, Size: 1, Nullable: true},
		{Name: "score", Kind: Real, Size: 4, Nullable: true},
		{Name: "created", Kind: Timestamp, Size: 8, Nullable: true},
	},

	// Multi-text: exercises offsetOf advancing past multiple variable-length columns
	{
		{Name: "a", Kind: Varchar, Size: 255},
		{Name: "b", Kind: Text, Size: 255},
		{Name: "c", Kind: JSON, Size: 255},
	},
}

// --- seed helpers -----------------------------------------------------------

// makeSeedInt8 returns valid marshaled bytes for a single INT8 column with value 42.
func makeSeedInt8() []byte {
	cols := []Column{{Name: "v", Kind: Int8, Size: 8}}
	row := NewRowWithValues(cols, []OptionalValue{{Value: int64(42), Valid: true}})
	data, _ := row.Marshal()
	return data
}

// makeSeedFixed returns valid marshaled bytes for all fixed-size column types.
func makeSeedFixed() []byte {
	cols := []Column{
		{Name: "b", Kind: Boolean, Size: 1},
		{Name: "i4", Kind: Int4, Size: 4},
		{Name: "i8", Kind: Int8, Size: 8},
		{Name: "r", Kind: Real, Size: 4},
		{Name: "d", Kind: Double, Size: 8},
		{Name: "ts", Kind: Timestamp, Size: 8},
		{Name: "u", Kind: UUID, Size: 16},
	}
	var uid UUIDValue
	copy(uid[:], "abcdefghijklmnop")
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: true, Valid: true},
		{Value: int32(100), Valid: true},
		{Value: int64(200), Valid: true},
		{Value: float32(1.5), Valid: true},
		{Value: float64(3.14), Valid: true},
		{Value: TimestampMicros(1_000_000), Valid: true},
		{Value: uid, Valid: true},
	})
	data, _ := row.Marshal()
	return data
}

// makeSeedText returns valid marshaled bytes for a single Varchar column.
func makeSeedText() []byte {
	cols := []Column{{Name: "v", Kind: Varchar, Size: 255}}
	tp := NewTextPointer([]byte("hello fuzz"))
	row := NewRowWithValues(cols, []OptionalValue{{Value: tp, Valid: true}})
	data, _ := row.Marshal()
	return data
}

// makeSeedMixed returns valid marshaled bytes for a mixed schema (INT8 + Varchar + INT4 + Boolean).
func makeSeedMixed() []byte {
	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "email", Kind: Varchar, Size: 255, Nullable: true},
		{Name: "age", Kind: Int4, Size: 4, Nullable: true},
		{Name: "ok", Kind: Boolean, Size: 1, Nullable: true},
	}
	tp := NewTextPointer([]byte("alice@example.com"))
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: tp, Valid: true},
		{Value: int32(30), Valid: true},
		{Value: true, Valid: true},
	})
	data, _ := row.Marshal()
	return data
}
