package minisql

import (
	"testing"
)

// BenchmarkCellUnmarshal measures the performance of cell unmarshaling
// with the single-allocation optimization
func BenchmarkCellUnmarshal(b *testing.B) {
	columns := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: 255, Name: "email"},
		{Kind: Varchar, Size: 255, Name: "name"},
		{Kind: Boolean, Size: 1, Name: "active"},
		{Kind: Int4, Size: 4, Name: "score"},
		{Kind: Timestamp, Size: 8, Name: "created"},
	}

	// Marshal some sample data
	buf := make([]byte, 0, 1024)

	// NullBitmask (8 bytes)
	buf = append(buf, make([]byte, 8)...)

	// Key (8 bytes)
	buf = append(buf, make([]byte, 8)...)
	marshalUint64(buf, uint64(123), 8)

	// Int8 (8 bytes)
	buf = append(buf, make([]byte, 8)...)
	marshalInt64(buf, int64(123), 16)

	// Varchar email (4 bytes length + data)
	email := "test@example.com"
	buf = append(buf, make([]byte, 4)...)
	marshalInt32(buf, int32(len(email)), 24)
	buf = append(buf, []byte(email)...)

	// Varchar name (4 bytes length + data)
	name := "John Doe"
	currentOffset := 28 + len(email)
	buf = append(buf, make([]byte, 4)...)
	marshalInt32(buf, int32(len(name)), uint64(currentOffset))
	buf = append(buf, []byte(name)...)

	// Boolean (1 byte)
	currentOffset += 4 + len(name)
	buf = append(buf, make([]byte, 1)...)
	marshalBool(buf, true, uint64(currentOffset))

	// Int4 (4 bytes)
	currentOffset += 1
	buf = append(buf, make([]byte, 4)...)
	marshalInt32(buf, int32(100), uint64(currentOffset))

	// Timestamp (8 bytes)
	currentOffset += 4
	buf = append(buf, make([]byte, 8)...)
	marshalInt64(buf, int64(1640000000), uint64(currentOffset))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		testCell := Cell{}
		_, err := testCell.Unmarshal(columns, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
