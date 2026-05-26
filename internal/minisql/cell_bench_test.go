package minisql

import (
	"testing"
)

// BenchmarkCellUnmarshal measures the performance of cell unmarshaling.
func BenchmarkCellUnmarshal(b *testing.B) {
	columns := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: 255, Name: "email"},
		{Kind: Varchar, Size: 255, Name: "name"},
		{Kind: Boolean, Size: 1, Name: "active"},
		{Kind: Int4, Size: 4, Name: "score"},
		{Kind: Timestamp, Size: 8, Name: "created"},
	}

	// Build a cell using the new self-describing format.
	src := Cell{
		NullBitmask: 0,
		Key:         123,
		ColumnCount: uint8(len(columns)),
		TypeCodes:   make([]byte, len(columns)),
	}
	for i, col := range columns {
		src.TypeCodes[i] = byte(kindToTypeCode(col.Kind))
	}

	// Marshal value bytes manually to match the packed layout.
	email := "test@example.com"
	name := "John Doe"
	valueBuf := make([]byte, 0, 128)
	// id (Int8)
	tmp8 := make([]byte, 8)
	marshalInt64(tmp8, 123, 0)
	valueBuf = append(valueBuf, tmp8...)
	// email (Varchar: 4-byte length + data)
	tmp4 := make([]byte, 4)
	marshalInt32(tmp4, int32(len(email)), 0)
	valueBuf = append(valueBuf, tmp4...)
	valueBuf = append(valueBuf, []byte(email)...)
	// name (Varchar)
	marshalInt32(tmp4, int32(len(name)), 0)
	valueBuf = append(valueBuf, tmp4...)
	valueBuf = append(valueBuf, []byte(name)...)
	// active (Boolean)
	valueBuf = append(valueBuf, 1)
	// score (Int4)
	marshalInt32(tmp4, 100, 0)
	valueBuf = append(valueBuf, tmp4...)
	// created (Timestamp/Int8)
	marshalInt64(tmp8, 1640000000, 0)
	valueBuf = append(valueBuf, tmp8...)
	src.Value = valueBuf

	// Marshal the whole cell into a buffer.
	buf := make([]byte, src.Size())
	src.Marshal(buf)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		testCell := Cell{}
		_, err := testCell.Unmarshal(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
