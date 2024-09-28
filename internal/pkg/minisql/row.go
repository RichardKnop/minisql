package minisql

import (
	"unsafe"
)

type Row struct {
	Columns []Column
	Values  []interface{}
}

func (r Row) Size() int {
	return rowSize(r.Columns...)
}

func rowSize(columns ...Column) int {
	size := 0
	for _, aColumn := range columns {
		size += aColumn.Size
	}
	return size
}

func (r Row) columnOffset(idx int) int {
	offset := 0
	for i := 0; i < idx; i++ {
		offset += r.Columns[i].Size
	}
	return offset
}

func (r Row) Marshal() ([]byte, error) {
	buf := make([]byte, r.Size())

	for i, aColumn := range r.Columns {
		offset := r.columnOffset(i)
		switch aColumn.Kind {
		case Int4:
			value := r.Values[i].(int32)
			serializeInt4(value, buf, offset)
		case Int8:
			value := r.Values[i].(int64)
			serializeInt8(value, buf, offset)
		case Varchar:
			value := r.Values[i].(string)
			serializeString(value, buf, offset)
		}
	}

	return buf, nil
}

func UnmarshalRow(buf []byte, aRow *Row) error {
	aRow.Values = make([]interface{}, 0, len(aRow.Columns))
	for i, aColumn := range aRow.Columns {
		offset := aRow.columnOffset(i)
		switch aColumn.Kind {
		case Int4:
			value := deserializeToInt4(buf, offset)
			aRow.Values = append(aRow.Values, value)
		case Int8:
			value := deserializeToInt8(buf, offset)
			aRow.Values = append(aRow.Values, value)
		case Varchar:
			value := deserializeToString(buf, offset, aColumn.Size)
			aRow.Values = append(aRow.Values, value)
		}
	}

	return nil
}

func serializeInt4(value int32, buf []byte, offset int) {
	src := unsafe.Pointer(&value)
	theSrc := *((*[4]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToInt4(buf []byte, offset int) int32 {
	destValue := int32(0)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[4]byte)(dest))

	copy(theDest[:], buf[offset:offset+4])

	return destValue
}

func serializeInt8(value int64, buf []byte, offset int) {
	src := unsafe.Pointer(&value)
	theSrc := *((*[8]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToInt8(buf []byte, offset int) int64 {
	destValue := int64(0)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[8]byte)(dest))

	copy(theDest[:], buf[offset:offset+8])

	return destValue
}

func serializeString(value string, buf []byte, offset int) {
	const size = unsafe.Sizeof(value)
	src := unsafe.Pointer(&value)
	theSrc := *((*[size]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToString(buf []byte, offset int, length int) string {
	destValue := ""
	const size = unsafe.Sizeof(destValue)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[size]byte)(dest))

	copy(theDest[:], buf[offset:offset+length])

	return string(destValue)
}
