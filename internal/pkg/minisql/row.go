package minisql

import (
	"fmt"
	"unsafe"
)

// TODO - RowID will be used as key for B-tree data structure
type RowID int64

type Row struct {
	ID      RowID
	Columns []Column
	Values  []any
}

func NewRow(columns []Column) Row {
	return Row{Columns: columns}
}

// Size calculates a size of a row record including a header and payload
func (r Row) Size() uint32 {
	size := uint32(0)
	for _, aColumn := range r.Columns {
		size += aColumn.Size
	}
	return size
}

func (r Row) GetColumn(name string) (Column, bool) {
	for _, aColumn := range r.Columns {
		if aColumn.Name == name {
			return aColumn, true
		}
	}
	return Column{}, false
}

func (r Row) columnOffset(idx int) uint32 {
	offset := uint32(0)
	for i := 0; i < idx; i++ {
		offset += r.Columns[i].Size
	}
	return offset
}

func (r Row) appendValues(fields []string, values []any) Row {
	var (
		found    = false
		fieldIdx = 0
	)
	for _, aColumn := range r.Columns {
		for i, field := range fields {
			if field == aColumn.Name {
				found = true
				fieldIdx = i
				break
			}
		}
		if found {
			r.Values = append(r.Values, values[fieldIdx])
		} else {
			r.Values = append(r.Values, nil)
		}
	}
	return r
}

// func (r Row) headerSize() uint32 {
// 	// First we start with header, first byte determines total number of bytes in the header
// 	size := 1
// 	// Following are n bytes where n is number of columns, each one defines datatype and size of payload:
// 	// 0 - NULL
// 	// 1 - INT4
// 	// 2 - INT8
// 	// 3 - VARCHAR
// 	size += len(r.Columns)
// 	return uint32(size)
// }

// func (r Row) header() []byte {
// 	headerSize := r.headerSize()
// 	header := make([]byte, headerSize)
// 	header[0] = byte(headerSize)
// 	for i, aColumn := range r.Columns {
// 		if r.Values[i] == nil {
// 			header[i+1] = 0
// 		} else {
// 			switch aColumn.Kind {
// 			case Int4:
// 				header[i+1] = 1
// 			case Int8:
// 				header[i+1] = 2
// 			case Varchar:
// 				header[i+1] = 3
// 			}
// 		}
// 	}
// 	return header
// }

// TODO - handle NULL values
func (r Row) Marshal() ([]byte, error) {
	buf := make([]byte, r.Size())

	for i, aColumn := range r.Columns {
		offset := r.columnOffset(i)
		switch aColumn.Kind {
		case Int4:
			value, ok := r.Values[i].(int32)
			if !ok {
				return nil, fmt.Errorf("could not cast value to int32")
			}
			serializeInt4(value, buf, offset)
			offset += uint32(aColumn.Size)
		case Int8:
			value, ok := r.Values[i].(int64)
			if !ok {
				return nil, fmt.Errorf("could not cast value to int64")
			}
			serializeInt8(value, buf, offset)
			offset += uint32(aColumn.Size)
		case Varchar:
			value, ok := r.Values[i].(string)
			if !ok {
				return nil, fmt.Errorf("could not cast value to string")
			}
			serializeString(value, buf, offset)
			offset += uint32(aColumn.Size)
		}
	}

	return buf, nil
}

// TODO - handle NULL values
func UnmarshalRow(buf []byte, aRow *Row) error {
	aRow.Values = make([]any, 0, len(aRow.Columns))
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

func serializeInt4(value int32, buf []byte, offset uint32) {
	src := unsafe.Pointer(&value)
	theSrc := *((*[4]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToInt4(buf []byte, offset uint32) int32 {
	destValue := int32(0)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[4]byte)(dest))

	copy(theDest[:], buf[offset:offset+4])

	return destValue
}

func serializeInt8(value int64, buf []byte, offset uint32) {
	src := unsafe.Pointer(&value)
	theSrc := *((*[8]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToInt8(buf []byte, offset uint32) int64 {
	destValue := int64(0)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[8]byte)(dest))

	copy(theDest[:], buf[offset:offset+8])

	return destValue
}

func serializeString(value string, buf []byte, offset uint32) {
	const size = unsafe.Sizeof(value)
	src := unsafe.Pointer(&value)
	theSrc := *((*[size]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToString(buf []byte, offset, length uint32) string {
	destValue := ""
	const size = unsafe.Sizeof(destValue)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[size]byte)(dest))

	copy(theDest[:], buf[offset:offset+length])

	return string(destValue)
}
