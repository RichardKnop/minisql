package minisql

import (
	"bytes"
	"fmt"
)

// TODO - RowID will be used as key for B-tree data structure
type RowID int64

type Row struct {
	ID      RowID
	Columns []Column
	Values  []any
}

// MaxCells returns how many rows can be stored in a single page
func (r Row) MaxCells() uint32 {
	return maxCells(r.Size())
}

func maxCells(rowSize uint64) uint32 {
	return uint32(PageSize / (rowSize + 8)) // +8 for int64 row ID
}

func NewRow(columns []Column) Row {
	return Row{Columns: columns}
}

// Size calculates a size of a row record including a header and payload
func (r Row) Size() uint64 {
	size := uint64(0)
	for _, aColumn := range r.Columns {
		size += uint64(aColumn.Size)
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

// TODO - handle NULL values
func (r *Row) Marshal() ([]byte, error) {
	buf := make([]byte, r.Size())

	for i, aColumn := range r.Columns {
		offset := r.columnOffset(i)
		switch aColumn.Kind {
		case Int4:
			value, ok := r.Values[i].(int32)
			if !ok {
				_, ok = r.Values[i].(int64)
				if !ok {
					return nil, fmt.Errorf("could not cast value to either int64 or int32")
				}
				value = int32(r.Values[i].(int64))
			}
			buf[offset+0] = byte(value >> 0)
			offset += 1
			buf[offset+0] = byte(value >> 8)
			offset += 1
			buf[offset+0] = byte(value >> 16)
			offset += 1
			buf[offset+0] = byte(value >> 24)
			offset += 1
		case Int8:
			value, ok := r.Values[i].(int64)
			if !ok {
				return nil, fmt.Errorf("could not cast value to int64")
			}
			buf[offset+0] = byte(value >> 0)
			offset += 1
			buf[offset+0] = byte(value >> 8)
			offset += 1
			buf[offset+0] = byte(value >> 16)
			offset += 1
			buf[offset+0] = byte(value >> 24)
			offset += 1
			buf[offset+0] = byte(value >> 32)
			offset += 1
			buf[offset+0] = byte(value >> 40)
			offset += 1
			buf[offset+0] = byte(value >> 48)
			offset += 1
			buf[offset+0] = byte(value >> 56)
			offset += 1
		case Varchar:
			value, ok := r.Values[i].(string)
			if !ok {
				return nil, fmt.Errorf("could not cast value to string")
			}
			src := make([]byte, len(value))
			copy(src, []byte(value))
			copy(buf[offset:], src)
			offset += aColumn.Size
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
			value := 0 |
				(uint32(buf[offset+0+0]) << 0) |
				(uint32(buf[offset+1+0]) << 8) |
				(uint32(buf[offset+2+0]) << 16) |
				(uint32(buf[offset+3+0]) << 24)
			aRow.Values = append(aRow.Values, int32(value))
			offset += 4
		case Int8:
			value := 0 |
				(uint64(buf[offset+0+0]) << 0) |
				(uint64(buf[offset+1+0]) << 8) |
				(uint64(buf[offset+2+0]) << 16) |
				(uint64(buf[offset+3+0]) << 24) |
				(uint64(buf[offset+4+0]) << 32) |
				(uint64(buf[offset+5+0]) << 40) |
				(uint64(buf[offset+6+0]) << 48) |
				(uint64(buf[offset+7+0]) << 56)
			aRow.Values = append(aRow.Values, int64(value))
		case Varchar:
			dst := make([]byte, aColumn.Size)
			copy(dst, buf[offset:aColumn.Size])
			aRow.Values = append(aRow.Values, string(bytes.Trim(dst, "\x00")))
		}
	}

	return nil
}
