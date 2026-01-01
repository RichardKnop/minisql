package minisql

type CompositeKey struct {
	Columns []Column
	Values  []any
	// Store the binary representation for comparison (excludes length prefixes for varchars)
	Comparison []byte
}

func NewCompositeKey(columns []Column, values ...any) CompositeKey {
	ck := CompositeKey{
		Columns: columns,
		Values:  values,
	}
	if len(values) > 0 {
		ck.generateComparison()
	}
	return ck
}

func (ck CompositeKey) Size() uint64 {
	size := 0
	for i, aColumn := range ck.Columns {
		switch aColumn.Kind {
		case Boolean:
			size += 1
		case Int4:
			size += 4
		case Int8, Timestamp:
			size += 8
		case Real:
			size += 4
		case Double:
			size += 8
		case Varchar:
			size += varcharLengthPrefixSize + len(ck.Values[i].(string))
		}
	}
	return uint64(size)
}

func (ck CompositeKey) Prefix(columns int) CompositeKey {
	if columns > len(ck.Columns) {
		columns = len(ck.Columns)
	}

	offset := uint64(0)
	for i := 0; i < columns; i++ {
		switch ck.Columns[i].Kind {
		case Boolean:
			offset += 1
		case Int4:
			offset += 4
		case Int8, Timestamp:
			offset += 8
		case Real:
			offset += 4
		case Double:
			offset += 8
		case Varchar:
			offset += uint64(len(ck.Values[i].(string)))
		}
	}

	return CompositeKey{Comparison: ck.Comparison[:offset]}
}

func (ck *CompositeKey) Marshal(buf []byte, i uint64) error {
	offset := uint64(0)
	for j, aColumn := range ck.Columns {
		switch aColumn.Kind {
		case Boolean:
			marshalBool(buf, ck.Values[j].(bool), offset)
			offset += 1
		case Int4:
			marshalInt32(buf, ck.Values[j].(int32), offset)
			offset += 4
		case Int8, Timestamp:
			marshalInt64(buf, ck.Values[j].(int64), offset)
			offset += 8
		case Real:
			marshalFloat32(buf, ck.Values[j].(float32), offset)
			offset += 4
		case Double:
			marshalFloat64(buf, ck.Values[j].(float64), offset)
			offset += 8
		case Varchar:
			data := []byte(ck.Values[j].(string))
			marshalUint32(buf, uint32(len(data)), offset)
			offset += 4
			copy(buf[offset:offset+uint64(len(data))], data)
			offset += uint64(len(data))
		}
	}

	return nil
}

func (ck *CompositeKey) Unmarshal(buf []byte, i uint64) (uint64, error) {
	var (
		offset     = uint64(0)
		compOffset = uint64(0)
	)
	// We don't know the size of the composite key upfront, so allocate a largest possible buffer.
	// We limit max combined size to MaxInlineVarchar plus add potential lenghth prefixes.
	comparison := make([]byte, MaxInlineVarchar*uint64(len(ck.Columns)*varcharLengthPrefixSize))
	for _, aColumn := range ck.Columns {
		switch aColumn.Kind {
		case Boolean:
			ck.Values = append(ck.Values, unmarshalBool(buf, offset))
			copy(comparison[compOffset:compOffset+1], buf[offset:offset+1])
			compOffset += 1
			offset += 1
		case Int4:
			ck.Values = append(ck.Values, unmarshalInt32(buf, offset))
			copy(comparison[compOffset:compOffset+4], buf[offset:offset+4])
			compOffset += 4
			offset += 4
		case Int8, Timestamp:
			ck.Values = append(ck.Values, unmarshalInt64(buf, offset))
			copy(comparison[compOffset:compOffset+8], buf[offset:offset+8])
			compOffset += 8
			offset += 8
		case Real:
			ck.Values = append(ck.Values, unmarshalFloat32(buf, offset))
			copy(comparison[compOffset:compOffset+4], buf[offset:offset+4])
			compOffset += 4
			offset += 4
		case Double:
			ck.Values = append(ck.Values, unmarshalFloat64(buf, offset))
			copy(comparison[compOffset:compOffset+8], buf[offset:offset+8])
			compOffset += 8
			offset += 8
		case Varchar:
			length := uint64(unmarshalUint32(buf, offset))
			offset += 4
			ck.Values = append(ck.Values, string(buf[offset:offset+length]))
			copy(comparison[compOffset:compOffset+length], buf[offset:offset+length])
			compOffset += length
			offset += length
		}
	}
	ck.Comparison = comparison[:compOffset]

	return offset, nil
}

func (ck *CompositeKey) generateComparison() {
	offset := uint64(0)

	for j, aColumn := range ck.Columns {
		switch aColumn.Kind {
		case Boolean:
			buf := make([]byte, 1)
			ck.Comparison = append(ck.Comparison, buf...)
			marshalBool(ck.Comparison, ck.Values[j].(bool), offset)
			offset += 1
		case Int4:
			buf := make([]byte, 4)
			ck.Comparison = append(ck.Comparison, buf...)
			marshalInt32(ck.Comparison, ck.Values[j].(int32), offset)

			offset += 4
		case Int8, Timestamp:
			buf := make([]byte, 8)
			ck.Comparison = append(ck.Comparison, buf...)
			marshalInt64(ck.Comparison, ck.Values[j].(int64), offset)
			offset += 8
		case Real:
			buf := make([]byte, 4)
			ck.Comparison = append(ck.Comparison, buf...)
			marshalFloat32(ck.Comparison, ck.Values[j].(float32), offset)
			offset += 4
		case Double:
			buf := make([]byte, 8)
			ck.Comparison = append(ck.Comparison, buf...)
			marshalFloat64(ck.Comparison, ck.Values[j].(float64), offset)
			offset += 8
		case Varchar:
			data := []byte(ck.Values[j].(string))
			ck.Comparison = append(ck.Comparison, data...)
			offset += uint64(len(data))
		}
	}
}
