package minisql

// CompositeKey holds the column definitions and values for a multi-column index key.
type CompositeKey struct {
	Columns []Column
	Values  []any
	// Store the binary representation for comparison (excludes length prefixes for varchars)
	Comparison []byte
}

// NewCompositeKey constructs a CompositeKey from the given columns and values.
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

// Size returns the serialised byte size of the composite key.
func (ck CompositeKey) Size() uint64 {
	size := 0
	for i, col := range ck.Columns {
		switch col.Kind {
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

// Prefix returns a CompositeKey containing only the first n columns' comparison bytes.
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

// Marshal serialises the composite key into buf starting at offset i.
func (ck *CompositeKey) Marshal(buf []byte, i uint64) error {
	offset := uint64(0)
	for j, col := range ck.Columns {
		switch col.Kind {
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

// Unmarshal deserialises a composite key from buf starting at offset i, returning bytes consumed.
func (ck *CompositeKey) Unmarshal(buf []byte, i uint64) (uint64, error) {
	// Pass 1: scan buf to compute the exact comparison size so we allocate precisely.
	// For fixed-width types the size is known from the column schema; for Varchar we
	// read the length prefix from buf (4 bytes ahead of the data).
	compSize := uint64(0)
	scanOff := uint64(0)
	for _, col := range ck.Columns {
		switch col.Kind {
		case Boolean:
			compSize += 1
			scanOff += 1
		case Int4, Real:
			compSize += 4
			scanOff += 4
		case Int8, Timestamp, Double:
			compSize += 8
			scanOff += 8
		case Varchar:
			length := uint64(unmarshalUint32(buf, scanOff))
			compSize += length
			scanOff += 4 + length
		}
	}

	comparison := make([]byte, compSize)
	var (
		offset     = uint64(0)
		compOffset = uint64(0)
	)
	for _, col := range ck.Columns {
		switch col.Kind {
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

// comparisonSize returns the byte length of the comparison buffer, which intentionally
// excludes the length prefix for Varchar columns (unlike Size() which includes it).
func (ck CompositeKey) comparisonSize() uint64 {
	size := uint64(0)
	for i, col := range ck.Columns {
		switch col.Kind {
		case Boolean:
			size += 1
		case Int4, Real:
			size += 4
		case Int8, Timestamp, Double:
			size += 8
		case Varchar:
			size += uint64(len(ck.Values[i].(string)))
		}
	}
	return size
}

func (ck *CompositeKey) generateComparison() {
	ck.Comparison = make([]byte, ck.comparisonSize())
	offset := uint64(0)

	for j, col := range ck.Columns {
		switch col.Kind {
		case Boolean:
			marshalBool(ck.Comparison, ck.Values[j].(bool), offset)
			offset += 1
		case Int4:
			marshalInt32(ck.Comparison, ck.Values[j].(int32), offset)
			offset += 4
		case Int8, Timestamp:
			marshalInt64(ck.Comparison, ck.Values[j].(int64), offset)
			offset += 8
		case Real:
			marshalFloat32(ck.Comparison, ck.Values[j].(float32), offset)
			offset += 4
		case Double:
			marshalFloat64(ck.Comparison, ck.Values[j].(float64), offset)
			offset += 8
		case Varchar:
			data := ck.Values[j].(string)
			copy(ck.Comparison[offset:], data)
			offset += uint64(len(data))
		}
	}
}
