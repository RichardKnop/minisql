package minisql

// TypeCode is a one-byte tag stored per column in every leaf cell.  It records
// the on-disk type of the column as it existed when the row was written,
// independently of the current schema.  This makes cells self-describing:
//
//   - Cell.Unmarshal no longer needs the table schema to decode byte widths.
//   - Lazy ADD COLUMN: rows written before a column was added carry a smaller
//     ColumnCount; readers return the column default for positions ≥ ColumnCount.
//   - Tombstone DROP COLUMN: the schema marks a column Deleted=true.  Old rows
//     still have real bytes at that position (TypeCode carries the original kind
//     so the reader can advance past them).  New rows written after DROP carry
//     TypeCodeNull at the deleted position (0 bytes, NullBitmask bit set).
type TypeCode byte

// TypeCode constants map each ColumnKind to its one-byte on-disk tag.
// TypeCodeNull marks dropped-column placeholder slots (0 bytes in the value area).
// TypeCodeText covers Varchar, Text, and JSON — all use the 4-byte length-prefix + data encoding.
const (
	TypeCodeNull      TypeCode = 0
	TypeCodeBool      TypeCode = 1
	TypeCodeInt4      TypeCode = 2
	TypeCodeInt8      TypeCode = 3
	TypeCodeReal      TypeCode = 4
	TypeCodeDouble    TypeCode = 5
	TypeCodeTimestamp TypeCode = 6
	TypeCodeUUID      TypeCode = 7
	TypeCodeText      TypeCode = 8
	TypeCodeVector    TypeCode = 9
)

// kindToTypeCode maps a ColumnKind to its TypeCode.
func kindToTypeCode(k ColumnKind) TypeCode {
	switch k {
	case Boolean:
		return TypeCodeBool
	case Int4:
		return TypeCodeInt4
	case Int8:
		return TypeCodeInt8
	case Real:
		return TypeCodeReal
	case Double:
		return TypeCodeDouble
	case Timestamp:
		return TypeCodeTimestamp
	case UUID:
		return TypeCodeUUID
	case Varchar, Text, JSON:
		return TypeCodeText
	case Vector:
		return TypeCodeVector
	default:
		return TypeCodeNull
	}
}

// TypeCodesFromColumns builds the TypeCodes slice for a column list.
// Deleted columns get TypeCodeNull; all others get their declared kind's code.
func TypeCodesFromColumns(columns []Column) []byte {
	codes := make([]byte, len(columns))
	for i, col := range columns {
		if col.Deleted {
			codes[i] = byte(TypeCodeNull)
		} else {
			codes[i] = byte(kindToTypeCode(col.Kind))
		}
	}
	return codes
}

// typeCodeFixedSize returns the fixed byte width for the TypeCode's value in
// the packed cell buffer.  Returns 0 for TypeCodeNull and -1 for TypeCodeText
// (variable-length; caller must read the 4-byte length prefix).
func typeCodeFixedSize(tc TypeCode) int {
	switch tc {
	case TypeCodeNull:
		return 0
	case TypeCodeBool:
		return 1
	case TypeCodeInt4, TypeCodeReal:
		return 4
	case TypeCodeInt8, TypeCodeDouble, TypeCodeTimestamp:
		return 8
	case TypeCodeUUID:
		return 16
	case TypeCodeText:
		return -1
	case TypeCodeVector:
		return 8 // 4-byte dims + 4-byte first overflow page index
	default:
		return 0
	}
}
