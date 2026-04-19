package minisql

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/RichardKnop/minisql/pkg/bitwise"
)

// OptionalValue ...
type OptionalValue struct {
	Value any
	Valid bool
}

// RowID ...
type RowID uint64

// Row ...
type Row struct {
	Key     RowID
	Columns []Column
	Values  []OptionalValue

	columnCache map[string]int
}

func maxCells(rowSize uint64) uint32 {
	// base header is +6, leaf/internal header +8
	// and uint64 row ID per cell
	// hence we divide by rowSize + 8 + 8
	return uint32((PageSize - headerSize()) / (rowSize + 8 + 8))
}

// NewRow ...
func NewRow(columns []Column) Row {
	row := Row{
		Columns:     columns,
		columnCache: make(map[string]int, len(columns)),
	}
	for i, col := range columns {
		row.columnCache[col.Name] = i
	}
	return row
}

// NewRowWithValues ...
func NewRowWithValues(columns []Column, values []OptionalValue) Row {
	row := Row{
		Columns:     columns,
		Values:      values,
		columnCache: make(map[string]int, len(columns)),
	}
	for i, col := range columns {
		row.columnCache[col.Name] = i
	}
	return row
}

// Size calculates a size of a row record excluding null bitmask and row ID
func (r Row) Size() uint64 {
	size := uint64(0)
	for i, col := range r.Columns {
		// Skip NULL values - they take no space (tracked in bitmask)
		if !r.Values[i].Valid {
			continue
		}

		if !col.Kind.IsText() {
			size += uint64(col.Size)
			continue
		}
		size += varcharLengthPrefixSize

		s, ok := r.Values[i].Value.(string)
		if ok {
			if uint64(len(s)) <= MaxInlineVarchar {
				size += uint64(len(s))
			} else {
				size += 4 // first overflow page index
			}
			continue
		}

		tp, ok := r.Values[i].Value.(TextPointer)
		if ok {
			if uint64(len(tp.Data)) <= MaxInlineVarchar {
				size += uint64(len(tp.Data))
			} else {
				size += 4 // first overflow page index
			}
			continue
		}

		panic(fmt.Sprintf("cannot calculate size for non-string/textpointer value for text column: %v, type: %T", r.Values[i].Value, r.Values[i].Value))
	}
	return size
}

// OnlyFields ...
func (r Row) OnlyFields(fields ...Field) Row {
	filteredRow := Row{
		Key:         r.Key,
		Columns:     make([]Column, len(fields)),
		Values:      make([]OptionalValue, len(fields)),
		columnCache: make(map[string]int, len(fields)),
	}

	// Pre-allocate exact size and write directly by index
	outIdx := 0
	for _, field := range fields {
		// For fields with an alias prefix, look for "alias.name" format
		// For fields without, look for just "name"
		var lookupName string
		if field.AliasPrefix != "" {
			lookupName = field.AliasPrefix + "." + field.Name
		} else {
			lookupName = field.Name
		}

		if _, idx := r.GetColumn(lookupName); idx >= 0 {
			filteredRow.Columns[outIdx] = r.Columns[idx]
			filteredRow.columnCache[field.Name] = outIdx // Store without prefix for Scan
			filteredRow.Values[outIdx] = r.Values[idx]
			outIdx++
		}
	}

	// Trim to actual size if some fields weren't found
	if outIdx < len(fields) {
		filteredRow.Columns = filteredRow.Columns[:outIdx]
		filteredRow.Values = filteredRow.Values[:outIdx]
	}

	return filteredRow
}

// GetColumn ...
func (r Row) GetColumn(name string) (Column, int) {
	if idx, ok := r.columnCache[name]; ok {
		return r.Columns[idx], idx
	}
	return Column{}, -1
}

// GetValue ...
func (r Row) GetValue(name string) (OptionalValue, bool) {
	idx, ok := r.columnCache[name]
	if !ok || idx >= len(r.Values) {
		return OptionalValue{}, false
	}
	return r.Values[idx], true
}

// GetValuesForColumns ...
func (r Row) GetValuesForColumns(columns []Column) ([]OptionalValue, bool) {
	// Pre-allocate exact size and write directly by index
	values := make([]OptionalValue, len(columns))
	for i, col := range columns {
		value, ok := r.GetValue(col.Name)
		if !ok {
			return nil, false
		}
		values[i] = value
	}

	return values, true
}

// SetValue returns true if value has changed
func (r Row) SetValue(name string, value OptionalValue) (Row, bool) {
	columnIdx, ok := r.columnCache[name]
	if !ok {
		return r, false
	}
	if !compareValue(r.Columns[columnIdx].Kind, r.Values[columnIdx], value) {
		r.Values[columnIdx] = value
		return r, true
	}
	return r, false
}

func compareValue(kind ColumnKind, v1, v2 OptionalValue) bool {
	if !v1.Valid && !v2.Valid {
		return true
	}
	if v1.Valid != v2.Valid {
		return false
	}
	if !kind.IsText() {
		return v1.Value == v2.Value
	}
	tp1, ok := v1.Value.(TextPointer)
	if !ok {
		return false
	}
	tp2, ok := v2.Value.(TextPointer)
	if !ok {
		return false
	}
	return tp1.IsEqual(tp2)
}

// Clone ...
func (r Row) Clone() Row {
	rowCopy := Row{
		Key:         r.Key,
		Columns:     r.Columns,
		columnCache: r.columnCache,
		Values:      make([]OptionalValue, len(r.Values)),
	}
	copy(rowCopy.Values, r.Values)
	return rowCopy
}

// AppendValues ...
func (r Row) AppendValues(fields []Field, values []OptionalValue) Row {
	for _, col := range r.Columns {
		var (
			found    = false
			fieldIdx = 0
		)
		for i, field := range fields {
			if field.Name == col.Name {
				found = true
				fieldIdx = i
				break
			}
		}
		if found {
			r.Values = append(r.Values, values[fieldIdx])
		} else {
			r.Values = append(r.Values, OptionalValue{})
		}
	}
	return r
}

// Marshal ...
func (r Row) Marshal() ([]byte, error) {
	// Single allocation: allocate exact size upfront instead of using append
	size := r.Size()
	buf := make([]byte, size)

	offset := uint64(0)
	for i, col := range r.Columns {
		if !r.Values[i].Valid {
			continue // NULL values take no space (tracked in bitmask)
		}
		switch col.Kind {
		case Boolean:
			value, ok := r.Values[i].Value.(bool)
			if !ok {
				return nil, errors.New("could not cast value to bool")
			}
			marshalBool(buf, value, offset)
			offset += 1
		case Int4:
			value, ok := r.Values[i].Value.(int32)
			if !ok {
				n, ok2 := r.Values[i].Value.(int64)
				if !ok2 {
					return nil, fmt.Errorf("could not cast value for column %s to either int64 or int32", col.Name)
				}
				if n < math.MinInt32 || n > math.MaxInt32 {
					return nil, fmt.Errorf("value %d overflows INT4 for column %s", n, col.Name)
				}
				value = int32(n)
			}
			marshalInt32(buf, value, offset)
			offset += 4
		case Int8:
			value, ok := r.Values[i].Value.(int64)
			if !ok {
				return nil, fmt.Errorf("could not cast value for column %s to int64", col.Name)
			}
			marshalInt64(buf, value, offset)
			offset += 8
		case Real:
			value, ok := r.Values[i].Value.(float32)
			if !ok {
				_, ok = r.Values[i].Value.(float64)
				if !ok {
					return nil, fmt.Errorf("could not cast value for column %s to either float64 or float32", col.Name)
				}
				value = float32(r.Values[i].Value.(float64))
			}
			marshalFloat32(buf, value, offset)
			offset += 4
		case Double:
			value, ok := r.Values[i].Value.(float64)
			if !ok {
				return nil, fmt.Errorf("could not cast value for column %s to float64", col.Name)
			}
			marshalFloat64(buf, value, offset)
			offset += 8
		case Varchar, Text:
			textPointer, ok := r.Values[i].Value.(TextPointer)
			if !ok {
				return nil, fmt.Errorf("could not cast value for column %s to text pointer", col.Name)
			}

			if err := textPointer.Marshal(buf, offset); err != nil {
				return nil, err
			}
			offset += textPointer.Size()
		case Timestamp:
			value, ok := r.Values[i].Value.(Time)
			if !ok {
				return nil, fmt.Errorf("could not cast value for column %s to time", col.Name)
			}
			marshalInt64(buf, value.TotalMicroseconds(), offset)
			offset += 8
		}
	}

	return buf, nil
}

// Unmarshal decodes cell into a Row. For columns not in selectedFields, an empty
// OptionalValue is inserted to maintain index alignment.
func (r Row) Unmarshal(cell Cell, selectedFields ...Field) (Row, error) {
	r.Key = cell.Key

	// Initialize column cache if not already present
	if r.columnCache == nil {
		r.columnCache = make(map[string]int, len(r.Columns))
		for i, col := range r.Columns {
			r.columnCache[col.Name] = i
		}
	}

	if len(selectedFields) == 0 {
		r.Values = make([]OptionalValue, len(r.Columns))
		return r, nil
	}

	// Pre-allocate exact size and write directly by index instead of append
	r.Values = make([]OptionalValue, len(r.Columns))

	offset := 0
	for i, col := range r.Columns {
		// Check if column is NULL
		isNull := bitwise.IsSet(cell.NullBitmask, i)

		// If column not selected, skip it but track offset (inline linear search, zero allocation)
		if !isColumnSelected(col.Name, selectedFields) {
			r.Values[i] = OptionalValue{Valid: false}
			if !isNull {
				// Skip over the data without unmarshaling
				offset += int(r.getColumnSize(col, cell.Value, offset))
			}
			continue
		}

		if isNull {
			r.Values[i] = OptionalValue{Valid: false}
			continue
		}
		switch col.Kind {
		case Boolean:
			value := unmarshalBool(cell.Value, uint64(offset))
			r.Values[i] = OptionalValue{Value: value, Valid: true}
			offset += 1
		case Int4:
			value := unmarshalInt32(cell.Value, uint64(offset))
			r.Values[i] = OptionalValue{Value: int32(value), Valid: true}
			offset += 4
		case Int8:
			value := unmarshalInt64(cell.Value, uint64(offset))
			r.Values[i] = OptionalValue{Value: int64(value), Valid: true}
			offset += 8
		case Real:
			value := unmarshalFloat32(cell.Value, uint64(offset))
			r.Values[i] = OptionalValue{Value: value, Valid: true}
			offset += 4
		case Double:
			value := unmarshalFloat64(cell.Value, uint64(offset))
			r.Values[i] = OptionalValue{Value: value, Valid: true}
			offset += 8
		case Varchar, Text:
			textPointer := TextPointer{}
			if err := textPointer.Unmarshal(cell.Value, uint64(offset)); err != nil {
				return Row{}, err
			}
			if textPointer.IsInline() {
				textPointer.Data = bytes.Trim(textPointer.Data, "\x00")
			}
			offset += int(textPointer.Size())
			r.Values[i] = OptionalValue{Value: textPointer, Valid: true}
		case Timestamp:
			value := unmarshalInt64(cell.Value, uint64(offset))
			r.Values[i] = OptionalValue{Value: FromMicroseconds(int64(value)), Valid: true}
			offset += 8
		}
	}

	return r, nil
}

// isColumnSelected reports whether colName appears in selectedFields.
// It uses a linear scan so that callers avoid allocating a map for small field lists.
func isColumnSelected(colName string, selectedFields []Field) bool {
	for j := range selectedFields {
		if selectedFields[j].Name == colName {
			return true
		}
	}
	return false
}

func (r Row) getColumnSize(col Column, data []byte, offset int) uint64 {
	switch col.Kind {
	case Boolean:
		return 1
	case Int4:
		return 4
	case Int8:
		return 8
	case Real:
		return 4
	case Double:
		return 8
	case Varchar, Text:
		// Read length prefix to determine size
		length := unmarshalUint32(data, uint64(offset))
		return TextPointer{Length: length}.Size()
	case Timestamp:
		return 8
	}
	return 0
}

// CheckOneOrMore checks whether row satisfies one or more sets of conditions
// (cond1 AND cond2) OR (cond3 and cond4) ... etc
func (r Row) CheckOneOrMore(conditions OneOrMore) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}

	for _, condGroup := range conditions {
		ok, err := r.CheckConditions(condGroup)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

// CheckConditions ...
func (r Row) CheckConditions(condGroup Conditions) (bool, error) {
	if len(condGroup) == 0 {
		return true, nil
	}

	groupConditionResult := true
	for _, cond := range condGroup {
		ok, err := r.checkCondition(cond)
		if err != nil {
			return false, err
		}

		if !ok {
			groupConditionResult = false
			break
		}
	}

	if groupConditionResult {
		return true, nil
	}

	return false, nil
}

func (r Row) checkCondition(cond Condition) (bool, error) {
	// left side is field, right side is literal value
	if cond.Operand1.IsField() && !cond.Operand2.IsField() {
		return r.compareFieldValue(cond.Operand1, cond.Operand2, cond.Operator)
	}

	// left side is literal value, right side is field
	if cond.Operand2.IsField() && !cond.Operand1.IsField() {
		return r.compareFieldValue(cond.Operand2, cond.Operand1, cond.Operator)
	}

	// both left and right are fields, compare 2 row values
	if cond.Operand1.IsField() && cond.Operand2.IsField() {
		return r.compareFields(cond.Operand1, cond.Operand2, cond.Operator)
	}

	// both left and right are literal values, compare them
	return cond.Operand1.Value == cond.Operand2.Value, nil
}

func (r Row) compareFieldValue(fieldOperand, valueOperand Operand, operator Operator) (bool, error) {
	if fieldOperand.Type != OperandField {
		return false, fmt.Errorf("field operand invalid, type '%d'", fieldOperand.Type)
	}
	if valueOperand.Type == OperandField {
		return false, errors.New("cannot compare column value against field operand")
	}
	name := fieldOperand.Value.(Field).Name
	col, idx := r.GetColumn(name)
	if idx < 0 {
		return false, fmt.Errorf("row does not contain column '%s'", name)
	}
	fieldValue, ok := r.GetValue(col.Name)
	if !ok {
		return false, fmt.Errorf("row does not have '%s' column", name)
	}

	switch valueOperand.Type {
	case OperandNull:
		switch operator {
		case Eq:
			return !fieldValue.Valid, nil
		case Ne:
			return fieldValue.Valid, nil
		default:
			return false, errors.New("only '=' and '!=' operators supported when comparing against NULL")
		}
	case OperandList:
		switch operator {
		case In, NotIn:
			switch col.Kind {
			case Boolean:
				return false, errors.New("IN / NOT IN operator not supported for boolean columns")
			case Int4:
				foundInList, err := isInListInt4(fieldValue.Value, valueOperand.Value)
				if operator == In {
					return foundInList, err
				}
				return !foundInList, err
			case Int8:
				foundInList, err := isInListInt8(fieldValue.Value, valueOperand.Value)
				if operator == In {
					return foundInList, err
				}
				return !foundInList, err
			case Real:
				foundInList, err := isInListReal(fieldValue.Value, valueOperand.Value)
				if operator == In {
					return foundInList, err
				}
				return !foundInList, err
			case Double:
				foundInList, err := isInListDouble(fieldValue.Value, valueOperand.Value)
				if operator == In {
					return foundInList, err
				}
				return !foundInList, err
			case Varchar, Text:
				foundInList, err := isInListText(fieldValue.Value, valueOperand.Value)
				if operator == In {
					return foundInList, err
				}
				return !foundInList, err
			case Timestamp:
				foundInList, err := isInListTimestamp(fieldValue.Value, valueOperand.Value)
				if operator == In {
					return foundInList, err
				}
				return !foundInList, err
			default:
				return false, fmt.Errorf("unknown column kind '%s'", col.Kind)
			}

		case Between, NotBetween:
			list, ok := valueOperand.Value.([]any)
			if !ok || len(list) != 2 {
				return false, errors.New("BETWEEN requires exactly 2 bounds")
			}
			var (
				inRange bool
				err     error
			)
			switch col.Kind {
			case Boolean:
				return false, errors.New("BETWEEN operator not supported for boolean columns")
			case Int4:
				inRange, err = isBetweenInt4(int64(fieldValue.Value.(int32)), list[0], list[1])
			case Int8:
				inRange, err = isBetweenInt8(fieldValue.Value, list[0], list[1])
			case Real:
				inRange, err = isBetweenReal(float64(fieldValue.Value.(float32)), list[0], list[1])
			case Double:
				inRange, err = isBetweenDouble(fieldValue.Value, list[0], list[1])
			case Varchar, Text:
				inRange, err = isBetweenText(fieldValue.Value, list[0], list[1])
			case Timestamp:
				inRange, err = isBetweenTimestamp(fieldValue.Value, list[0], list[1])
			default:
				return false, fmt.Errorf("unknown column kind '%s'", col.Kind)
			}
			if err != nil {
				return false, err
			}
			if operator == Between {
				return inRange, nil
			}
			return !inRange, nil

		default:
			return false, errors.New("only 'IN', 'NOT IN', 'BETWEEN', and 'NOT BETWEEN' operators supported when comparing against list")
		}
	}

	if !fieldValue.Valid {
		return false, nil // NULL cannot be compared to non-NULL value
	}

	if (operator == Like || operator == NotLike) && col.Kind != Varchar && col.Kind != Text {
		return false, errors.New("LIKE / NOT LIKE operator only supported for TEXT and VARCHAR columns")
	}

	switch col.Kind {
	case Boolean:
		return compareBoolean(fieldValue.Value.(bool), valueOperand.Value.(bool), operator)
	case Int4:
		// Int values from parser always come back as int64, int4 row data
		// will come back as int32 and int8 as int64
		return compareInt4(int64(fieldValue.Value.(int32)), valueOperand.Value.(int64), operator)
	case Int8:
		return compareInt8(fieldValue.Value.(int64), valueOperand.Value.(int64), operator)
	case Real:
		return compareReal(float64(fieldValue.Value.(float32)), valueOperand.Value.(float64), operator)
	case Double:
		return compareDouble(fieldValue.Value.(float64), valueOperand.Value.(float64), operator)
	case Varchar, Text:
		return compareText(fieldValue.Value, valueOperand.Value, operator)
	case Timestamp:
		return compareTimestamp(fieldValue.Value, valueOperand.Value, operator)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", col.Kind)
	}
}

func (r Row) compareFields(field1, field2 Operand, operator Operator) (bool, error) {
	if !field1.IsField() {
		return false, fmt.Errorf("field 1 operand invalid, type '%d'", field1.Type)
	}
	if !field2.IsField() {
		return false, fmt.Errorf("field 2 operand invalid, type '%d'", field2.Type)
	}

	if field1.Value == field2.Value {
		return true, nil
	}

	// Extract field names with alias prefix for JOIN support
	f1 := field1.Value.(Field)
	name1 := f1.Name
	if f1.AliasPrefix != "" {
		name1 = f1.AliasPrefix + "." + f1.Name
	}
	aColumn1, idx1 := r.GetColumn(name1)
	if idx1 < 0 {
		return false, fmt.Errorf("row does not contain column '%s'", name1)
	}

	f2 := field2.Value.(Field)
	name2 := f2.Name
	if f2.AliasPrefix != "" {
		name2 = f2.AliasPrefix + "." + f2.Name
	}
	aColumn2, idx2 := r.GetColumn(name2)
	if idx2 < 0 {
		return false, fmt.Errorf("row does not contain column '%s'", name2)
	}

	if aColumn1.Kind != aColumn2.Kind {
		return false, nil
	}

	value1, ok := r.GetValue(aColumn1.Name)
	if !ok {
		return false, fmt.Errorf("row does not have '%s' column", name1)
	}
	value2, ok := r.GetValue(aColumn2.Name)
	if !ok {
		return false, fmt.Errorf("row does not have '%s' column", name2)
	}

	switch aColumn1.Kind {
	case Boolean:
		return compareBoolean(value1.Value, value2.Value, operator)
	case Int4:
		return compareInt4(value1.Value, value2.Value, operator)
	case Int8:
		return compareInt8(value1.Value, value2.Value, operator)
	case Real:
		return compareReal(value1.Value, value2.Value, operator)
	case Double:
		return compareDouble(value1.Value, value2.Value, operator)
	case Varchar, Text:
		return compareText(value1.Value, value2.Value, operator)
	case Timestamp:
		return compareTimestamp(value1.Value, value2.Value, operator)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", aColumn1.Kind)
	}
}

// NullBitmask returns a bitmask representing which columns are NULL
func (r Row) NullBitmask() uint64 {
	var bitmask uint64 = 0
	for i, val := range r.Values {
		if !val.Valid {
			bitmask = bitwise.Set(bitmask, int(i))
		}
	}
	return bitmask
}

func marshalBool(buf []byte, b bool, i uint64) []byte {
	if b {
		buf[i] = byte(1)
		return buf
	}
	buf[i] = byte(0)
	return buf
}

func unmarshalBool(buf []byte, i uint64) bool {
	return buf[i] == 1
}

func marshalUint32(buf []byte, n uint32, i uint64) []byte {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
	return buf
}

func unmarshalUint32(buf []byte, i uint64) uint32 {
	return 0 |
		(uint32(buf[i+0]) << 0) |
		(uint32(buf[i+1]) << 8) |
		(uint32(buf[i+2]) << 16) |
		(uint32(buf[i+3]) << 24)
}

func marshalUint64(buf []byte, n, i uint64) []byte {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
	buf[i+4] = byte(n >> 32)
	buf[i+5] = byte(n >> 40)
	buf[i+6] = byte(n >> 48)
	buf[i+7] = byte(n >> 56)
	return buf
}

func unmarshalUint64(buf []byte, i uint64) uint64 {
	return 0 | (uint64(buf[i+0]) << 0) |
		(uint64(buf[i+1]) << 8) |
		(uint64(buf[i+2]) << 16) |
		(uint64(buf[i+3]) << 24) |
		(uint64(buf[i+4]) << 32) |
		(uint64(buf[i+5]) << 40) |
		(uint64(buf[i+6]) << 48) |
		(uint64(buf[i+7]) << 56)
}

func marshalInt8(buf []byte, n int8, i uint64) []byte {
	buf[i] = byte(n >> 0)
	return buf
}

func unmarshalInt8(buf []byte, i uint64) int8 {
	return 0 | (int8(buf[i]) << 0)
}

func marshalInt32(buf []byte, n int32, i uint64) []byte {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
	return buf
}

func unmarshalInt32(buf []byte, i uint64) int32 {
	return 0 |
		(int32(buf[i+0]) << 0) |
		(int32(buf[i+1]) << 8) |
		(int32(buf[i+2]) << 16) |
		(int32(buf[i+3]) << 24)
}

func marshalInt64(buf []byte, n int64, i uint64) []byte {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
	buf[i+4] = byte(n >> 32)
	buf[i+5] = byte(n >> 40)
	buf[i+6] = byte(n >> 48)
	buf[i+7] = byte(n >> 56)
	return buf
}

func unmarshalInt64(buf []byte, i uint64) int64 {
	return 0 |
		(int64(buf[i+0]) << 0) |
		(int64(buf[i+1]) << 8) |
		(int64(buf[i+2]) << 16) |
		(int64(buf[i+3]) << 24) |
		(int64(buf[i+4]) << 32) |
		(int64(buf[i+5]) << 40) |
		(int64(buf[i+6]) << 48) |
		(int64(buf[i+7]) << 56)
}

func marshalFloat32(buf []byte, n float32, i uint64) []byte {
	bits := math.Float32bits(n)
	buf[i+0] = byte(bits >> 24)
	buf[i+1] = byte(bits >> 16)
	buf[i+2] = byte(bits >> 8)
	buf[i+3] = byte(bits >> 0)
	return buf
}

func unmarshalFloat32(buf []byte, i uint64) float32 {
	return math.Float32frombits(0 |
		(uint32(buf[i+0]) << 24) |
		(uint32(buf[i+1]) << 16) |
		(uint32(buf[i+2]) << 8) |
		(uint32(buf[i+3]) << 0))
}

func marshalFloat64(buf []byte, n float64, i uint64) []byte {
	bits := math.Float64bits(n)
	buf[i+0] = byte(bits >> 56)
	buf[i+1] = byte(bits >> 48)
	buf[i+2] = byte(bits >> 40)
	buf[i+3] = byte(bits >> 32)
	buf[i+4] = byte(bits >> 24)
	buf[i+5] = byte(bits >> 16)
	buf[i+6] = byte(bits >> 8)
	buf[i+7] = byte(bits >> 0)
	return buf
}

func unmarshalFloat64(buf []byte, i uint64) float64 {
	return math.Float64frombits(0 |
		(uint64(buf[i+0]) << 56) |
		(uint64(buf[i+1+0]) << 48) |
		(uint64(buf[i+2+0]) << 40) |
		(uint64(buf[i+3+0]) << 32) |
		(uint64(buf[i+4+0]) << 24) |
		(uint64(buf[i+5+0]) << 16) |
		(uint64(buf[i+6+0]) << 8) |
		(uint64(buf[i+7+0]) << 0))
}
