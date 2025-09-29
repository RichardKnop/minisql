package minisql

import (
	"bytes"
	"fmt"
)

type Row struct {
	Columns []Column
	Values  []any
	key     uint64
	// for updates, we store cursor internally
	cursor Cursor
}

// MaxCells returns how many rows can be stored in a single page
func (r Row) MaxCells() uint32 {
	return maxCells(r.Size())
}

func maxCells(rowSize uint64) uint32 {
	// base header is +6, leaf/internal header +8
	// int64 row ID per cell hence we divide by rowSize + 8
	return uint32((PageSize - 6 - 8) / (rowSize + 8))
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

func (r Row) GetValue(name string) (any, bool) {
	var (
		found     bool
		columnIdx = 0
	)
	for i, aColumn := range r.Columns {
		if aColumn.Name == name {
			found = true
			columnIdx = i
			break
		}
	}
	if !found {
		return nil, false
	}
	return r.Values[columnIdx], true
}

func (r Row) SetValue(name string, value any) bool {
	var (
		found     bool
		columnIdx = 0
	)
	for i, aColumn := range r.Columns {
		if aColumn.Name == name {
			found = true
			columnIdx = i
			break
		}
	}
	if !found {
		return false
	}
	r.Values[columnIdx] = value
	return true
}

func (r Row) Clone() Row {
	aClone := Row{
		Columns: make([]Column, 0, len(r.Columns)),
		Values:  make([]any, 0, len(r.Values)),
		key:     r.key,
	}
	aClone.Columns = append(aClone.Columns, r.Columns...)
	aClone.Values = append(aClone.Values, r.Values...)
	return aClone
}

func (r Row) columnOffset(idx int) uint32 {
	offset := uint32(0)
	for i := range idx {
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
			copy(dst, buf[offset:offset+aColumn.Size])
			aRow.Values = append(aRow.Values, string(bytes.Trim(dst, "\x00")))
		}
	}

	return nil
}

// CheckOneOrMore checks whether row satisfies one or more sets of conditions
// (cond1 AND cond2) OR (cond3 and cond4) ... etc
func (r Row) CheckOneOrMore(conditions OneOrMore) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}

	for _, aConditionGroup := range conditions {
		groupConditionResult := true
		for _, aCondition := range aConditionGroup {
			ok, err := r.checkCondition(aCondition)
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
	}

	return false, nil
}

func (r Row) checkCondition(aCondition Condition) (bool, error) {
	// left side is field, right side is literal value
	if aCondition.Operand1.IsField() && !aCondition.Operand2.IsField() {
		return r.compareFieldValue(aCondition.Operand1, aCondition.Operand2, aCondition.Operator)
	}

	// left side is literal value, right side is field
	if aCondition.Operand2.IsField() && !aCondition.Operand1.IsField() {
		return r.compareFieldValue(aCondition.Operand2, aCondition.Operand1, aCondition.Operator)
	}

	// both left and right are fields, compare 2 row values
	if aCondition.Operand1.IsField() && aCondition.Operand2.IsField() {
		return r.compareFields(aCondition.Operand1, aCondition.Operand2, aCondition.Operator)
	}

	// both left and right are literal values, compare them
	return aCondition.Operand1.Value == aCondition.Operand2.Value, nil
}

func (r Row) compareFieldValue(fieldOperand, valueOperand Operand, operator Operator) (bool, error) {
	if fieldOperand.Type != Field {
		return false, fmt.Errorf("field operand invalid, type '%d'", fieldOperand.Type)
	}
	if valueOperand.Type == Field {
		return false, fmt.Errorf("cannot compare column value against field operand")
	}
	name := fmt.Sprint(fieldOperand.Value)
	aColumn, ok := r.GetColumn(name)
	if !ok {
		return false, fmt.Errorf("row does not contain column '%s'", name)
	}
	value, ok := r.GetValue(aColumn.Name)
	if !ok {
		return false, fmt.Errorf("row does not have '%s' column", name)
	}

	switch aColumn.Kind {
	case Int4:
		// Int values from parser always come back as int64, int4 row data
		// will come back as int32 and int8 as int64
		return compareInt4(int64(value.(int32)), valueOperand.Value.(int64), operator)
	case Int8:
		return compareInt8(value.(int64), valueOperand.Value.(int64), operator)
	case Varchar:
		return compareVarchar(value, valueOperand.Value, operator)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", aColumn.Kind)
	}
}

func (r Row) compareFields(field1, field2 Operand, operator Operator) (bool, error) {
	if !field1.IsField() {
		return false, fmt.Errorf("field operand invalid, type '%d'", field1.Type)
	}
	if field2.IsField() {
		return false, fmt.Errorf("field operand invalid, type '%d'", field2.Type)
	}

	if field1.Value == field2.Value {
		return true, nil
	}

	name1 := fmt.Sprint(field1.Value)
	aColumn1, ok := r.GetColumn(name1)
	if !ok {
		return false, fmt.Errorf("row does not contain column '%s'", name1)
	}
	name2 := fmt.Sprint(field2.Value)
	aColumn2, ok := r.GetColumn(name2)
	if !ok {
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
	case Int4:
		return compareInt4(value1, value2, operator)
	case Int8:
		return compareInt8(value1, value2, operator)
	case Varchar:
		return compareVarchar(value1, value2, operator)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", aColumn1.Kind)
	}
}

func compareInt4(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value1)
	}
	theValue2, ok := value2.(int64)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as int64", value2)
	}
	switch operator {
	case Eq:
		return int32(theValue1) == int32(theValue2), nil
	case Ne:
		return int32(theValue1) != int32(theValue2), nil
	case Gt:
		return int32(theValue1) > int32(theValue2), nil
	case Lt:
		return int32(theValue1) < int32(theValue2), nil
	case Gte:
		return int32(theValue1) >= int32(theValue2), nil
	case Lte:
		return int32(theValue1) <= int32(theValue2), nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareInt8(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value1)
	}
	theValue2, ok := value2.(int64)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as int64", value2)
	}
	switch operator {
	case Eq:
		return theValue1 == theValue2, nil
	case Ne:
		return theValue1 != theValue2, nil
	case Gt:
		return theValue1 > theValue2, nil
	case Lt:
		return theValue1 < theValue2, nil
	case Gte:
		return theValue1 >= theValue2, nil
	case Lte:
		return theValue1 <= theValue2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareVarchar(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(string)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as string", value1)
	}
	theValue2, ok := value2.(string)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as string", value2)
	}
	// From Golang dosc (https://go.dev/ref/spec#Comparison_operators)
	// Two string values are compared lexically byte-wise.
	switch operator {
	case Eq:
		return theValue1 == theValue2, nil
	case Ne:
		return theValue1 != theValue2, nil
	case Gt:
		return theValue1 > theValue2, nil
	case Lt:
		return theValue1 < theValue2, nil
	case Gte:
		return theValue1 >= theValue2, nil
	case Lte:
		return theValue1 <= theValue2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}
