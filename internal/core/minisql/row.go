package minisql

import (
	"bytes"
	"fmt"

	"github.com/RichardKnop/minisql/pkg/bitwise"
)

type OptionalValue struct {
	Value any
	Valid bool
}

type Row struct {
	Key     uint64
	Columns []Column
	Values  []OptionalValue
	// for updates, we store cursor internally
	cursor Cursor
}

// MaxCells returns how many rows can be stored in a single page
func (r Row) MaxCells() uint32 {
	return maxCells(r.Size())
}

func maxCells(rowSize uint64) uint32 {
	// base header is +6, leaf/internal header +8
	// and uint64 row ID per cell
	// hence we divide by rowSize + 8 + 8
	return uint32((PageSize - 6 - 8) / (rowSize + 8 + 8))
}

func NewRow(columns []Column) Row {
	return Row{Columns: columns}
}

// Size calculates a size of a row record excluding null bitmask and row ID
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

func (r Row) GetValue(name string) (OptionalValue, bool) {
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
		return OptionalValue{}, false
	}
	return r.Values[columnIdx], true
}

func (r Row) SetValue(name string, value OptionalValue) bool {
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
		Values:  make([]OptionalValue, 0, len(r.Values)),
		Key:     r.Key,
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

func (r Row) appendValues(fields []string, values []OptionalValue) Row {
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
			r.Values = append(r.Values, OptionalValue{})
		}
	}
	return r
}

func (r *Row) Marshal() ([]byte, error) {
	buf := make([]byte, r.Size())

	for i, aColumn := range r.Columns {
		offset := r.columnOffset(i)
		if !r.Values[i].Valid {
			src := make([]byte, aColumn.Size)
			copy(buf[offset:], src)
			continue
		}
		switch aColumn.Kind {
		case Boolean:
			value, ok := r.Values[i].Value.(bool)
			if !ok {
				return nil, fmt.Errorf("could not cast value to bool")
			}
			if value {
				buf[offset] = byte(1)
			} else {
				buf[offset] = byte(0)
			}
		case Int4:
			value, ok := r.Values[i].Value.(int32)
			if !ok {
				_, ok = r.Values[i].Value.(int64)
				if !ok {
					return nil, fmt.Errorf("could not cast value to either int64 or int32")
				}
				value = int32(r.Values[i].Value.(int64))
			}
			marshalInt32(buf, value, uint64(offset))
		case Int8:
			value, ok := r.Values[i].Value.(int64)
			if !ok {
				return nil, fmt.Errorf("could not cast value to int64")
			}
			marshalInt64(buf, value, uint64(offset))
		case Real:
			value, ok := r.Values[i].Value.(float32)
			if !ok {
				_, ok = r.Values[i].Value.(float64)
				if !ok {
					return nil, fmt.Errorf("could not cast value to either float64 or float32")
				}
				value = float32(r.Values[i].Value.(float64))
			}
			marshalFloat32(buf, value, uint64(offset))
		case Double:
			value, ok := r.Values[i].Value.(float64)
			if !ok {
				return nil, fmt.Errorf("could not cast value to float64")
			}
			marshalFloat64(buf, value, uint64(offset))
		case Varchar:
			src := make([]byte, aColumn.Size)
			value, ok := r.Values[i].Value.(string)
			if !ok {
				return nil, fmt.Errorf("could not cast value to string")
			}
			copy(src, []byte(value))
			copy(buf[offset:], src)
		}
	}

	return buf, nil
}

func UnmarshalRow(aCell Cell, aRow *Row) error {
	aRow.Key = aCell.Key
	aRow.Values = make([]OptionalValue, 0, len(aRow.Columns))
	for i, aColumn := range aRow.Columns {
		if bitwise.IsSet(aCell.NullBitmask, i) {
			aRow.Values = append(aRow.Values, OptionalValue{Valid: false})
			continue
		}
		offset := aRow.columnOffset(i)
		switch aColumn.Kind {
		case Boolean:
			value := (uint32(aCell.Value[:][offset+0+0]) << 0)
			aRow.Values = append(aRow.Values, OptionalValue{Value: value == uint32(1), Valid: true})
		case Int4:
			value := unmarshalInt32(aCell.Value[:], uint64(offset))
			aRow.Values = append(aRow.Values, OptionalValue{Value: int32(value), Valid: true})
		case Int8:
			value := unmarshalInt64(aCell.Value[:], uint64(offset))
			aRow.Values = append(aRow.Values, OptionalValue{Value: int64(value), Valid: true})
		case Real:
			aRow.Values = append(aRow.Values, OptionalValue{Value: unmarshalFloat32(aCell.Value[:], uint64(offset)), Valid: true})
		case Double:
			aRow.Values = append(aRow.Values, OptionalValue{Value: unmarshalFloat64(aCell.Value[:], uint64(offset)), Valid: true})
		case Varchar:
			dst := make([]byte, aColumn.Size)
			copy(dst, aCell.Value[:][offset:offset+aColumn.Size])
			aRow.Values = append(aRow.Values, OptionalValue{Value: string(bytes.Trim(dst, "\x00")), Valid: true})
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

	if valueOperand.Type == Null {
		switch operator {
		case Eq:
			return !value.Valid, nil
		case Ne:
			return value.Valid, nil
		default:
			return false, fmt.Errorf("only '=' and '!=' operators supported when comparing against NULL")
		}
	}

	switch aColumn.Kind {
	case Int4:
		// Int values from parser always come back as int64, int4 row data
		// will come back as int32 and int8 as int64
		return compareInt4(int64(value.Value.(int32)), valueOperand.Value.(int64), operator)
	case Int8:
		return compareInt8(value.Value.(int64), valueOperand.Value.(int64), operator)
	case Varchar:
		return compareVarchar(value.Value, valueOperand.Value, operator)
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
		return compareInt4(value1.Value, value2.Value, operator)
	case Int8:
		return compareInt8(value1.Value, value2.Value, operator)
	case Varchar:
		return compareVarchar(value1.Value, value2.Value, operator)
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
