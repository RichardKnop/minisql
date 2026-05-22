package minisql

import (
	"context"
	"errors"
	"fmt"
)

func compileRowViewFilterForColumns(columns []Column, pager TxPager, conditions OneOrMore) func(context.Context, RowView) (bool, error) {
	if len(conditions) == 0 {
		return nil
	}
	columnIndexes := make(map[string]int, len(columns))
	for i := range columns {
		columnIndexes[columns[i].Name] = i
	}
	return func(ctx context.Context, view RowView) (bool, error) {
		return view.CheckOneOrMoreWithColumnIndexes(ctx, pager, conditions, columnIndexes)
	}
}

func rowViewFilterSupports(columns []Column, conditions OneOrMore) bool {
	for _, group := range conditions {
		for _, cond := range group {
			if cond.Operand1.Type == OperandExpr || cond.Operand2.Type == OperandExpr {
				return false
			}
		}
	}
	return true
}

// CheckOneOrMoreWithColumnIndexes evaluates conditions against lazily decoded
// cell data. It mirrors Row.CheckOneOrMoreWithColumnIndexes without allocating a
// []OptionalValue for the whole row.
func (rv RowView) CheckOneOrMoreWithColumnIndexes(ctx context.Context, pager TxPager, conditions OneOrMore, columnIndexes map[string]int) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}

	for _, condGroup := range conditions {
		ok, err := rv.checkConditionsWithColumnIndexes(ctx, pager, condGroup, columnIndexes)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (rv RowView) checkConditionsWithColumnIndexes(ctx context.Context, pager TxPager, condGroup Conditions, columnIndexes map[string]int) (bool, error) {
	if len(condGroup) == 0 {
		return true, nil
	}

	for _, cond := range condGroup {
		ok, err := rv.checkConditionWithColumnIndexes(ctx, pager, cond, columnIndexes)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (rv RowView) checkConditionWithColumnIndexes(ctx context.Context, pager TxPager, cond Condition, columnIndexes map[string]int) (bool, error) {
	if cond.Operand1.Type == OperandExpr {
		return false, errRowViewUnsupportedCondition
	}

	if cond.Operand1.IsField() && !cond.Operand2.IsField() {
		return rv.compareFieldValueWithColumnIndexes(ctx, pager, cond.Operand1, cond.Operand2, cond.Operator, columnIndexes)
	}

	if cond.Operand2.IsField() && !cond.Operand1.IsField() {
		return rv.compareFieldValueWithColumnIndexes(ctx, pager, cond.Operand2, cond.Operand1, cond.Operator, columnIndexes)
	}

	if cond.Operand1.IsField() && cond.Operand2.IsField() {
		return rv.compareFieldsWithColumnIndexes(ctx, pager, cond.Operand1, cond.Operand2, cond.Operator, columnIndexes)
	}

	return cond.Operand1.Value == cond.Operand2.Value, nil
}

func (rv RowView) compareFieldValueWithColumnIndexes(ctx context.Context, pager TxPager, fieldOperand, valueOperand Operand, operator Operator, columnIndexes map[string]int) (bool, error) {
	if fieldOperand.Type != OperandField {
		return false, fmt.Errorf("field operand invalid, type '%d'", fieldOperand.Type)
	}
	if valueOperand.Type == OperandField {
		return false, errors.New("cannot compare column value against field operand")
	}

	field := fieldOperand.Value.(Field)
	colIdx, ok := rowViewColumnIndex(field, columnIndexes)
	if !ok {
		return false, fmt.Errorf("row does not contain column '%s'", field.Name)
	}
	col := rv.columns[colIdx]
	fieldValue, err := rv.ValueAtWithOverflow(ctx, pager, colIdx)
	if err != nil {
		return false, err
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
		return compareRowViewFieldList(col.Kind, fieldValue, valueOperand, operator)
	}

	if !fieldValue.Valid {
		return false, nil
	}

	return compareRowViewFieldValue(col.Kind, fieldValue, valueOperand, operator)
}

func (rv RowView) compareFieldsWithColumnIndexes(ctx context.Context, pager TxPager, field1, field2 Operand, operator Operator, columnIndexes map[string]int) (bool, error) {
	idx1, ok := rowViewColumnIndex(field1.Value.(Field), columnIndexes)
	if !ok {
		return false, fmt.Errorf("row does not contain column '%s'", field1.Value.(Field).Name)
	}
	idx2, ok := rowViewColumnIndex(field2.Value.(Field), columnIndexes)
	if !ok {
		return false, fmt.Errorf("row does not contain column '%s'", field2.Value.(Field).Name)
	}

	col1 := rv.columns[idx1]
	col2 := rv.columns[idx2]
	if col1.Kind != col2.Kind {
		return false, fmt.Errorf("columns '%s' and '%s' have different types", col1.Name, col2.Name)
	}

	value1, err := rv.ValueAtWithOverflow(ctx, pager, idx1)
	if err != nil {
		return false, err
	}
	value2, err := rv.ValueAtWithOverflow(ctx, pager, idx2)
	if err != nil {
		return false, err
	}
	if !value1.Valid || !value2.Valid {
		return false, nil
	}
	return compareRowViewValues(col1.Kind, value1, value2, operator)
}

func rowViewColumnIndex(field Field, columnIndexes map[string]int) (int, bool) {
	var (
		colIdx int
		ok     bool
	)
	if field.AliasPrefix != "" {
		var buf [256]byte
		n := copy(buf[:], field.AliasPrefix)
		buf[n] = '.'
		n += 1
		n += copy(buf[n:], field.Name)
		colIdx, ok = columnIndexes[string(buf[:n])]
	}
	if !ok {
		colIdx, ok = columnIndexes[field.Name]
	}
	return colIdx, ok
}

func compareRowViewFieldList(kind ColumnKind, fieldValue OptionalValue, valueOperand Operand, operator Operator) (bool, error) {
	switch operator {
	case In, NotIn:
		found, err := isRowViewValueInList(kind, fieldValue, valueOperand.Value)
		if operator == In {
			return found, err
		}
		return !found, err
	case Between, NotBetween:
		inRange, err := isRowViewValueBetween(kind, fieldValue, valueOperand.Value)
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

func compareRowViewFieldValue(kind ColumnKind, fieldValue OptionalValue, valueOperand Operand, operator Operator) (bool, error) {
	if (operator == Like || operator == NotLike) && kind != Varchar && kind != Text {
		return false, errors.New("LIKE / NOT LIKE operator only supported for TEXT and VARCHAR columns")
	}

	switch kind {
	case Boolean:
		return compareBoolean(fieldValue.Value.(bool), valueOperand.Value.(bool), operator)
	case Int4:
		return compareInt4(int64(fieldValue.Value.(int32)), valueOperand.Value.(int64), operator)
	case Int8:
		return compareInt8(fieldValue.Value.(int64), valueOperand.Value.(int64), operator)
	case Real:
		return compareReal(fieldValue.Value.(float32), float32(valueOperand.Value.(float64)), operator)
	case Double:
		return compareDouble(fieldValue.Value.(float64), valueOperand.Value.(float64), operator)
	case Varchar, Text:
		return compareText(fieldValue.Value.(TextPointer), valueOperand.Value.(TextPointer), operator)
	case Timestamp:
		return compareTimestamp(fieldValue.Value.(TimestampMicros), valueOperand.Value.(TimestampMicros), operator)
	case UUID:
		u1, err := toUUIDValue(fieldValue.Value)
		if err != nil {
			return false, err
		}
		u2, err := toUUIDValue(valueOperand.Value)
		if err != nil {
			return false, err
		}
		return compareUUID(u1, u2, operator)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", kind)
	}
}

func compareRowViewValues(kind ColumnKind, value1, value2 OptionalValue, operator Operator) (bool, error) {
	switch kind {
	case Boolean:
		return compareBoolean(value1.Value.(bool), value2.Value.(bool), operator)
	case Int4:
		return compareInt4(int64(value1.Value.(int32)), int64(value2.Value.(int32)), operator)
	case Int8:
		return compareInt8(value1.Value.(int64), value2.Value.(int64), operator)
	case Real:
		return compareReal(value1.Value.(float32), value2.Value.(float32), operator)
	case Double:
		return compareDouble(value1.Value.(float64), value2.Value.(float64), operator)
	case Varchar, Text:
		return compareText(value1.Value.(TextPointer), value2.Value.(TextPointer), operator)
	case Timestamp:
		return compareTimestamp(value1.Value.(TimestampMicros), value2.Value.(TimestampMicros), operator)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", kind)
	}
}

func isRowViewValueInList(kind ColumnKind, fieldValue OptionalValue, list any) (bool, error) {
	switch kind {
	case Boolean:
		return false, errors.New("IN / NOT IN operator not supported for boolean columns")
	case Int4:
		return isInListInt4(fieldValue.Value, list)
	case Int8:
		return isInListInt8(fieldValue.Value, list)
	case Real:
		return isInListReal(fieldValue.Value, list)
	case Double:
		return isInListDouble(fieldValue.Value, list)
	case Varchar, Text:
		return isInListText(fieldValue.Value, list)
	case Timestamp:
		return isInListTimestamp(fieldValue.Value, list)
	case UUID:
		return isInListUUID(fieldValue.Value, list)
	default:
		return false, fmt.Errorf("unknown column kind '%s'", kind)
	}
}

func isRowViewValueBetween(kind ColumnKind, fieldValue OptionalValue, bounds any) (bool, error) {
	list, ok := bounds.([]any)
	if !ok || len(list) != 2 {
		return false, errors.New("BETWEEN requires exactly 2 bounds")
	}
	switch kind {
	case Boolean:
		return false, errors.New("BETWEEN operator not supported for boolean columns")
	case Int4:
		return isBetweenInt4(int64(fieldValue.Value.(int32)), list[0], list[1])
	case Int8:
		return isBetweenInt8(fieldValue.Value, list[0], list[1])
	case Real:
		return isBetweenReal(float64(fieldValue.Value.(float32)), list[0], list[1])
	case Double:
		return isBetweenDouble(fieldValue.Value, list[0], list[1])
	case Varchar, Text:
		return isBetweenText(fieldValue.Value, list[0], list[1])
	case Timestamp:
		return isBetweenTimestamp(fieldValue.Value, list[0], list[1])
	default:
		return false, fmt.Errorf("unknown column kind '%s'", kind)
	}
}

var errRowViewUnsupportedCondition = errors.New("row view unsupported condition")
