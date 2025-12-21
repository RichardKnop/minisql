package minisql

import (
	"fmt"
)

type Operator int

const (
	// Eq -> "="
	Eq Operator = iota + 1
	// Ne -> "!="
	Ne
	// Gt -> ">"
	Gt
	// Lt -> "<"
	Lt
	// Gte -> ">="
	Gte
	// Lte -> "<="
	Lte
	// In -> "IN (...)"
	In
	// NotIn -> "NOT IN (...)"
	NotIn
)

func (o Operator) String() string {
	switch o {
	case Eq:
		return "="
	case Ne:
		return "!="
	case Gt:
		return ">"
	case Lt:
		return "<"
	case Gte:
		return ">="
	case Lte:
		return "<="
	case In:
		return "IN"
	case NotIn:
		return "NOT IN"
	default:
		return "Unknown"
	}
}

type OperandType int

const (
	OperandField OperandType = iota + 1
	OperandNull
	OperandQuotedString
	OperandBoolean
	OperandInteger
	OperandFloat
	OperandList
)

type Operand struct {
	Type  OperandType
	Value any
}

// IsField determines whether the operand is a literal or a field name
func (o Operand) IsField() bool {
	return o.Type == OperandField
}

type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 Operand
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand2 is the right hand side operand
	Operand2 Operand
}

func (c Condition) Operands() []Operand {
	return []Operand{c.Operand1, c.Operand2}
}

// IsValidCondition checks that all fields of the condition are set
func IsValidCondition(c Condition) bool {
	if c.Operand1.Type == 0 {
		return false
	}
	if c.Operand1.Value == 0 {
		return false
	}
	if c.Operator == 0 {
		return false
	}
	if c.Operand2.Type == 0 {
		return false
	}
	if c.Operand2.Value == 0 {
		return false
	}

	return true
}

type Conditions []Condition

// OneOrMore contains a slice of multiple groups of singular condition, each
// group joined by OR boolean operator. Every singular condition in each group
// is joined by AND with other conditions in the same slice.
type OneOrMore []Conditions

func NewOneOrMore(conditionGroups ...Conditions) OneOrMore {
	return OneOrMore(conditionGroups)
}

func (o OneOrMore) LastCondition() (Condition, bool) {
	if len(o) == 0 {
		return Condition{}, false
	}
	lastConditionGroup := o[len(o)-1]
	if len(lastConditionGroup) > 0 {
		return lastConditionGroup[len(lastConditionGroup)-1], true
	}
	return Condition{}, false
}

func (o OneOrMore) Append(aCondition Condition) OneOrMore {
	if len(o) == 0 {
		o = append(o, make(Conditions, 0, 1))
	}
	o[len(o)-1] = append(o[len(o)-1], aCondition)
	return o
}

func (o OneOrMore) UpdateLast(aCondition Condition) {
	o[len(o)-1][len(o[len(o)-1])-1] = aCondition
}

func FieldIsEqual(fieldName string, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: Eq,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

func FieldIsNotEqual(fieldName string, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: Ne,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

func FieldIsInAny(fieldName string, values ...any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: In,
		Operand2: Operand{
			Type:  OperandList,
			Value: values,
		},
	}
}

func FieldIsNotInAny(fieldName string, values ...any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: NotIn,
		Operand2: Operand{
			Type:  OperandList,
			Value: values,
		},
	}
}

func FieldIsNull(fieldName string) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: Eq,
		Operand2: Operand{
			Type: OperandNull,
		},
	}
}

func FieldIsNotNull(fieldName string) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: Ne,
		Operand2: Operand{
			Type: OperandNull,
		},
	}
}

func FieldIsGreater(fieldName string, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: fieldName,
		},
		Operator: Gt,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

func compareBoolean(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(bool)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as bool", value1)
	}
	theValue2, ok := value2.(bool)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as bool", value2)
	}
	switch operator {
	case Eq:
		return theValue1 == theValue2, nil
	case Ne:
		return theValue1 != theValue2, nil
	case Gt:
		return false, fmt.Errorf("cannot compare boolean values with '>'")
	case Lt:
		return false, fmt.Errorf("cannot compare boolean values with '<'")
	case Gte:
		return false, fmt.Errorf("cannot compare boolean values with '>='")
	case Lte:
		return false, fmt.Errorf("cannot compare boolean values with '<='")
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
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

func compareReal(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(float64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float64", value1)
	}
	theValue2, ok := value2.(float64)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as float64", value2)
	}
	switch operator {
	case Eq:
		return float32(theValue1) == float32(theValue2), nil
	case Ne:
		return float32(theValue1) != float32(theValue2), nil
	case Gt:
		return float32(theValue1) > float32(theValue2), nil
	case Lt:
		return float32(theValue1) < float32(theValue2), nil
	case Gte:
		return float32(theValue1) >= float32(theValue2), nil
	case Lte:
		return float32(theValue1) <= float32(theValue2), nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareDouble(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(float64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float64", value1)
	}
	theValue2, ok := value2.(float64)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as float64", value2)
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

func compareText(value1, value2 any, operator Operator) (bool, error) {
	// From Golang dosc (https://go.dev/ref/spec#Comparison_operators)
	// Two string values are compared lexically byte-wise.
	theValue1, ok := value1.(TextPointer)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TextPointer", value1)
	}
	theValue2, ok := value2.(TextPointer)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as TextPointer", value2)
	}
	switch operator {
	case Eq:
		return theValue1.String() == theValue2.String(), nil
	case Ne:
		return theValue1.String() != theValue2.String(), nil
	case Gt:
		return theValue1.String() > theValue2.String(), nil
	case Lt:
		return theValue1.String() < theValue2.String(), nil
	case Gte:
		return theValue1.String() >= theValue2.String(), nil
	case Lte:
		return theValue1.String() <= theValue2.String(), nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareTimestamp(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(Time)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as Time", value1)
	}
	theValue2, ok := value2.(Time)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as Time", value2)
	}
	switch operator {
	case Eq:
		return theValue1.TotalMicroseconds() == theValue2.TotalMicroseconds(), nil
	case Ne:
		return theValue1.TotalMicroseconds() != theValue2.TotalMicroseconds(), nil
	case Gt:
		return theValue1.TotalMicroseconds() > theValue2.TotalMicroseconds(), nil
	case Lt:
		return theValue1.TotalMicroseconds() < theValue2.TotalMicroseconds(), nil
	case Gte:
		return theValue1.TotalMicroseconds() >= theValue2.TotalMicroseconds(), nil
	case Lte:
		return theValue1.TotalMicroseconds() <= theValue2.TotalMicroseconds(), nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func isInListInt4(value, list any) (bool, error) {
	_, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		match, err := compareInt4(value, listValue, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func isInListInt8(value, list any) (bool, error) {
	_, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		match, err := compareInt8(value, listValue, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func isInListReal(value, list any) (bool, error) {
	_, ok := value.(float64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		match, err := compareReal(value, listValue, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func isInListDouble(value, list any) (bool, error) {
	_, ok := value.(float64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		match, err := compareDouble(value, listValue, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func isInListText(value, list any) (bool, error) {
	_, ok := value.(TextPointer)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TextPointer", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		match, err := compareText(value, listValue, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func isInListTimestamp(value, list any) (bool, error) {
	_, ok := value.(Time)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as Time", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		match, err := compareTimestamp(value, listValue, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}
