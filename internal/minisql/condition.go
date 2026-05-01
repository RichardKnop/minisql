package minisql

import (
	"errors"
	"fmt"
	"math"
)

// Operator represents a SQL comparison operator used in WHERE conditions.
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
	// Like -> "LIKE"
	Like
	// NotLike -> "NOT LIKE"
	NotLike
	// Between -> "BETWEEN ... AND ..."
	Between
	// NotBetween -> "NOT BETWEEN ... AND ..."
	NotBetween
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
	case Like:
		return "LIKE"
	case NotLike:
		return "NOT LIKE"
	case Between:
		return "BETWEEN"
	case NotBetween:
		return "NOT BETWEEN"
	default:
		return "Unknown"
	}
}

// OperandType classifies the value on either side of a condition operator.
type OperandType int

// OperandType constants define the kinds of operand values.
const (
	// OperandField means the operand is a table column name.
	OperandField OperandType = iota + 1
	OperandPlaceholder
	OperandNull
	OperandQuotedString
	OperandBoolean
	OperandInteger
	OperandFloat
	OperandList
)

// Operand holds a typed value on one side of a condition expression.
type Operand struct {
	Value any
	Type  OperandType
}

// IsField determines whether the operand is a literal or a field name
func (o Operand) IsField() bool {
	return o.Type == OperandField
}

// Condition represents a single predicate: operand1 operator operand2.
type Condition struct {
	Operand1 Operand
	Operand2 Operand
	Operator Operator
}

// Operands returns both operands of the condition as a slice.
func (c Condition) Operands() []Operand {
	return []Operand{c.Operand1, c.Operand2}
}

// IsValidCondition checks that all fields of the condition are set
func IsValidCondition(c Condition) bool {
	if c.Operand1.Type == 0 {
		return false
	}
	if c.Operator == 0 {
		return false
	}
	if c.Operand2.Type == 0 {
		return false
	}

	return true
}

// Conditions is a slice of Condition values joined by AND within a single OR group.
type Conditions []Condition

// OneOrMore contains a slice of multiple groups of singular condition, each
// group joined by OR boolean operator. Every singular condition in each group
// is joined by AND with other conditions in the same slice.
type OneOrMore []Conditions

// NewOneOrMore creates a new OneOrMore from the given condition groups.
func NewOneOrMore(conditionGroups ...Conditions) OneOrMore {
	return OneOrMore(conditionGroups)
}

// LastCondition returns the last condition in the last group, if any.
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

// Append adds a condition to the last group, creating a new group if empty.
func (o OneOrMore) Append(cond Condition) OneOrMore {
	if len(o) == 0 {
		o = append(o, make(Conditions, 0, 1))
	}
	o[len(o)-1] = append(o[len(o)-1], cond)
	return o
}

// UpdateLast replaces the last condition in the last group.
func (o OneOrMore) UpdateLast(cond Condition) {
	o[len(o)-1][len(o[len(o)-1])-1] = cond
}

// FieldIsEqual creates a field = value condition.
func FieldIsEqual(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Eq,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsNotEqual creates a field != value condition.
func FieldIsNotEqual(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Ne,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsInAny creates a field IN (values...) condition.
func FieldIsInAny(field Field, values ...any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: In,
		Operand2: Operand{
			Type:  OperandList,
			Value: values,
		},
	}
}

// FieldIsNotInAny creates a field NOT IN (values...) condition.
func FieldIsNotInAny(field Field, values ...any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: NotIn,
		Operand2: Operand{
			Type:  OperandList,
			Value: values,
		},
	}
}

// FieldIsNull creates a field IS NULL condition.
func FieldIsNull(field Field) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Eq,
		Operand2: Operand{
			Type: OperandNull,
		},
	}
}

// FieldIsNotNull creates a field IS NOT NULL condition.
func FieldIsNotNull(field Field) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Ne,
		Operand2: Operand{
			Type: OperandNull,
		},
	}
}

// FieldIsGreater creates a field > value condition.
func FieldIsGreater(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Gt,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsGreaterOrEqual creates a field >= value condition.
func FieldIsGreaterOrEqual(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Gte,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsLess creates a field < value condition.
func FieldIsLess(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Lt,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsLike creates a field LIKE value condition.
func FieldIsLike(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Like,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsBetween creates a field BETWEEN low AND high condition.
func FieldIsBetween(field Field, low, high any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Between,
		Operand2: Operand{
			Type:  OperandList,
			Value: []any{low, high},
		},
	}
}

// FieldIsNotBetween creates a field NOT BETWEEN low AND high condition.
func FieldIsNotBetween(field Field, low, high any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: NotBetween,
		Operand2: Operand{
			Type:  OperandList,
			Value: []any{low, high},
		},
	}
}

// FieldIsNotLike creates a field NOT LIKE value condition.
func FieldIsNotLike(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: NotLike,
		Operand2: Operand{
			Type:  operandType,
			Value: value,
		},
	}
}

// FieldIsLessOrEqual creates a field <= value condition.
func FieldIsLessOrEqual(field Field, operandType OperandType, value any) Condition {
	return Condition{
		Operand1: Operand{
			Type:  OperandField,
			Value: field,
		},
		Operator: Lte,
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
		return false, errors.New("cannot compare boolean values with '>'")
	case Lt:
		return false, errors.New("cannot compare boolean values with '<'")
	case Gte:
		return false, errors.New("cannot compare boolean values with '>='")
	case Lte:
		return false, errors.New("cannot compare boolean values with '<='")
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func toInt64ForInt4(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("value '%v' cannot be cast as int32 or int64", v)
	}
}

func compareInt4(value1, value2 any, operator Operator) (bool, error) {
	theValue1, err := toInt64ForInt4(value1)
	if err != nil {
		return false, err
	}
	theValue2, err := toInt64ForInt4(value2)
	if err != nil {
		return false, err
	}
	// Validate that both values are within the INT4 range before comparing.
	for _, v := range []int64{theValue1, theValue2} {
		if v < math.MinInt32 || v > math.MaxInt32 {
			return false, fmt.Errorf("value %d is out of INT4 range", v)
		}
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
	case Like:
		return likeMatch(theValue2.String(), theValue1.String()), nil
	case NotLike:
		return !likeMatch(theValue2.String(), theValue1.String()), nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareTimestamp(value1, value2 any, operator Operator) (bool, error) {
	theValue1, ok := value1.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TimestampMicros", value1)
	}
	theValue2, ok := value2.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("operand value '%v' cannot be cast as TimestampMicros", value2)
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
	_, ok := value.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TimestampMicros", value)
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

// isBetween* functions check whether value falls within [low, high] inclusive.
// They reuse the existing compare* functions to avoid duplicating comparison logic.

func isBetweenInt4(value, low, high any) (bool, error) {
	geq, err := compareInt4(value, low, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareInt4(value, high, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenInt8(value, low, high any) (bool, error) {
	geq, err := compareInt8(value, low, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareInt8(value, high, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenReal(value, low, high any) (bool, error) {
	geq, err := compareReal(value, low, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareReal(value, high, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenDouble(value, low, high any) (bool, error) {
	geq, err := compareDouble(value, low, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareDouble(value, high, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenText(value, low, high any) (bool, error) {
	geq, err := compareText(value, low, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareText(value, high, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenTimestamp(value, low, high any) (bool, error) {
	geq, err := compareTimestamp(value, low, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareTimestamp(value, high, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}
