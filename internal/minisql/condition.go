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
	// OperandSubquery holds a *Statement SELECT that is evaluated before the
	// outer scan begins and replaced with a concrete scalar or list value.
	OperandSubquery
	// OperandExpr holds an *Expr that is evaluated against the row at runtime.
	// Used for JSON path expressions on the left-hand side of a WHERE condition.
	OperandExpr
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

func compareBoolean(v1, v2 bool, operator Operator) (bool, error) {
	switch operator {
	case Eq:
		return v1 == v2, nil
	case Ne:
		return v1 != v2, nil
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

func compareInt4(v1, v2 int64, operator Operator) (bool, error) {
	// Validate that both values are within the INT4 range before comparing.
	if v1 < math.MinInt32 || v1 > math.MaxInt32 {
		return false, fmt.Errorf("value %d is out of INT4 range", v1)
	}
	if v2 < math.MinInt32 || v2 > math.MaxInt32 {
		return false, fmt.Errorf("value %d is out of INT4 range", v2)
	}
	switch operator {
	case Eq:
		return v1 == v2, nil
	case Ne:
		return v1 != v2, nil
	case Gt:
		return v1 > v2, nil
	case Lt:
		return v1 < v2, nil
	case Gte:
		return v1 >= v2, nil
	case Lte:
		return v1 <= v2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareInt8(v1, v2 int64, operator Operator) (bool, error) {
	switch operator {
	case Eq:
		return v1 == v2, nil
	case Ne:
		return v1 != v2, nil
	case Gt:
		return v1 > v2, nil
	case Lt:
		return v1 < v2, nil
	case Gte:
		return v1 >= v2, nil
	case Lte:
		return v1 <= v2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

// compareReal compares two float32 values. Field values are stored as float32;
// the operand is narrowed from float64 at the call site.
func compareReal(v1, v2 float32, operator Operator) (bool, error) {
	switch operator {
	case Eq:
		return v1 == v2, nil
	case Ne:
		return v1 != v2, nil
	case Gt:
		return v1 > v2, nil
	case Lt:
		return v1 < v2, nil
	case Gte:
		return v1 >= v2, nil
	case Lte:
		return v1 <= v2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareDouble(v1, v2 float64, operator Operator) (bool, error) {
	switch operator {
	case Eq:
		return v1 == v2, nil
	case Ne:
		return v1 != v2, nil
	case Gt:
		return v1 > v2, nil
	case Lt:
		return v1 < v2, nil
	case Gte:
		return v1 >= v2, nil
	case Lte:
		return v1 <= v2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

// compareText compares two TextPointer values lexicographically byte-wise.
func compareText(v1, v2 TextPointer, operator Operator) (bool, error) {
	s1 := v1.String()
	s2 := v2.String()
	switch operator {
	case Eq:
		return s1 == s2, nil
	case Ne:
		return s1 != s2, nil
	case Gt:
		return s1 > s2, nil
	case Lt:
		return s1 < s2, nil
	case Gte:
		return s1 >= s2, nil
	case Lte:
		return s1 <= s2, nil
	case Like:
		return likeMatch(s2, s1), nil
	case NotLike:
		return !likeMatch(s2, s1), nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareTimestamp(v1, v2 TimestampMicros, operator Operator) (bool, error) {
	switch operator {
	case Eq:
		return v1 == v2, nil
	case Ne:
		return v1 != v2, nil
	case Gt:
		return v1 > v2, nil
	case Lt:
		return v1 < v2, nil
	case Gte:
		return v1 >= v2, nil
	case Lte:
		return v1 <= v2, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func compareUUID(v1, v2 UUIDValue, operator Operator) (bool, error) {
	// UUIDs are compared lexicographically by their 16-byte representation.
	cmp := uuidCompare(v1, v2)
	switch operator {
	case Eq:
		return cmp == 0, nil
	case Ne:
		return cmp != 0, nil
	case Gt:
		return cmp > 0, nil
	case Lt:
		return cmp < 0, nil
	case Gte:
		return cmp >= 0, nil
	case Lte:
		return cmp <= 0, nil
	}
	return false, fmt.Errorf("unknown operator '%s'", operator)
}

func uuidCompare(a, b UUIDValue) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func isInListUUID(value, list any) (bool, error) {
	v, err := toUUIDValue(value)
	if err != nil {
		return false, fmt.Errorf("value '%v' cannot be cast as UUIDValue: %w", value, err)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, err := toUUIDValue(listValue)
		if err != nil {
			return false, err
		}
		match, err := compareUUID(v, lv, Eq)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func isInListInt4(value, list any) (bool, error) {
	v, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, ok := listValue.(int64)
		if !ok {
			return false, fmt.Errorf("list value '%v' cannot be cast as int64", listValue)
		}
		match, err := compareInt4(v, lv, Eq)
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
	v, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, ok := listValue.(int64)
		if !ok {
			return false, fmt.Errorf("list value '%v' cannot be cast as int64", listValue)
		}
		match, err := compareInt8(v, lv, Eq)
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
	v, ok := value.(float32)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float32", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, ok := listValue.(float64)
		if !ok {
			return false, fmt.Errorf("list value '%v' cannot be cast as float64", listValue)
		}
		match, err := compareReal(v, float32(lv), Eq)
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
	v, ok := value.(float64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float64", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, ok := listValue.(float64)
		if !ok {
			return false, fmt.Errorf("list value '%v' cannot be cast as float64", listValue)
		}
		match, err := compareDouble(v, lv, Eq)
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
	v, ok := value.(TextPointer)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TextPointer", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, ok := listValue.(TextPointer)
		if !ok {
			return false, fmt.Errorf("list value '%v' cannot be cast as TextPointer", listValue)
		}
		match, err := compareText(v, lv, Eq)
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
	v, ok := value.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TimestampMicros", value)
	}
	theList, ok := list.([]any)
	if !ok {
		return false, fmt.Errorf("list '%v' cannot be cast as []any", list)
	}
	for _, listValue := range theList {
		lv, ok := listValue.(TimestampMicros)
		if !ok {
			return false, fmt.Errorf("list value '%v' cannot be cast as TimestampMicros", listValue)
		}
		match, err := compareTimestamp(v, lv, Eq)
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

func isBetweenInt4(value, low, high any) (bool, error) {
	v, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value)
	}
	lo, ok := low.(int64)
	if !ok {
		return false, fmt.Errorf("BETWEEN low bound '%v' cannot be cast as int64", low)
	}
	hi, ok := high.(int64)
	if !ok {
		return false, fmt.Errorf("BETWEEN high bound '%v' cannot be cast as int64", high)
	}
	geq, err := compareInt4(v, lo, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareInt4(v, hi, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenInt8(value, low, high any) (bool, error) {
	v, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as int64", value)
	}
	lo, ok := low.(int64)
	if !ok {
		return false, fmt.Errorf("BETWEEN low bound '%v' cannot be cast as int64", low)
	}
	hi, ok := high.(int64)
	if !ok {
		return false, fmt.Errorf("BETWEEN high bound '%v' cannot be cast as int64", high)
	}
	return v >= lo && v <= hi, nil
}

func isBetweenReal(value, low, high any) (bool, error) {
	v, ok := value.(float32)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float32", value)
	}
	lo, ok := low.(float64)
	if !ok {
		return false, fmt.Errorf("BETWEEN low bound '%v' cannot be cast as float64", low)
	}
	hi, ok := high.(float64)
	if !ok {
		return false, fmt.Errorf("BETWEEN high bound '%v' cannot be cast as float64", high)
	}
	return v >= float32(lo) && v <= float32(hi), nil
}

func isBetweenDouble(value, low, high any) (bool, error) {
	v, ok := value.(float64)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as float64", value)
	}
	lo, ok := low.(float64)
	if !ok {
		return false, fmt.Errorf("BETWEEN low bound '%v' cannot be cast as float64", low)
	}
	hi, ok := high.(float64)
	if !ok {
		return false, fmt.Errorf("BETWEEN high bound '%v' cannot be cast as float64", high)
	}
	return v >= lo && v <= hi, nil
}

func isBetweenText(value, low, high any) (bool, error) {
	v, ok := value.(TextPointer)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TextPointer", value)
	}
	lo, ok := low.(TextPointer)
	if !ok {
		return false, fmt.Errorf("BETWEEN low bound '%v' cannot be cast as TextPointer", low)
	}
	hi, ok := high.(TextPointer)
	if !ok {
		return false, fmt.Errorf("BETWEEN high bound '%v' cannot be cast as TextPointer", high)
	}
	geq, err := compareText(v, lo, Gte)
	if err != nil {
		return false, err
	}
	leq, err := compareText(v, hi, Lte)
	if err != nil {
		return false, err
	}
	return geq && leq, nil
}

func isBetweenTimestamp(value, low, high any) (bool, error) {
	v, ok := value.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("value '%v' cannot be cast as TimestampMicros", value)
	}
	lo, ok := low.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("BETWEEN low bound '%v' cannot be cast as TimestampMicros", low)
	}
	hi, ok := high.(TimestampMicros)
	if !ok {
		return false, fmt.Errorf("BETWEEN high bound '%v' cannot be cast as TimestampMicros", high)
	}
	return v >= lo && v <= hi, nil
}
