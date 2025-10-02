package minisql

import (
	"context"
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
	default:
		return "Unknown"
	}
}

type OperandType int

const (
	Field OperandType = iota + 1
	QuotedString
	Integer
)

type Operand struct {
	Type  OperandType
	Value any
}

// IsField determines whether the operand is a literal or a field name
func (o Operand) IsField() bool {
	return o.Type == Field
}

type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 Operand
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand2 is the right hand side operand
	Operand2 Operand
}

type Conditions []Condition

// OneOrMore contains a slice of multiple groups of singular condition, each
// group joined by OR boolean operator. Every singular condition in each group
// is joined by AND with other conditions in the same slice.
type OneOrMore []Conditions

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

func FieldIsIn(fieldName string, operandType OperandType, values ...any) OneOrMore {
	oneOrMore := make(OneOrMore, 0, len(values))
	for _, v := range values {
		oneOrMore = append(oneOrMore, Conditions{
			{
				Operand1: Operand{
					Type:  Field,
					Value: fieldName,
				},
				Operator: Eq,
				Operand2: Operand{
					Type:  operandType,
					Value: v,
				},
			},
		})
	}
	return oneOrMore
}

type StatementKind int

const (
	CreateTable StatementKind = iota + 1
	DropTable
	Insert
	Select
	Update
	Delete
)

type ColumnKind int

const (
	Int4 ColumnKind = iota + 1
	Int8
	Varchar
)

func (k ColumnKind) String() string {
	switch k {
	case Int4:
		return "Int4"
	case Int8:
		return "Int8"
	case Varchar:
		return "Varchar"
	default:
		return "Unknown"
	}
}

type Column struct {
	Kind     ColumnKind
	Size     uint32
	Nullable bool
	Name     string
}

type Statement struct {
	Kind        StatementKind
	IfNotExists bool
	TableName   string
	Columns     []Column // use for CREATE TABLE
	Fields      []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases     map[string]string
	Inserts     [][]any
	Updates     map[string]any
	Conditions  OneOrMore // used for WHERE
}

type Iterator func(ctx context.Context) (Row, error)

type StatementResult struct {
	Columns      []Column
	Rows         Iterator
	RowsAffected int
}
