package minisql

import (
	"context"
	"fmt"
	"unicode/utf8"
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
	Null
	QuotedString
	Integer
	Float
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

func FieldIsNotIn(fieldName string, operandType OperandType, values ...any) OneOrMore {
	oneOrMore := make(OneOrMore, 0, len(values))
	for _, v := range values {
		oneOrMore = append(oneOrMore, Conditions{
			{
				Operand1: Operand{
					Type:  Field,
					Value: fieldName,
				},
				Operator: Ne,
				Operand2: Operand{
					Type:  operandType,
					Value: v,
				},
			},
		})
	}
	return oneOrMore
}

func FieldIsNull(fieldName string) OneOrMore {
	return OneOrMore{
		{
			{
				Operand1: Operand{
					Type:  Field,
					Value: fieldName,
				},
				Operator: Eq,
				Operand2: Operand{
					Type: Null,
				},
			},
		},
	}
}

func FieldIsNotNull(fieldName string) OneOrMore {
	return OneOrMore{
		{
			{
				Operand1: Operand{
					Type:  Field,
					Value: fieldName,
				},
				Operator: Ne,
				Operand2: Operand{
					Type: Null,
				},
			},
		},
	}
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
	Boolean ColumnKind = iota + 1
	Int4
	Int8
	Real
	Double
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
	Inserts     [][]OptionalValue
	Updates     map[string]OptionalValue
	Conditions  OneOrMore // used for WHERE
}

func (s Statement) ReadOnly() bool {
	return s.Kind == Select
}

func (s Statement) Validate(aTable *Table) error {
	if len(s.Inserts) > 0 {
		// TODO - handle default values
		if len(s.Columns) != len(aTable.Columns) {
			return fmt.Errorf("insert: expected %d columns, got %d", len(aTable.Columns), len(s.Columns))
		}
		if len(s.Fields) != len(aTable.Columns) {
			return fmt.Errorf("insert: expected %d fields, got %d", len(aTable.Columns), len(s.Fields))
		}
		for i, aField := range s.Fields {
			aColumn, ok := aTable.ColumnByName(aField)
			if !ok {
				return fmt.Errorf("unknown field %q in table %q", aField, aTable.Name)
			}
			for _, anInsert := range s.Inserts {
				if len(anInsert) != len(s.Fields) {
					return fmt.Errorf("insert: expected %d values, got %d", len(s.Fields), len(anInsert))
				}
				if !anInsert[i].Valid && !aColumn.Nullable {
					return fmt.Errorf("field %q cannot be NULL", aField)
				}
				if anInsert[i].Valid {
					if aColumn.Kind == Varchar && !utf8.ValidString(anInsert[i].Value.(string)) {
						return fmt.Errorf("field %q expects valid UTF-8 string", aField)
					}
				}

			}
		}
		return nil
	}

	if len(s.Updates) > 0 {
		for _, aField := range s.Fields {
			aColumn, ok := aTable.ColumnByName(aField)
			if !ok {
				return fmt.Errorf("unknown field %q in table %q", aField, aTable.Name)
			}
			if !s.Updates[aField].Valid && !aColumn.Nullable {
				return fmt.Errorf("field %q cannot be NULL", aField)
			}
			if s.Updates[aField].Valid {
				if aColumn.Kind == Varchar && !utf8.ValidString(s.Updates[aField].Value.(string)) {
					return fmt.Errorf("field %q expects valid UTF-8 string", aField)
				}
			}
		}
		return nil
	}

	return nil
}

type Iterator func(ctx context.Context) (Row, error)

type StatementResult struct {
	Columns      []Column
	Rows         Iterator
	RowsAffected int
}
