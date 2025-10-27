package minisql

import (
	"context"
	"fmt"
	"strings"
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
	OperandField OperandType = iota + 1
	OperandNull
	OperandQuotedString
	OperandBoolean
	OperandInteger
	OperandFloat
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

type Conditions []Condition

// OneOrMore contains a slice of multiple groups of singular condition, each
// group joined by OR boolean operator. Every singular condition in each group
// is joined by AND with other conditions in the same slice.
type OneOrMore []Conditions

func NewOneOrMore(conditionGroups ...Conditions) OneOrMore {
	return OneOrMore(conditionGroups)
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

func FieldIsIn(fieldName string, operandType OperandType, value any) Condition {
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

func FieldIsInAny(fieldName string, operandType OperandType, values ...any) OneOrMore {
	conditions := make(OneOrMore, 0, len(values))
	for _, v := range values {
		conditions = append(conditions, Conditions{
			{
				Operand1: Operand{
					Type:  OperandField,
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
	return conditions
}

func FieldIsNotIn(fieldName string, operandType OperandType, value any) Condition {
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

func FieldIsNotInAny(fieldName string, operandType OperandType, values ...any) OneOrMore {
	conditions := make(OneOrMore, 0, len(values))
	for _, v := range values {
		conditions = append(conditions, Conditions{
			{
				Operand1: Operand{
					Type:  OperandField,
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
	return conditions
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
	case Boolean:
		return "boolean"
	case Int4:
		return "int4"
	case Int8:
		return "int8"
	case Real:
		return "real"
	case Double:
		return "double"
	case Varchar:
		return "varchar"
	default:
		return "unknown"
	}
}

type Column struct {
	Kind          ColumnKind
	Size          uint32
	PrimaryKey    bool
	Autoincrement bool
	Nullable      bool
	Name          string
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
	switch s.Kind {
	case CreateTable:
		if len(s.TableName) == 0 {
			return fmt.Errorf("table name is required")
		}

		if len(s.Columns) == 0 {
			return fmt.Errorf("at least one column is required")
		}

		if len(s.Columns) > MaxColumns {
			return fmt.Errorf("maximum number of columns is %d", MaxColumns)
		}

		remainingPageSpace := remainingPageSpace(s.Columns)

		if remainingPageSpace < 0 {
			return fmt.Errorf("row size %d exceeds maximum allowed %d", UsablePageSize-remainingPageSpace, UsablePageSize)
		}

		primaryKeyCount := 0
		for _, aColumn := range s.Columns {
			if aColumn.PrimaryKey {
				primaryKeyCount += 1
			}
		}
		if primaryKeyCount > 1 {
			return fmt.Errorf("only one primary key column is supported")
		}

		return nil
	case Insert:
		if len(s.Inserts) == 0 {
			return fmt.Errorf("at least one row to insert is required")
		}
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
	case Update:
		if len(s.Updates) == 0 {
			return fmt.Errorf("at least one field to update is required")
		}
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

func (stmt Statement) CreateTableDDL() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("create table \"%s\" (\n", stmt.TableName))
	for i, col := range stmt.Columns {
		sb.WriteString(fmt.Sprintf("	%s %s", col.Name, col.Kind))
		if col.Kind == Varchar {
			sb.WriteString(fmt.Sprintf("(%d)", col.Size))
		}
		if !col.Nullable {
			sb.WriteString(" not null")
		}
		if i < len(stmt.Columns)-1 {
			sb.WriteString(",\n")
		}
	}
	sb.WriteString("\n);")
	return sb.String()
}

type Iterator func(ctx context.Context) (Row, error)

type StatementResult struct {
	Columns      []Column
	Rows         Iterator
	RowsAffected int
}
