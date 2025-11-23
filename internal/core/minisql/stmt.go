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
	Text
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
	case Text:
		return "text"
	default:
		return "unknown"
	}
}

func (k ColumnKind) IsText() bool {
	if k == Varchar {
		return true
	}
	if k == Text {
		return true
	}
	return false
}

type Column struct {
	Kind          ColumnKind
	Size          uint32
	PrimaryKey    bool
	Autoincrement bool
	Nullable      bool
	Name          string
}

func hasTextColumn(columns ...Column) bool {
	for _, aColumn := range columns {
		if aColumn.Kind.IsText() {
			return true
		}
	}
	return false
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

func (s Statement) HasField(name string) bool {
	for _, field := range s.Fields {
		if field == name {
			return true
		}
	}
	return false
}

func (s Statement) ReadOnly() bool {
	return s.Kind == Select
}

func (s Statement) Validate(aTable *Table) error {
	switch s.Kind {
	case CreateTable:
		return s.validateCreateTable()
	case Insert:
		return s.validateInsert(aTable)
	case Update:
		return s.validateUpdate(aTable)
	}

	return nil
}

func (s Statement) validateCreateTable() error {
	if len(s.TableName) == 0 {
		return fmt.Errorf("table name is required")
	}

	if len(s.Columns) == 0 {
		return fmt.Errorf("at least one column is required")
	}

	if len(s.Columns) > MaxColumns {
		return fmt.Errorf("maximum number of columns is %d", MaxColumns)
	}

	if !canInlinedRowFitInPage(s.Columns) {
		return fmt.Errorf("potential row size exceeds maximum allowed %d", UsablePageSize)
	}

	var (
		primaryKeyCount = 0
		nameMap         = map[string]struct{}{}
	)
	for _, aColumn := range s.Columns {
		if _, exists := nameMap[aColumn.Name]; exists {
			return fmt.Errorf("duplicate column name %s", aColumn.Name)
		}
		nameMap[aColumn.Name] = struct{}{}
		if aColumn.PrimaryKey {
			primaryKeyCount += 1
		}
	}
	if primaryKeyCount > 1 {
		return fmt.Errorf("only one primary key column is supported")
	}

	return nil
}

// Check whether a row with the given columns can fit in a page if all columns are inlined
func canInlinedRowFitInPage(columns []Column) bool {
	remaining := UsablePageSize
	for _, aColumn := range columns {
		if aColumn.Kind.IsText() {
			// For TEXT and VARCHAR, assume each column has maximum inline size
			// and will take 4+255 bytes each (length prefix + max varchar inline size)
			remaining -= (varcharLengthPrefixSize + MaxInlineVarchar)
		} else {
			remaining -= int(aColumn.Size)
		}
		if remaining < 0 {
			return false
		}
	}
	return true
}

func (s Statement) validateInsert(aTable *Table) error {
	if len(s.Inserts) == 0 {
		return fmt.Errorf("at least one row to insert is required")
	}
	if len(s.Columns) != len(aTable.Columns) {
		return fmt.Errorf("insert: expected %d columns, got %d", len(aTable.Columns), len(s.Columns))
	}
	for _, aColumn := range s.Columns {
		if !aColumn.Nullable {
			if aColumn.PrimaryKey && aColumn.Autoincrement {
				continue
			}
			if !s.HasField(aColumn.Name) {
				return fmt.Errorf("missing required field %q", aColumn.Name)
			}
		}
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
			if !anInsert[i].Valid && !aColumn.Nullable && !(aColumn.PrimaryKey && aColumn.Autoincrement) {
				return fmt.Errorf("field %q cannot be NULL", aField)
			}
			if anInsert[i].Valid {
				if aColumn.Kind.IsText() && !utf8.ValidString(anInsert[i].Value.(TextPointer).String()) {
					return fmt.Errorf("field %q expects valid UTF-8 string", aField)
				}
			}
			if aColumn.Kind.IsText() && anInsert[i].Valid {
				switch aColumn.Kind {
				case Varchar:
					if len([]byte(anInsert[i].Value.(TextPointer).String())) > int(aColumn.Size) {
						return fmt.Errorf("field %q exceeds maximum VARCHAR length of %d", aField, aColumn.Size)
					}
				case Text:
					if len([]byte(anInsert[i].Value.(TextPointer).String())) > MaxOverflowTextSize {
						return fmt.Errorf("field %q exceeds maximum TEXT length of %d", aField, MaxOverflowTextSize)
					}
				}

			}
		}
	}
	return nil
}

func (s Statement) validateUpdate(aTable *Table) error {
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
			if aColumn.Kind == Varchar && !utf8.ValidString(s.Updates[aField].Value.(TextPointer).String()) {
				return fmt.Errorf("field %q expects valid UTF-8 string", aField)
			}
		}
	}
	return nil
}

func (s Statement) CreateTableDDL() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("create table \"%s\" (\n", s.TableName))
	for i, col := range s.Columns {
		sb.WriteString(fmt.Sprintf("	%s %s", col.Name, col.Kind))
		if col.Kind == Varchar {
			sb.WriteString(fmt.Sprintf("(%d)", col.Size))
		}
		if col.PrimaryKey {
			sb.WriteString(" primary key")
			if col.Autoincrement {
				sb.WriteString(" autoincrement")
			}
		} else if !col.Nullable {
			sb.WriteString(" not null")
		}
		if i < len(s.Columns)-1 {
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

func (stmt Statement) InsertForColumn(name string, insertIdx int) (OptionalValue, bool) {
	fieldIdx := -1
	for i, colName := range stmt.Fields {
		if colName == name {
			fieldIdx = i
			break
		}
	}
	if fieldIdx == -1 {
		return OptionalValue{}, false
	}
	if insertIdx < 0 || insertIdx >= len(stmt.Inserts) {
		return OptionalValue{}, false
	}
	value := stmt.Inserts[insertIdx][fieldIdx]

	return value, true
}

func (stmt Statement) ColumnIdx(name string) int {
	for i, colName := range stmt.Fields {
		if colName == name {
			return i
		}
	}
	return -1
}
