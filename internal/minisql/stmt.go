package minisql

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

type StatementKind int

const (
	CreateTable StatementKind = iota + 1
	DropTable
	Insert
	Select
	Update
	Delete
	BeginTransaction
	CommitTransaction
	RollbackTransaction
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
	Timestamp
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
	case Timestamp:
		return "timestamp"
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
	Kind            ColumnKind
	Size            uint32
	PrimaryKey      bool
	Autoincrement   bool
	Nullable        bool
	DefaultValue    OptionalValue
	DefaultValueNow bool
	Name            string
}

func textOverflowColumns(columns ...Column) []Column {
	overflowColumns := make([]Column, 0, len(columns))
	for _, aColumn := range columns {
		if !aColumn.Kind.IsText() {
			continue
		}
		if aColumn.Kind == Varchar && aColumn.Size <= MaxInlineVarchar {
			continue
		}
		overflowColumns = append(overflowColumns, aColumn)
	}
	return overflowColumns
}

func textOverflowFields(columns ...Column) []Field {
	overflowFields := make([]Field, 0, len(columns))
	for _, aColumn := range columns {
		if !aColumn.Kind.IsText() {
			continue
		}
		if aColumn.Kind == Varchar && aColumn.Size <= MaxInlineVarchar {
			continue
		}
		overflowFields = append(overflowFields, Field{Name: aColumn.Name})
	}
	return overflowFields
}

type Field struct {
	Name string
}

type Direction int

const (
	Asc Direction = iota + 1
	Desc
)

func (d Direction) String() string {
	switch d {
	case Asc:
		return "ASC"
	case Desc:
		return "DESC"
	default:
		return "UNKNOWN"
	}
}

type OrderBy struct {
	Field     Field
	Direction Direction
}

type Statement struct {
	Kind        StatementKind
	IfNotExists bool
	TableName   string
	Columns     []Column // use for CREATE TABLE
	Fields      []Field  // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases     map[string]string
	Inserts     [][]OptionalValue
	Updates     map[string]OptionalValue
	Conditions  OneOrMore // used for WHERE
	OrderBy     []OrderBy
	Limit       OptionalValue
	Offset      OptionalValue
	fetchedRows int64
}

func (s Statement) HasField(name string) bool {
	for _, field := range s.Fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

func (s Statement) ReadOnly() bool {
	return s.Kind == Select
}

func (s Statement) ColumnByName(name string) (Column, bool) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return s.Columns[i], true
		}
	}
	return Column{}, false
}

// Prepare performs any necessary preparation on the statement before validation/execution.
func (s *Statement) Prepare(now Time) error {
	switch s.Kind {
	case Insert:
		if err := s.prepareInsert(now); err != nil {
			return err
		}
	case Update:
		if err := s.prepareUpdate(); err != nil {
			return err
		}
	}
	return s.prepareWhere()
}

// prepareInsert makes sure to add any nullable columns that are missing from the
// insert statement, setting them to NULL. It also converts timestamp string values to int64.
func (s *Statement) prepareInsert(now Time) error {
	for i, aColumn := range s.Columns {
		if !s.HasField(aColumn.Name) {
			s.Fields = slices.Insert(s.Fields, i, Field{Name: aColumn.Name})
			for j := range s.Inserts {
				var value OptionalValue
				if aColumn.DefaultValue.Valid {
					value = aColumn.DefaultValue
				} else if aColumn.DefaultValueNow {
					value = OptionalValue{Valid: true, Value: now}
				}
				s.Inserts[j] = slices.Insert(s.Inserts[j], i, value)
			}
		}

		if aColumn.Kind != Timestamp {
			continue
		}

		fieldIdx := i
		for j := range s.Inserts {
			if !s.Inserts[j][fieldIdx].Valid {
				continue
			}
			timestamp, err := parseTimeValue(s.Inserts[j][fieldIdx].Value)
			if err != nil {
				return err
			}
			s.Inserts[j][fieldIdx].Value = timestamp
		}
	}
	return nil
}

func (s *Statement) prepareUpdate() error {
	if len(s.Updates) == 0 {
		return nil
	}
	for _, aField := range s.Fields {
		aColumn, ok := s.ColumnByName(aField.Name)
		if !ok {
			return fmt.Errorf("unknown field %q in table %q", aField.Name, s.TableName)
		}
		if aColumn.Kind != Timestamp {
			continue
		}
		for aColumnName, updateValue := range s.Updates {
			if aColumnName != aField.Name || !updateValue.Valid {
				continue
			}
			timestamp, err := parseTimeValue(updateValue.Value)
			if err != nil {
				return err
			}
			s.Updates[aColumnName] = OptionalValue{Valid: true, Value: timestamp}
		}
	}
	return nil
}

// prepareWhere converts timestamp string values in WHERE conditions to Time.
func (s *Statement) prepareWhere() error {
	for i, aConditionGroup := range s.Conditions {
		for j, aCondition := range aConditionGroup {
			// left side is field, right side is literal value
			if aCondition.Operand1.IsField() && !aCondition.Operand2.IsField() {
				aColumn, ok := s.ColumnByName(aCondition.Operand1.Value.(string))
				if !ok {
					return fmt.Errorf("unknown field %q in table %q", aCondition.Operand1.Value.(string), s.TableName)
				}
				if aColumn.Kind != Timestamp {
					continue
				}
				if aCondition.Operand2.Type == OperandList {
					for k, value := range aCondition.Operand2.Value.([]any) {
						timestamp, err := parseTimeValue(value)
						if err != nil {
							return err
						}
						s.Conditions[i][j].Operand2.Value.([]any)[k] = timestamp
					}
				} else {
					timestamp, err := parseTimeValue(aCondition.Operand2.Value)
					if err != nil {
						return err
					}
					s.Conditions[i][j].Operand2.Value = timestamp
				}
			}
			// left side is literal value, right side is field
			if aCondition.Operand2.IsField() && !aCondition.Operand1.IsField() {
				aColumn, ok := s.ColumnByName(aCondition.Operand2.Value.(string))
				if !ok {
					return fmt.Errorf("unknown field %q in table %q", aCondition.Operand2.Value.(string), s.TableName)
				}
				if aColumn.Kind != Timestamp {
					continue
				}
				if aCondition.Operand1.Type == OperandList {
					for k, value := range aCondition.Operand1.Value.([]any) {
						timestamp, err := parseTimeValue(value)
						if err != nil {
							return err
						}
						s.Conditions[i][j].Operand1.Value.([]any)[k] = timestamp
					}
				} else {
					timestamp, err := parseTimeValue(aCondition.Operand1.Value)
					if err != nil {
						return err
					}
					s.Conditions[i][j].Operand1.Value = timestamp
				}
			}
		}
	}
	return nil
}

func parseTimeValue(value any) (Time, error) {
	_, ok := value.(Time)
	if ok {
		return value.(Time), nil
	}
	tp, ok := value.(TextPointer)
	if !ok {
		return Time{}, fmt.Errorf("timestamp field expects TextPointer value")
	}
	timestamp, err := ParseTimestamp(tp.String())
	if err != nil {
		return Time{}, fmt.Errorf("invalid timestamp format for field: %v", err)
	}
	return timestamp, nil
}

func (s Statement) Validate(aTable *Table) error {
	switch s.Kind {
	case CreateTable:
		if err := s.validateCreateTable(); err != nil {
			return err
		}
	case Insert:
		if err := s.validateInsert(aTable); err != nil {
			return err
		}
	case Update:
		if err := s.validateUpdate(aTable); err != nil {
			return err
		}
	case Select:
		if err := s.validateSelect(aTable); err != nil {
			return err
		}
	}

	if err := s.validateWhere(); err != nil {
		return err
	}

	return nil
}

func (s *Statement) validateCreateTable() error {
	if len(s.TableName) == 0 {
		return fmt.Errorf("table name is required")
	}

	if len(s.Conditions) > 0 {
		return fmt.Errorf("CREATE TABLE cannot have WHERE conditions")
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
		primaryKeyCount  = 0
		primaryKeyColumn Column
		nameMap          = map[string]struct{}{}
	)
	for i, aColumn := range s.Columns {
		if aColumn.DefaultValue.Valid {
			validColumn, err := validateDefaultValue(aColumn)
			if err != nil {
				return err
			}
			s.Columns[i] = validColumn
		}
		if _, exists := nameMap[aColumn.Name]; exists {
			return fmt.Errorf("duplicate column name %s", aColumn.Name)
		}
		nameMap[aColumn.Name] = struct{}{}
		if aColumn.PrimaryKey {
			primaryKeyColumn = aColumn
			primaryKeyCount += 1
		}
	}
	if primaryKeyCount > 1 {
		return fmt.Errorf("only one primary key column is supported")
	}
	if primaryKeyCount == 1 {
		if primaryKeyColumn.Nullable {
			return fmt.Errorf("primary key column cannot be nullable")
		}
		if primaryKeyColumn.Kind == Text {
			return fmt.Errorf("primary key cannot be of type TEXT")
		}
		if primaryKeyColumn.Kind == Varchar && primaryKeyColumn.Size > MaxIndexKeySize {
			return fmt.Errorf("primary key of type VARCHAR exceeds max index key size %d", MaxIndexKeySize)
		}
	}

	return nil
}

func validateDefaultValue(aColumn Column) (Column, error) {
	switch aColumn.Kind {
	case Boolean:
		_, ok := aColumn.DefaultValue.Value.(bool)
		if !ok {
			return aColumn, fmt.Errorf("default value '%s' is not a valid boolean", aColumn.DefaultValue.Value)
		}
	case Int4, Int8:
		_, ok := aColumn.DefaultValue.Value.(int64)
		if !ok {
			return aColumn, fmt.Errorf("default value '%s' is not a valid integer", aColumn.DefaultValue.Value)
		}
	case Real, Double:
		_, ok := aColumn.DefaultValue.Value.(float64)
		if !ok {
			return aColumn, fmt.Errorf("default value '%s' is not a valid float", aColumn.DefaultValue.Value)
		}
	case Text, Varchar:
		// If this is already a TextPointer, accept it as is
		_, ok := aColumn.DefaultValue.Value.(TextPointer)
		if ok {
			return aColumn, nil
		}
		// Otherwise, validate and transform to TextPointer
		_, ok = aColumn.DefaultValue.Value.(string)
		if !ok {
			return aColumn, fmt.Errorf("default value '%s' is not a valid string", aColumn.DefaultValue.Value)
		}
		if len(aColumn.DefaultValue.Value.(string)) > MaxInlineVarchar {
			return aColumn, fmt.Errorf("default value '%s' exceeds maximum inline text size of %d", aColumn.DefaultValue.Value, MaxInlineVarchar)
		}
		aColumn.DefaultValue.Value = NewTextPointer([]byte(aColumn.DefaultValue.Value.(string)))
	case Timestamp:
		// If this is already a Time, accept it as is
		_, ok := aColumn.DefaultValue.Value.(Time)
		if ok {
			return aColumn, nil
		}
		// Otherwise, validate and transform to Time
		_, ok = aColumn.DefaultValue.Value.(string)
		if !ok {
			return aColumn, fmt.Errorf("default value '%s' is not a valid string", aColumn.DefaultValue.Value)
		}
		timestamp, err := ParseTimestamp(aColumn.DefaultValue.Value.(string))
		if err != nil {
			return aColumn, fmt.Errorf("default value '%s' is not a valid timestamp: %v", aColumn.DefaultValue.Value, err)
		}
		aColumn.DefaultValue.Value = timestamp
	}
	return aColumn, nil
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

	if len(s.Conditions) > 0 {
		return fmt.Errorf("INSERT cannot have WHERE conditions")
	}

	for _, aColumn := range s.Columns {
		if aColumn.Nullable {
			continue
		}
		if aColumn.PrimaryKey && aColumn.Autoincrement || aColumn.DefaultValue.Valid {
			continue
		}
		if !s.HasField(aColumn.Name) {
			return fmt.Errorf("missing required field %q", aColumn.Name)
		}
	}
	for i, aField := range s.Fields {
		aColumn, ok := aTable.ColumnByName(aField.Name)
		if !ok {
			return fmt.Errorf("unknown field %q in table %q", aField.Name, aTable.Name)
		}
		for _, anInsert := range s.Inserts {
			if len(anInsert) != len(s.Fields) {
				return fmt.Errorf("insert: expected %d values, got %d", len(s.Fields), len(anInsert))
			}
			if err := s.validateColumnValue(aColumn, anInsert[i]); err != nil {
				return err
			}
			if aColumn.Kind.IsText() && anInsert[i].Valid {
				if !utf8.ValidString(anInsert[i].Value.(TextPointer).String()) {
					return fmt.Errorf("field %q expects valid UTF-8 string", aColumn.Name)
				}
				switch aColumn.Kind {
				case Varchar:
					if len([]byte(anInsert[i].Value.(TextPointer).String())) > int(aColumn.Size) {
						return fmt.Errorf("field %q exceeds maximum VARCHAR length of %d", aColumn.Name, aColumn.Size)
					}
				case Text:
					if len([]byte(anInsert[i].Value.(TextPointer).String())) > MaxOverflowTextSize {
						return fmt.Errorf("field %q exceeds maximum TEXT length of %d", aColumn.Name, MaxOverflowTextSize)
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
		aColumn, ok := aTable.ColumnByName(aField.Name)
		if !ok {
			return fmt.Errorf("unknown field %q in table %q", aField.Name, aTable.Name)
		}
		if err := s.validateColumnValue(aColumn, s.Updates[aField.Name]); err != nil {
			return err
		}
	}
	return nil
}

func (s Statement) validateSelect(aTable *Table) error {
	if len(s.Fields) == 0 {
		return fmt.Errorf("at least one field to select is required")
	}
	if s.Limit.Valid {
		limitValue, ok := s.Limit.Value.(int64)
		if !ok || limitValue < 0 {
			return fmt.Errorf("LIMIT must be a non-negative integer")
		}
	}
	if s.Offset.Valid {
		offsetValue, ok := s.Offset.Value.(int64)
		if !ok || offsetValue < 0 {
			return fmt.Errorf("OFFSET must be a non-negative integer")
		}
	}

	if s.IsSelectCountAll() {
		if len(s.OrderBy) > 0 {
			return fmt.Errorf("ORDER BY cannot be used with COUNT(*)")
		}
		if s.Offset.Valid {
			return fmt.Errorf("OFFSET cannot be used with COUNT(*)")
		}
		if s.Limit.Valid {
			return fmt.Errorf("LIMIT cannot be used with COUNT(*)")
		}
	}
	if s.IsSelectAll() || s.IsSelectCountAll() {
		return nil
	}

	fieldMap := map[string]struct{}{}
	for _, aField := range s.Fields {
		_, ok := aTable.ColumnByName(aField.Name)
		if !ok {
			return fmt.Errorf("unknown field %q in table %q", aField.Name, aTable.Name)
		}
		if _, exists := fieldMap[aField.Name]; exists {
			return fmt.Errorf("duplicate field %q in select statement", aField.Name)
		}
		fieldMap[aField.Name] = struct{}{}
	}

	if len(s.OrderBy) > 0 {
		for _, anOrderBy := range s.OrderBy {
			_, ok := aTable.ColumnByName(anOrderBy.Field.Name)
			if !ok {
				return fmt.Errorf("unknown field %q in ORDER BY clause", anOrderBy.Field.Name)
			}
		}
	}

	return nil
}

func (s Statement) validateColumnValue(aColumn Column, insertValue OptionalValue) error {
	if !insertValue.Valid && aColumn.PrimaryKey && !aColumn.Autoincrement {
		return fmt.Errorf("primary key on field %q cannot be NULL", aColumn.Name)
	}
	if !insertValue.Valid && !aColumn.Nullable && !aColumn.PrimaryKey {
		return fmt.Errorf("field %q cannot be NULL", aColumn.Name)
	}
	if !insertValue.Valid {
		return nil
	}
	switch aColumn.Kind {
	case Boolean:
		_, ok := insertValue.Value.(bool)
		if !ok {
			return fmt.Errorf("field %q expects BOOLEAN value", aColumn.Name)
		}
	case Int4:
		_, ok := insertValue.Value.(int64)
		if !ok {
			_, ok2 := insertValue.Value.(int32)
			if !ok2 {
				return fmt.Errorf("field %q expects INT4 value", aColumn.Name)
			}
		}
	case Int8:
		_, ok := insertValue.Value.(int64)
		if !ok {
			return fmt.Errorf("field %q expects INT8 value", aColumn.Name)
		}
	case Real:
		_, ok := insertValue.Value.(float64)
		if !ok {
			_, ok2 := insertValue.Value.(float32)
			if !ok2 {
				return fmt.Errorf("field %q expects REAL value", aColumn.Name)
			}
		}
	case Double:
		_, ok := insertValue.Value.(float64)
		if !ok {
			return fmt.Errorf("field %q expects DOUBLE value", aColumn.Name)
		}
	case Varchar, Text:
		tp, ok := insertValue.Value.(TextPointer)
		if !ok {
			return fmt.Errorf("field %q expects a text value", aColumn.Name)
		}
		if aColumn.Kind.IsText() && !utf8.ValidString(tp.String()) {
			return fmt.Errorf("field %q expects valid UTF-8 string", aColumn.Name)
		}
	case Timestamp:
		_, ok := insertValue.Value.(Time)
		if !ok {
			return fmt.Errorf("field %q expects time value", aColumn.Name)
		}
	}
	return nil
}

func (s Statement) validateWhere() error {
	for _, aConditionGroup := range s.Conditions {
		equalityMap := map[string][]any{}
		for _, aCondition := range aConditionGroup {
			if !IsValidCondition(aCondition) {
				return fmt.Errorf("invalid condition in WHERE clause")
			}
			if aCondition.Operand1.Type == OperandList {
				return fmt.Errorf("operand1 in WHERE condition cannot be a list")
			}
			if aCondition.Operand2.Type == OperandList {
				var valueType string
				for _, value := range aCondition.Operand2.Value.([]any) {
					if valueType == "" {
						valueType = fmt.Sprintf("%T", value)
						_, ok := value.(bool)
						if ok {
							return fmt.Errorf("IN / NOT IN operator not supported for boolean columns")
						}
						continue
					}
					if fmt.Sprintf("%T", value) != valueType {
						return fmt.Errorf("mixed operand types in WHERE condition list")
					}
				}
			}

			if args, ok := isEquality(aCondition); ok {
				fieldName := aCondition.Operand1.Value.(string)
				_, ok2 := equalityMap[fieldName]
				if !ok2 {
					equalityMap[fieldName] = args
				} else {
					return fmt.Errorf("conflicting equality conditions for field %q in WHERE clause", fieldName)
				}
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
		} else {
			if !col.Nullable {
				sb.WriteString(" not null")
			}
			if col.DefaultValueNow {
				sb.WriteString(" default now()")
			} else if col.DefaultValue.Valid {
				switch col.Kind {
				case Boolean:
					if col.DefaultValue.Value.(bool) {
						sb.WriteString(" default true")
					} else {
						sb.WriteString(" default false")
					}
				case Int4, Int8:
					sb.WriteString(fmt.Sprintf(" default %d", col.DefaultValue.Value.(int64)))
				case Real, Double:
					sb.WriteString(fmt.Sprintf(" default %f", col.DefaultValue.Value.(float64)))
				case Varchar, Text:
					sb.WriteString(fmt.Sprintf(" default '%s'", col.DefaultValue.Value.(TextPointer).String()))
				case Timestamp:
					sb.WriteString(fmt.Sprintf(" default '%s'", col.DefaultValue.Value.(Time).String()))
				}
			}
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
	Count        int64
	Rows         Iterator
	RowsAffected int
}

func (r StatementResult) CollectRows(ctx context.Context) []Row {
	results := []Row{}
	aRow, err := r.Rows(ctx)
	for ; err == nil; aRow, err = r.Rows(ctx) {
		results = append(results, aRow)
	}
	return results
}

func (stmt Statement) InsertForColumn(name string, insertIdx int) (OptionalValue, bool) {
	fieldIdx := -1
	for i, aField := range stmt.Fields {
		if aField.Name == name {
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

func (s Statement) ColumnIdx(name string) int {
	for i, aColumn := range s.Columns {
		if aColumn.Name == name {
			return i
		}
	}
	return -1
}

func (s Statement) IsSelectCountAll() bool {
	return s.ReadOnly() && len(s.Fields) == 1 && strings.ToUpper(s.Fields[0].Name) == "COUNT(*)"
}

func (s Statement) IsSelectAll() bool {
	return s.ReadOnly() && len(s.Fields) == 1 && s.Fields[0].Name == "*"
}
