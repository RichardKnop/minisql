package minisql

import (
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

func (s StatementKind) String() string {
	switch s {
	case CreateTable:
		return "CREATE TABLE"
	case DropTable:
		return "DROP TABLE"
	case Insert:
		return "INSERT"
	case Select:
		return "SELECT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case BeginTransaction:
		return "BEGIN TRANSACTION"
	case CommitTransaction:
		return "COMMIT TRANSACTION"
	case RollbackTransaction:
		return "ROLLBACK TRANSACTION"
	default:
		return "UNKNOWN"
	}
}

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
	Unique          bool
	Index           bool
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

type Function struct {
	Name string
}

const nowFunctionName = "NOW()"

var FunctionNow = Function{Name: nowFunctionName}

type Placeholder struct{}

type Statement struct {
	Kind        StatementKind
	IfNotExists bool
	TableName   string
	Columns     []Column // use for CREATE TABLE
	// Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	// and UPDATE (UPDATEDed field names as Updates map is not ordered)
	Fields      []Field
	Aliases     map[string]string
	Inserts     [][]OptionalValue
	Updates     map[string]OptionalValue
	Functions   map[string]Function // NOW(), etc.
	Conditions  OneOrMore           // used for WHERE
	OrderBy     []OrderBy
	Limit       OptionalValue
	Offset      OptionalValue
	fetchedRows int64
}

// NumPlaceholders returns the number of placeholder parameters (?) in the statement.
func (s Statement) NumPlaceholders() int {
	count := 0

	if s.Kind == Insert {
		for _, anInsert := range s.Inserts {
			for _, aValue := range anInsert {
				if _, ok := aValue.Value.(Placeholder); ok {
					count += 1
				}
			}
		}
	}

	if s.Kind == Update {
		for _, aValue := range s.Updates {
			if _, ok := aValue.Value.(Placeholder); ok {
				count += 1
			}
		}
	}

	for _, aConditionGroup := range s.Conditions {
		for _, aCondition := range aConditionGroup {
			if aCondition.Operand2.Type == OperandPlaceholder {
				count += 1
				continue
			}
			if aCondition.Operand2.Type == OperandList {
				for _, value := range aCondition.Operand2.Value.([]any) {
					if _, ok := value.(Placeholder); ok {
						count += 1
					}
				}
			}
		}
	}

	return count
}

func (s Statement) BindArguments(args ...any) (Statement, error) {
	if s.Kind == Insert {
		for i, anInsert := range s.Inserts {
			for j, aValue := range anInsert {
				if _, ok := aValue.Value.(Placeholder); !ok {
					continue
				}
				if len(args) == 0 {
					return Statement{}, fmt.Errorf("not enough arguments to bind placeholders")
				}
				if args[0] == nil {
					s.Inserts[i][j] = OptionalValue{}
				} else {
					s.Inserts[i][j].Value = args[0]
				}
				args = args[1:]
			}
		}
	}

	if s.Kind == Update {
		for _, aField := range s.Fields {
			aValue, ok := s.Updates[aField.Name]
			if !ok {
				continue
			}
			if _, ok := aValue.Value.(Placeholder); !ok {
				continue
			}
			if len(args) == 0 {
				return Statement{}, fmt.Errorf("not enough arguments to bind placeholders")
			}
			if args[0] == nil {
				s.Updates[aField.Name] = OptionalValue{}
			} else {
				s.Updates[aField.Name] = OptionalValue{Value: args[0], Valid: true}
			}
			args = args[1:]
		}
	}

	for i, aConditionGroup := range s.Conditions {
		for j, aCondition := range aConditionGroup {
			if aCondition.Operand2.Type == OperandPlaceholder {
				if len(args) == 0 {
					return Statement{}, fmt.Errorf("not enough arguments to bind placeholders")
				}
				aCondition.Operand2.Type = operandTypeFromAny(args[0])
				aCondition.Operand2.Value = args[0]
				s.Conditions[i][j] = aCondition
				args = args[1:]
				continue
			}
			if aCondition.Operand2.Type == OperandList {
				for i, value := range aCondition.Operand2.Value.([]any) {
					if _, ok := value.(Placeholder); !ok {
						continue
					}
					aCondition.Operand2.Type = operandTypeFromAny(args[0])
					aCondition.Operand2.Value.([]any)[i] = args[0]
					args = args[1:]
				}
				s.Conditions[i][j] = aCondition
			}
		}
	}

	return s, nil
}

func operandTypeFromAny(value any) OperandType {
	switch value.(type) {
	case int64, int32:
		return OperandInteger
	case float64, float32:
		return OperandFloat
	case bool:
		return OperandBoolean
	case string, TextPointer:
		return OperandQuotedString
	default:
		return 0
	}
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
func (s Statement) Prepare(now Time) (Statement, error) {
	switch s.Kind {
	case Insert:
		var err error
		s, err = s.prepareInsert(now)
		if err != nil {
			return Statement{}, err
		}
	case Update:
		var err error
		s, err = s.prepareUpdate(now)
		if err != nil {
			return Statement{}, err
		}
	}
	return s.prepareWhere()
}

// prepareInsert makes sure to add any nullable columns that are missing from the
// insert statement, setting them to NULL. It also converts timestamp string values to int64.
func (s Statement) prepareInsert(now Time) (Statement, error) {
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

		fieldIdx := i
		for j := range s.Inserts {
			if !s.Inserts[j][fieldIdx].Valid {
				continue
			}

			if fn, ok := s.Inserts[j][fieldIdx].Value.(Function); ok {
				if fn.Name == FunctionNow.Name {
					s.Inserts[j][fieldIdx].Value = now
				} else {
					return Statement{}, fmt.Errorf("unsupported function %q in INSERT", fn.Name)
				}
			}

			if aColumn.Kind != Timestamp {
				continue
			}

			timestamp, err := parseTimeValue(s.Inserts[j][fieldIdx].Value)
			if err != nil {
				return Statement{}, err
			}
			s.Inserts[j][fieldIdx].Value = timestamp
		}

	}
	return s, nil
}

func (s Statement) prepareUpdate(now Time) (Statement, error) {
	if len(s.Updates) == 0 {
		return s, nil
	}

	for name := range s.Updates {
		aColumn, ok := s.ColumnByName(name)
		if !ok {
			return Statement{}, fmt.Errorf("unknown field %q in table %q", name, s.TableName)
		}

		updateValue, ok := s.Updates[name]
		if !ok {
			return Statement{}, fmt.Errorf("missing update value for field %q", name)
		}

		if !updateValue.Valid {
			continue
		}

		if fn, ok := updateValue.Value.(Function); ok {
			if fn.Name == FunctionNow.Name {
				updateValue.Value = now
				s.Updates[name] = updateValue
			} else {
				return Statement{}, fmt.Errorf("unsupported function %q in UPDATE", fn.Name)
			}
		} else if aColumn.Kind == Timestamp {
			timestamp, err := parseTimeValue(updateValue.Value)
			if err != nil {
				return Statement{}, err
			}
			updateValue.Value = timestamp
			s.Updates[name] = updateValue
		}

	}

	return s, nil
}

// prepareWhere converts timestamp string values in WHERE conditions to Time.
func (s Statement) prepareWhere() (Statement, error) {
	for i, aConditionGroup := range s.Conditions {
		for j, aCondition := range aConditionGroup {
			// We only want to continue if left operand is a field and right operand is not a field.
			if !aCondition.Operand1.IsField() || aCondition.Operand2.IsField() {
				continue
			}
			aColumn, ok := s.ColumnByName(aCondition.Operand1.Value.(string))
			if !ok {
				return Statement{}, fmt.Errorf("unknown field %q in table %q", aCondition.Operand1.Value.(string), s.TableName)
			}
			if aColumn.Kind != Timestamp {
				continue
			}
			if aCondition.Operand2.Type == OperandNull {
				continue
			}
			if aCondition.Operand2.Type == OperandList {
				for k, value := range aCondition.Operand2.Value.([]any) {
					timestamp, err := parseTimeValue(value)
					if err != nil {
						return Statement{}, err
					}
					s.Conditions[i][j].Operand2.Value.([]any)[k] = timestamp
				}
				continue
			}
			timestamp, err := parseTimeValue(aCondition.Operand2.Value)
			if err != nil {
				return Statement{}, err
			}
			s.Conditions[i][j].Operand2.Value = timestamp
		}
	}
	return s, nil
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

// PrepareDefaultValues validates and prepares default values for columns in CREATE TABLE statements.
// In case of TIMESTAMP columns, it transforms string default values into Time.
func (s Statement) PrepareDefaultValues() (Statement, error) {
	for i, aColumn := range s.Columns {
		if !aColumn.DefaultValue.Valid {
			continue
		}
		if aColumn.Kind == Timestamp {
			// If this is already a Time, accept it as is
			_, ok := aColumn.DefaultValue.Value.(Time)
			if ok {
				return s, nil
			}
			// Otherwise, validate and transform to Time
			_, ok = aColumn.DefaultValue.Value.(TextPointer)
			if !ok {
				return s, fmt.Errorf("default value '%s' is not a valid TextPointer", aColumn.DefaultValue.Value)
			}
			timestamp, err := ParseTimestamp(aColumn.DefaultValue.Value.(TextPointer).String())
			if err != nil {
				return s, fmt.Errorf("default value '%s' is not a valid timestamp: %v", aColumn.DefaultValue.Value, err)
			}
			aColumn.DefaultValue.Value = timestamp
			s.Columns[i] = aColumn
		}

		if err := isValueValidForColumn(aColumn, aColumn.DefaultValue); err != nil {
			return s, fmt.Errorf("invalid default value: %w", err)
		}
	}
	return s, nil
}

func (s Statement) validateCreateTable() error {
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
		indexMap         = map[string]struct{}{}
	)
	for _, aColumn := range s.Columns {
		if _, exists := nameMap[aColumn.Name]; exists {
			return fmt.Errorf("duplicate column name %s", aColumn.Name)
		}
		nameMap[aColumn.Name] = struct{}{}

		if aColumn.PrimaryKey {
			if _, ok := indexMap[aColumn.Name]; ok {
				return fmt.Errorf("column %s can only have one index", aColumn.Name)
			}
			primaryKeyColumn = aColumn
			primaryKeyCount += 1
			indexMap[aColumn.Name] = struct{}{}
		}

		if aColumn.Unique {
			if _, ok := indexMap[aColumn.Name]; ok {
				return fmt.Errorf("column %s can only have one index", aColumn.Name)
			}
			if aColumn.Kind == Text {
				return fmt.Errorf("unique key cannot be of type TEXT")
			}
			if aColumn.Kind == Varchar && aColumn.Size > MaxIndexKeySize {
				return fmt.Errorf("unique key of type VARCHAR exceeds max index key size %d", MaxIndexKeySize)
			}
			indexMap[aColumn.Name] = struct{}{}
		}

		if aColumn.Index {
			if _, ok := indexMap[aColumn.Name]; ok {
				return fmt.Errorf("column %s can only have one index", aColumn.Name)
			}
			if aColumn.Kind == Text {
				return fmt.Errorf("secondary index key cannot be of type TEXT")
			}
			if aColumn.Kind == Varchar && aColumn.Size > MaxIndexKeySize {
				return fmt.Errorf("secondary index key of type VARCHAR exceeds max index key size %d", MaxIndexKeySize)
			}
			indexMap[aColumn.Name] = struct{}{}
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
		if primaryKeyColumn.Autoincrement && primaryKeyColumn.Kind != Int8 && primaryKeyColumn.Kind != Int4 {
			return fmt.Errorf("autoincrement primary key must be of type INT4 or INT8")
		}
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

func (s Statement) validateColumnValue(aColumn Column, aValue OptionalValue) error {
	if _, ok := aValue.Value.(Placeholder); ok {
		return fmt.Errorf("unbound placeholder in value for field %q", aColumn.Name)
	}
	if !aValue.Valid && aColumn.PrimaryKey && !aColumn.Autoincrement {
		return fmt.Errorf("primary key on field %q cannot be NULL", aColumn.Name)
	}
	if !aValue.Valid && !aColumn.Nullable && !aColumn.PrimaryKey {
		return fmt.Errorf("field %q cannot be NULL", aColumn.Name)
	}
	if err := isValueValidForColumn(aColumn, aValue); err != nil {
		return fmt.Errorf("invalid field value: %w", err)
	}
	return nil
}

func isValueValidForColumn(aColumn Column, aValue OptionalValue) error {
	if !aValue.Valid {
		return nil
	}
	switch aColumn.Kind {
	case Boolean:
		_, ok := aValue.Value.(bool)
		if !ok {
			return fmt.Errorf("expects BOOLEAN value for %q", aColumn.Name)
		}
	case Int4:
		_, ok := aValue.Value.(int64)
		if !ok {
			_, ok2 := aValue.Value.(int32)
			if !ok2 {
				return fmt.Errorf("expects INT4 value for %q", aColumn.Name)
			}
		}
	case Int8:
		_, ok := aValue.Value.(int64)
		if !ok {
			return fmt.Errorf("expects INT8 value for %q", aColumn.Name)
		}
	case Real:
		_, ok := aValue.Value.(float64)
		if !ok {
			_, ok2 := aValue.Value.(float32)
			if !ok2 {
				return fmt.Errorf("expects REAL value for %q", aColumn.Name)
			}
		}
	case Double:
		_, ok := aValue.Value.(float64)
		if !ok {
			return fmt.Errorf("expects DOUBLE value for %q", aColumn.Name)
		}
	case Varchar, Text:
		tp, ok := aValue.Value.(TextPointer)
		if !ok {
			return fmt.Errorf("expects a text value for %q", aColumn.Name)
		}
		switch aColumn.Kind {
		case Varchar:
			if len([]byte(aValue.Value.(TextPointer).String())) > int(aColumn.Size) {
				return fmt.Errorf("field %q exceeds maximum VARCHAR length of %d", aColumn.Name, aColumn.Size)
			}
		case Text:
			if len([]byte(aValue.Value.(TextPointer).String())) > MaxOverflowTextSize {
				return fmt.Errorf("field %q exceeds maximum TEXT length of %d", aColumn.Name, MaxOverflowTextSize)
			}
		}
		if !utf8.ValidString(tp.String()) {
			return fmt.Errorf("expects valid UTF-8 string for %q", aColumn.Name)
		}
	case Timestamp:
		_, ok := aValue.Value.(Time)
		if !ok {
			return fmt.Errorf("expects time value for %q", aColumn.Name)
		}
	}
	return nil
}

func (s Statement) validateWhere() error {
	for _, aConditionGroup := range s.Conditions {
		equalityMap := map[string][]any{}
		for _, aCondition := range aConditionGroup {
			if aCondition.Operand1.Type != OperandField {
				return fmt.Errorf("operand1 in WHERE condition must be a field")
			}
			if aCondition.Operand2.Type == OperandPlaceholder {
				return fmt.Errorf("unbound placeholder in WHERE clause")
			}
			if !IsValidCondition(aCondition) {
				return fmt.Errorf("invalid condition in WHERE clause")
			}
			if aCondition.Operand1.Type == OperandList {
				return fmt.Errorf("operand1 in WHERE condition cannot be a list")
			}
			if aCondition.Operand2.Type == OperandList {
				var valueType string
				for _, value := range aCondition.Operand2.Value.([]any) {
					if _, ok := value.(Placeholder); ok {
						return fmt.Errorf("unbound placeholder in WHERE clause")
					}
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

			if isEquality(aCondition) {
				fieldName := aCondition.Operand1.Value.(string)
				aColumn, ok := s.ColumnByName(fieldName)
				if !ok {
					return fmt.Errorf("unknown field %q in WHERE clause", aCondition.Operand1.Value.(string))
				}

				args, err := equalityKeys(aColumn, aCondition)
				if err != nil {
					return err
				}

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
			if col.Unique {
				sb.WriteString(" unique")
			}
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
