package minisql

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// ConflictAction describes what to do when an INSERT violates a uniqueness constraint.
type ConflictAction int

const (
	// ConflictActionNone is the default: propagate the error.
	ConflictActionNone ConflictAction = iota
	// ConflictActionDoNothing silently skips the offending row.
	ConflictActionDoNothing
	// ConflictActionDoUpdate applies the SET assignments to the conflicting row.
	ConflictActionDoUpdate
)

// StatementKind ...
type StatementKind int

// StatementKind constants enumerate the supported SQL statement types.
const (
	// CreateTable ...
	CreateTable StatementKind = iota + 1
	DropTable
	CreateIndex
	DropIndex
	Insert
	Select
	Update
	Delete
	BeginTransaction
	CommitTransaction
	RollbackTransaction
	Analyze
	Vacuum
	Pragma
)

func (s StatementKind) String() string {
	switch s {
	case CreateTable:
		return "CREATE TABLE"
	case DropTable:
		return "DROP TABLE"
	case CreateIndex:
		return "CREATE INDEX"
	case DropIndex:
		return "DROP INDEX"
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
	case Analyze:
		return "ANALYZE"
	case Vacuum:
		return "VACUUM"
	case Pragma:
		return "PRAGMA"
	default:
		return "UNKNOWN"
	}
}

// AggregateKind identifies the type of aggregate function.
type AggregateKind int

// AggregateKind constants enumerate the supported aggregate functions.
const (
	// AggregateCount ...
	AggregateCount AggregateKind = iota + 1 // COUNT(*)
	// AggregateSum ...
	AggregateSum // SUM(col)
	// AggregateAvg is the AVG aggregate function.
	AggregateAvg // AVG(col)
	// AggregateMin is the MIN aggregate function.
	AggregateMin // MIN(col)
	// AggregateMax is the MAX aggregate function.
	AggregateMax // MAX(col)
)

func (k AggregateKind) String() string {
	switch k {
	case AggregateCount:
		return "COUNT"
	case AggregateSum:
		return "SUM"
	case AggregateAvg:
		return "AVG"
	case AggregateMin:
		return "MIN"
	case AggregateMax:
		return "MAX"
	default:
		return "UNKNOWN"
	}
}

// AggregateExpr describes a single aggregate function call in a SELECT list.
// Parallel to Fields: Fields[i] holds the synthetic output column name (e.g. "SUM(price)"),
// Aggregates[i] holds the kind and source column.
// A zero-value AggregateExpr (Kind == 0) means the corresponding field is not an aggregate.
// Aggregates is only populated when the query contains at least one aggregate function.
type AggregateExpr struct {
	Kind   AggregateKind
	Column string // source column name; empty for COUNT(*)
}

// ColumnKind ...
type ColumnKind int

// ColumnKind constants enumerate the supported column data types.
const (
	// Boolean ...
	Boolean ColumnKind = iota + 1
	Int4
	Int8
	Real
	Double
	Varchar
	Text
	Timestamp
)

// IsInt ...
func (k ColumnKind) IsInt() bool {
	return k == Int4 || k == Int8
}

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

// IsText ...
func (k ColumnKind) IsText() bool {
	if k == Varchar {
		return true
	}
	if k == Text {
		return true
	}
	return false
}

// Column ...
type Column struct {
	Kind ColumnKind
	Size uint32
	// PrimaryKey      bool
	// Autoincrement   bool
	// Unique          bool
	Nullable        bool
	DefaultValue    OptionalValue
	DefaultValueNow bool
	Name            string
}

func fieldsFromColumns(columns ...Column) []Field {
	fields := make([]Field, 0, len(columns))
	for _, col := range columns {
		fields = append(fields, Field{Name: col.Name})
	}
	return fields
}

func textOverflowColumns(columns ...Column) []Column {
	overflowColumns := make([]Column, 0, len(columns))
	for _, col := range columns {
		if !col.Kind.IsText() {
			continue
		}
		if col.Kind == Varchar && col.Size <= MaxInlineVarchar {
			continue
		}
		overflowColumns = append(overflowColumns, col)
	}
	return overflowColumns
}

func textOverflowFields(columns ...Column) []Field {
	overflowFields := make([]Field, 0, len(columns))
	for _, col := range columns {
		if !col.Kind.IsText() {
			continue
		}
		if col.Kind == Varchar && col.Size <= MaxInlineVarchar {
			continue
		}
		overflowFields = append(overflowFields, Field{Name: col.Name})
	}
	return overflowFields
}

// Field ...
type Field struct {
	AliasPrefix string
	Name        string
	// Alias is the output column name when an AS clause is used on a computed
	// expression (e.g. SELECT price * 1.1 AS discounted).
	Alias string
	// Expr is non-nil when the field is a computed arithmetic expression rather
	// than a plain column reference.
	Expr *Expr
}

func (f Field) String() string {
	if f.AliasPrefix != "" {
		return fmt.Sprintf("%s.%s", f.AliasPrefix, f.Name)
	}
	return f.Name
}

// OutputName returns the name to use for this field in result column metadata.
// For computed expressions it returns the Alias (if set) or the expression string.
func (f Field) OutputName() string {
	if f.Expr != nil {
		if f.Alias != "" {
			return f.Alias
		}
		return f.Expr.String()
	}
	if f.Alias != "" {
		return f.Alias
	}
	return f.Name
}

// Direction ...
type Direction int

// Direction constants define the sort order for ORDER BY clauses.
const (
	// Asc ...
	Asc Direction = iota + 1
	// Desc is the descending sort direction.
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

// OrderBy ...
type OrderBy struct {
	Field     Field
	Direction Direction
}

// Function ...
type Function struct {
	Name string
}

const nowFunctionName = "NOW()"

// FunctionNow ...
var FunctionNow = Function{Name: nowFunctionName}

// Placeholder ...
type Placeholder struct{}

// ExcludedRef represents a reference to EXCLUDED.column_name inside an
// ON CONFLICT DO UPDATE SET clause.  At upsert time it resolves to the value
// that was proposed for insertion but rejected due to the conflict.
type ExcludedRef struct {
	Column string
}

// Statement ...
type Statement struct {
	Kind          StatementKind
	IfNotExists   bool
	TableName     string // for SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE etc
	TableAlias    string
	Joins         []Join
	IndexName     string        // for CREATE/DROP INDEX
	Target        string        // for ANALYZE
	PragmaName    string        // for PRAGMA
	Columns       []Column      // use for CREATE TABLE
	PrimaryKey    PrimaryKey    // use for CREATE TABLE
	UniqueIndexes []UniqueIndex // use for CREATE TABLE
	// Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	// and UPDATE (UPDATEDed field names as Updates map is not ordered)
	Distinct       bool
	Fields         []Field
	Aggregates     []AggregateExpr // parallel to Fields; only populated when query uses aggregate functions
	GroupBy        []Field         // columns in GROUP BY clause; only populated for grouped queries
	Having         OneOrMore       // HAVING conditions; only populated for grouped queries
	Aliases        map[string]string
	ConflictAction ConflictAction // INSERT ON CONFLICT action
	Inserts        [][]OptionalValue
	Updates        map[string]OptionalValue
	Functions      map[string]Function // NOW(), etc.
	Conditions     OneOrMore           // used for WHERE
	OrderBy        []OrderBy
	Limit          OptionalValue
	Offset         OptionalValue
}

// NumPlaceholders returns the number of placeholder parameters (?) in the statement.
func (s Statement) NumPlaceholders() int {
	count := 0

	if s.Kind == Insert {
		for _, anInsert := range s.Inserts {
			for _, val := range anInsert {
				if _, ok := val.Value.(Placeholder); ok {
					count += 1
				}
			}
		}
		// Also count placeholders in the DO UPDATE SET clause.
		if s.ConflictAction == ConflictActionDoUpdate {
			for _, val := range s.Updates {
				if _, ok := val.Value.(Placeholder); ok {
					count += 1
				}
			}
		}
	}

	if s.Kind == Update {
		for _, val := range s.Updates {
			if _, ok := val.Value.(Placeholder); ok {
				count += 1
			}
		}
	}

	for _, condGroup := range s.Conditions {
		for _, cond := range condGroup {
			if cond.Operand2.Type == OperandPlaceholder {
				count += 1
				continue
			}
			if cond.Operand2.Type == OperandList {
				for _, value := range cond.Operand2.Value.([]any) {
					if _, ok := value.(Placeholder); ok {
						count += 1
					}
				}
			}
		}
	}

	return count
}

// Clone ...
func (s Statement) Clone() Statement {
	stmt := Statement{
		Kind:           s.Kind,
		IfNotExists:    s.IfNotExists,
		TableName:      s.TableName,
		IndexName:      s.IndexName,
		PragmaName:     s.PragmaName,
		ConflictAction: s.ConflictAction,
		Columns:        s.Columns,
		Distinct:       s.Distinct,
		Fields:         s.Fields,
		Aggregates:     s.Aggregates, // slice of value types, safe to share
		Aliases:        s.Aliases,
		Inserts:        make([][]OptionalValue, len(s.Inserts)),
		Updates:        make(map[string]OptionalValue, len(s.Updates)),
		Functions:      s.Functions,
		Conditions:     make(OneOrMore, len(s.Conditions)),
		OrderBy:        s.OrderBy,
		Limit:          s.Limit,
		Offset:         s.Offset,
	}
	for i := range s.Inserts {
		stmt.Inserts[i] = make([]OptionalValue, len(s.Inserts[i]))
		copy(stmt.Inserts[i], s.Inserts[i])
	}
	maps.Copy(stmt.Updates, s.Updates)
	for i := range s.Conditions {
		stmt.Conditions[i] = make([]Condition, len(s.Conditions[i]))
		copy(stmt.Conditions[i], s.Conditions[i])
	}
	return stmt
}

// BindArguments ...
func (s Statement) BindArguments(args ...any) (Statement, error) {
	// Clone statement so we can keep using the preparement statement
	// with different arguments without modifying it
	stmt := s.Clone()

	if s.Kind == Insert {
		for i, anInsert := range stmt.Inserts {
			for j, val := range anInsert {
				if _, ok := val.Value.(Placeholder); !ok {
					continue
				}
				if len(args) == 0 {
					return Statement{}, errors.New("not enough arguments to bind placeholders")
				}
				if args[0] == nil {
					stmt.Inserts[i][j] = OptionalValue{}
				} else {
					stmt.Inserts[i][j].Value = args[0]
				}
				args = args[1:]
			}
		}
		// Bind DO UPDATE SET placeholders in field order.
		if s.ConflictAction == ConflictActionDoUpdate {
			insertFieldCount := len(stmt.Fields) - len(stmt.Updates)
			for _, field := range stmt.Fields[insertFieldCount:] {
				val, ok := stmt.Updates[field.Name]
				if !ok {
					continue
				}
				if _, ok := val.Value.(Placeholder); !ok {
					continue
				}
				if len(args) == 0 {
					return Statement{}, errors.New("not enough arguments to bind placeholders")
				}
				if args[0] == nil {
					stmt.Updates[field.Name] = OptionalValue{}
				} else {
					stmt.Updates[field.Name] = OptionalValue{Value: args[0], Valid: true}
				}
				args = args[1:]
			}
		}
	}

	if s.Kind == Update {
		for _, field := range stmt.Fields {
			val, ok := stmt.Updates[field.Name]
			if !ok {
				continue
			}
			if _, ok := val.Value.(Placeholder); !ok {
				continue
			}
			if len(args) == 0 {
				return Statement{}, errors.New("not enough arguments to bind placeholders")
			}
			if args[0] == nil {
				stmt.Updates[field.Name] = OptionalValue{}
			} else {
				stmt.Updates[field.Name] = OptionalValue{Value: args[0], Valid: true}
			}
			args = args[1:]
		}
	}

	for i, condGroup := range stmt.Conditions {
		for j, cond := range condGroup {
			if cond.Operand2.Type == OperandPlaceholder {
				if len(args) == 0 {
					return Statement{}, errors.New("not enough arguments to bind placeholders")
				}
				cond.Operand2.Type = operandTypeFromAny(args[0])
				cond.Operand2.Value = args[0]
				stmt.Conditions[i][j] = cond
				args = args[1:]
				continue
			}
			if cond.Operand2.Type == OperandList {
				origList := cond.Operand2.Value.([]any)
				newList := make([]any, len(origList))
				copy(newList, origList)
				for k, value := range newList {
					if _, ok := value.(Placeholder); !ok {
						continue
					}
					if len(args) == 0 {
						return Statement{}, errors.New("not enough arguments to bind placeholders")
					}
					newList[k] = args[0]
					args = args[1:]
				}
				cond.Operand2.Value = newList
				stmt.Conditions[i][j] = cond
			}
		}
	}

	return stmt, nil
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

// HasField ...
func (s Statement) HasField(name string) bool {
	for _, field := range s.Fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

// ReadOnly ...
func (s Statement) ReadOnly() bool {
	return s.Kind == Select || s.Kind == Pragma
}

// IsDDL ...
func (s Statement) IsDDL() bool {
	return s.Kind == CreateTable || s.Kind == DropTable || s.Kind == CreateIndex || s.Kind == DropIndex
}

// ColumnByName ...
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
	case CreateTable:
		var err error
		s, err = s.prepareCreateTable()
		if err != nil {
			return Statement{}, err
		}
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

// prepareCreateTable validates and prepares default values for columns in CREATE TABLE statements.
// In case of TIMESTAMP columns, it transforms string default values into Time.
func (s Statement) prepareCreateTable() (Statement, error) {
	for i, col := range s.Columns {
		if !col.DefaultValue.Valid {
			continue
		}
		if col.Kind == Timestamp {
			// If this is already a Time, accept it as is
			_, ok := col.DefaultValue.Value.(Time)
			if ok {
				return s, nil
			}
			// Otherwise, validate and transform to Time
			_, ok = col.DefaultValue.Value.(TextPointer)
			if !ok {
				return s, fmt.Errorf("default value '%s' is not a valid TextPointer", col.DefaultValue.Value)
			}
			timestamp, err := ParseTimestamp(col.DefaultValue.Value.(TextPointer).String())
			if err != nil {
				return s, fmt.Errorf("default value '%s' is not a valid timestamp: %w", col.DefaultValue.Value, err)
			}
			col.DefaultValue.Value = timestamp
			s.Columns[i] = col
		}

		if err := isValueValidForColumn(col, col.DefaultValue); err != nil {
			return s, fmt.Errorf("invalid default value: %w", err)
		}
	}
	return s, nil
}

// prepareInsert makes sure to add any nullable columns that are missing from the
// insert statement, setting them to NULL. It also converts timestamp string values to int64.
func (s Statement) prepareInsert(now Time) (Statement, error) {
	// For ON CONFLICT DO UPDATE, the DO UPDATE SET field names are appended to
	// s.Fields by setUpdate(). Strip them before expanding column defaults so
	// that the slices.Insert calls below only operate on the INSERT fields.
	// They are re-appended at the end.
	var updateFields []Field
	if s.ConflictAction == ConflictActionDoUpdate && len(s.Updates) > 0 {
		insertFieldCount := len(s.Fields) - len(s.Updates)
		if insertFieldCount >= 0 {
			updateFields = slices.Clone(s.Fields[insertFieldCount:])
			s.Fields = s.Fields[:insertFieldCount]
		}
	}

	for i, col := range s.Columns {
		if !s.HasField(col.Name) {
			s.Fields = slices.Insert(s.Fields, i, Field{Name: col.Name})
			for j := range s.Inserts {
				var value OptionalValue
				if col.DefaultValue.Valid {
					value = col.DefaultValue
				} else if col.DefaultValueNow {
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

			if col.Kind != Timestamp {
				continue
			}

			timestamp, err := parseTimeValue(s.Inserts[j][fieldIdx].Value)
			if err != nil {
				return Statement{}, err
			}
			s.Inserts[j][fieldIdx].Value = timestamp
		}

	}

	// Re-append the DO UPDATE SET fields that were stripped at the start.
	if len(updateFields) > 0 {
		s.Fields = append(s.Fields, updateFields...)
	}

	// Resolve function values and timestamp strings in the DO UPDATE SET clause.
	// prepareUpdate is only called for Kind==Update, so we do it explicitly here.
	if s.ConflictAction == ConflictActionDoUpdate {
		for name, val := range s.Updates {
			if !val.Valid {
				continue
			}
			// ExcludedRef is resolved at execution time — leave it alone.
			if _, ok := val.Value.(ExcludedRef); ok {
				continue
			}
			if fn, ok := val.Value.(Function); ok {
				if fn.Name != FunctionNow.Name {
					return Statement{}, fmt.Errorf("unsupported function %q in ON CONFLICT DO UPDATE", fn.Name)
				}
				val.Value = now
				s.Updates[name] = val
				continue
			}
			col, ok := s.ColumnByName(name)
			if !ok || col.Kind != Timestamp {
				continue
			}
			timestamp, err := parseTimeValue(val.Value)
			if err != nil {
				return Statement{}, fmt.Errorf("invalid timestamp in ON CONFLICT DO UPDATE SET %s: %w", name, err)
			}
			val.Value = timestamp
			s.Updates[name] = val
		}
	}

	return s, nil
}

func (s Statement) prepareUpdate(now Time) (Statement, error) {
	if len(s.Updates) == 0 {
		return s, nil
	}

	for name := range s.Updates {
		col, ok := s.ColumnByName(name)
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

		// Arithmetic expressions are evaluated at execution time against the row —
		// skip all static type preparation for them.
		if _, ok := updateValue.Value.(*Expr); ok {
			continue
		}

		if fn, ok := updateValue.Value.(Function); ok {
			if fn.Name == FunctionNow.Name {
				updateValue.Value = now
				s.Updates[name] = updateValue
			} else {
				return Statement{}, fmt.Errorf("unsupported function %q in UPDATE", fn.Name)
			}
		} else if col.Kind == Timestamp {
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
	for i, condGroup := range s.Conditions {
		for j, cond := range condGroup {
			// We only want to continue if left operand is a field and right operand is not a field.
			if !cond.Operand1.IsField() || cond.Operand2.IsField() {
				continue
			}
			field, ok := cond.Operand1.Value.(Field)
			if !ok {
				return Statement{}, errors.New("invalid field in WHERE condition")
			}

			// Skip validation for fields from joined tables (have alias prefix)
			// They will be validated during query planning
			if field.AliasPrefix != "" && field.AliasPrefix != s.TableAlias {
				continue
			}

			col, ok := s.ColumnByName(field.Name)
			if !ok {
				return Statement{}, fmt.Errorf("unknown field %q in table %q", field.Name, s.TableName)
			}
			if col.Kind != Timestamp {
				continue
			}
			if cond.Operand2.Type == OperandNull {
				continue
			}
			if cond.Operand2.Type == OperandList {
				for k, value := range cond.Operand2.Value.([]any) {
					timestamp, err := parseTimeValue(value)
					if err != nil {
						return Statement{}, err
					}
					s.Conditions[i][j].Operand2.Value.([]any)[k] = timestamp
				}
				continue
			}
			timestamp, err := parseTimeValue(cond.Operand2.Value)
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
		return Time{}, errors.New("timestamp field expects TextPointer value")
	}
	timestamp, err := ParseTimestamp(tp.String())
	if err != nil {
		return Time{}, fmt.Errorf("invalid timestamp format for field: %w", err)
	}
	return timestamp, nil
}

// Validate ...
func (s Statement) Validate(table *Table) error {
	switch s.Kind {
	case CreateTable:
		if err := s.validateCreateTable(); err != nil {
			return err
		}
	case Insert:
		if err := s.validateInsert(table); err != nil {
			return err
		}
	case Update:
		if err := s.validateUpdate(table); err != nil {
			return err
		}
	case Select:
		if err := s.validateSelect(table); err != nil {
			return err
		}
	case CreateIndex:
		return s.validateCreateIndex(table)
	case DropIndex:
		return s.validateDropIndex()
	case Pragma:
		return s.validatePragma()
	}

	if err := s.validateWhere(); err != nil {
		return err
	}

	return nil
}

func (s Statement) validatePragma() error {
	if s.PragmaName == "" {
		return errors.New("pragma name is required")
	}
	return nil
}

func (s Statement) validateCreateTable() error {
	if s.TableName == "" {
		return errors.New("table name is required")
	}

	if utf8.RuneCountInString(s.TableName) > MaxInlineVarchar {
		return fmt.Errorf("table name exceeds maximum length of %d", MaxInlineVarchar)
	}

	if len(s.Conditions) > 0 {
		return errors.New("CREATE TABLE cannot have WHERE conditions")
	}

	if len(s.Columns) == 0 {
		return errors.New("at least one column is required")
	}

	if len(s.Columns) > MaxColumns {
		return fmt.Errorf("maximum number of columns is %d", MaxColumns)
	}

	if !canInlinedRowFitInPage(s.Columns) {
		return fmt.Errorf("potential row size exceeds maximum allowed %d", UsablePageSize)
	}

	if utf8.RuneCountInString(s.DDL()) > maximumSchemaSQL {
		return fmt.Errorf("table definition too long, maximum length is %d", maximumSchemaSQL)
	}

	if len(s.PrimaryKey.Columns) > 1 && s.PrimaryKey.Autoincrement {
		return errors.New("autoincrement primary key cannot be composite")
	}

	nameMap := map[string]struct{}{}
	for _, col := range s.Columns {
		if _, ok := nameMap[col.Name]; ok {
			return fmt.Errorf("duplicate column name %q", col.Name)
		}
		if col.Name == "" {
			return errors.New("column name cannot be empty")
		}
		nameMap[col.Name] = struct{}{}
	}

	indexMap := map[string]struct{}{}

	if _, ok := indexMap[indexColumnHash(s.PrimaryKey.Columns)]; ok {
		return fmt.Errorf("columns %s can only have one index", columnNames(s.PrimaryKey.Columns))
	}
	indexMap[indexColumnHash(s.PrimaryKey.Columns)] = struct{}{}

	size := uint32(0)
	for _, col := range s.PrimaryKey.Columns {
		if col.Nullable {
			return errors.New("primary key column cannot be nullable")
		}
		if col.Kind == Text {
			return errors.New("primary key cannot be of type TEXT")
		}
		size += col.Size
	}
	if size > MaxIndexKeySize {
		return fmt.Errorf("primary key size exceeds max index key size %d", MaxIndexKeySize)
	}
	if s.PrimaryKey.Autoincrement && s.PrimaryKey.Columns[0].Kind != Int8 && s.PrimaryKey.Columns[0].Kind != Int4 {
		return errors.New("autoincrement primary key must be of type INT4 or INT8")
	}

	for _, uniqueIndex := range s.UniqueIndexes {
		if _, ok := indexMap[indexColumnHash(uniqueIndex.Columns)]; ok {
			return fmt.Errorf("columns %s can only have one index", columnNames(uniqueIndex.Columns))
		}
		indexMap[indexColumnHash(uniqueIndex.Columns)] = struct{}{}

		size := uint32(0)
		for _, col := range uniqueIndex.Columns {
			if col.Kind == Text {
				return errors.New("unique index key cannot be of type TEXT")
			}
			size += col.Size
		}
		if size > MaxIndexKeySize {
			return fmt.Errorf("unique index key size exceeds max index key size %d", MaxIndexKeySize)
		}
	}

	return nil
}

func (s Statement) validateCreateIndex(table *Table) error {
	if s.IndexName == "" {
		return errors.New("index name is required")
	}

	if s.TableName == "" {
		return errors.New("table name is required")
	}

	if utf8.RuneCountInString(s.IndexName) > MaxInlineVarchar {
		return fmt.Errorf("index name exceeds maximum length of %d", MaxInlineVarchar)
	}

	if len(s.Columns) == 0 {
		return errors.New("at least one column is required")
	}

	if table.HasIndexOnColumns(s.Columns) {
		return fmt.Errorf("columns %s can only have one index", columnNames(s.Columns))
	}

	if utf8.RuneCountInString(s.DDL()) > maximumSchemaSQL {
		return fmt.Errorf("index definition too long, maximum length is %d", maximumSchemaSQL)
	}

	return nil
}

func (s Statement) validateDropIndex() error {
	if s.IndexName == "" {
		return errors.New("index name is required")
	}

	return nil
}

func columnNames(columns []Column) string {
	var result strings.Builder
	for i, col := range columns {
		if i > 0 {
			result.WriteString(", ")
		}
		result.WriteString(col.Name)
	}
	return result.String()
}

// Check whether a row with the given columns can fit in a page if all columns are inlined
func canInlinedRowFitInPage(columns []Column) bool {
	remaining := UsablePageSize
	for _, col := range columns {
		if col.Kind.IsText() {
			// For TEXT and VARCHAR, assume each column has maximum inline size
			// and will take 4+255 bytes each (length prefix + max varchar inline size)
			remaining -= (varcharLengthPrefixSize + MaxInlineVarchar)
		} else {
			remaining -= int(col.Size)
		}
		if remaining < 0 {
			return false
		}
	}
	return true
}

func (s Statement) validateInsert(table *Table) error {
	if len(s.Inserts) == 0 {
		return errors.New("at least one row to insert is required")
	}

	if len(s.Columns) != len(table.Columns) {
		return fmt.Errorf("insert: expected %d columns, got %d", len(table.Columns), len(s.Columns))
	}

	if len(s.Conditions) > 0 {
		return errors.New("INSERT cannot have WHERE conditions")
	}

	var (
		hasPk    bool
		pkColumn Column
	)
	if table.HasPrimaryKey() && len(table.PrimaryKey.Columns) == 1 {
		hasPk = true
		pkColumn = table.PrimaryKey.Columns[0]
	}

	for _, col := range s.Columns {
		if col.Nullable {
			continue
		}
		if col.DefaultValue.Valid {
			continue
		}
		if hasPk && col.Name == pkColumn.Name && table.PrimaryKey.Autoincrement {
			continue
		}
		if !s.HasField(col.Name) {
			return fmt.Errorf("missing required field %q", col.Name)
		}
	}

	// For ON CONFLICT DO UPDATE the DO UPDATE SET fields are appended to s.Fields
	// after the INSERT fields. Only the INSERT portion should be validated here.
	insertFieldCount := len(s.Fields) - len(s.Updates)

	for i, field := range s.Fields {
		if i >= insertFieldCount {
			break
		}
		col, ok := table.ColumnByName(field.Name)
		if !ok {
			return fmt.Errorf("unknown field %q in table %q", field.Name, table.Name)
		}
		for _, anInsert := range s.Inserts {
			if len(anInsert) != insertFieldCount {
				return fmt.Errorf("insert: expected %d values, got %d", insertFieldCount, len(anInsert))
			}
			if err := s.validateColumnValue(table, col, anInsert[i]); err != nil {
				return err
			}
		}
	}

	// Validate EXCLUDED.col references: the referenced column must exist in the table.
	for _, val := range s.Updates {
		ref, ok := val.Value.(ExcludedRef)
		if !ok {
			continue
		}
		if _, exists := table.ColumnByName(ref.Column); !exists {
			return fmt.Errorf("EXCLUDED.%s: column %q does not exist in table %q", ref.Column, ref.Column, table.Name)
		}
	}

	return nil
}

func (s Statement) validateUpdate(table *Table) error {
	if len(s.Updates) == 0 {
		return errors.New("at least one field to update is required")
	}
	for _, field := range s.Fields {
		col, ok := table.ColumnByName(field.Name)
		if !ok {
			return fmt.Errorf("unknown field %q in table %q", field.Name, table.Name)
		}
		updateVal := s.Updates[field.Name]
		// Arithmetic expressions are evaluated at execution time — skip static type validation.
		if _, isExpr := updateVal.Value.(*Expr); isExpr {
			continue
		}
		if err := s.validateColumnValue(table, col, updateVal); err != nil {
			return err
		}
	}
	return nil
}

func (s Statement) validateSelect(table *Table) error {
	if len(s.Fields) == 0 {
		return errors.New("at least one field to select is required")
	}
	if s.Limit.Valid {
		limitValue, ok := s.Limit.Value.(int64)
		if !ok || limitValue < 0 {
			return errors.New("LIMIT must be a non-negative integer")
		}
	}
	if s.Offset.Valid {
		offsetValue, ok := s.Offset.Value.(int64)
		if !ok || offsetValue < 0 {
			return errors.New("OFFSET must be a non-negative integer")
		}
	}

	if s.IsSelectCountAll() {
		if len(s.OrderBy) > 0 {
			return errors.New("ORDER BY cannot be used with COUNT(*)")
		}
		if s.Offset.Valid {
			return errors.New("OFFSET cannot be used with COUNT(*)")
		}
		if s.Limit.Valid {
			return errors.New("LIMIT cannot be used with COUNT(*)")
		}
	}

	// GROUP BY cannot be combined with JOINs (not yet supported).
	if len(s.GroupBy) > 0 && len(s.Joins) > 0 {
		return errors.New("GROUP BY cannot be combined with JOIN")
	}

	// HAVING requires GROUP BY.
	if len(s.Having) > 0 && len(s.GroupBy) == 0 {
		return errors.New("HAVING requires GROUP BY")
	}

	// HAVING condition fields must be aggregate functions or GROUP BY columns.
	if len(s.Having) > 0 {
		groupBySet := make(map[string]struct{}, len(s.GroupBy))
		for _, f := range s.GroupBy {
			groupBySet[f.Name] = struct{}{}
		}
		for _, group := range s.Having {
			for _, cond := range group {
				if !cond.Operand1.IsField() {
					continue
				}
				field, ok := cond.Operand1.Value.(Field)
				if !ok {
					continue
				}
				if isHavingAggregateRef(field.Name) {
					continue
				}
				if _, ok := groupBySet[field.Name]; !ok {
					return fmt.Errorf("HAVING references %q which is not a GROUP BY column or aggregate function", field.Name)
				}
			}
		}
	}

	// Aggregate function validation (SUM, AVG, MIN, MAX).
	if s.IsSelectAggregate() {
		// Build a set of GROUP BY column names for O(1) lookup.
		groupBySet := make(map[string]struct{}, len(s.GroupBy))
		for _, f := range s.GroupBy {
			groupBySet[f.Name] = struct{}{}
		}

		for i, field := range s.Fields {
			agg := s.Aggregates[i]
			if agg.Kind == 0 {
				// Non-aggregate column must appear in GROUP BY.
				if _, ok := groupBySet[field.Name]; !ok {
					return fmt.Errorf("non-aggregate column %q must appear in GROUP BY", field.Name)
				}
				continue
			}
			if agg.Column == "" {
				continue // COUNT(*) has no source column to validate
			}
			col, ok := table.ColumnByName(agg.Column)
			if !ok {
				return fmt.Errorf("unknown column %q referenced in %s", agg.Column, agg.Kind)
			}
			if agg.Kind == AggregateSum || agg.Kind == AggregateAvg {
				switch col.Kind {
				case Int4, Int8, Real, Double:
					// OK — numeric column
				default:
					return fmt.Errorf("column %q must be numeric for %s", agg.Column, agg.Kind)
				}
			}
		}

		// Validate GROUP BY columns exist in the table schema.
		for _, f := range s.GroupBy {
			if _, ok := table.ColumnByName(f.Name); !ok {
				return fmt.Errorf("unknown GROUP BY column %q in table %q", f.Name, table.Name)
			}
		}

		// ORDER BY for aggregate queries is allowed but not validated against the schema
		// because the ORDER BY field may be a synthetic aggregate result name.
		return nil
	}

	if !s.IsSelectAll() && !s.IsSelectCountAll() {
		// Skip field validation for JOINs - fields come from multiple tables
		// and will be validated during execution when all tables are available
		if len(s.Joins) == 0 {
			fieldMap := map[string]struct{}{}
			for _, field := range s.Fields {
				// Computed expression fields (e.g. price * 1.1) are not table columns —
				// their source columns are validated at execution time via exprSourceFields.
				if field.Expr != nil {
					continue
				}
				_, ok := table.ColumnByName(field.Name)
				if !ok {
					return fmt.Errorf("unknown field %q in table %q", field.Name, table.Name)
				}
				if _, exists := fieldMap[field.String()]; exists {
					return fmt.Errorf("duplicate field %q in select statement", field.Name)
				}
				fieldMap[field.String()] = struct{}{}
			}
		}
	}

	if len(s.Joins) > 0 {
		var (
			tableMap = map[string]struct{}{
				table.Name: {},
			}
			aliasMap = map[string]struct{}{}
		)
		if s.TableAlias == "" {
			return errors.New("table must have alias when query contains JOIN")
		}
		aliasMap[s.TableAlias] = struct{}{}
		for _, join := range s.Joins {
			if join.TableAlias == "" {
				return errors.New("JOIN must have a table alias")
			}

			_, ok := tableMap[join.TableName]
			if ok {
				return fmt.Errorf("duplicate table name %q in JOINs", join.TableName)
			}
			tableMap[join.TableName] = struct{}{}

			_, ok = aliasMap[join.TableAlias]
			if ok {
				return fmt.Errorf("duplicate table alias %q in JOINs", join.TableAlias)
			}
			aliasMap[join.TableAlias] = struct{}{}

			if len(join.Conditions) == 0 {
				return errors.New("JOIN must have at least one condition")
			}

			for _, cond := range join.Conditions {
				if cond.Operand1.Type != OperandField {
					return errors.New("operand1 in JOIN condition must be a field")
				}
				if _, ok := cond.Operand1.Value.(Field); !ok {
					return errors.New("operand1 in JOIN condition must be a field")
				}
				if cond.Operand2.Type != OperandField {
					return errors.New("operand2 in JOIN condition must be a field")
				}
				if _, ok := cond.Operand2.Value.(Field); !ok {
					return errors.New("operand2 in JOIN condition must be a field")
				}
				if !IsValidCondition(cond) {
					return errors.New("invalid condition in JOIN clause")
				}
				// TODO - validate that the fields in the JOIN conditions exist in the respective tables
			}
		}
		// JOIN validation complete - fields will be validated during execution
		return nil
	}

	if len(s.OrderBy) > 0 {
		for _, anOrderBy := range s.OrderBy {
			_, ok := table.ColumnByName(anOrderBy.Field.Name)
			if !ok {
				return fmt.Errorf("unknown field %q in ORDER BY clause", anOrderBy.Field.Name)
			}
		}
	}

	return nil
}

func (s Statement) validateColumnValue(table *Table, col Column, val OptionalValue) error {
	if _, ok := val.Value.(Placeholder); ok {
		return fmt.Errorf("unbound placeholder in value for field %q", col.Name)
	}
	var isPkColumn bool
	if table.HasPrimaryKey() && len(table.PrimaryKey.Columns) == 1 && col.Name == table.PrimaryKey.Columns[0].Name {
		isPkColumn = true
	}
	if !val.Valid && isPkColumn && !table.PrimaryKey.Autoincrement {
		return fmt.Errorf("primary key on field %q cannot be NULL", col.Name)
	}
	if !val.Valid && !col.Nullable && !isPkColumn {
		return fmt.Errorf("field %q cannot be NULL", col.Name)
	}
	if err := isValueValidForColumn(col, val); err != nil {
		return fmt.Errorf("invalid field value: %w", err)
	}
	return nil
}

func isValueValidForColumn(col Column, val OptionalValue) error {
	if !val.Valid {
		return nil
	}
	// Skip validation for expression values — these are evaluated at execution time.
	if _, isExpr := val.Value.(*Expr); isExpr {
		return nil
	}
	switch col.Kind {
	case Boolean:
		_, ok := val.Value.(bool)
		if !ok {
			return fmt.Errorf("expects BOOLEAN value for %q", col.Name)
		}
	case Int4:
		_, ok := val.Value.(int64)
		if !ok {
			_, ok2 := val.Value.(int32)
			if !ok2 {
				return fmt.Errorf("expects INT4 value for %q", col.Name)
			}
		}
	case Int8:
		_, ok := val.Value.(int64)
		if !ok {
			return fmt.Errorf("expects INT8 value for %q", col.Name)
		}
	case Real:
		_, ok := val.Value.(float64)
		if !ok {
			_, ok2 := val.Value.(float32)
			if !ok2 {
				return fmt.Errorf("expects REAL value for %q", col.Name)
			}
		}
	case Double:
		_, ok := val.Value.(float64)
		if !ok {
			return fmt.Errorf("expects DOUBLE value for %q", col.Name)
		}
	case Varchar, Text:
		tp, ok := val.Value.(TextPointer)
		if !ok {
			return fmt.Errorf("expects a text value for %q", col.Name)
		}
		switch col.Kind {
		case Varchar:
			if utf8.RuneCountInString(val.Value.(TextPointer).String()) > int(col.Size) {
				return fmt.Errorf("field %q exceeds maximum VARCHAR length of %d", col.Name, col.Size)
			}
		case Text:
			if utf8.RuneCountInString(val.Value.(TextPointer).String()) > MaxOverflowTextSize {
				return fmt.Errorf("field %q exceeds maximum TEXT length of %d", col.Name, MaxOverflowTextSize)
			}
		}
		if !utf8.ValidString(tp.String()) {
			return fmt.Errorf("expects valid UTF-8 string for %q", col.Name)
		}
	case Timestamp:
		_, ok := val.Value.(Time)
		if !ok {
			return fmt.Errorf("expects time value for %q", col.Name)
		}
	}
	return nil
}

func (s Statement) validateWhere() error {
	for _, condGroup := range s.Conditions {
		equalityMap := map[string][]any{}
		for _, cond := range condGroup {
			if cond.Operand1.Type != OperandField {
				return errors.New("operand1 in WHERE condition must be a field")
			}
			if _, ok := cond.Operand1.Value.(Field); !ok {
				return errors.New("operand1 in WHERE condition must be a field")
			}
			if cond.Operand2.Type == OperandPlaceholder {
				return errors.New("unbound placeholder in WHERE clause")
			}
			if !IsValidCondition(cond) {
				return errors.New("invalid condition in WHERE clause")
			}
			if cond.Operand1.Type == OperandList {
				return errors.New("operand1 in WHERE condition cannot be a list")
			}
			if cond.Operand2.Type == OperandList {
				var valueType string
				for _, value := range cond.Operand2.Value.([]any) {
					if _, ok := value.(Placeholder); ok {
						return errors.New("unbound placeholder in WHERE clause")
					}
					if valueType == "" {
						valueType = fmt.Sprintf("%T", value)
						_, ok := value.(bool)
						if ok {
							return errors.New("IN / NOT IN operator not supported for boolean columns")
						}
						continue
					}
					if fmt.Sprintf("%T", value) != valueType {
						return errors.New("mixed operand types in WHERE condition list")
					}
				}
			}

			if isEquality(cond) {
				field := cond.Operand1.Value.(Field)

				// Skip validation for fields from joined tables (have alias prefix)
				// They will be validated during query planning
				if field.AliasPrefix != "" && field.AliasPrefix != s.TableAlias {
					continue
				}

				col, ok := s.ColumnByName(field.Name)
				if !ok {
					return fmt.Errorf("unknown field %q in WHERE clause", field.Name)
				}

				args, err := equalityKeys(col, cond)
				if err != nil {
					return err
				}

				if _, ok2 := equalityMap[field.Name]; !ok2 {
					equalityMap[field.Name] = args
				}
			}
		}
	}
	return nil
}

// DDL ...
func (s Statement) DDL() string {
	switch s.Kind {
	case CreateTable:
		return s.createTableDDL()
	case CreateIndex:
		return s.createIndexDDL()
	default:
		return ""
	}
}

func (s Statement) createTableDDL() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "create table \"%s\" (\n", s.TableName)

	var pkColumn string
	if len(s.PrimaryKey.Columns) == 1 {
		pkColumn = s.PrimaryKey.Columns[0].Name
	}
	uniqueKeys := map[string]struct{}{}
	for _, uniqueIndex := range s.UniqueIndexes {
		if len(uniqueIndex.Columns) == 1 {
			uniqueKeys[uniqueIndex.Columns[0].Name] = struct{}{}
		}
	}

	for i, col := range s.Columns {
		fmt.Fprintf(&sb, "	%s %s", col.Name, col.Kind)
		if col.Kind == Varchar {
			fmt.Fprintf(&sb, "(%d)", col.Size)
		}
		if col.Name == pkColumn {
			sb.WriteString(" primary key")
			if s.PrimaryKey.Autoincrement {
				sb.WriteString(" autoincrement")
			}
		} else {
			if _, ok := uniqueKeys[col.Name]; ok {
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
					fmt.Fprintf(&sb, " default %d", col.DefaultValue.Value.(int64))
				case Real, Double:
					fmt.Fprintf(&sb, " default %f", col.DefaultValue.Value.(float64))
				case Varchar, Text:
					fmt.Fprintf(&sb, " default '%s'", col.DefaultValue.Value.(TextPointer).String())
				case Timestamp:
					fmt.Fprintf(&sb, " default '%s'", col.DefaultValue.Value.(Time).String())
				}
			}
		}
		if i < len(s.Columns)-1 {
			sb.WriteString(",\n")
		}
	}
	if len(s.PrimaryKey.Columns) > 1 {
		sb.WriteString(",\n")
		sb.WriteString("	primary key (")
		for j, col := range s.PrimaryKey.Columns {
			sb.WriteString(col.Name)
			if j < len(s.PrimaryKey.Columns)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
	}
	for _, uniqueIndex := range s.UniqueIndexes {
		if len(uniqueIndex.Columns) == 1 {
			continue
		}
		sb.WriteString(",\n")
		sb.WriteString("	unique (")
		for j, col := range uniqueIndex.Columns {
			sb.WriteString(col.Name)
			if j < len(uniqueIndex.Columns)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
	}

	// TODO : add table-level constraints (composite primary keys, unique keys, foreign keys, etc.)

	sb.WriteString("\n);")
	return sb.String()
}

func (s Statement) createIndexDDL() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "create index \"%s\" on \"%s\" (\n", s.IndexName, s.TableName)

	for i, col := range s.Columns {
		fmt.Fprintf(&sb, "	%s", col.Name)

		if i < len(s.Columns)-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString("\n);")
	return sb.String()
}

// insertValueForColumnName returns the proposed INSERT value for the named
// column at the given row index. Only the INSERT portion of s.Fields is
// searched (DO UPDATE SET fields at the tail are excluded).
func (s Statement) insertValueForColumnName(insertIdx int, colName string) (OptionalValue, bool) {
	if insertIdx < 0 || insertIdx >= len(s.Inserts) {
		return OptionalValue{}, false
	}
	insertFieldCount := len(s.Fields) - len(s.Updates)
	for i, field := range s.Fields[:insertFieldCount] {
		if field.Name == colName {
			if i >= len(s.Inserts[insertIdx]) {
				return OptionalValue{}, false
			}
			return s.Inserts[insertIdx][i], true
		}
	}
	return OptionalValue{}, false
}

// resolveExcludedRefs returns a copy of the statement where every ExcludedRef
// value in Updates has been replaced with the corresponding proposed INSERT
// value for the row at insertIdx. Statements without ExcludedRef values are
// returned unchanged (no allocation).
func (s Statement) resolveExcludedRefs(insertIdx int) Statement {
	if s.ConflictAction != ConflictActionDoUpdate {
		return s
	}
	hasRef := false
	for _, val := range s.Updates {
		if _, ok := val.Value.(ExcludedRef); ok {
			hasRef = true
			break
		}
	}
	if !hasRef {
		return s
	}
	resolved := maps.Clone(s.Updates)
	for colName, val := range resolved {
		ref, ok := val.Value.(ExcludedRef)
		if !ok {
			continue
		}
		if proposed, found := s.insertValueForColumnName(insertIdx, ref.Column); found {
			resolved[colName] = proposed
		}
	}
	s.Updates = resolved
	return s
}

// InsertValuesForColumns ...
func (s Statement) InsertValuesForColumns(insertIdx int, columns ...Column) []OptionalValue {
	values := make([]OptionalValue, 0, len(columns))
	for _, col := range columns {
		fieldIdx := -1
		for i, field := range s.Fields {
			if field.Name == col.Name {
				fieldIdx = i
				break
			}
		}
		if fieldIdx == -1 {
			continue
		}
		if insertIdx < 0 || insertIdx >= len(s.Inserts) {
			continue
		}
		values = append(values, s.Inserts[insertIdx][fieldIdx])
	}

	return values
}

// ColumnIdx ...
func (s Statement) ColumnIdx(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// IsSelectCountAll ...
func (s Statement) IsSelectCountAll() bool {
	return s.ReadOnly() && len(s.Fields) == 1 && strings.ToUpper(s.Fields[0].Name) == "COUNT(*)"
}

// IsSelectAll ...
func (s Statement) IsSelectAll() bool {
	return s.ReadOnly() && len(s.Fields) == 1 && s.Fields[0].Name == "*"
}

// IsSelectAggregate returns true when the SELECT list contains at least one
// aggregate function (SUM, AVG, MIN, MAX).  COUNT(*) alone uses the legacy
// IsSelectCountAll path and does NOT set this flag.
func (s Statement) IsSelectAggregate() bool {
	for _, agg := range s.Aggregates {
		if agg.Kind != 0 {
			return true
		}
	}
	return false
}

// IsSelectGroupBy returns true when the SELECT has a GROUP BY clause.
func (s Statement) IsSelectGroupBy() bool {
	return len(s.GroupBy) > 0
}

// isHavingAggregateRef reports whether name is a synthetic aggregate column
// reference as produced by the parser (e.g. "SUM(price)", "COUNT(*)").
func isHavingAggregateRef(name string) bool {
	upper := strings.ToUpper(name)
	if upper == "COUNT(*)" {
		return true
	}
	for _, prefix := range []string{"SUM(", "AVG(", "MIN(", "MAX("} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}
