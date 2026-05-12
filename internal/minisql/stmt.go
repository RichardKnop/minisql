package minisql

import (
	"errors"
	"fmt"
	"maps"
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

// StatementKind identifies which SQL statement a parsed Statement represents.
type StatementKind int

// StatementKind constants enumerate the supported SQL statement types.
const (
	// CreateTable is a CREATE TABLE DDL statement.
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
	Explain
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
	case Explain:
		return "EXPLAIN"
	default:
		return "UNKNOWN"
	}
}

// AggregateKind identifies the type of aggregate function.
type AggregateKind int

// AggregateKind constants enumerate the supported aggregate functions.
const (
	// AggregateCount is the COUNT(*) aggregate function.
	AggregateCount AggregateKind = iota + 1 // COUNT(*)
	// AggregateSum is the SUM aggregate function.
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
	Column string
	Kind   AggregateKind
}

// ColumnKind identifies the data type of a table column.
type ColumnKind int

// ColumnKind constants enumerate the supported column data types.
const (
	// Boolean is the BOOLEAN column type (stored as int8).
	Boolean ColumnKind = iota + 1
	Int4
	Int8
	Real
	Double
	Varchar
	Text
	Timestamp
	JSON
	UUID
)

// IsInt reports whether the column kind is an integer type (INT4 or INT8).
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
	case JSON:
		return "json"
	case UUID:
		return "uuid"
	default:
		return "unknown"
	}
}

// IsText reports whether the column kind stores variable-length text (VARCHAR, TEXT, or JSON).
func (k ColumnKind) IsText() bool {
	return k == Varchar || k == Text || k == JSON
}

// IsUUID reports whether the column kind is UUID.
func (k ColumnKind) IsUUID() bool {
	return k == UUID
}

// Column describes a single column in a table's schema, including its data type,
// size, nullability, default value, and optional CHECK constraint.
type Column struct {
	DefaultValue    OptionalValue
	CheckCond       *ConditionNode // parsed CHECK expression (nil if no CHECK constraint)
	Name            string
	Check           string // raw SQL text of CHECK expression, e.g. "age > 0"
	Kind            ColumnKind
	Size            uint32
	Nullable        bool
	DefaultValueNow bool
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

// Field represents a single item in a SELECT list (or GROUP BY / ORDER BY clause).
// For plain column references only Name is set; for expressions Expr is non-nil;
// Alias holds the AS alias, and AliasPrefix holds the table alias for qualified names.
type Field struct {
	Expr        *Expr
	AliasPrefix string
	Name        string
	Alias       string
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

// Direction specifies ascending or descending sort order for ORDER BY clauses.
type Direction int

// Direction constants define the sort order for ORDER BY clauses.
const (
	// Asc is the ascending sort direction.
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

// IndexMethod identifies the access method used by a secondary index.
type IndexMethod int

const (
	// IndexMethodBTree is the default scalar B+ tree index method.
	IndexMethodBTree IndexMethod = iota
	// IndexMethodFullText indexes text tokens for MATCH predicates.
	IndexMethodFullText
	// IndexMethodInverted is reserved for future JSON inverted indexes.
	IndexMethodInverted
)

func (m IndexMethod) String() string {
	switch m {
	case IndexMethodBTree:
		return "btree"
	case IndexMethodFullText:
		return "fulltext"
	case IndexMethodInverted:
		return "inverted"
	default:
		return "unknown"
	}
}

const (
	// TextSearchTokenizerSimple is the only supported tokenizer in the initial
	// full-text search implementation.
	TextSearchTokenizerSimple = "simple"
)

func isSupportedIndexTokenizer(tokenizer string) bool {
	return tokenizer == TextSearchTokenizerSimple
}

// OrderBy pairs a field with its sort direction for an ORDER BY clause.
type OrderBy struct {
	Field     Field
	Direction Direction
}

// Function represents a SQL scalar function reference by name.
type Function struct {
	Name string
}

const nowFunctionName = "NOW()"

// FunctionNow is the sentinel value used for the NOW() scalar function in default values.
var FunctionNow = Function{Name: nowFunctionName}

// Placeholder is the sentinel type for a ? bind parameter in a prepared statement.
type Placeholder struct{}

// ExcludedRef represents a reference to EXCLUDED.column_name inside an
// ON CONFLICT DO UPDATE SET clause.  At upsert time it resolves to the value
// that was proposed for insertion but rejected due to the conflict.
type ExcludedRef struct {
	Column string
}

// UnionClause represents a UNION or UNION ALL branch appended to a SELECT statement.
type UnionClause struct {
	Stmt Statement
	All  bool
}

// CTE represents a single Common Table Expression.
type CTE struct {
	Name string
	Body *Statement
}

// Statement is the central data structure produced by the parser. It describes
// a single SQL statement of any kind (DML, DDL, transaction control, etc.) and
// is passed through preparation, binding, validation, and execution unchanged.
type Statement struct {
	PrimaryKey         PrimaryKey
	Aliases            map[string]string
	Functions          map[string]Function
	Updates            map[string]OptionalValue
	Offset             OptionalValue
	Limit              OptionalValue
	TableName          string
	TableAlias         string
	IndexExpression    *Expr // nil = column index; non-nil = expression index
	IndexName          string
	IndexWhereClause   string // raw SQL of the partial index predicate (empty = full index)
	IndexExpressionSQL string // raw SQL of the index expression (empty = column index)
	IndexTokenizer     string // tokenizer option for full-text indexes
	Target             string
	PragmaName         string
	PragmaValue        string
	Fields             []Field
	Inserts            [][]OptionalValue
	Aggregates         []AggregateExpr
	GroupBy            []Field
	Having             OneOrMore
	Unions             []UnionClause
	Joins              []Join
	OrderBy            []OrderBy
	UniqueIndexes      []UniqueIndex
	ForeignKeys        []ForeignKey
	Columns            []Column
	Conditions         OneOrMore
	ReturningFields    []Field
	ExplainStatement   *Statement
	FromSubquery       *Statement // non-nil when FROM clause is a derived table
	FromSubqueryAlias  string     // alias for the derived table (e.g. "t" in FROM (...) t)
	CTEs               []CTE      // non-nil for WITH … SELECT statements
	Kind               StatementKind
	IndexMethod        IndexMethod
	ConflictAction     ConflictAction
	IfNotExists        bool
	ExplainAnalyze     bool
	Distinct           bool
}

// NumPlaceholders returns the number of placeholder parameters (?) in the statement.
func (s Statement) NumPlaceholders() int {
	if s.Kind == Explain && s.ExplainStatement != nil {
		return s.ExplainStatement.NumPlaceholders()
	}

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

	for _, cte := range s.CTEs {
		count += cte.Body.NumPlaceholders()
	}

	return count
}

// Clone returns a deep copy of the statement, safe to mutate independently.
// Used by BindArguments so that a prepared statement can be re-bound with
// different arguments without corrupting the original parsed form.
func (s Statement) Clone() Statement {
	// For INSERT, Fields are always fully rebuilt by prepareInsert — share the
	// reference to avoid an allocation that immediately becomes dead.
	// For all other kinds, deep-copy so that downstream mutations (e.g. BindArguments
	// filling Placeholder values) don't corrupt the prepared statement.
	var fields []Field
	if s.Kind == Insert {
		fields = s.Fields
	} else {
		fields = make([]Field, len(s.Fields))
		copy(fields, s.Fields)
	}

	stmt := Statement{
		Kind:               s.Kind,
		IfNotExists:        s.IfNotExists,
		TableName:          s.TableName,
		TableAlias:         s.TableAlias,
		IndexName:          s.IndexName,
		IndexWhereClause:   s.IndexWhereClause,
		IndexExpression:    s.IndexExpression,
		IndexExpressionSQL: s.IndexExpressionSQL,
		IndexTokenizer:     s.IndexTokenizer,
		Target:             s.Target,
		PragmaName:         s.PragmaName,
		PragmaValue:        s.PragmaValue,
		ConflictAction:     s.ConflictAction,
		Columns:            s.Columns,
		Distinct:           s.Distinct,
		Fields:             fields,
		Aggregates:         s.Aggregates, // slice of value types, safe to share
		Aliases:            s.Aliases,
		Inserts:            make([][]OptionalValue, len(s.Inserts)),
		Functions:          s.Functions,
		Conditions:         make(OneOrMore, len(s.Conditions)),
		GroupBy:            s.GroupBy,
		Having:             s.Having,
		Joins:              s.Joins,
		OrderBy:            s.OrderBy,
		Limit:              s.Limit,
		Offset:             s.Offset,
		ReturningFields:    s.ReturningFields,
		ExplainAnalyze:     s.ExplainAnalyze,
		IndexMethod:        s.IndexMethod,
		FromSubqueryAlias:  s.FromSubqueryAlias,
		ForeignKeys:        s.ForeignKeys, // slice of value structs, safe to share
	}
	for i := range s.Inserts {
		stmt.Inserts[i] = make([]OptionalValue, len(s.Inserts[i]))
		copy(stmt.Inserts[i], s.Inserts[i])
	}
	// Only allocate the Updates map when there is actually something to copy.
	if len(s.Updates) > 0 {
		stmt.Updates = make(map[string]OptionalValue, len(s.Updates))
		maps.Copy(stmt.Updates, s.Updates)
	}
	for i := range s.Conditions {
		stmt.Conditions[i] = make([]Condition, len(s.Conditions[i]))
		copy(stmt.Conditions[i], s.Conditions[i])
		// Deep-copy subquery operands so parallel bind calls don't race.
		for j, cond := range stmt.Conditions[i] {
			if cond.Operand2.Type == OperandSubquery {
				if sub, ok := cond.Operand2.Value.(*Statement); ok {
					cloned := sub.Clone()
					stmt.Conditions[i][j].Operand2.Value = &cloned
				}
			}
		}
	}
	if len(s.Unions) > 0 {
		stmt.Unions = make([]UnionClause, len(s.Unions))
		for i, u := range s.Unions {
			stmt.Unions[i] = UnionClause{All: u.All, Stmt: u.Stmt.Clone()}
		}
	}
	if s.ExplainStatement != nil {
		inner := s.ExplainStatement.Clone()
		stmt.ExplainStatement = &inner
	}
	if s.FromSubquery != nil {
		inner := s.FromSubquery.Clone()
		stmt.FromSubquery = &inner
	}
	if len(s.CTEs) > 0 {
		stmt.CTEs = make([]CTE, len(s.CTEs))
		for i, cte := range s.CTEs {
			inner := cte.Body.Clone()
			stmt.CTEs[i] = CTE{Name: cte.Name, Body: &inner}
		}
	}
	return stmt
}

// BindArguments substitutes ? placeholders in the statement with the provided
// arguments in left-to-right order. It clones the statement first so the
// original prepared form is not modified and can be reused with different args.
func (s Statement) BindArguments(args ...any) (Statement, error) {
	// Clone statement so we can keep using the preparement statement
	// with different arguments without modifying it
	stmt := s.Clone()

	if s.Kind == Explain && stmt.ExplainStatement != nil {
		inner, err := stmt.ExplainStatement.BindArguments(args...)
		if err != nil {
			return Statement{}, err
		}
		stmt.ExplainStatement = &inner
		return stmt, nil
	}

	// Bind CTE body placeholders first (they appear before main query in SQL).
	for i, cte := range stmt.CTEs {
		n := cte.Body.NumPlaceholders()
		if len(args) < n {
			return Statement{}, errors.New("not enough arguments to bind CTE placeholders")
		}
		bound, err := cte.Body.BindArguments(args[:n]...)
		if err != nil {
			return Statement{}, err
		}
		stmt.CTEs[i].Body = &bound
		args = args[n:]
	}

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
	case int64, int32, TimestampMicros:
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

// HasField reports whether the SELECT field list contains a column with the given name.
func (s Statement) HasField(name string) bool {
	for _, field := range s.Fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

// ReadOnly reports whether the statement modifies no data (SELECT, PRAGMA, or EXPLAIN).
func (s Statement) ReadOnly() bool {
	return s.Kind == Select || s.Kind == Pragma || s.Kind == Explain
}

// IsDDL reports whether the statement is a data-definition statement
// (CREATE TABLE, DROP TABLE, CREATE INDEX, or DROP INDEX).
func (s Statement) IsDDL() bool {
	return s.Kind == CreateTable || s.Kind == DropTable || s.Kind == CreateIndex || s.Kind == DropIndex
}

// ColumnByName looks up a column in the statement's schema by name.
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
	case CreateIndex:
		// Partial index WHERE conditions are validated against the full table schema
		// later in createIndex/initSecondaryIndex. Skip prepareWhere here because
		// s.Columns only contains the indexed columns, not all table columns.
		return s, nil
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
			// If this is already a TimestampMicros value, accept it as is
			_, ok := col.DefaultValue.Value.(TimestampMicros)
			if ok {
				return s, nil
			}
			// Otherwise, validate and transform TextPointer → TimestampMicros
			_, ok = col.DefaultValue.Value.(TextPointer)
			if !ok {
				return s, fmt.Errorf("default value '%s' is not a valid TextPointer", col.DefaultValue.Value)
			}
			timestamp, err := ParseTimestamp(col.DefaultValue.Value.(TextPointer).String())
			if err != nil {
				return s, fmt.Errorf("default value '%s' is not a valid timestamp: %w", col.DefaultValue.Value, err)
			}
			col.DefaultValue.Value = TimestampMicros(timestamp.TotalMicroseconds())
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
	nCols := len(s.Columns)

	// How many of s.Fields belong to the INSERT side (vs. the appended DO UPDATE
	// SET fields that setUpdate() tacks on at the end).
	nInsertFields := len(s.Fields)
	if s.ConflictAction == ConflictActionDoUpdate && len(s.Updates) > 0 {
		if n := len(s.Fields) - len(s.Updates); n >= 0 {
			nInsertFields = n
		}
	}
	nUpdateFields := len(s.Fields) - nInsertFields

	// Build colFieldIdx: for each table column (by position), the index into the
	// original s.Fields where that column appears, or -1 if it was omitted.
	// One O(C²) pass, but C ≤ 64 so it is effectively O(1).
	colFieldIdx := make([]int, nCols)
	for i := range colFieldIdx {
		colFieldIdx[i] = -1
	}
	for i, col := range s.Columns {
		for k := 0; k < nInsertFields; k++ {
			if s.Fields[k].Name == col.Name {
				colFieldIdx[i] = k
				break
			}
		}
	}

	// Rebuild Fields in table-column order with a single allocation.
	// Append the DO UPDATE SET fields (if any) at the end.
	newFields := make([]Field, nCols, nCols+nUpdateFields)
	for i, col := range s.Columns {
		if k := colFieldIdx[i]; k >= 0 {
			newFields[i] = s.Fields[k]
		} else {
			newFields[i] = Field{Name: col.Name}
		}
	}
	if nUpdateFields > 0 {
		newFields = append(newFields, s.Fields[nInsertFields:]...)
	}
	s.Fields = newFields

	// Rebuild each insert row in column order, applying defaults and resolving
	// NOW() / timestamp values inline — one allocation per row.
	for j := range s.Inserts {
		newRow := make([]OptionalValue, nCols)
		for i, col := range s.Columns {
			k := colFieldIdx[i]
			var val OptionalValue
			if k >= 0 {
				val = s.Inserts[j][k]
			} else {
				// Column was omitted — apply table default.
				if col.DefaultValue.Valid {
					val = col.DefaultValue
				} else if col.DefaultValueNow {
					val = OptionalValue{Valid: true, Value: TimestampMicros(now.TotalMicroseconds())}
				}
				newRow[i] = val
				continue
			}

			if val.Valid {
				if fn, ok := val.Value.(Function); ok {
					if fn.Name == FunctionNow.Name {
						val.Value = TimestampMicros(now.TotalMicroseconds())
					} else {
						return Statement{}, fmt.Errorf("unsupported function %q in INSERT", fn.Name)
					}
				} else if col.Kind == Timestamp {
					timestamp, err := parseTimeValue(val.Value)
					if err != nil {
						return Statement{}, err
					}
					val.Value = timestamp
				} else if col.Kind == UUID {
					uv, err := toUUIDValue(val.Value)
					if err != nil {
						return Statement{}, fmt.Errorf("column %q: %w", col.Name, err)
					}
					val.Value = uv
				}
			}
			newRow[i] = val
		}
		s.Inserts[j] = newRow
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
				val.Value = TimestampMicros(now.TotalMicroseconds())
				s.Updates[name] = val
				continue
			}
			col, ok := s.ColumnByName(name)
			if !ok {
				continue
			}
			switch col.Kind {
			case Timestamp:
				timestamp, err := parseTimeValue(val.Value)
				if err != nil {
					return Statement{}, fmt.Errorf("invalid timestamp in ON CONFLICT DO UPDATE SET %s: %w", name, err)
				}
				val.Value = timestamp
				s.Updates[name] = val
			case UUID:
				uv, err := toUUIDValue(val.Value)
				if err != nil {
					return Statement{}, fmt.Errorf("invalid UUID in ON CONFLICT DO UPDATE SET %s: %w", name, err)
				}
				val.Value = uv
				s.Updates[name] = val
			}
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
				updateValue.Value = TimestampMicros(now.TotalMicroseconds())
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
		} else if col.Kind == UUID {
			uv, err := toUUIDValue(updateValue.Value)
			if err != nil {
				return Statement{}, fmt.Errorf("column %q: %w", name, err)
			}
			updateValue.Value = uv
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
			if col.Kind != Timestamp && col.Kind != UUID {
				continue
			}
			if cond.Operand2.Type == OperandNull {
				continue
			}
			if cond.Operand2.Type == OperandList {
				for k, value := range cond.Operand2.Value.([]any) {
					if col.Kind == Timestamp {
						converted, err := parseTimeValue(value)
						if err != nil {
							return Statement{}, err
						}
						s.Conditions[i][j].Operand2.Value.([]any)[k] = converted
					} else {
						uv, err := toUUIDValue(value)
						if err != nil {
							return Statement{}, fmt.Errorf("field %q: %w", field.Name, err)
						}
						s.Conditions[i][j].Operand2.Value.([]any)[k] = uv
					}
				}
				continue
			}
			if col.Kind == Timestamp {
				timestamp, err := parseTimeValue(cond.Operand2.Value)
				if err != nil {
					return Statement{}, err
				}
				s.Conditions[i][j].Operand2.Value = timestamp
			} else {
				uv, err := toUUIDValue(cond.Operand2.Value)
				if err != nil {
					return Statement{}, fmt.Errorf("field %q: %w", field.Name, err)
				}
				s.Conditions[i][j].Operand2.Value = uv
			}
		}
	}
	return s, nil
}

func parseTimeValue(value any) (TimestampMicros, error) {
	if micros, ok := value.(TimestampMicros); ok {
		return micros, nil
	}
	tp, ok := value.(TextPointer)
	if !ok {
		return 0, errors.New("timestamp field expects TextPointer value")
	}
	timestamp, err := ParseTimestamp(tp.String())
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp format for field: %w", err)
	}
	return TimestampMicros(timestamp.TotalMicroseconds()), nil
}

// validateJoinTree recursively checks every join node in the join tree for:
// duplicate table names, duplicate aliases, missing aliases, missing conditions,
// and invalid condition operand types. tableMap and aliasMap accumulate seen
// values across the entire tree so duplicates at any depth are caught.
func validateJoinTree(joins []Join, tableMap, aliasMap map[string]struct{}) error {
	for _, join := range joins {
		if join.TableAlias == "" {
			return errors.New("JOIN must have a table alias")
		}
		if _, ok := tableMap[join.TableName]; ok {
			return fmt.Errorf("duplicate table name %q in JOINs", join.TableName)
		}
		tableMap[join.TableName] = struct{}{}
		if _, ok := aliasMap[join.TableAlias]; ok {
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
		}
		if len(join.Joins) > 0 {
			if err := validateJoinTree(join.Joins, tableMap, aliasMap); err != nil {
				return err
			}
		}
	}
	return nil
}

// Validate checks the statement for semantic correctness against the given table's
// schema (column existence, type compatibility, constraint satisfaction, etc.).
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

	for _, fk := range s.ForeignKeys {
		if len(fk.Columns) == 0 {
			return errors.New("foreign key: column list cannot be empty")
		}
		if fk.TargetTable == "" {
			return errors.New("foreign key: referenced table cannot be empty")
		}
		if len(fk.TargetColumns) == 0 {
			return errors.New("foreign key: referenced column list cannot be empty")
		}
		if len(fk.Columns) != len(fk.TargetColumns) {
			return errors.New("foreign key: local and referenced column counts must match")
		}
		colByName := make(map[string]Column, len(s.Columns))
		for _, col := range s.Columns {
			colByName[col.Name] = col
		}
		for _, colName := range fk.Columns {
			if _, exists := colByName[colName]; !exists {
				return fmt.Errorf("foreign key column %q does not exist in table %q", colName, s.TableName)
			}
		}
		// SET NULL requires every FK column to be nullable.
		if fk.OnDelete == FKActionSetNull || fk.OnUpdate == FKActionSetNull {
			for _, colName := range fk.Columns {
				col := colByName[colName]
				if !col.Nullable {
					return fmt.Errorf("foreign key SET NULL: column %q in table %q must be nullable", colName, s.TableName)
				}
			}
		}
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

	if s.IndexMethod == IndexMethodBTree && table.HasIndexOnColumns(s.Columns) {
		return fmt.Errorf("columns %s can only have one index", columnNames(s.Columns))
	}

	if utf8.RuneCountInString(s.DDL()) > maximumSchemaSQL {
		return fmt.Errorf("index definition too long, maximum length is %d", maximumSchemaSQL)
	}

	if err := s.validateCreateIndexMethod(table); err != nil {
		return err
	}

	return nil
}

func (s Statement) validateCreateIndexMethod(table *Table) error {
	switch s.IndexMethod {
	case IndexMethodBTree:
		if s.IndexTokenizer != "" {
			return errors.New("btree indexes do not support tokenizer options")
		}
		return nil
	case IndexMethodFullText:
		if s.IndexExpression != nil {
			return errors.New("full-text indexes do not support expression keys yet")
		}
		if len(s.Columns) != 1 {
			return errors.New("full-text indexes require exactly one column")
		}
		if s.IndexTokenizer == "" {
			return errors.New("full-text indexes require a tokenizer")
		}
		if !isSupportedIndexTokenizer(s.IndexTokenizer) {
			return fmt.Errorf("unsupported full-text tokenizer %q", s.IndexTokenizer)
		}
		col, ok := table.ColumnByName(s.Columns[0].Name)
		if !ok {
			return fmt.Errorf("column %s does not exist on table %s", s.Columns[0].Name, s.TableName)
		}
		if col.Kind != Text && col.Kind != Varchar {
			return fmt.Errorf("full-text index column %q must be TEXT or VARCHAR", col.Name)
		}
		return nil
	case IndexMethodInverted:
		if s.IndexExpression != nil {
			return errors.New("inverted indexes do not support expression keys yet")
		}
		if s.IndexTokenizer != "" {
			return errors.New("inverted indexes do not support tokenizer options")
		}
		if len(s.Columns) != 1 {
			return errors.New("inverted indexes require exactly one column")
		}
		col, ok := table.ColumnByName(s.Columns[0].Name)
		if !ok {
			return fmt.Errorf("column %s does not exist on table %s", s.Columns[0].Name, s.TableName)
		}
		if col.Kind != JSON {
			return fmt.Errorf("inverted index column %q must be JSON", col.Name)
		}
		return nil
	default:
		return fmt.Errorf("unknown index method %s", s.IndexMethod)
	}
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
		tableMap := map[string]struct{}{table.Name: {}}
		aliasMap := map[string]struct{}{}
		if s.TableAlias == "" {
			return errors.New("table must have alias when query contains JOIN")
		}
		aliasMap[s.TableAlias] = struct{}{}
		if err := validateJoinTree(s.Joins, tableMap, aliasMap); err != nil {
			return err
		}
		// Field-level validation deferred to execution (columns span multiple tables).
		return nil
	}

	if len(s.OrderBy) > 0 {
		for _, anOrderBy := range s.OrderBy {
			_, ok := table.ColumnByName(anOrderBy.Field.Name)
			if !ok && !s.HasOutputField(anOrderBy.Field.Name) {
				return fmt.Errorf("unknown field %q in ORDER BY clause", anOrderBy.Field.Name)
			}
		}
	}

	return nil
}

// HasOutputField reports whether SELECT emits a field with the given output name.
func (s Statement) HasOutputField(name string) bool {
	for _, field := range s.Fields {
		if field.OutputName() == name {
			return true
		}
	}
	return false
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
		_, ok := val.Value.(TimestampMicros)
		if !ok {
			return fmt.Errorf("expects timestamp value for %q", col.Name)
		}
	case JSON:
		tp, ok := val.Value.(TextPointer)
		if !ok {
			return fmt.Errorf("expects a text value for JSON column %q", col.Name)
		}
		if _, err := normaliseJSON(tp.String()); err != nil {
			return fmt.Errorf("field %q: %w", col.Name, err)
		}
	case UUID:
		switch v := val.Value.(type) {
		case UUIDValue:
			// already validated
		case TextPointer:
			if _, err := ParseUUID(v.String()); err != nil {
				return fmt.Errorf("field %q: %w", col.Name, err)
			}
		default:
			return fmt.Errorf("expects a UUID string for column %q", col.Name)
		}
	}
	return nil
}

func (s Statement) validateWhere() error {
	for _, condGroup := range s.Conditions {
		equalityMap := map[string][]any{}
		for _, cond := range condGroup {
			if cond.Operand1.Type != OperandField && cond.Operand1.Type != OperandExpr {
				return errors.New("operand1 in WHERE condition must be a field")
			}
			if cond.Operand1.Type == OperandField {
				if _, ok := cond.Operand1.Value.(Field); !ok {
					return errors.New("operand1 in WHERE condition must be a field")
				}
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

// DDL returns the canonical SQL DDL string for the statement (CREATE TABLE or
// CREATE INDEX), used to persist the schema to the database header. Returns ""
// for non-DDL statement kinds.
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
			if !col.Nullable {
				sb.WriteString(" not null")
			}
			if _, ok := uniqueKeys[col.Name]; ok {
				sb.WriteString(" unique")
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
					fmt.Fprintf(&sb, " default '%s'", FromMicroseconds(int64(col.DefaultValue.Value.(TimestampMicros))).String())
				}
			}
			if col.Check != "" {
				fmt.Fprintf(&sb, " check (%s)", col.Check)
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

	for _, fk := range s.ForeignKeys {
		sb.WriteString(",\n")
		childCols := make([]string, len(fk.Columns))
		parentCols := make([]string, len(fk.TargetColumns))
		for i, c := range fk.Columns {
			childCols[i] = `"` + c + `"`
		}
		for i, c := range fk.TargetColumns {
			parentCols[i] = `"` + c + `"`
		}
		fmt.Fprintf(&sb, "\tconstraint \"%s\" foreign key (%s) references \"%s\" (%s) on delete %s on update %s",
			fk.Name, strings.Join(childCols, ", "), fk.TargetTable, strings.Join(parentCols, ", "),
			fk.OnDelete.String(), fk.OnUpdate.String())
	}

	sb.WriteString("\n);")
	return sb.String()
}

func (s Statement) createIndexDDL() string {
	var sb strings.Builder
	switch s.IndexMethod {
	case IndexMethodFullText:
		fmt.Fprintf(&sb, "create fulltext index \"%s\" on \"%s\" (\n", s.IndexName, s.TableName)
	case IndexMethodInverted:
		fmt.Fprintf(&sb, "create inverted index \"%s\" on \"%s\" (\n", s.IndexName, s.TableName)
	default:
		fmt.Fprintf(&sb, "create index \"%s\" on \"%s\" (\n", s.IndexName, s.TableName)
	}

	for i, col := range s.Columns {
		if s.IndexExpression != nil && i == 0 {
			fmt.Fprintf(&sb, "	%s", s.IndexExpressionSQL)
		} else {
			fmt.Fprintf(&sb, "	%s", col.Name)
		}

		if i < len(s.Columns)-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString("\n)")
	if s.IndexTokenizer != "" {
		fmt.Fprintf(&sb, " with (tokenizer = '%s')", s.IndexTokenizer)
	}
	if s.IndexWhereClause != "" {
		fmt.Fprintf(&sb, " where %s", s.IndexWhereClause)
	}
	sb.WriteString(";")
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

// InsertValuesForColumns returns the insert values for the given columns from the
// insertIdx-th row in the INSERT statement, preserving the column order.
// Columns not present in the INSERT field list are omitted from the result.
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

// ColumnIdx returns the 0-based index of the named column in the statement's
// schema, or -1 if not found.
func (s Statement) ColumnIdx(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// IsSelectCountAll reports whether the statement is a bare COUNT(*) query with
// no WHERE clause or JOIN, eligible for the O(1) cached row-count fast path.
func (s Statement) IsSelectCountAll() bool {
	return s.ReadOnly() && len(s.Fields) == 1 && strings.ToUpper(s.Fields[0].Name) == "COUNT(*)"
}

// IsSelectAll reports whether the statement is a SELECT * (wildcard) query.
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
