package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errCreateTableExpectedOpeningParens     = errors.New("at CREATE TABLE: expected opening parens")
	errCreateTableNoColumns                 = errors.New("at CREATE TABLE: no columns specified")
	errCreateTableInvalidColumDef           = errors.New("at CREATE TABLE: invalid column definition")
	errCreateTableMultiplePrimaryKeys       = errors.New("at CREATE TABLE: multiple PRIMARY KEY columns specified")
	errCreateTablePrimaryKeyTextNotAllowed  = errors.New("at CREATE TABLE: primary key cannot be of type TEXT")
	errCreateTablePrimaryKeyVarcharTooLarge = fmt.Errorf("at CREATE TABLE: primary key of type VARCHAR exceeds max index key size %d", minisql.MaxIndexKeySize)
	errCreateTableUniqueTextNotAllowed      = errors.New("at CREATE TABLE: unique key cannot be of type TEXT")
	errCreateTableUniqueVarcharTooLarge     = fmt.Errorf("at CREATE TABLE: unique key of type VARCHAR exceeds max index key size %d", minisql.MaxIndexKeySize)
	errCreateTableDefaultValueExpected      = errors.New("at CREATE TABLE: expected default value after DEFAULT")
)

func (p *parserItem) doParseCreateTable() error {
	switch p.step {
	case stepCreateTableIfNotExists:
		ifnotExists := p.peek()
		p.step = stepCreateTableName
		if strings.ToUpper(ifnotExists) != "IF NOT EXISTS" {
			return nil
		}
		p.IfNotExists = true
		p.pop()
		p.step = stepCreateTableName
	case stepCreateTableName:
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at CREATE TABLE: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepCreateTableOpeningParens
	case stepCreateTableOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return p.wrapErr(errCreateTableExpectedOpeningParens)
		}
		p.pop()
		p.step = stepCreateTableColumn
	case stepCreateTableColumn:
		token := p.peek()

		switch strings.ToUpper(token) {
		case "PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "CONSTRAINT":
			p.step = stepCreateTableConstraint
			return nil
		}

		if !isIdentifier(token) {
			return p.wrapErr(errCreateTableNoColumns)
		}
		p.Columns = append(p.Columns, minisql.Column{
			Name: token,
		})
		p.pop()
		p.step = stepCreateTableColumnDef
	case stepCreateTableColumnDef:
		columnDef := p.peek()
		col, ok := isColumnDef(columnDef)
		if !ok {
			return p.wrapErr(errCreateTableInvalidColumDef)
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Kind = col.Kind
		if col.Kind == minisql.Varchar {
			p.step = stepCreateTableVarcharLength
		} else {
			p.Columns[len(p.Columns)-1].Size = col.Size
			p.step = stepCreateTableColumnPrimaryKey
		}
	case stepCreateTableVarcharLength:
		sizeToken := p.peek()
		size, err := strconv.Atoi(sizeToken)
		if err != nil {
			return p.errorf("at CREATE TABLE: varchar size '%s' must be an integer", sizeToken)
		}
		if size <= 0 {
			return p.errorf("at CREATE TABLE: varchar size must be a positive integer")
		}
		if size > minisql.MaxOverflowTextSize {
			return p.errorf("at CREATE TABLE: varchar size must be > 0 and <= %d", minisql.MaxOverflowTextSize)
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Size = uint32(size)
		closingParens := p.peek()
		if closingParens != ")" {
			return p.errorf("at CREATE TABLE: expecting closing parenthesis after varchar size")
		}
		p.pop()
		p.step = stepCreateTableColumnPrimaryKey
	case stepCreateTableColumnPrimaryKey:
		primaryKey := p.peek()
		if primaryKey != "PRIMARY KEY" && primaryKey != "PRIMARY KEY AUTOINCREMENT" {
			p.step = stepCreateTableColumnNullNotNull
			return nil
		}
		if len(p.PrimaryKey.Columns) > 0 {
			return p.wrapErr(errCreateTableMultiplePrimaryKeys)
		}
		col := p.Columns[len(p.Columns)-1]
		if col.Kind == minisql.Text {
			return p.wrapErr(errCreateTablePrimaryKeyTextNotAllowed)
		}
		if col.Kind == minisql.Varchar && col.Size > minisql.MaxIndexKeySize {
			return p.wrapErr(errCreateTablePrimaryKeyVarcharTooLarge)
		}
		if primaryKey == "PRIMARY KEY AUTOINCREMENT" {
			p.PrimaryKey.Autoincrement = true
		}
		p.PrimaryKey.Name = minisql.PrimaryKeyName(p.TableName)
		p.PrimaryKey.Columns = append(p.PrimaryKey.Columns, p.Columns[len(p.Columns)-1])
		p.Columns[len(p.Columns)-1].Nullable = false
		p.pop()
		p.step = stepCreateTableCommaOrClosingParens
	case stepCreateTableColumnNullNotNull:
		nullNotNull := p.peek()
		p.step = stepCreateTableColumnUnique
		switch nullNotNull {
		case "NOT NULL":
			p.Columns[len(p.Columns)-1].Nullable = false
		case "NULL":
			p.Columns[len(p.Columns)-1].Nullable = true
		default:
			// Default to nullable if not specified
			p.Columns[len(p.Columns)-1].Nullable = true
			return nil
		}
		p.pop()
	case stepCreateTableColumnUnique:
		unique := strings.ToUpper(p.peek())
		p.step = stepCreateTableColumnDefaultValue
		if unique != "UNIQUE" {
			return nil
		}
		col := p.Columns[len(p.Columns)-1]
		if col.Kind == minisql.Text {
			return p.wrapErr(errCreateTableUniqueTextNotAllowed)
		}
		if col.Kind == minisql.Varchar && col.Size > minisql.MaxIndexKeySize {
			return p.wrapErr(errCreateTableUniqueVarcharTooLarge)
		}
		p.UniqueIndexes = append(p.UniqueIndexes, minisql.UniqueIndex{
			IndexInfo: minisql.IndexInfo{
				Name:    minisql.UniqueIndexName(p.TableName, col.Name),
				Columns: p.Columns[len(p.Columns)-1 : len(p.Columns)],
			},
		})
		p.pop()
		// Allow DEFAULT and CHECK to follow UNIQUE.
		p.step = stepCreateTableColumnDefaultValue
	case stepCreateTableColumnDefaultValue:
		defaultRWord := p.peek()
		p.step = stepCreateTableColumnCheck
		if defaultRWord != "DEFAULT" {
			return nil
		}
		p.pop()
		if strings.ToUpper(p.peek()) == "NOW()" {
			if p.Columns[len(p.Columns)-1].Kind != minisql.Timestamp {
				return p.errorf("at CREATE TABLE: NOW() default value is only valid for TIMESTAMP columns")
			}
			p.Columns[len(p.Columns)-1].DefaultValueNow = true
			p.pop()
			return nil
		}
		defaultValue, n := p.peekValue()
		if n == 0 {
			return p.wrapErr(errCreateTableDefaultValueExpected)
		}
		if err := isDefaultValueValid(p.Columns[len(p.Columns)-1], defaultValue); err != nil {
			return err
		}
		p.pop()
		if _, ok := defaultValue.(string); ok {
			defaultValue = minisql.NewTextPointer([]byte(defaultValue.(string)))
		}
		p.Columns[len(p.Columns)-1].DefaultValue = minisql.OptionalValue{
			Value: defaultValue,
			Valid: true,
		}
	case stepCreateTableColumnCheck:
		checkRWord := strings.ToUpper(p.peek())
		p.step = stepCreateTableColumnFKRef
		if checkRWord != "CHECK" {
			return nil
		}
		p.pop() // consume "CHECK"
		if p.peek() != "(" {
			return p.errorf("at CREATE TABLE: expected '(' after CHECK")
		}
		p.pop() // consume "("
		startPos := p.i
		node, err := p.parseCondExpr()
		if err != nil {
			return err
		}
		if p.peek() != ")" {
			return p.errorf("at CREATE TABLE: expected ')' after CHECK expression")
		}
		rawExpr := strings.TrimSpace(p.sql[startPos:p.i])
		p.pop() // consume ")"
		p.Columns[len(p.Columns)-1].Check = rawExpr
		p.Columns[len(p.Columns)-1].CheckCond = node
	case stepCreateTableConstraint:
		token := strings.ToUpper(p.peek())
		switch token {
		case "PRIMARY KEY":
			p.pop()
			p.step = stepCreateTableConstraintPrimaryKey
			return nil
		case "UNIQUE":
			p.pop()
			p.step = stepCreateTableConstraintUniqueKey
			return nil
		case "FOREIGN KEY":
			p.pop()
			p.fkInProgress = minisql.ForeignKey{}
			p.fkAfterStep = stepCreateTableConstraint
			p.step = stepCreateTableConstraintForeignKey
			return nil
		case "CONSTRAINT":
			p.pop()
			// Peek the optional constraint name (identifier; quotes are stripped by peek)
			name := p.peek()
			if isIdentifier(name) {
				p.fkInProgress = minisql.ForeignKey{Name: name}
				p.pop()
			} else {
				p.fkInProgress = minisql.ForeignKey{}
			}
			// Expect FOREIGN KEY
			if strings.ToUpper(p.peek()) != "FOREIGN KEY" {
				return p.errorf("at CREATE TABLE: expected FOREIGN KEY after CONSTRAINT")
			}
			p.pop()
			p.fkAfterStep = stepCreateTableConstraint
			p.step = stepCreateTableConstraintForeignKey
			return nil
		}
		if token != ")" {
			return p.errorf("at CREATE TABLE: expected PRIMARY KEY, UNIQUE, FOREIGN KEY, CONSTRAINT, or closing parens")
		}
		p.pop()
		p.step = stepStatementEnd
	case stepCreateTableConstraintPrimaryKey:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return p.wrapErr(errCreateTableExpectedOpeningParens)
		}
		p.pop()
		p.PrimaryKey.Name = minisql.PrimaryKeyName(p.TableName)
		p.step = stepCreateTableConstraintPrimaryKeyColumn
	case stepCreateTableConstraintUniqueKey:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return p.wrapErr(errCreateTableExpectedOpeningParens)
		}
		p.pop()
		p.UniqueIndexes = append(p.UniqueIndexes, minisql.UniqueIndex{})
		p.step = stepCreateTableConstraintUniqueKeyColumn
	case stepCreateTableConstraintPrimaryKeyColumn:
		columnName := p.peek()
		if !isIdentifier(columnName) {
			return p.errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		var foundCol minisql.Column
		for _, col := range p.Columns {
			if col.Name == columnName {
				foundCol = col
				break
			}
		}
		if foundCol.Name == "" {
			return p.errorf("at CREATE TABLE: primary key column '%s' does not exist", columnName)
		}
		p.PrimaryKey.Columns = append(p.PrimaryKey.Columns, foundCol)
		p.step = stepCreateTableConstraintPrimaryKeyCommaOrClosingParens
	case stepCreateTableConstraintUniqueKeyColumn:
		columnName := p.peek()
		if !isIdentifier(columnName) {
			return p.errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		var foundCol minisql.Column
		for _, col := range p.Columns {
			if col.Name == columnName {
				foundCol = col
				break
			}
		}
		p.UniqueIndexes[len(p.UniqueIndexes)-1].Columns = append(p.UniqueIndexes[len(p.UniqueIndexes)-1].Columns, foundCol)
		p.step = stepCreateTableConstraintUniqueKeyCommaOrClosingParens
	case stepCreateTableConstraintPrimaryKeyCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateTableConstraintPrimaryKeyColumn
			return nil
		}
		p.step = stepCreateTableConstraint
	case stepCreateTableConstraintUniqueKeyCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateTableConstraintUniqueKeyColumn
			return nil
		}
		p.UniqueIndexes[len(p.UniqueIndexes)-1].Name = minisql.UniqueIndexName(p.TableName, columnNames(p.UniqueIndexes[len(p.UniqueIndexes)-1].Columns)...)
		p.step = stepCreateTableConstraint
	case stepCreateTableColumnFKRef:
		// Optional inline REFERENCES clause after a column definition.
		if strings.ToUpper(p.peek()) == "REFERENCES" {
			p.pop()
			// Inline REFERENCES: the FK column is the last defined column.
			p.fkInProgress = minisql.ForeignKey{
				Columns: []string{p.Columns[len(p.Columns)-1].Name},
			}
			p.fkAfterStep = stepCreateTableCommaOrClosingParens
			p.step = stepCreateTableFKParentTable
			return nil
		}
		p.step = stepCreateTableCommaOrClosingParens

	case stepCreateTableFKParentTable:
		tableName := p.peek()
		if !isIdentifier(tableName) {
			return p.errorf("at CREATE TABLE: expected parent table name after REFERENCES")
		}
		p.fkInProgress.TargetTable = tableName
		p.pop()
		p.step = stepCreateTableFKParentOpenParens

	case stepCreateTableFKParentOpenParens:
		if p.peek() != "(" {
			return p.errorf("at CREATE TABLE: expected '(' after REFERENCES table name")
		}
		p.pop()
		p.step = stepCreateTableFKParentColumn

	// stepCreateTableFKParentColumn: collect one or more parent column names.
	case stepCreateTableFKParentColumn:
		colName := p.peek()
		if !isIdentifier(colName) {
			return p.errorf("at CREATE TABLE: expected parent column name")
		}
		p.fkInProgress.TargetColumns = append(p.fkInProgress.TargetColumns, colName)
		p.pop()
		p.step = stepCreateTableFKParentColumnCommaOrClose

	case stepCreateTableFKParentColumnCommaOrClose:
		if p.peek() == "," {
			p.pop()
			p.step = stepCreateTableFKParentColumn
			return nil
		}
		if p.peek() != ")" {
			return p.errorf("at CREATE TABLE: expected ',' or ')' after parent column name")
		}
		p.pop()
		p.step = stepCreateTableFKOnDeleteOrUpdate

	case stepCreateTableFKOnDeleteOrUpdate:
		token := strings.ToUpper(p.peek())
		switch token {
		case "ON DELETE":
			p.pop()
			p.fkActionTarget = "onDelete"
			p.step = stepCreateTableFKActionKind
		case "ON UPDATE":
			p.pop()
			p.fkActionTarget = "onUpdate"
			p.step = stepCreateTableFKActionKind
		default:
			// No more FK clauses — finalize and continue.
			p.finalizeFKInProgress()
			p.step = p.fkAfterStep
		}

	case stepCreateTableFKActionKind:
		token := strings.ToUpper(p.peek())
		var action minisql.FKAction
		switch token {
		case "RESTRICT":
			action = minisql.FKActionRestrict
		case "NO ACTION":
			action = minisql.FKActionNoAction
		case "SET NULL":
			action = minisql.FKActionSetNull
		case "CASCADE":
			action = minisql.FKActionCascade
		default:
			return p.errorf("at CREATE TABLE: expected RESTRICT, NO ACTION, SET NULL, or CASCADE")
		}
		p.pop()
		if p.fkActionTarget == "onDelete" {
			p.fkInProgress.OnDelete = action
		} else {
			p.fkInProgress.OnUpdate = action
		}
		p.step = stepCreateTableFKOnDeleteOrUpdate // loop: check for more ON DELETE/UPDATE

	// Table-level FOREIGN KEY (child column list — one or more columns).
	case stepCreateTableConstraintForeignKey:
		if p.peek() != "(" {
			return p.errorf("at CREATE TABLE: expected '(' after FOREIGN KEY")
		}
		p.pop()
		p.step = stepCreateTableConstraintForeignKeyColumn

	case stepCreateTableConstraintForeignKeyColumn:
		colName := p.peek()
		if !isIdentifier(colName) {
			return p.errorf("at CREATE TABLE: expected column name in FOREIGN KEY clause")
		}
		p.fkInProgress.Columns = append(p.fkInProgress.Columns, colName)
		p.pop()
		if p.peek() == "," {
			p.pop() // consume comma, loop for next column
			return nil
		}
		if p.peek() != ")" {
			return p.errorf("at CREATE TABLE: expected ')' after FOREIGN KEY column list")
		}
		p.pop()
		// Now expect REFERENCES.
		if strings.ToUpper(p.peek()) != "REFERENCES" {
			return p.errorf("at CREATE TABLE: expected REFERENCES after FOREIGN KEY (...)")
		}
		p.pop()
		p.step = stepCreateTableFKParentTable

	case stepCreateTableCommaOrClosingParens:
		commaOrClosingParens := strings.ToUpper(p.peek())
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateTableColumn
			return nil
		}
		p.step = stepStatementEnd
	}
	return nil
}

// finalizeFKInProgress assigns a name (if not set) and appends fkInProgress to ForeignKeys.
func (p *parserItem) finalizeFKInProgress() {
	fk := p.fkInProgress
	if fk.Name == "" {
		fk.Name = minisql.AutoFKName(p.TableName, fk.TargetTable, fk.Columns)
	}
	if fk.OnDelete == 0 {
		fk.OnDelete = minisql.FKActionRestrict
	}
	if fk.OnUpdate == 0 {
		fk.OnUpdate = minisql.FKActionRestrict
	}
	p.ForeignKeys = append(p.ForeignKeys, fk)
	p.fkInProgress = minisql.ForeignKey{}
}

func columnNames(columns []minisql.Column) []string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, col.Name)
	}
	return names
}

func isDefaultValueValid(column minisql.Column, valueToken any) error {
	switch column.Kind {
	case minisql.Boolean:
		_, ok := valueToken.(bool)
		if !ok {
			return fmt.Errorf("at CREATE TABLE: default value '%s' is not a valid boolean", valueToken)
		}
	case minisql.Int4, minisql.Int8:
		_, ok := valueToken.(int64)
		if !ok {
			return fmt.Errorf("at CREATE TABLE: default value '%s' is not a valid integer", valueToken)
		}
	case minisql.Real, minisql.Double:
		_, ok := valueToken.(float64)
		if !ok {
			return fmt.Errorf("at CREATE TABLE: default value '%s' is not a valid float", valueToken)
		}
	case minisql.Text, minisql.Varchar, minisql.Timestamp:
		_, ok := valueToken.(string)
		if !ok {
			return fmt.Errorf("at CREATE TABLE: default value '%s' is not a valid string", valueToken)
		}
	}
	return nil
}

func (p *parserItem) doParseDropTable() error {
	if p.step == stepDropTableName {
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at DROP TABLE: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}

func isColumnDef(token string) (minisql.Column, bool) {
	switch strings.ToUpper(token) {
	case "BOOLEAN":
		return minisql.Column{Kind: minisql.Boolean, Size: 1}, true
	case "INT4":
		return minisql.Column{Kind: minisql.Int4, Size: 4}, true
	case "INT8":
		return minisql.Column{Kind: minisql.Int8, Size: 8}, true
	case "REAL":
		return minisql.Column{Kind: minisql.Real, Size: 4}, true
	case "DOUBLE":
		return minisql.Column{Kind: minisql.Double, Size: 8}, true
	case "TEXT":
		return minisql.Column{Kind: minisql.Text}, true
	case "VARCHAR(":
		return minisql.Column{Kind: minisql.Varchar}, true
	case "TIMESTAMP":
		return minisql.Column{Kind: minisql.Timestamp, Size: 8}, true
	case "JSON":
		return minisql.Column{Kind: minisql.JSON}, true
	default:
		return minisql.Column{}, false
	}
}
