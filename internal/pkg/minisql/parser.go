package minisql

import (
	"context"
	"fmt"
	"regexp"
	"strings"
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

type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
}

type StatementKind int

const (
	CreateTable StatementKind = iota + 1
	Insert
	Select
	Update
	Delete
)

type ColumnKind int

const (
	Integer ColumnKind = iota + 1
	Varchar
)

type Column struct {
	Kind ColumnKind
}

type Statement struct {
	Kind       StatementKind
	TableName  string
	Conditions []Condition
	Updates    map[string]string
	Inserts    [][]string
	Fields     []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases    map[string]string
	Columns    []Column
}

type step int

const (
	stepBeginning step = iota + 1
	stepCreateTableName
	stepCreateTableOpeningParens
	stepCreateTableColumns
	stepCreateTableCommaOrClosingParens
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepInsertTable
	stepInsertFieldsOpeningParens
	stepInsertFields
	stepInsertFieldsCommaOrClosingParens
	stepInsertValuesOpeningParens
	stepInsertValuesRWord
	stepInsertValues
	stepInsertValuesCommaOrClosingParens
	stepInsertValuesCommaBeforeOpeningParens
	stepUpdateTable
	stepUpdateSet
	stepUpdateField
	stepUpdateEquals
	stepUpdateValue
	stepUpdateComma
	stepDeleteFromTable
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
)

type parser struct {
	Statement
	i               int // where we are in the query
	sql             string
	step            step
	err             error
	nextUpdateField string
}

func NewParser(sql string) *parser {
	return &parser{
		sql:  strings.TrimSpace(sql),
		step: stepBeginning,
	}
}

func (p *parser) Parse(ctx context.Context) (Statement, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

var (
	errInvalidStatementKind          = fmt.Errorf("invalid statement kind")
	errSelectWithoutFields           = fmt.Errorf("at SELECT: expected field to SELECT")
	errUpdateExpectedEquals          = fmt.Errorf("at UPDATE: expected '='")
	errUpdateExpectedQuotedValue     = fmt.Errorf("at UPDATE: expected quoted value")
	errInsertFieldValueCountMismatch = fmt.Errorf("at INSERT INTO: value count doesn't match field count")
	errInsertNoFields                = fmt.Errorf("at INSERT INTO: expected at least one field to insert")
)

func (p *parser) doParse() (Statement, error) {
	for {
		if p.i >= len(p.sql) {
			return p.Statement, p.err
		}
		switch p.step {
		// -----------------
		// QUERY TYPE
		//------------------
		case stepBeginning:
			switch strings.ToUpper(p.peek()) {
			case "CREATE TABLE":
				p.Kind = CreateTable
				p.pop()
				p.step = stepCreateTableName
			case "SELECT":
				p.Kind = Select
				p.pop()
				p.step = stepSelectField
			case "INSERT INTO":
				p.Kind = Insert
				p.pop()
				p.step = stepInsertTable
			case "UPDATE":
				p.Kind = Update
				p.pop()
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.Kind = Delete
				p.pop()
				p.step = stepDeleteFromTable
			default:
				return p.Statement, errInvalidStatementKind
			}
		// -----------------
		// CREATE TABLE
		//------------------
		case stepCreateTableName,
			stepCreateTableOpeningParens,
			stepCreateTableColumns,
			stepCreateTableCommaOrClosingParens:
			continueLoop, err := p.doParseCreateTable()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
		// -----------------
		// INSERT INTO
		//------------------
		case stepInsertTable,
			stepInsertFieldsOpeningParens,
			stepInsertFields,
			stepInsertFieldsCommaOrClosingParens,
			stepInsertValuesRWord,
			stepInsertValuesOpeningParens,
			stepInsertValues,
			stepInsertValuesCommaOrClosingParens,
			stepInsertValuesCommaBeforeOpeningParens:
			continueLoop, err := p.doParseInsert()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
		// -----------------
		// SELECT
		//------------------
		case stepSelectField,
			stepSelectComma,
			stepSelectFrom,
			stepSelectFromTable:
			continueLoop, err := p.doParseSelect()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
		// -----------------
		// UPDATE
		//------------------
		case stepUpdateTable,
			stepUpdateSet,
			stepUpdateField,
			stepUpdateEquals,
			stepUpdateValue,
			stepUpdateComma:
			continueLoop, err := p.doParseUpdate()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
		// -----------------
		// DELETE FROM
		//------------------
		case stepDeleteFromTable:
			continueLoop, err := p.doParseDelete()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
		// -----------------
		// WHERE
		//------------------
		case stepWhere,
			stepWhereField,
			stepWhereOperator,
			stepWhereValue,
			stepWhereAnd:
			continueLoop, err := p.doParseWhere()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
		}
	}
}

func (p *parser) doParseCreateTable() (bool, error) {
	switch p.step {
	case stepCreateTableName:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at CREATE FROM: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepCreateTableOpeningParens
	case stepCreateTableOpeningParens:
		openingParens := p.peek()
		fmt.Println(openingParens)
		if len(openingParens) != 1 || openingParens != "(" {
			return false, fmt.Errorf("at CREATE TABLE: expected opening parens")
		}
		p.pop()
		p.step = stepCreateTableColumns
	case stepCreateTableColumns:
		// TODO
	case stepCreateTableCommaOrClosingParens:
		// TODO
	}
	return false, nil
}

func (p *parser) doParseInsert() (bool, error) {
	switch p.step {
	case stepInsertTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at INSERT INTO: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepInsertFieldsOpeningParens
	case stepInsertFieldsOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return false, fmt.Errorf("at INSERT INTO: expected opening parens")
		}
		p.pop()
		p.step = stepInsertFields
	case stepInsertFields:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return false, errInsertNoFields
		}
		p.Fields = append(p.Fields, identifier)
		p.pop()
		p.step = stepInsertFieldsCommaOrClosingParens
	case stepInsertFieldsCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return false, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertFields
			return true, nil
		}
		p.step = stepInsertValuesRWord
	case stepInsertValuesRWord:
		valuesRWord := p.peek()
		if strings.ToUpper(valuesRWord) != "VALUES" {
			return false, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	case stepInsertValuesOpeningParens:
		openingParens := p.peek()
		if openingParens != "(" {
			return false, fmt.Errorf("at INSERT INTO: expected opening parens")
		}
		p.Inserts = append(p.Inserts, []string{})
		p.pop()
		p.step = stepInsertValues
	case stepInsertValues:
		quotedValue, ln := p.peekQuotedStringWithLength()
		if ln == 0 {
			return false, fmt.Errorf("at INSERT INTO: expected quoted value")
		}
		p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], quotedValue)
		p.pop()
		p.step = stepInsertValuesCommaOrClosingParens
	case stepInsertValuesCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return false, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertValues
			return true, nil
		}
		currentInsertRow := p.Inserts[len(p.Inserts)-1]
		if len(currentInsertRow) < len(p.Fields) {
			return false, errInsertFieldValueCountMismatch
		}
		p.step = stepInsertValuesCommaBeforeOpeningParens
	case stepInsertValuesCommaBeforeOpeningParens:
		commaRWord := p.peek()
		if strings.ToUpper(commaRWord) != "," {
			return false, fmt.Errorf("at INSERT INTO: expected comma")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	}
	return false, nil
}

func (p *parser) doParseSelect() (bool, error) {
	switch p.step {
	case stepSelectField:
		identifier := p.peek()
		if !isIdentifierOrAsterisk(identifier) {
			return false, errSelectWithoutFields
		}
		p.Fields = append(p.Fields, identifier)
		p.pop()
		maybeFrom := p.peek()
		if strings.ToUpper(maybeFrom) == "AS" {
			p.pop()
			alias := p.peek()
			if !isIdentifier(alias) {
				return false, fmt.Errorf("at SELECT: expected field alias for \"" + identifier + " as\" to SELECT")
			}
			if p.Aliases == nil {
				p.Aliases = make(map[string]string)
			}
			p.Aliases[identifier] = alias
			p.pop()
			maybeFrom = p.peek()
		}
		if strings.ToUpper(maybeFrom) == "FROM" {
			p.step = stepSelectFrom
			return true, nil
		}
		p.step = stepSelectComma
	case stepSelectComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			return false, fmt.Errorf("at SELECT: expected comma or FROM")
		}
		p.pop()
		p.step = stepSelectField
	case stepSelectFrom:
		fromRWord := p.peek()
		if strings.ToUpper(fromRWord) != "FROM" {
			return false, fmt.Errorf("at SELECT: expected FROM")
		}
		p.pop()
		p.step = stepSelectFromTable
	case stepSelectFromTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at SELECT: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
	}
	return false, nil
}

func (p *parser) doParseUpdate() (bool, error) {
	switch p.step {
	case stepUpdateTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at UPDATE: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepUpdateSet
	case stepUpdateSet:
		setRWord := p.peek()
		if setRWord != "SET" {
			return false, fmt.Errorf("at UPDATE: expected 'SET'")
		}
		p.pop()
		p.step = stepUpdateField
	case stepUpdateField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return false, fmt.Errorf("at UPDATE: expected at least one field to update")
		}
		p.nextUpdateField = identifier
		p.pop()
		p.step = stepUpdateEquals
	case stepUpdateEquals:
		equalsRWord := p.peek()
		if equalsRWord != "=" {
			return false, errUpdateExpectedEquals
		}
		p.pop()
		p.step = stepUpdateValue
	case stepUpdateValue:
		quotedValue, ln := p.peekQuotedStringWithLength()
		if ln == 0 {
			return false, errUpdateExpectedQuotedValue
		}
		p.setUpdate(p.nextUpdateField, quotedValue)
		p.nextUpdateField = ""
		p.pop()
		maybeWhere := p.peek()
		if strings.ToUpper(maybeWhere) == "WHERE" {
			p.step = stepWhere
			return true, nil
		}
		p.step = stepUpdateComma
	case stepUpdateComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			return false, fmt.Errorf("at UPDATE: expected ','")
		}
		p.pop()
		p.step = stepUpdateField
	}
	return false, nil
}

func (p *parser) doParseDelete() (bool, error) {
	switch p.step {
	case stepDeleteFromTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at DELETE FROM: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
	}
	return false, nil
}

func (p *parser) doParseWhere() (bool, error) {
	switch p.step {
	case stepWhere:
		whereRWord := p.peek()
		if strings.ToUpper(whereRWord) != "WHERE" {
			return false, fmt.Errorf("expected WHERE")
		}
		p.pop()
		p.step = stepWhereField
	case stepWhereField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return false, fmt.Errorf("at WHERE: expected field")
		}
		p.Statement.Conditions = append(
			p.Statement.Conditions,
			Condition{Operand1: identifier, Operand1IsField: true},
		)
		p.pop()
		p.step = stepWhereOperator
	case stepWhereOperator:
		operator := p.peek()
		currentCondition := p.Conditions[len(p.Conditions)-1]
		switch operator {
		case "=":
			currentCondition.Operator = Eq
		case ">":
			currentCondition.Operator = Gt
		case ">=":
			currentCondition.Operator = Gte
		case "<":
			currentCondition.Operator = Lt
		case "<=":
			currentCondition.Operator = Lte
		case "!=":
			currentCondition.Operator = Ne
		default:
			return false, fmt.Errorf("at WHERE: unknown operator")
		}
		p.Conditions[len(p.Conditions)-1] = currentCondition
		p.pop()
		p.step = stepWhereValue
	case stepWhereValue:
		currentCondition := p.Conditions[len(p.Conditions)-1]
		identifier := p.peek()
		if isIdentifier(identifier) {
			currentCondition.Operand2 = identifier
			currentCondition.Operand2IsField = true
		} else {
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				return false, fmt.Errorf("at WHERE: expected quoted value")
			}
			currentCondition.Operand2 = quotedValue
			currentCondition.Operand2IsField = false
		}
		p.Conditions[len(p.Conditions)-1] = currentCondition
		p.pop()
		p.step = stepWhereAnd
	case stepWhereAnd:
		andRWord := p.peek()
		if strings.ToUpper(andRWord) != "AND" {
			return false, fmt.Errorf("expected AND")
		}
		p.pop()
		p.step = stepWhereField
	}
	return false, nil
}

func (p *parser) setUpdate(field, value string) {
	if p.Updates == nil {
		p.Updates = make(map[string]string)
	}
	p.Updates[field] = value
}

func (p *parser) peek() string {
	peeked, _ := p.peekWithLength()
	return peeked
}

func (p *parser) pop() string {
	peeked, len := p.peekWithLength()
	p.i += len
	p.popWhitespace()
	return peeked
}

func (p *parser) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

var reservedWords = []string{
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<",
	"CREATE TABLE", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	"WHERE", "FROM", "SET", "AS",
}

func (p *parser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}
	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength()
	}
	return p.peekIdentifierWithLength()
}

func (p *parser) peekQuotedStringWithLength() (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' && p.sql[i-1] != '\\' {
			return p.sql[p.i+1 : i], len(p.sql[p.i+1:i]) + 2 // +2 for the two quotes
		}
	}
	return "", 0
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if matched, _ := regexp.MatchString(`[a-zA-Z0-9_*]`, string(p.sql[i])); !matched {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

var (
	errCreateTableNoColumns         = fmt.Errorf("at CREATE TABLE: no columns specified")
	errEmptyWhereClause             = fmt.Errorf("at WHERE: empty WHERE clause")
	errEmptyStatementKind           = fmt.Errorf("statement kind cannot be empty")
	errEmptyTableName               = fmt.Errorf("table name cannot be empty")
	errWhereWithoutOperator         = fmt.Errorf("at WHERE: condition without operator")
	errWhereRequiredForUpdateDelete = fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	errNoRowsToInsert               = fmt.Errorf("at INSERT INTO: need at least one row to insert")
)

func (p *parser) validate() error {
	if len(p.Conditions) == 0 && p.step == stepWhereField {
		return errEmptyWhereClause
	}
	if p.Kind == 0 {
		return errEmptyStatementKind
	}
	if p.TableName == "" {
		return errEmptyTableName
	}
	if p.Kind == CreateTable && len(p.Columns) == 0 {
		return errCreateTableNoColumns
	}
	if len(p.Conditions) == 0 && (p.Kind == Update || p.Kind == Delete) {
		return errWhereRequiredForUpdateDelete
	}
	for _, c := range p.Conditions {
		if c.Operator == 0 {
			return errWhereWithoutOperator
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2IsField {
			return fmt.Errorf("at WHERE: condition with empty right side operand")
		}
	}
	if p.Kind == Insert && len(p.Inserts) == 0 {
		return errNoRowsToInsert
	}
	if p.Kind == Insert {
		for _, i := range p.Inserts {
			if len(i) != len(p.Fields) {
				return errInsertFieldValueCountMismatch
			}
		}
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func isIdentifierOrAsterisk(s string) bool {
	return isIdentifier(s) || s == "*"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
