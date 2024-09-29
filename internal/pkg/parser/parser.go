package parser

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

var (
	errInvalidStatementKind         = fmt.Errorf("invalid statement kind")
	errEmptyStatementKind           = fmt.Errorf("statement kind cannot be empty")
	errEmptyTableName               = fmt.Errorf("table name cannot be empty")
	errEmptyWhereClause             = fmt.Errorf("at WHERE: empty WHERE clause")
	errWhereWithoutOperator         = fmt.Errorf("at WHERE: condition without operator")
	errWhereRequiredForUpdateDelete = fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
)

var reservedWords = []string{
	// operators
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<",
	// column types
	"INT4", "INT8", "VARCHAR(",
	// statement types
	"CREATE TABLE", "DROP TABLE", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	// statement other
	"WHERE", "FROM", "SET", "AS",
}

type step int

const (
	stepBeginning step = iota + 1
	stepCreateTableName
	stepCreateTableOpeningParens
	stepCreateTableColumn
	stepCreateTableColumnDef
	stepCreateTableVarcharLength
	stepCreateTableCommaOrClosingParens
	stepDropTableName
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
	minisql.Statement
	i               int // where we are in the query
	sql             string
	step            step
	err             error
	nextUpdateField string
}

func New(sql string) *parser {
	return &parser{
		sql:  strings.TrimSpace(sql),
		step: stepBeginning,
	}
}

func (p *parser) Parse(ctx context.Context) (minisql.Statement, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) doParse() (minisql.Statement, error) {
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
				p.Kind = minisql.CreateTable
				p.pop()
				p.step = stepCreateTableName
			case "DROP TABLE":
				p.Kind = minisql.DropTable
				p.pop()
				p.step = stepDropTableName
			case "SELECT":
				p.Kind = minisql.Select
				p.pop()
				p.step = stepSelectField
			case "INSERT INTO":
				p.Kind = minisql.Insert
				p.pop()
				p.step = stepInsertTable
			case "UPDATE":
				p.Kind = minisql.Update
				p.pop()
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.Kind = minisql.Delete
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
			stepCreateTableColumn,
			stepCreateTableColumnDef,
			stepCreateTableVarcharLength,
			stepCreateTableCommaOrClosingParens:
			continueLoop, err := p.doParseCreateTable()
			if err != nil {
				return p.Statement, err
			}
			if continueLoop {
				continue
			}
			// -----------------
			// DROP TABLE
			//------------------
		case stepDropTableName:
			continueLoop, err := p.doParseDropTable()
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
			minisql.Condition{
				Operand1:        identifier,
				Operand1IsField: true,
			},
		)
		p.pop()
		p.step = stepWhereOperator
	case stepWhereOperator:
		operator := p.peek()
		currentCondition := p.Conditions[len(p.Conditions)-1]
		switch operator {
		case "=":
			currentCondition.Operator = minisql.Eq
		case ">":
			currentCondition.Operator = minisql.Gt
		case ">=":
			currentCondition.Operator = minisql.Gte
		case "<":
			currentCondition.Operator = minisql.Lt
		case "<=":
			currentCondition.Operator = minisql.Lte
		case "!=":
			currentCondition.Operator = minisql.Ne
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

func (p *parser) peepIntWithLength() (int, int) {
	if len(p.sql) < p.i || !unicode.IsDigit(rune(p.sql[p.i])) {
		return 0, 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if unicode.IsDigit(rune(p.sql[i])) {
			continue
		}
		intValue, err := strconv.Atoi(p.sql[p.i:i])
		if err != nil {
			return 0, 0
		}
		return intValue, len(p.sql[p.i:i])
	}
	intValue, err := strconv.Atoi(p.sql[p.i:len(p.sql)])
	if err != nil {
		return 0, 0
	}
	return intValue, len(p.sql[p.i:len(p.sql)])
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if matched, _ := regexp.MatchString(`[a-zA-Z0-9_*]`, string(p.sql[i])); !matched {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

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
	if p.Kind == minisql.CreateTable && len(p.Columns) == 0 {
		return errCreateTableNoColumns
	}
	if len(p.Conditions) == 0 && (p.Kind == minisql.Update || p.Kind == minisql.Delete) {
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
	if p.Kind == minisql.Insert && len(p.Inserts) == 0 {
		return errNoRowsToInsert
	}
	if p.Kind == minisql.Insert {
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
