package parser

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errInvalidStatementKind = fmt.Errorf("invalid statement kind")
	errEmptyStatementKind   = fmt.Errorf("statement kind cannot be empty")
	errEmptyTableName       = fmt.Errorf("table name cannot be empty")
)

var reservedWords = []string{
	// operators
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<",
	// column types
	"BOOLEAN", "INT4", "INT8", "REAL", "DOUBLE", "VARCHAR(",
	// statement types
	"CREATE TABLE", "DROP TABLE", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	// statement other
	"*", "IS NULL", "IS NOT NULL", "NOT NULL", "NULL", "IF NOT EXISTS", "WHERE", "FROM", "SET", "AS",
}

type step int

const (
	stepBeginning step = iota + 1
	stepCreateTableIfNotExists
	stepCreateTableName
	stepCreateTableOpeningParens
	stepCreateTableColumn
	stepCreateTableColumnDef
	stepCreateTableVarcharLength
	stepCreateTableColumnNullNotNull
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
	stepWhereConditionField
	stepWhereConditionOperator
	stepWhereConditionValue
	stepWhereOperator
)

type parser struct {
	minisql.Statement
	i               int // where we are in the query
	sql             string
	step            step
	err             error
	nextUpdateField string
}

func New() *parser {
	return new(parser)
}

func (p *parser) Parse(ctx context.Context, sql string) (minisql.Statement, error) {
	p.reset()
	p.setSQL(sql)

	p.i = 0
	p.err = nil
	p.nextUpdateField = ""

	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) setSQL(sql string) *parser {
	p.sql = strings.TrimSpace(sql)
	return p
}

func (p *parser) reset() {
	p.Statement = minisql.Statement{}
	p.sql = ""
	p.step = stepBeginning
	p.i = 0
	p.err = nil
	p.nextUpdateField = ""
}

func (p *parser) doParse() (minisql.Statement, error) {
	for p.i < len(p.sql) {
		switch p.step {
		// -----------------
		// QUERY TYPE
		//------------------
		case stepBeginning:
			switch strings.ToUpper(p.peek()) {
			case "CREATE TABLE":
				p.Kind = minisql.CreateTable
				p.pop()
				p.step = stepCreateTableIfNotExists
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
		case stepCreateTableIfNotExists,
			stepCreateTableName,
			stepCreateTableOpeningParens,
			stepCreateTableColumn,
			stepCreateTableColumnDef,
			stepCreateTableVarcharLength,
			stepCreateTableColumnNullNotNull,
			stepCreateTableCommaOrClosingParens:
			if err := p.doParseCreateTable(); err != nil {
				return p.Statement, err
			}
			// -----------------
			// DROP TABLE
			//------------------
		case stepDropTableName:
			if err := p.doParseDropTable(); err != nil {
				return p.Statement, err
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
			if err := p.doParseInsert(); err != nil {
				return p.Statement, err
			}
		// -----------------
		// SELECT
		//------------------
		case stepSelectField,
			stepSelectComma,
			stepSelectFrom,
			stepSelectFromTable:
			if err := p.doParseSelect(); err != nil {
				return p.Statement, err
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
			_, err := p.doParseUpdate()
			if err != nil {
				return p.Statement, err
			}
		// -----------------
		// DELETE FROM
		//------------------
		case stepDeleteFromTable:
			if err := p.doParseDelete(); err != nil {
				return p.Statement, err
			}
		// -----------------
		// WHERE
		//------------------
		case stepWhere,
			stepWhereConditionField,
			stepWhereConditionOperator,
			stepWhereConditionValue,
			stepWhereOperator:
			if err := p.doParseWhere(); err != nil {
				return p.Statement, err
			}
		}
	}
	return p.Statement, p.err
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
	// First check for reserved words
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}
	// Next for quoted string literals
	if p.sql[p.i] == '\'' {
		return p.peekQuotedStringWithLength()
	}
	// Next for numbers (floats or integers)
	if unicode.IsDigit(rune(p.sql[p.i])) {
		_, ln := p.peekNumberWithLength()
		if ln > 0 {
			return p.sql[p.i : p.i+ln], ln
		}
	}
	// And finally for identifiers
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

func (p *parser) peekIntWithLength() (int64, int) {
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
		return int64(intValue), len(p.sql[p.i:i])
	}
	intValue, err := strconv.Atoi(p.sql[p.i:len(p.sql)])
	if err != nil {
		return 0, 0
	}
	return int64(intValue), len(p.sql[p.i:len(p.sql)])
}

func (p *parser) peekNumberWithLength() (float64, int) {
	if len(p.sql) < p.i || !unicode.IsDigit(rune(p.sql[p.i])) {
		return 0.0, 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if unicode.IsDigit(rune(p.sql[i])) || p.sql[i] == '.' {
			continue
		}
		floatValue, err := strconv.ParseFloat(p.sql[p.i:i], 64)
		if err != nil {
			return 0.0, 0
		}
		return floatValue, len(p.sql[p.i:i])
	}
	floatValue, err := strconv.ParseFloat(p.sql[p.i:len(p.sql)], 64)
	if err != nil {
		return 0.0, 0
	}
	return floatValue, len(p.sql[p.i:len(p.sql)])
}

func (p *parser) peekNumberOrQuotedStringWithLength() (any, int) {
	number, ln := p.peekNumberWithLength()
	if ln > 0 {
		if float64(int64(number)) == number {
			return int64(number), ln
		}
		return number, ln
	}
	quotedValue, ln := p.peekQuotedStringWithLength()
	if ln > 0 {
		return quotedValue, ln
	}
	return nil, 0
}

var identifierCharRegexp = regexp.MustCompile(`[\"a-zA-Z_0-9]`)

func (p *parser) peekIdentifierWithLength() (string, int) {
	var i int
	for i = p.i; i < len(p.sql); i++ {
		if !identifierCharRegexp.MatchString(string(p.sql[i])) {
			break
		}
	}
	identifier := p.sql[p.i:i]
	return strings.Trim(identifier, "\""), len(identifier)
}

func (p *parser) validate() error {
	if len(p.Conditions) == 0 && p.step == stepWhereConditionField {
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
	for _, aConditionGroup := range p.Conditions {
		for _, aCondition := range aConditionGroup {
			if aCondition.Operator == 0 {
				return errWhereWithoutOperator
			}
			if aCondition.Operand1.Value == "" && aCondition.Operand1.Type == minisql.Field {
				return fmt.Errorf("at WHERE: condition with empty left side operand")
			}
			if aCondition.Operand2.Value == "" && aCondition.Operand2.Type == minisql.Field {
				return fmt.Errorf("at WHERE: condition with empty right side operand")
			}
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

var identifierRegexp = regexp.MustCompile(`(\"[a-zA-Z_][a-zA-Z_0-9]*\"|[a-zA-Z_][a-zA-Z_0-9]*)`)

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	return identifierRegexp.MatchString(s)
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
