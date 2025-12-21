package parser

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/RichardKnop/minisql/internal/minisql"
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
	"BOOLEAN", "INT4", "INT8", "REAL", "DOUBLE", "TEXT", "VARCHAR(", "TIMESTAMP",
	// statement types
	"CREATE TABLE", "DROP TABLE", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	// statement other
	"*", "PRIMARY KEY AUTOINCREMENT", "PRIMARY KEY", "DEFAULT", "NOT NULL", "NULL",
	"IS NULL", "IS NOT NULL", "TRUE", "FALSE", "NOW()",
	"IF NOT EXISTS", "WHERE", "FROM", "SET", "ASC", "DESC", "AS", "IN (", "NOT IN (",
	"ORDER BY", "LIMIT", "OFFSET",
	"BEGIN", "COMMIT", "ROLLBACK",
	";",
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
	stepCreateTableColumnPrimaryKey
	stepCreateTableColumnNullNotNull
	stepCreateTableColumnDefaultValue
	stepCreateTableCommaOrClosingParens
	stepDropTableName
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
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepSelectOrderBy
	stepSelectOrderByField
	stepSelectOrderByComma
	stepSelectLimit
	stepSelectOffset
	stepWhere
	stepWhereConditionField
	stepWhereConditionOperator
	stepWhereConditionValue
	stepWhereConditionListValue
	stepWhereConditionListValueCommaOrEnd
	stepWhereOperator
	stepStatementEnd
)

type parser struct {
	minisql.Statement
	i               int // where we are in the query
	sql             string
	step            step
	nextUpdateField string
}

func New() *parser {
	return new(parser)
}

func (p *parser) Parse(ctx context.Context, sql string) ([]minisql.Statement, error) {
	sql = strings.Join(strings.Fields(sql), " ")
	p.reset()
	p.setSQL(sql)

	p.i = 0
	p.nextUpdateField = ""

	statements, err := p.doParse()

	p.logError(err)
	return statements, err
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
	p.nextUpdateField = ""
}

func (p *parser) doParse() ([]minisql.Statement, error) {
	var statements []minisql.Statement
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
			case "BEGIN":
				p.Kind = minisql.BeginTransaction
				p.pop()
				p.step = stepStatementEnd
			case "COMMIT":
				p.Kind = minisql.CommitTransaction
				p.pop()
				p.step = stepStatementEnd
			case "ROLLBACK":
				p.Kind = minisql.RollbackTransaction
				p.pop()
				p.step = stepStatementEnd
			default:
				return statements, errInvalidStatementKind
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
			stepCreateTableColumnPrimaryKey,
			stepCreateTableColumnNullNotNull,
			stepCreateTableColumnDefaultValue,
			stepCreateTableCommaOrClosingParens:
			if err := p.doParseCreateTable(); err != nil {
				return statements, err
			}
			// -----------------
			// DROP TABLE
			//------------------
		case stepDropTableName:
			if err := p.doParseDropTable(); err != nil {
				return statements, err
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
				return statements, err
			}
		// -----------------
		// SELECT
		//------------------
		case stepSelectField,
			stepSelectComma,
			stepSelectFrom,
			stepSelectFromTable,
			stepSelectOrderBy,
			stepSelectOrderByField,
			stepSelectOrderByComma,
			stepSelectLimit,
			stepSelectOffset:
			if err := p.doParseSelect(); err != nil {
				return statements, err
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
			if err := p.doParseUpdate(); err != nil {
				return statements, err
			}
		// -----------------
		// DELETE FROM
		//------------------
		case stepDeleteFromTable:
			if err := p.doParseDelete(); err != nil {
				return statements, err
			}
		// -----------------
		// WHERE
		//------------------
		case stepWhere,
			stepWhereConditionField,
			stepWhereConditionOperator,
			stepWhereConditionValue,
			stepWhereConditionListValue,
			stepWhereConditionListValueCommaOrEnd,
			stepWhereOperator:
			if err := p.doParseWhere(); err != nil {
				return statements, err
			}
		case stepStatementEnd:
			semicolon := p.peek()
			if semicolon != ";" && len(semicolon) != 0 {
				return statements, fmt.Errorf("expected semicolon")
			}
			if semicolon == ";" {
				p.pop()
				if err := p.validate(p.Statement); err != nil {
					return nil, err
				}
				statements = append(statements, p.Statement)
				if p.i < len(p.sql)-1 {
					p.step = stepBeginning
					p.Statement = minisql.Statement{}
					p.nextUpdateField = ""
				} else {
					return statements, nil
				}
			}
		}
	}

	if p.step != stepStatementEnd {
		if err := p.validate(p.Statement); err != nil {
			return nil, err
		}
		statements = append(statements, p.Statement)
	}

	return statements, nil
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

func (p *parser) peekBooleanWithLength() (bool, int) {
	boolValue := strings.ToUpper(p.peek())
	if boolValue == "TRUE" || boolValue == "FALSE" {
		return boolValue == "TRUE", len(boolValue)
	}
	return false, 0
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

func (p *parser) peekValue() (any, int) {
	boolean, ln := p.peekBooleanWithLength()
	if ln > 0 {
		return boolean, ln
	}
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

func (p *parser) validate(stmt minisql.Statement) error {
	if len(stmt.Conditions) == 0 && p.step == stepWhereConditionField {
		return errEmptyWhereClause
	}
	if stmt.Kind == 0 {
		return errEmptyStatementKind
	}
	if stmt.Kind == minisql.BeginTransaction || stmt.Kind == minisql.CommitTransaction || stmt.Kind == minisql.RollbackTransaction {
		return nil
	}
	if stmt.TableName == "" {
		return errEmptyTableName
	}
	if stmt.Kind == minisql.CreateTable {
		if len(stmt.Columns) == 0 {
			return errCreateTableNoColumns
		}
		primareKeysNum := 0
		for _, aColumn := range stmt.Columns {
			if aColumn.PrimaryKey {
				primareKeysNum++
			}
		}
		if primareKeysNum > 1 {
			return errCreateTableMultiplePrimaryKeys
		}
	}
	for _, aConditionGroup := range stmt.Conditions {
		for _, aCondition := range aConditionGroup {
			if aCondition.Operator == 0 {
				return errWhereWithoutOperator
			}
			if aCondition.Operand1.Value == "" && aCondition.Operand1.Type == minisql.OperandField {
				return fmt.Errorf("at WHERE: condition with empty left side operand")
			}
			if aCondition.Operand2.Value == "" && aCondition.Operand2.Type == minisql.OperandField {
				return fmt.Errorf("at WHERE: condition with empty right side operand")
			}
		}
	}
	if stmt.Kind == minisql.Insert && len(stmt.Inserts) == 0 {
		return errNoRowsToInsert
	}
	if stmt.Kind == minisql.Insert {
		for _, i := range stmt.Inserts {
			if len(i) != len(stmt.Fields) {
				return errInsertFieldValueCountMismatch
			}
		}
	}
	if stmt.Kind == minisql.Update && len(stmt.Updates) == 0 {
		return errNoFieldsToUpdate
	}
	return nil
}

func (p *parser) logError(err error) {
	if err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(err)
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
