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
	errEmptyIndexName       = fmt.Errorf("index name cannot be empty")
)

var reservedWords = []string{
	// operators
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "IN (", "NOT IN (", "?",
	// column types
	"BOOLEAN", "INT4", "INT8", "REAL", "DOUBLE", "TEXT", "VARCHAR(", "TIMESTAMP",
	// statement types
	"CREATE TABLE", "DROP TABLE", "CREATE INDEX", "DROP INDEX",
	"SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	// statement other
	"*", "COUNT(*)", "ORDER BY", "LIMIT", "OFFSET",
	"PRIMARY KEY AUTOINCREMENT", "PRIMARY KEY", "DEFAULT", "NOT NULL", "NULL", "UNIQUE",
	"IS NULL", "IS NOT NULL", "TRUE", "FALSE", "NOW()",
	"IF NOT EXISTS", "WHERE", "FROM", "SET", "ASC", "DESC", "AS",
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
	stepCreateTableColumnUnique
	stepCreateTableColumnDefaultValue
	stepCreateTableConstraint
	stepCreateTableConstraintPrimaryKey
	stepCreateTableConstraintUniqueKey
	stepCreateTableConstraintPrimaryKeyColumn
	stepCreateTableConstraintUniqueKeyColumn
	stepCreateTableConstraintPrimaryKeyCommaOrClosingParens
	stepCreateTableConstraintUniqueKeyCommaOrClosingParens
	stepCreateTableCommaOrClosingParens
	stepDropTableName
	stepCreateIndexIfNotExists
	stepCreateIndexName
	stepCreateIndexOn
	stepCreateIndexOnTable
	stepCreateIndexOpeningParens
	stepCreateIndexColumn
	stepCreateIndexCommaOrClosingParens
	stepDropIndexName
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
}

type parserItem struct {
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

	item := &parserItem{
		sql:  strings.Join(strings.Fields(sql), " "),
		step: stepBeginning,
	}
	statements, err := item.doParse()

	item.logError(err)

	return statements, err
}

func (p *parserItem) doParse() ([]minisql.Statement, error) {
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
			case "CREATE INDEX":
				p.Kind = minisql.CreateIndex
				p.pop()
				p.step = stepCreateIndexIfNotExists
			case "DROP INDEX":
				p.Kind = minisql.DropIndex
				p.pop()
				p.step = stepDropIndexName
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
			stepCreateTableColumnUnique,
			stepCreateTableColumnDefaultValue,
			stepCreateTableConstraint,
			stepCreateTableConstraintPrimaryKey,
			stepCreateTableConstraintUniqueKey,
			stepCreateTableConstraintPrimaryKeyColumn,
			stepCreateTableConstraintUniqueKeyColumn,
			stepCreateTableConstraintPrimaryKeyCommaOrClosingParens,
			stepCreateTableConstraintUniqueKeyCommaOrClosingParens,
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
		// CREATE INDEX
		//------------------
		case stepCreateIndexIfNotExists,
			stepCreateIndexName,
			stepCreateIndexOn,
			stepCreateIndexOnTable,
			stepCreateIndexOpeningParens,
			stepCreateIndexColumn,
			stepCreateIndexCommaOrClosingParens:
			if err := p.doParseCreateIndex(); err != nil {
				return statements, err
			}
		// -----------------
		// DROP INDEX
		//------------------
		case stepDropIndexName:
			if err := p.doParseDropIndex(); err != nil {
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

func (p *parserItem) peek() string {
	peeked, _ := p.peekWithLength()
	return peeked
}

func (p *parserItem) pop() string {
	peeked, len := p.peekWithLength()
	p.i += len
	p.popWhitespace()
	return peeked
}

func (p *parserItem) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

func (p *parserItem) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	// First check for reserved words, however we need to be careful here. For example,
	// we don't want to match "DESC" when the next token is "description".
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token != rWord {
			continue
		}

		// Make sure the next character is not a continuation of an identifier
		if p.i+len(rWord) < len(p.sql) {
			var (
				lastChar = p.sql[p.i+len(rWord)-1]
				nextChar = p.sql[p.i+len(rWord)]
			)
			if identifierCharRegexp.MatchString(string(lastChar)) && identifierCharRegexp.MatchString(string(nextChar)) {
				break
			}
		}

		return token, len(token)
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

func (p *parserItem) peekQuotedStringWithLength() (string, int) {
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

func (p *parserItem) peekBooleanWithLength() (bool, int) {
	boolValue := strings.ToUpper(p.peek())
	if boolValue == "TRUE" || boolValue == "FALSE" {
		return boolValue == "TRUE", len(boolValue)
	}
	return false, 0
}

func (p *parserItem) peekIntWithLength() (int64, int) {
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

func (p *parserItem) peekNumberWithLength() (float64, int) {
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

func (p *parserItem) peekValue() (any, int) {
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

func (p *parserItem) peekIdentifierWithLength() (string, int) {
	var i int
	for i = p.i; i < len(p.sql); i++ {
		if !identifierCharRegexp.MatchString(string(p.sql[i])) {
			break
		}
	}
	identifier := p.sql[p.i:i]
	return strings.Trim(identifier, "\""), len(identifier)
}

func (p *parserItem) validate(stmt minisql.Statement) error {
	if len(stmt.Conditions) == 0 && p.step == stepWhereConditionField {
		return errEmptyWhereClause
	}
	if stmt.Kind == 0 {
		return errEmptyStatementKind
	}
	if stmt.Kind == minisql.BeginTransaction || stmt.Kind == minisql.CommitTransaction || stmt.Kind == minisql.RollbackTransaction {
		return nil
	}
	if stmt.Kind == minisql.CreateIndex || stmt.Kind == minisql.DropIndex {
		if stmt.IndexName == "" {
			return errEmptyIndexName
		}
		if stmt.Kind == minisql.CreateIndex && stmt.TableName == "" {
			return errEmptyTableName
		}
		if stmt.Kind == minisql.CreateIndex && len(stmt.Columns) == 0 {
			return errCreateIndexNoColumns
		}
	} else if stmt.TableName == "" {
		return errEmptyTableName
	}
	if stmt.Kind == minisql.CreateTable {
		if len(stmt.Columns) == 0 {
			return errCreateTableNoColumns
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

func (p *parserItem) logError(err error) {
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
