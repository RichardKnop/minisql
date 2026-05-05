package parser

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errInvalidStatementKind = errors.New("invalid statement kind")
	errEmptyStatementKind   = errors.New("statement kind cannot be empty")
	errEmptyTableName       = errors.New("table name cannot be empty")
	errEmptyIndexName       = errors.New("index name cannot be empty")
	errEmptyPragmaName      = errors.New("pragma name cannot be empty")
)

var (
	identifierCharRegexp = regexp.MustCompile(`[\"a-zA-Z_0-9]`)
	// Matches valid identifiers including qualified names (e.g., table.column, schema.table.column)
	// Supports both quoted ("my table") and unquoted (table_name) segments
	identifierRegexp = regexp.MustCompile(`^(\"[a-zA-Z_][a-zA-Z_0-9]*\"|[a-zA-Z_][a-zA-Z_0-9]*)(\.(\"[a-zA-Z_][a-zA-Z_0-9]*\"|[a-zA-Z_][a-zA-Z_0-9]*))*`)
)

var reservedWords = []string{
	// operators
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "IN (", "NOT IN (", "?",
	// arithmetic operators
	"+", "-", "/",
	// column types
	"BOOLEAN", "INT4", "INT8", "REAL", "DOUBLE", "TEXT", "VARCHAR(", "TIMESTAMP",
	// statement types
	"EXPLAIN ANALYZE", "EXPLAIN",
	"CREATE TABLE", "DROP TABLE", "CREATE INDEX", "DROP INDEX",
	"SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	// statement other
	"*", "COUNT(*)", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET",
	"PRIMARY KEY AUTOINCREMENT", "PRIMARY KEY", "DEFAULT", "NOT NULL", "NULL", "UNIQUE",
	"IS NULL", "IS NOT NULL", "NOT BETWEEN", "NOT LIKE", "BETWEEN", "LIKE", "TRUE", "FALSE", "NOW()",
	"CHECK",
	"IF NOT EXISTS", "WHERE", "FROM", "SET", "ASC", "DESC", "AS",
	"BEGIN", "COMMIT", "ROLLBACK", "ANALYZE", "VACUUM",
	"PRAGMA",
	"INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "ON CONFLICT", "ON",
	"DO UPDATE", "DO NOTHING",
	"DISTINCT",
	"UNION ALL", "UNION",
	"RETURNING",
	"WITH",
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
	stepCreateTableColumnCheck
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
	stepInsertOnConflictDo
	stepInsertOnConflictUpdateSet
	stepInsertOnConflictUpdateField
	stepInsertOnConflictUpdateEquals
	stepInsertOnConflictUpdateValue
	stepInsertOnConflictUpdateComma
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
	stepSelectJoin
	stepSelectJoinTable
	stepSelectJoinConditionField
	stepSelectJoinConditionOperator
	stepSelectJoinConditionValue
	stepSelectGroupBy
	stepSelectGroupByComma
	stepSelectHaving
	stepSelectOrderBy
	stepSelectOrderByField
	stepSelectOrderByComma
	stepSelectLimit
	stepSelectOffset
	stepWhere
	stepAnalyze
	stepPragma
	stepReturningField
	stepReturningComma
	stepWithCTEName
	stepWithCTEAs
	stepWithCTECommaOrSelect
	stepStatementEnd
)

type parser struct{}

type parserItem struct {
	minisql.Statement
	i                 int // where we are in the query
	sql               string
	step              step
	nextUpdateField   string
	joinInProgress    minisql.Join
	cteNameInProgress string
}

// New returns a new SQL parser.
func New() *parser {
	return new(parser)
}

// Parse parses the given SQL string and returns a slice of statements.
func (p *parser) Parse(ctx context.Context, sql string) ([]minisql.Statement, error) {
	item := &parserItem{
		sql:  strings.Join(strings.Fields(sql), " "),
		step: stepBeginning,
	}
	statements, err := item.doParse()
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
			case "WITH":
				p.pop()
				p.step = stepWithCTEName
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
			case "ANALYZE":
				p.Kind = minisql.Analyze
				p.pop()
				p.step = stepAnalyze
			case "VACUUM":
				p.Kind = minisql.Vacuum
				p.pop()
				p.step = stepStatementEnd
			case "PRAGMA":
				p.Kind = minisql.Pragma
				p.pop()
				p.step = stepPragma
			case "EXPLAIN ANALYZE":
				if err := p.parseExplain(true); err != nil {
					return statements, err
				}
			case "EXPLAIN":
				if err := p.parseExplain(false); err != nil {
					return statements, err
				}
			default:
				return statements, p.wrapErr(errInvalidStatementKind)
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
			stepCreateTableColumnCheck,
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
			stepInsertValuesCommaBeforeOpeningParens,
			stepInsertOnConflictDo,
			stepInsertOnConflictUpdateSet,
			stepInsertOnConflictUpdateField,
			stepInsertOnConflictUpdateEquals,
			stepInsertOnConflictUpdateValue,
			stepInsertOnConflictUpdateComma:
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
			stepSelectJoin,
			stepSelectJoinTable,
			stepSelectJoinConditionField,
			stepSelectJoinConditionOperator,
			stepSelectJoinConditionValue,
			stepSelectGroupBy,
			stepSelectGroupByComma,
			stepSelectHaving,
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
		case stepWhere:
			if err := p.doParseWhere(); err != nil {
				return statements, err
			}
		// -----------------
		// ANALYZE
		//------------------
		case stepAnalyze:
			if err := p.doParseAnalyze(); err != nil {
				return statements, err
			}
		case stepPragma:
			if err := p.doParsePragma(); err != nil {
				return statements, err
			}
		// -----------------
		// RETURNING
		//------------------
		case stepReturningField, stepReturningComma:
			if err := p.doParseReturning(); err != nil {
				return statements, err
			}
		// -----------------
		// WITH (CTE)
		//------------------
		case stepWithCTEName:
			if err := p.doParseWithCTEName(); err != nil {
				return statements, err
			}
		case stepWithCTEAs:
			if err := p.doParseWithCTEAs(); err != nil {
				return statements, err
			}
		case stepWithCTECommaOrSelect:
			if err := p.doParseWithCTECommaOrSelect(); err != nil {
				return statements, err
			}
		case stepStatementEnd:
			// For SELECT statements, intercept UNION / UNION ALL before requiring a semicolon.
			if p.Kind == minisql.Select {
				next := strings.ToUpper(p.peek())
				if next == "UNION ALL" || next == "UNION" {
					all := next == "UNION ALL"
					p.pop() // consume "UNION [ALL]"
					// Parse the right-hand SELECT from the remaining SQL.
					rest := &parserItem{
						sql:  p.sql[p.i:],
						step: stepBeginning,
					}
					unionStmts, err := rest.doParse()
					if err != nil {
						return statements, fmt.Errorf("UNION: %w", err)
					}
					if len(unionStmts) != 1 {
						return statements, p.errorf("at UNION: expected exactly one SELECT after UNION")
					}
					p.Unions = append(p.Unions, minisql.UnionClause{
						All:  all,
						Stmt: unionStmts[0],
					})
					// Advance past everything rest consumed.
					p.i += rest.i
					if err := p.validate(p.Statement); err != nil {
						return nil, err
					}
					statements = append(statements, p.Statement)
					// rest consumed all remaining SQL (including any trailing semicolons).
					return statements, nil
				}
			}
			// RETURNING can follow any DML statement (INSERT, UPDATE, DELETE).
			if p.Kind == minisql.Insert || p.Kind == minisql.Update || p.Kind == minisql.Delete {
				if strings.ToUpper(p.peek()) == "RETURNING" {
					p.pop()
					p.step = stepReturningField
					continue
				}
			}
			semicolon := p.peek()
			if semicolon != ";" && semicolon != "" {
				return statements, p.errorf("at STATEMENT: expected semicolon")
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

	// Also handle statements (e.g. VACUUM) that are valid at EOF without a
	// trailing semicolon: they set p.step = stepStatementEnd before the loop
	// ends but are not yet in the slice.
	if p.step != stepStatementEnd || p.Kind != 0 {
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
	peeked, n := p.peekWithLength()
	p.i += n
	p.popWhitespace()
	return peeked
}

func (p *parserItem) popWhitespace() {
	for p.i < len(p.sql) && p.sql[p.i] == ' ' {
		p.i += 1
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

func (p *parserItem) peekIdentifierWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	match := identifierRegexp.FindString(p.sql[p.i:])
	if match == "" {
		return "", 0
	}

	// Remove quotes but preserve the dot-separated structure
	identifier := strings.ReplaceAll(match, "\"", "")
	return identifier, len(match)
}

func (p *parserItem) validate(stmt minisql.Statement) error {
	if len(stmt.Conditions) == 0 && p.step == stepWhere {
		return errEmptyWhereClause
	}
	if stmt.Kind == 0 {
		return errEmptyStatementKind
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
	} else if stmt.TableName == "" &&
		stmt.FromSubquery == nil &&
		stmt.Kind != minisql.Analyze &&
		stmt.Kind != minisql.Vacuum &&
		stmt.Kind != minisql.Pragma &&
		stmt.Kind != minisql.Explain {
		return errEmptyTableName
	}
	if stmt.Kind == minisql.CreateTable {
		if len(stmt.Columns) == 0 {
			return errCreateTableNoColumns
		}
	}
	for _, condGroup := range stmt.Conditions {
		for _, cond := range condGroup {
			if cond.Operator == 0 {
				return errWhereWithoutOperator
			}
			if cond.Operand1.Type == minisql.OperandField && cond.Operand1.Value.(minisql.Field).Name == "" {
				return errors.New("at WHERE: condition with empty left side operand")
			}
			if cond.Operand2.Type == minisql.OperandField && cond.Operand2.Value.(minisql.Field).Name == "" {
				return errors.New("at WHERE: condition with empty right side operand")
			}
		}
	}
	if stmt.Kind == minisql.Insert && len(stmt.Inserts) == 0 {
		return errNoRowsToInsert
	}
	if stmt.Kind == minisql.Insert {
		// Fields contains INSERT column names first, then DO UPDATE SET column names
		// (appended by setUpdate). Only the INSERT portion must match the row values.
		insertFieldCount := len(stmt.Fields) - len(stmt.Updates)
		for _, i := range stmt.Inserts {
			if len(i) != insertFieldCount {
				return errInsertFieldValueCountMismatch
			}
		}
	}
	if stmt.Kind == minisql.Update && len(stmt.Updates) == 0 {
		return errNoFieldsToUpdate
	}
	return nil
}

func isIdentifier(s string) bool {
	if slices.Contains(reservedWords, strings.ToUpper(s)) {
		return false
	}
	return identifierRegexp.MatchString(s)
}
