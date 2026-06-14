package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errNoRowsToInsert                = errors.New("at INSERT INTO: need at least one row to insert")
	errInsertFieldValueCountMismatch = errors.New("at INSERT INTO: value count doesn't match field count")
	errInsertNoFields                = errors.New("at INSERT INTO: expected at least one field to insert")
)

// insertSelectBoundary returns the byte offset in upperSQL (already normalised,
// upper-cased) where the SELECT sub-statement ends. It stops at the first
// occurrence of "ON CONFLICT" or "RETURNING" (INSERT-level keywords) at
// paren depth 0 and outside string literals, or at the end of the string.
func insertSelectBoundary(upperSQL string) int {
	depth := 0
	i := 0
	for i < len(upperSQL) {
		switch upperSQL[i] {
		case '\'':
			i += 1
			for i < len(upperSQL) && upperSQL[i] != '\'' {
				i += 1
			}
			if i < len(upperSQL) {
				i += 1 // skip closing quote
			}
		case '(':
			depth += 1
			i += 1
		case ')':
			depth -= 1
			i += 1
		default:
			if depth == 0 {
				rem := upperSQL[i:]
				if isInsertKeywordAt(rem, "ON CONFLICT") || isInsertKeywordAt(rem, "RETURNING") {
					return i
				}
			}
			i += 1
		}
	}
	return i
}

// isInsertKeywordAt reports whether rem starts with kw followed by a space,
// semicolon, or end-of-string (i.e. kw is a complete token, not a prefix of
// a longer identifier).
func isInsertKeywordAt(rem, kw string) bool {
	if !strings.HasPrefix(rem, kw) {
		return false
	}
	rest := rem[len(kw):]
	return rest == "" || rest[0] == ' ' || rest[0] == ';'
}

func (p *parserItem) doParseInsert() error {
	switch p.step {
	case stepInsertTable:
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at INSERT INTO: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepInsertFieldsOpeningParens
	case stepInsertFieldsOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return p.errorf("at INSERT INTO: expected opening parens")
		}
		p.pop()
		p.step = stepInsertFields
	case stepInsertFields:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.wrapErr(errInsertNoFields)
		}
		p.Fields = append(p.Fields, minisql.Field{Name: identifier})
		p.pop()
		p.step = stepInsertFieldsCommaOrClosingParens
	case stepInsertFieldsCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertFields
			return nil
		}
		p.step = stepInsertValuesRWord
	case stepInsertValuesRWord:
		valuesRWord := p.peek()
		if strings.ToUpper(valuesRWord) == "SELECT" {
			// INSERT INTO … SELECT — parse the SELECT as a sub-statement.
			//
			// Limit the sub-parser to the SELECT portion only: stop before any
			// INSERT-level keyword (ON CONFLICT, RETURNING) at paren depth 0.
			// This prevents the SELECT parser from consuming those tokens.
			boundary := insertSelectBoundary(p.upperSQL[p.i:])
			rest := &parserItem{
				sql:      p.sql[p.i : p.i+boundary],
				upperSQL: p.upperSQL[p.i : p.i+boundary],
				step:     stepBeginning,
			}
			selectStmts, err := rest.doParse()
			if err != nil {
				return fmt.Errorf("INSERT INTO … SELECT: %w", err)
			}
			if len(selectStmts) != 1 {
				return p.errorf("at INSERT INTO … SELECT: expected exactly one SELECT statement")
			}
			if selectStmts[0].Kind != minisql.Select {
				return p.errorf("at INSERT INTO … SELECT: expected SELECT statement")
			}
			selectStmt := selectStmts[0]
			p.InsertSelectStmt = &selectStmt
			p.i += rest.i
			// Transition to stepInsertValuesCommaBeforeOpeningParens so the normal
			// INSERT post-value handling processes ON CONFLICT / RETURNING / end.
			p.step = stepInsertValuesCommaBeforeOpeningParens
			return nil
		}
		if strings.ToUpper(valuesRWord) != "VALUES" {
			return p.errorf("at INSERT INTO: expected VALUES or SELECT")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	case stepInsertValuesOpeningParens:
		openingParens := p.peek()
		if openingParens != "(" {
			return p.errorf("at INSERT INTO: expected opening parens")
		}
		p.Inserts = append(p.Inserts, []minisql.OptionalValue{})
		p.pop()
		p.step = stepInsertValues
	case stepInsertValues:
		specialValue := strings.ToUpper(p.peek())
		if specialValue == "?" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Value: minisql.Placeholder{}, Valid: true})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		if specialValue == "NULL" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Valid: false})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		if specialValue == "NOW()" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Value: minisql.FunctionNow, Valid: true})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		if specialValue == "GEN_RANDOM_UUID()" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Value: minisql.FunctionGenRandomUUID, Valid: true})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		// Scalar function call (e.g. ARGON2ID_HASH(?), UPPER('x')).
		if isBuiltinFunction(specialValue) {
			expr, err := p.parseExpr()
			if err != nil {
				return p.errorf("at INSERT INTO: %v", err)
			}
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1],
				minisql.OptionalValue{Value: expr, Valid: true})
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		value, ln := p.peekValue()
		if ln > 0 {
			var insertValue minisql.OptionalValue
			if strValue, ok := value.(string); ok {
				insertValue = minisql.OptionalValue{Value: minisql.NewTextPointer([]byte(strValue)), Valid: true}
			} else {
				insertValue = minisql.OptionalValue{Value: value, Valid: true}
			}
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], insertValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		return p.errorf("at INSERT INTO: expected value")
	case stepInsertValuesCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertValues
			return nil
		}
		currentInsertRow := p.Inserts[len(p.Inserts)-1]
		if len(currentInsertRow) < len(p.Fields) {
			return p.wrapErr(errInsertFieldValueCountMismatch)
		}
		p.step = stepInsertValuesCommaBeforeOpeningParens
	case stepInsertValuesCommaBeforeOpeningParens:
		commaOrEnd := p.peek()
		if commaOrEnd == ";" || commaOrEnd == "" {
			p.step = stepStatementEnd
			return nil
		}
		if strings.ToUpper(commaOrEnd) == "RETURNING" {
			p.pop()
			p.step = stepReturningField
			return nil
		}
		if strings.ToUpper(commaOrEnd) == "ON CONFLICT" {
			p.pop()
			p.step = stepInsertOnConflictDo
			return nil
		}
		if commaOrEnd != "," {
			return p.errorf("at INSERT INTO: expected comma or ON CONFLICT")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	case stepInsertOnConflictDo:
		switch strings.ToUpper(p.peek()) {
		case "DO NOTHING":
			p.ConflictAction = minisql.ConflictActionDoNothing
			p.pop()
			p.step = stepStatementEnd
		case "DO UPDATE":
			p.ConflictAction = minisql.ConflictActionDoUpdate
			p.pop()
			p.step = stepInsertOnConflictUpdateSet
		default:
			return p.errorf("at INSERT INTO ON CONFLICT: expected DO NOTHING or DO UPDATE")
		}
	case stepInsertOnConflictUpdateSet:
		if strings.ToUpper(p.peek()) != "SET" {
			return p.errorf("at INSERT INTO ON CONFLICT DO UPDATE: expected SET")
		}
		p.pop()
		p.step = stepInsertOnConflictUpdateField
	case stepInsertOnConflictUpdateField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.errorf("at INSERT INTO ON CONFLICT DO UPDATE SET: expected field name")
		}
		p.nextUpdateField = identifier
		p.pop()
		p.step = stepInsertOnConflictUpdateEquals
	case stepInsertOnConflictUpdateEquals:
		if p.peek() != "=" {
			return p.errorf("at INSERT INTO ON CONFLICT DO UPDATE SET: expected '='")
		}
		p.pop()
		p.step = stepInsertOnConflictUpdateValue
	case stepInsertOnConflictUpdateValue:
		token := p.peek()
		// EXCLUDED.column_name — reference to the proposed (rejected) row's value.
		if strings.HasPrefix(strings.ToUpper(token), "EXCLUDED.") {
			colName := token[len("EXCLUDED."):]
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{
				Value: minisql.ExcludedRef{Column: colName},
				Valid: true,
			})
			p.nextUpdateField = ""
			p.pop()
			p.step = stepInsertOnConflictUpdateComma
			return nil
		}
		specialValue := strings.ToUpper(token)
		switch specialValue {
		case "?":
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Value: minisql.Placeholder{}, Valid: true})
			p.nextUpdateField = ""
			p.pop()
		case "NULL":
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Valid: false})
			p.nextUpdateField = ""
			p.pop()
		case "NOW()":
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Value: minisql.FunctionNow, Valid: true})
			p.nextUpdateField = ""
			p.pop()
		case "GEN_RANDOM_UUID()":
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Value: minisql.FunctionGenRandomUUID, Valid: true})
			p.nextUpdateField = ""
			p.pop()
		default:
			value, ln := p.peekValue()
			if ln == 0 {
				return p.errorf("at INSERT INTO ON CONFLICT DO UPDATE SET: expected value")
			}
			var v minisql.OptionalValue
			if strValue, ok := value.(string); ok {
				v = minisql.OptionalValue{Value: minisql.NewTextPointer([]byte(strValue)), Valid: true}
			} else {
				v = minisql.OptionalValue{Value: value, Valid: true}
			}
			p.setUpdate(p.nextUpdateField, v)
			p.nextUpdateField = ""
			p.pop()
		}
		p.step = stepInsertOnConflictUpdateComma
	case stepInsertOnConflictUpdateComma:
		commaOrEnd := p.peek()
		if commaOrEnd == ";" || commaOrEnd == "" {
			p.step = stepStatementEnd
			return nil
		}
		if strings.ToUpper(commaOrEnd) == "RETURNING" {
			p.pop()
			p.step = stepReturningField
			return nil
		}
		if commaOrEnd != "," {
			return p.errorf("at INSERT INTO ON CONFLICT DO UPDATE SET: expected ',' or end of statement")
		}
		p.pop()
		p.step = stepInsertOnConflictUpdateField
	}
	return nil
}
