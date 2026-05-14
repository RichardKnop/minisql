package parser

import (
	"errors"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errUpdateExpectetSet              = errors.New("at UPDATE: expected 'SET'")
	errUpdateExpectedEquals           = errors.New("at UPDATE: expected '='")
	errUpdateExpectedQuotedValueOrInt = errors.New("at UPDATE: expected quoted value or int")
	errNoFieldsToUpdate               = errors.New("at UPDATE: expected at least one field to update")
)

func (p *parserItem) doParseUpdate() error {
	switch p.step {
	case stepUpdateTable:
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at UPDATE: expected table name")
		}
		p.TableName = tableName
		p.pop()
		// Optional target alias: UPDATE employees e SET … or UPDATE employees AS e SET …
		if next := strings.ToUpper(p.peek()); next == "AS" {
			p.pop()
			alias := p.peek()
			if !isIdentifier(alias) {
				return p.errorf("at UPDATE: expected alias after AS")
			}
			p.TableAlias = alias
			p.pop()
		} else if next != "" && next != "SET" && isIdentifier(p.peek()) {
			p.TableAlias = p.peek()
			p.pop()
		}
		p.step = stepUpdateSet
	case stepUpdateSet:
		setRWord := p.peek()
		if strings.ToUpper(setRWord) != "SET" {
			return p.wrapErr(errUpdateExpectetSet)
		}
		p.pop()
		p.step = stepUpdateField
	case stepUpdateField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.wrapErr(errNoFieldsToUpdate)
		}
		p.nextUpdateField = identifier
		p.pop()
		p.step = stepUpdateEquals
	case stepUpdateEquals:
		equalsRWord := p.peek()
		if equalsRWord != "=" {
			return p.wrapErr(errUpdateExpectedEquals)
		}
		p.pop()
		p.step = stepUpdateValue
	case stepUpdateValue:
		specialValue := strings.ToUpper(p.peek())
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
		case "TRUE":
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Value: true, Valid: true})
			p.nextUpdateField = ""
			p.pop()
		case "FALSE":
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Value: false, Valid: true})
			p.nextUpdateField = ""
			p.pop()
		default:
			// Quoted string literals are not arithmetic expressions — handle them
			// with the existing peekValue path to preserve text type.
			if p.i < len(p.sql) && p.sql[p.i] == '\'' {
				value, ln := p.peekValue()
				if ln == 0 {
					return p.wrapErr(errUpdateExpectedQuotedValueOrInt)
				}
				strValue := value.(string)
				p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Value: minisql.NewTextPointer([]byte(strValue)), Valid: true})
				p.nextUpdateField = ""
				p.pop()
				break
			}
			// Everything else (numeric literals, column refs, arithmetic) goes
			// through the expression parser.
			expr, err := p.parseExpr()
			if err != nil {
				return p.wrapErr(errUpdateExpectedQuotedValueOrInt)
			}
			var updateValue minisql.OptionalValue
			// Plain numeric/bool literal — no expression overhead needed.
			if expr.FuncName == "" && !expr.IsNull && expr.Column == "" && expr.Left == nil && expr.CaseClauses == nil {
				updateValue = minisql.OptionalValue{Value: expr.Literal, Valid: true}
			} else {
				// Column reference, binary expression, or function call — store as
				// *Expr for runtime evaluation against the current row.
				updateValue = minisql.OptionalValue{Value: expr, Valid: true}
			}
			p.setUpdate(p.nextUpdateField, updateValue)
			p.nextUpdateField = ""
		}
		maybeNext := strings.ToUpper(p.peek())
		if maybeNext == "WHERE" {
			p.step = stepWhere
			return nil
		}
		if maybeNext == "FROM" {
			p.pop()
			p.step = stepUpdateFrom
			return nil
		}
		p.step = stepUpdateComma
	case stepUpdateComma:
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
		if strings.ToUpper(commaOrEnd) == "FROM" {
			p.pop()
			p.step = stepUpdateFrom
			return nil
		}
		if commaOrEnd != "," {
			return p.errorf("at UPDATE: expected ','")
		}
		p.pop()
		p.step = stepUpdateField
	case stepUpdateFrom:
		next := p.peek()
		if next == "(" {
			// FROM (SELECT …) alias
			p.pop() // consume "("
			if strings.ToUpper(p.peek()) != "SELECT" {
				return p.errorf("at UPDATE FROM: expected SELECT inside parentheses")
			}
			subStmt, err := p.parseSubquery()
			if err != nil {
				return err
			}
			p.UpdateFromSubquery = subStmt
			// Optional AS before alias
			if strings.ToUpper(p.peek()) == "AS" {
				p.pop()
			}
			alias, _ := p.peekIdentifierWithLength()
			if !isIdentifier(alias) {
				return p.errorf("at UPDATE FROM: expected alias after subquery")
			}
			p.UpdateFromAlias = alias
			p.pop()
		} else {
			// FROM table_name [AS] [alias]
			tableName, _ := p.peekIdentifierWithLength()
			if !isIdentifier(tableName) {
				return p.errorf("at UPDATE FROM: expected table name")
			}
			p.UpdateFromTable = tableName
			p.pop()
			// Optional alias
			if strings.ToUpper(p.peek()) == "AS" {
				p.pop()
				alias, _ := p.peekIdentifierWithLength()
				if !isIdentifier(alias) {
					return p.errorf("at UPDATE FROM: expected alias after AS")
				}
				p.UpdateFromAlias = alias
				p.pop()
			} else if a := p.peek(); a != "" && strings.ToUpper(a) != "WHERE" && a != ";" && isIdentifier(a) {
				p.UpdateFromAlias = a
				p.pop()
			}
		}
		// FROM clause parsed; optional WHERE follows
		if strings.ToUpper(p.peek()) == "WHERE" {
			p.step = stepWhere
		} else {
			p.step = stepStatementEnd
		}
	}
	return nil
}

func (p *parserItem) setUpdate(field string, value minisql.OptionalValue) {
	if p.Updates == nil {
		p.Updates = make(map[string]minisql.OptionalValue)
	}
	p.Fields = append(p.Fields, minisql.Field{Name: field})
	p.Updates[field] = value
}
