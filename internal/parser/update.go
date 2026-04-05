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
			// Plain numeric literal — no expression overhead needed.
			if expr.Column == "" && expr.Left == nil {
				updateValue = minisql.OptionalValue{Value: expr.Literal, Valid: true}
			} else {
				// Column reference or binary expression — store as *Expr for
				// runtime evaluation against the current row.
				updateValue = minisql.OptionalValue{Value: expr, Valid: true}
			}
			p.setUpdate(p.nextUpdateField, updateValue)
			p.nextUpdateField = ""
		}
		maybeWhere := p.peek()
		if strings.ToUpper(maybeWhere) == "WHERE" {
			p.step = stepWhere
			return nil
		}
		p.step = stepUpdateComma
	case stepUpdateComma:
		commaOrEnd := p.peek()
		if commaOrEnd == ";" {
			p.step = stepStatementEnd
			return nil
		}
		if commaOrEnd != "," {
			return p.errorf("at UPDATE: expected ','")
		}
		p.pop()
		p.step = stepUpdateField
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
