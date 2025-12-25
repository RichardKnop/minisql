package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errUpdateExpectetSet              = fmt.Errorf("at UPDATE: expected 'SET'")
	errUpdateExpectedEquals           = fmt.Errorf("at UPDATE: expected '='")
	errUpdateExpectedQuotedValueOrInt = fmt.Errorf("at UPDATE: expected quoted value or int")
	errNoFieldsToUpdate               = fmt.Errorf("at UPDATE: expected at least one field to update")
)

func (p *parser) doParseUpdate() error {
	switch p.step {
	case stepUpdateTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return fmt.Errorf("at UPDATE: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepUpdateSet
	case stepUpdateSet:
		setRWord := p.peek()
		if strings.ToUpper(setRWord) != "SET" {
			return errUpdateExpectetSet
		}
		p.pop()
		p.step = stepUpdateField
	case stepUpdateField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return fmt.Errorf("at UPDATE: expected at least one field to update")
		}
		p.nextUpdateField = identifier
		p.pop()
		p.step = stepUpdateEquals
	case stepUpdateEquals:
		equalsRWord := p.peek()
		if equalsRWord != "=" {
			return errUpdateExpectedEquals
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
		default:
			value, ln := p.peekValue()
			if ln == 0 {
				return errUpdateExpectedQuotedValueOrInt
			}
			var updateValue minisql.OptionalValue
			if strValue, ok := value.(string); ok {
				updateValue = minisql.OptionalValue{Value: minisql.NewTextPointer([]byte(strValue)), Valid: true}
			} else {
				updateValue = minisql.OptionalValue{Value: value, Valid: true}
			}
			p.setUpdate(p.nextUpdateField, updateValue)
			p.nextUpdateField = ""
			p.pop()
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
			return fmt.Errorf("at UPDATE: expected ','")
		}
		p.pop()
		p.step = stepUpdateField
	}
	return nil
}

func (p *parser) setUpdate(field string, value minisql.OptionalValue) {
	if p.Updates == nil {
		p.Updates = make(map[string]minisql.OptionalValue)
	}
	p.Updates[field] = value
}
