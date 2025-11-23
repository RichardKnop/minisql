package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errUpdateExpectetSet              = fmt.Errorf("at UPDATE: expected 'SET'")
	errUpdateExpectedEquals           = fmt.Errorf("at UPDATE: expected '='")
	errUpdateExpectedQuotedValueOrInt = fmt.Errorf("at UPDATE: expected quoted value or int")
	errNoFieldsToUpdate               = fmt.Errorf("at UPDATE: expected at least one field to update")
)

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
		if strings.ToUpper(setRWord) != "SET" {
			return false, errUpdateExpectetSet
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
		nullRWord := p.peek()
		if strings.ToUpper(nullRWord) == "NULL" {
			p.setUpdate(p.nextUpdateField, minisql.OptionalValue{Valid: false})
			p.nextUpdateField = ""
			p.pop()
		} else {
			value, ln := p.peekNumberOrQuotedStringWithLength()
			if ln == 0 {
				return false, errUpdateExpectedQuotedValueOrInt
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

func (p *parser) setUpdate(field string, value minisql.OptionalValue) {
	if p.Updates == nil {
		p.Updates = make(map[string]minisql.OptionalValue)
	}
	p.Updates[field] = value
}
