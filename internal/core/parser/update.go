package parser

import (
	"fmt"
	"strings"
)

var (
	errUpdateExpectedEquals           = fmt.Errorf("at UPDATE: expected '='")
	errUpdateExpectedQuotedValueOrInt = fmt.Errorf("at UPDATE: expected quoted value or int")
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
			return false, fmt.Errorf("at UPDATE: expected 'SET'")
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
		value, ln := p.peekIntOrQuotedStringWithLength()
		if ln == 0 {
			return false, errUpdateExpectedQuotedValueOrInt
		}
		p.setUpdate(p.nextUpdateField, value)
		p.nextUpdateField = ""
		p.pop()
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

func (p *parser) setUpdate(field string, value any) {
	if p.Updates == nil {
		p.Updates = make(map[string]any)
	}
	p.Updates[field] = value
}
