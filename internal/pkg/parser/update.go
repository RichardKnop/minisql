package parser

import (
	"fmt"
	"strings"
)

var (
	errUpdateExpectedEquals      = fmt.Errorf("at UPDATE: expected '='")
	errUpdateExpectedQuotedValue = fmt.Errorf("at UPDATE: expected quoted value")
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
		if setRWord != "SET" {
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
		quotedValue, ln := p.peekQuotedStringWithLength()
		if ln == 0 {
			return false, errUpdateExpectedQuotedValue
		}
		p.setUpdate(p.nextUpdateField, quotedValue)
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

func (p *parser) setUpdate(field, value string) {
	if p.Updates == nil {
		p.Updates = make(map[string]string)
	}
	p.Updates[field] = value
}
