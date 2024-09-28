package parser

import (
	"fmt"
	"strings"
)

var (
	errSelectWithoutFields = fmt.Errorf("at SELECT: expected field to SELECT")
)

func (p *parser) doParseSelect() (bool, error) {
	switch p.step {
	case stepSelectField:
		identifier := p.peek()
		if !isIdentifierOrAsterisk(identifier) {
			return false, errSelectWithoutFields
		}
		p.Fields = append(p.Fields, identifier)
		p.pop()
		maybeFrom := p.peek()
		if strings.ToUpper(maybeFrom) == "AS" {
			p.pop()
			alias := p.peek()
			if !isIdentifier(alias) {
				return false, fmt.Errorf("at SELECT: expected field alias for \"" + identifier + " as\" to SELECT")
			}
			if p.Aliases == nil {
				p.Aliases = make(map[string]string)
			}
			p.Aliases[identifier] = alias
			p.pop()
			maybeFrom = p.peek()
		}
		if strings.ToUpper(maybeFrom) == "FROM" {
			p.step = stepSelectFrom
			return true, nil
		}
		p.step = stepSelectComma
	case stepSelectComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			return false, fmt.Errorf("at SELECT: expected comma or FROM")
		}
		p.pop()
		p.step = stepSelectField
	case stepSelectFrom:
		fromRWord := p.peek()
		if strings.ToUpper(fromRWord) != "FROM" {
			return false, fmt.Errorf("at SELECT: expected FROM")
		}
		p.pop()
		p.step = stepSelectFromTable
	case stepSelectFromTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at SELECT: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
	}
	return false, nil
}
