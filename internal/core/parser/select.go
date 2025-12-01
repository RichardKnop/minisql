package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errSelectWithoutFields     = fmt.Errorf("at SELECT: expected field to SELECT")
	errSelectExpectedTableName = fmt.Errorf("at SELECT: expected table name identifier")
)

func (p *parser) doParseSelect() error {
	switch p.step {
	case stepSelectField:
		identifier := p.peek()
		if !isIdentifierOrAsterisk(identifier) {
			return errSelectWithoutFields
		}
		p.Fields = append(p.Fields, minisql.Field{Name: identifier})
		p.pop()
		maybeFrom := p.peek()
		if strings.ToUpper(maybeFrom) == "AS" {
			p.pop()
			alias := p.peek()
			if !isIdentifier(alias) {
				return fmt.Errorf("at SELECT: expected field alias for \"" + identifier + " as\" to SELECT")
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
			return nil
		}
		p.step = stepSelectComma
	case stepSelectComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			return fmt.Errorf("at SELECT: expected comma or FROM")
		}
		p.pop()
		p.step = stepSelectField
	case stepSelectFrom:
		fromRWord := p.peek()
		if strings.ToUpper(fromRWord) != "FROM" {
			return fmt.Errorf("at SELECT: expected FROM")
		}
		p.pop()
		p.step = stepSelectFromTable
	case stepSelectFromTable:
		tableName, _ := p.peekIdentifierWithLength()
		if !isIdentifier(tableName) {
			return errSelectExpectedTableName
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
	}
	return nil
}
