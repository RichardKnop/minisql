package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errSelectWithoutFields        = fmt.Errorf("at SELECT: expected field to SELECT")
	errSelectExpectedTableName    = fmt.Errorf("at SELECT: expected table name identifier")
	errCannotCombineAsterisk      = fmt.Errorf(`at SELECT: cannot combine "*" with other fields`)
	errCannotCombineCountAsterisk = fmt.Errorf(`at SELECT: cannot combine "COUNT(*)" with other fields`)
	errExpectedFrom               = fmt.Errorf("at SELECT: expected FROM")
)

/*
SELECT select_list

	    FROM table_expression
		[ WHERE ... ]
	    [ ORDER BY ... ]
	    [ LIMIT { count | ALL } ]
	    [ OFFSET start ]
*/
func (p *parserItem) doParseSelect() error {
	switch p.step {
	case stepSelectField:
		identifier := p.peek()
		if !isIdentifier(identifier) && identifier != "*" && strings.ToUpper(identifier) != "COUNT(*)" {
			return errSelectWithoutFields
		}

		p.Fields = append(p.Fields, minisql.Field{Name: identifier})
		p.pop()
		maybeFrom := strings.ToUpper(p.peek())

		// Handle * for selecting all rows
		if identifier == "*" {
			if len(p.Fields) > 1 {
				return errCannotCombineAsterisk
			}
			if maybeFrom != "FROM" {
				return errExpectedFrom
			}
			p.step = stepSelectFrom
			return nil
		}

		// Handle COUNT(*) special case
		if identifier == "COUNT(*)" {
			if len(p.Fields) > 1 {
				return errCannotCombineCountAsterisk
			}
			if maybeFrom != "FROM" {
				return errExpectedFrom
			}
			p.step = stepSelectFrom
			return nil
		}

		// Otherwise we expect an AS alias, FROM or comma and next field
		switch maybeFrom {
		case "AS":
			p.pop()
			alias := p.peek()
			if !isIdentifier(alias) {
				return fmt.Errorf(`at SELECT: expected field alias for "identifier as"`)
			}
			if p.Aliases == nil {
				p.Aliases = make(map[string]string)
			}
			p.Aliases[identifier] = alias
			p.pop()
			maybeFrom = p.peek()
		case "FROM":
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
		from := strings.ToUpper(p.peek())
		if from != "FROM" {
			return errExpectedFrom
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
		p.step = stepSelectOrderBy
	case stepSelectOrderBy:
		offsetRWord := p.peek()
		if strings.ToUpper(offsetRWord) != "ORDER BY" {
			p.step = stepSelectLimit
			return nil
		}
		p.pop()
		p.step = stepSelectOrderByField
	case stepSelectOrderByField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			if len(p.OrderBy) == 0 {
				return fmt.Errorf(`at ORDER BY: expected identifier`)
			}
			p.step = stepSelectLimit
			return nil
		}
		if identifier == "*" {
			return fmt.Errorf(`at ORDER BY: cannot order by "*"`)
		}
		p.pop()
		// Start with default direction as ASC
		theDirection := minisql.Asc
		if direction := strings.ToUpper(p.peek()); direction == "ASC" || direction == "DESC" {
			if direction == "DESC" {
				theDirection = minisql.Desc
			}
			p.pop()
		}
		p.OrderBy = append(p.OrderBy, minisql.OrderBy{
			Field:     minisql.Field{Name: identifier},
			Direction: theDirection,
		})
		p.step = stepSelectOrderByComma
	case stepSelectOrderByComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			p.step = stepSelectLimit
			return nil
		}
		p.pop()
		p.step = stepSelectOrderByField
	case stepSelectLimit:
		limitRWord := p.peek()
		if strings.ToUpper(limitRWord) != "LIMIT" {
			p.step = stepSelectOffset
			return nil
		}
		p.pop()
		limitValue, n := p.peekIntWithLength()
		if n == 0 {
			return fmt.Errorf("at SELECT: expected integer value for LIMIT")
		}
		p.Limit = minisql.OptionalValue{Value: limitValue, Valid: true}
		p.pop()
		p.step = stepSelectOffset
	case stepSelectOffset:
		offsetRWord := p.peek()
		if strings.ToUpper(offsetRWord) != "OFFSET" {
			if !p.Offset.Valid {
				p.step = stepWhere
				return nil
			}
			p.step = stepStatementEnd
			return nil
		}
		p.pop()
		offsetValue, n := p.peekIntWithLength()
		if n == 0 {
			return fmt.Errorf("at SELECT: expected integer value for OFFSET")
		}
		p.Offset = minisql.OptionalValue{Value: offsetValue, Valid: true}
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}
