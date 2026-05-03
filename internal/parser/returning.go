package parser

import (
	"github.com/RichardKnop/minisql/internal/minisql"
)

// doParseReturning handles stepReturningField and stepReturningComma.
// The "RETURNING" keyword itself is consumed at each transition site before
// setting stepReturningField, so this function only sees field names.
func (p *parserItem) doParseReturning() error {
	switch p.step {
	case stepReturningField:
		identifier := p.peek()
		if !isIdentifier(identifier) && identifier != "*" {
			return p.errorf("at RETURNING: expected column name or *")
		}
		p.ReturningFields = append(p.ReturningFields, minisql.Field{Name: identifier})
		p.pop()
		p.step = stepReturningComma
	case stepReturningComma:
		if p.peek() == "," {
			p.pop()
			p.step = stepReturningField
		} else {
			p.step = stepStatementEnd
		}
	}
	return nil
}
