package parser

import (
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func (p *parserItem) doParseWithCTEName() error {
	name, _ := p.peekIdentifierWithLength()
	if !isIdentifier(name) {
		return p.errorf("at WITH: expected CTE name after WITH or ','")
	}
	p.cteNameInProgress = name
	p.pop()
	p.step = stepWithCTEAs
	return nil
}

func (p *parserItem) doParseWithCTEAs() error {
	if strings.ToUpper(p.peek()) != "AS" {
		return p.errorf("at WITH: expected AS after CTE name %q", p.cteNameInProgress)
	}
	p.pop() // consume AS
	if p.peek() != "(" {
		return p.errorf("at WITH: expected '(' after AS")
	}
	p.pop() // consume "("
	if strings.ToUpper(p.peek()) != "SELECT" {
		return p.errorf("at WITH: expected SELECT inside CTE parentheses")
	}
	subStmt, err := p.parseSubquery()
	if err != nil {
		return err
	}
	p.CTEs = append(p.CTEs, minisql.CTE{Name: p.cteNameInProgress, Body: subStmt})
	p.cteNameInProgress = ""
	p.step = stepWithCTECommaOrSelect
	return nil
}

func (p *parserItem) doParseWithCTECommaOrSelect() error {
	switch strings.ToUpper(p.peek()) {
	case ",":
		p.pop()
		p.step = stepWithCTEName
	case "SELECT":
		p.Kind = minisql.Select
		p.pop()
		p.step = stepSelectField
	default:
		return p.errorf("at WITH: expected ',' or SELECT after CTE definition, got %q", p.peek())
	}
	return nil
}
