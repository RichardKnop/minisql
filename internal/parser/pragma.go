package parser

import "strings"

func (p *parserItem) doParsePragma() error {
	if p.step != stepPragma {
		return nil
	}

	name := p.peek()
	if name == "" || !isIdentifier(name) {
		return p.wrapErr(errEmptyPragmaName)
	}

	p.PragmaName = strings.ToLower(name)
	p.pop()

	// Optional setter: PRAGMA name = value
	if p.peek() == "=" {
		p.pop() // consume "="
		value := p.peek()
		if value == "" {
			return p.errorf("at PRAGMA: expected value after '='")
		}
		p.PragmaValue = strings.ToLower(value)
		p.pop()
	}

	p.step = stepStatementEnd
	return nil
}
