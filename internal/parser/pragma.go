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
	p.step = stepStatementEnd
	return nil
}
