package parser

func (p *parserItem) doParseAnalyze() error {
	switch p.step {
	case stepAnalyze:
		name := p.peek()
		if len(name) > 0 && isIdentifier(name) {
			p.Target = name
			p.pop()
		}
		p.step = stepStatementEnd
		return nil
	}
	return nil
}
