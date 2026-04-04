package parser

func (p *parserItem) doParseAnalyze() error {
	if p.step == stepAnalyze {
		name := p.peek()
		if name != "" && isIdentifier(name) {
			p.Target = name
			p.pop()
		}
		p.step = stepStatementEnd
		return nil
	}
	return nil
}
