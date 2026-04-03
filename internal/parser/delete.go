package parser

func (p *parserItem) doParseDelete() error {
	if p.step == stepDeleteFromTable {
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at DELETE FROM: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
		return nil
	}
	return nil
}
