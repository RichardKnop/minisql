package parser

func (p *parserItem) doParseDelete() error {
	switch p.step {
	case stepDeleteFromTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return p.errorf("at DELETE FROM: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
		return nil
	}
	return nil
}
