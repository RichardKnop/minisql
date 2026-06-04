package parser

func (p *parserItem) doParseDelete() error {
	switch p.step {
	case stepDeleteFromTable:
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at DELETE FROM: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
	case stepTruncateTable:
		tableName := p.peek()
		if tableName == "" {
			return p.errorf("at TRUNCATE TABLE: expected table name")
		}
		p.TableName = tableName
		p.pop()
		// No WHERE clause for TRUNCATE — go straight to end.
		p.step = stepStatementEnd
	}
	return nil
}
