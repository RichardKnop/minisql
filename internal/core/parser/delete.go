package parser

import (
	"fmt"
)

func (p *parser) doParseDelete() error {
	switch p.step {
	case stepDeleteFromTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return fmt.Errorf("at DELETE FROM: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
		return nil
	}
	return nil
}
