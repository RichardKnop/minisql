package parser

import (
	"fmt"
)

func (p *parser) doParseDelete() (bool, error) {
	switch p.step {
	case stepDeleteFromTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at DELETE FROM: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepWhere
	}
	return false, nil
}
