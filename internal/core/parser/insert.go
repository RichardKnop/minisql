package parser

import (
	"fmt"
	"strings"
)

var (
	errNoRowsToInsert                = fmt.Errorf("at INSERT INTO: need at least one row to insert")
	errInsertFieldValueCountMismatch = fmt.Errorf("at INSERT INTO: value count doesn't match field count")
	errInsertNoFields                = fmt.Errorf("at INSERT INTO: expected at least one field to insert")
)

func (p *parser) doParseInsert() (bool, error) {
	switch p.step {
	case stepInsertTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at INSERT INTO: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepInsertFieldsOpeningParens
	case stepInsertFieldsOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return false, fmt.Errorf("at INSERT INTO: expected opening parens")
		}
		p.pop()
		p.step = stepInsertFields
	case stepInsertFields:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return false, errInsertNoFields
		}
		p.Fields = append(p.Fields, identifier)
		p.pop()
		p.step = stepInsertFieldsCommaOrClosingParens
	case stepInsertFieldsCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return false, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertFields
			return true, nil
		}
		p.step = stepInsertValuesRWord
	case stepInsertValuesRWord:
		valuesRWord := p.peek()
		if strings.ToUpper(valuesRWord) != "VALUES" {
			return false, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	case stepInsertValuesOpeningParens:
		openingParens := p.peek()
		if openingParens != "(" {
			return false, fmt.Errorf("at INSERT INTO: expected opening parens")
		}
		p.Inserts = append(p.Inserts, []any{})
		p.pop()
		p.step = stepInsertValues
	case stepInsertValues:
		intValue, ln := p.peepIntWithLength()
		if ln > 0 {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], intValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return true, nil
		}
		quotedValue, ln := p.peekQuotedStringWithLength()
		if ln > 0 {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], quotedValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return true, nil
		}
		return false, fmt.Errorf("at INSERT INTO: expected quoted value or int value")
	case stepInsertValuesCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return false, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertValues
			return true, nil
		}
		currentInsertRow := p.Inserts[len(p.Inserts)-1]
		if len(currentInsertRow) < len(p.Fields) {
			return false, errInsertFieldValueCountMismatch
		}
		p.step = stepInsertValuesCommaBeforeOpeningParens
	case stepInsertValuesCommaBeforeOpeningParens:
		commaRWord := p.peek()
		if strings.ToUpper(commaRWord) != "," {
			return false, fmt.Errorf("at INSERT INTO: expected comma")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	}
	return false, nil
}
