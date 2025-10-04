package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errNoRowsToInsert                = fmt.Errorf("at INSERT INTO: need at least one row to insert")
	errInsertFieldValueCountMismatch = fmt.Errorf("at INSERT INTO: value count doesn't match field count")
	errInsertNoFields                = fmt.Errorf("at INSERT INTO: expected at least one field to insert")
)

func (p *parser) doParseInsert() error {
	switch p.step {
	case stepInsertTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return fmt.Errorf("at INSERT INTO: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepInsertFieldsOpeningParens
	case stepInsertFieldsOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return fmt.Errorf("at INSERT INTO: expected opening parens")
		}
		p.pop()
		p.step = stepInsertFields
	case stepInsertFields:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return errInsertNoFields
		}
		p.Fields = append(p.Fields, identifier)
		p.pop()
		p.step = stepInsertFieldsCommaOrClosingParens
	case stepInsertFieldsCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return fmt.Errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertFields
			return nil
		}
		p.step = stepInsertValuesRWord
	case stepInsertValuesRWord:
		valuesRWord := p.peek()
		if strings.ToUpper(valuesRWord) != "VALUES" {
			return fmt.Errorf("at INSERT INTO: expected 'VALUES'")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	case stepInsertValuesOpeningParens:
		openingParens := p.peek()
		if openingParens != "(" {
			return fmt.Errorf("at INSERT INTO: expected opening parens")
		}
		p.Inserts = append(p.Inserts, []minisql.OptionalValue{})
		p.pop()
		p.step = stepInsertValues
	case stepInsertValues:
		nullRWord := p.peek()
		if strings.ToUpper(nullRWord) == "NULL" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Valid: false})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		value, ln := p.peekNumberOrQuotedStringWithLength()
		if ln > 0 {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Value: value, Valid: true})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		return fmt.Errorf("at INSERT INTO: expected quoted value or int value")
	case stepInsertValuesCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return fmt.Errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertValues
			return nil
		}
		currentInsertRow := p.Inserts[len(p.Inserts)-1]
		if len(currentInsertRow) < len(p.Fields) {
			return errInsertFieldValueCountMismatch
		}
		p.step = stepInsertValuesCommaBeforeOpeningParens
	case stepInsertValuesCommaBeforeOpeningParens:
		commaRWord := p.peek()
		if strings.ToUpper(commaRWord) != "," {
			return fmt.Errorf("at INSERT INTO: expected comma")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	}
	return nil
}
