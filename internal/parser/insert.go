package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errNoRowsToInsert                = fmt.Errorf("at INSERT INTO: need at least one row to insert")
	errInsertFieldValueCountMismatch = fmt.Errorf("at INSERT INTO: value count doesn't match field count")
	errInsertNoFields                = fmt.Errorf("at INSERT INTO: expected at least one field to insert")
)

func (p *parserItem) doParseInsert() error {
	switch p.step {
	case stepInsertTable:
		tableName := p.peek()
		if len(tableName) == 0 {
			return p.errorf("at INSERT INTO: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepInsertFieldsOpeningParens
	case stepInsertFieldsOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return p.errorf("at INSERT INTO: expected opening parens")
		}
		p.pop()
		p.step = stepInsertFields
	case stepInsertFields:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.wrapErr(errInsertNoFields)
		}
		p.Fields = append(p.Fields, minisql.Field{Name: identifier})
		p.pop()
		p.step = stepInsertFieldsCommaOrClosingParens
	case stepInsertFieldsCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at INSERT INTO: expected comma or closing parens")
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
			return p.errorf("at INSERT INTO: expected VALUES")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	case stepInsertValuesOpeningParens:
		openingParens := p.peek()
		if openingParens != "(" {
			return p.errorf("at INSERT INTO: expected opening parens")
		}
		p.Inserts = append(p.Inserts, []minisql.OptionalValue{})
		p.pop()
		p.step = stepInsertValues
	case stepInsertValues:
		specialValue := strings.ToUpper(p.peek())
		if specialValue == "?" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Value: minisql.Placeholder{}, Valid: true})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		if specialValue == "NULL" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Valid: false})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		if specialValue == "NOW()" {
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], minisql.OptionalValue{Value: minisql.FunctionNow, Valid: true})
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		value, ln := p.peekValue()
		if ln > 0 {
			var insertValue minisql.OptionalValue
			if strValue, ok := value.(string); ok {
				insertValue = minisql.OptionalValue{Value: minisql.NewTextPointer([]byte(strValue)), Valid: true}
			} else {
				insertValue = minisql.OptionalValue{Value: value, Valid: true}
			}
			p.Inserts[len(p.Inserts)-1] = append(p.Inserts[len(p.Inserts)-1], insertValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
			return nil
		}
		return p.errorf("at INSERT INTO: expected value")
	case stepInsertValuesCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at INSERT INTO: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepInsertValues
			return nil
		}
		currentInsertRow := p.Inserts[len(p.Inserts)-1]
		if len(currentInsertRow) < len(p.Fields) {
			return p.wrapErr(errInsertFieldValueCountMismatch)
		}
		p.step = stepInsertValuesCommaBeforeOpeningParens
	case stepInsertValuesCommaBeforeOpeningParens:
		commaOrEnd := p.peek()
		if commaOrEnd == ";" {
			p.step = stepStatementEnd
			return nil
		}
		if commaOrEnd != "," {
			return p.errorf("at INSERT INTO: expected comma")
		}
		p.pop()
		p.step = stepInsertValuesOpeningParens
	}
	return nil
}
