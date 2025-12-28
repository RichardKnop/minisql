package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errCreateIndexExpectedOpeningParens = fmt.Errorf("at CREATE INDEX: expected opening parens")
	errCreateIndexNoColumns             = fmt.Errorf("at CREATE INDEX: no columns specified")
)

func (p *parserItem) doParseCreateIndex() error {
	switch p.step {
	case stepCreateIndexIfNotExists:
		ifnotExists := p.peek()
		p.step = stepCreateIndexName
		if strings.ToUpper(ifnotExists) != "IF NOT EXISTS" {
			return nil
		}
		p.IfNotExists = true
		p.pop()
		p.step = stepCreateIndexName
	case stepCreateIndexName:
		indexName := p.peek()
		if !isIdentifier(indexName) {
			return fmt.Errorf("at CREATE INDEX: expected index name")
		}
		p.IndexName = indexName
		p.pop()
		p.step = stepCreateIndexOn
	case stepCreateIndexOn:
		onToken := p.peek()
		if strings.ToUpper(onToken) != "ON" {
			return fmt.Errorf("at CREATE INDEX: expected ON")
		}
		p.pop()
		p.step = stepCreateIndexOnTable
	case stepCreateIndexOnTable:
		tableName := p.peek()
		if !isIdentifier(tableName) {
			return fmt.Errorf("at CREATE INDEX: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepCreateIndexOpeningParens
	case stepCreateIndexOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return errCreateIndexExpectedOpeningParens
		}
		p.pop()
		p.step = stepCreateIndexColumn
	case stepCreateIndexColumn:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return errCreateIndexNoColumns
		}
		p.Columns = append(p.Columns, minisql.Column{
			Name: identifier,
		})
		p.pop()
		p.step = stepCreateIndexCommaOrClosingParens
	case stepCreateIndexCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return fmt.Errorf("at CREATE INDEX: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateIndexColumn
			return nil
		}
		p.step = stepStatementEnd
	}
	return nil
}

func (p *parserItem) doParseDropIndex() error {
	switch p.step {
	case stepDropIndexName:
		indexName := p.peek()
		if len(indexName) == 0 {
			return fmt.Errorf("at DROP INDEX: expected index name")
		}
		p.IndexName = indexName
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}
