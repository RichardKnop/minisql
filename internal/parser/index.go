package parser

import (
	"errors"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errCreateIndexExpectedOpeningParens = errors.New("at CREATE INDEX: expected opening parens")
	errCreateIndexNoColumns             = errors.New("at CREATE INDEX: no columns specified")
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
			return p.errorf("at CREATE INDEX: expected index name")
		}
		p.IndexName = indexName
		p.pop()
		p.step = stepCreateIndexOn
	case stepCreateIndexOn:
		onToken := p.peek()
		if strings.ToUpper(onToken) != "ON" {
			return p.errorf("at CREATE INDEX: expected ON")
		}
		p.pop()
		p.step = stepCreateIndexOnTable
	case stepCreateIndexOnTable:
		tableName := p.peek()
		if !isIdentifier(tableName) {
			return p.errorf("at CREATE INDEX: expected table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepCreateIndexOpeningParens
	case stepCreateIndexOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return p.wrapErr(errCreateIndexExpectedOpeningParens)
		}
		p.pop()
		p.step = stepCreateIndexColumn
	case stepCreateIndexColumn:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.wrapErr(errCreateIndexNoColumns)
		}
		p.Columns = append(p.Columns, minisql.Column{
			Name: identifier,
		})
		p.pop()
		p.step = stepCreateIndexCommaOrClosingParens
	case stepCreateIndexCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return p.errorf("at CREATE INDEX: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateIndexColumn
			return nil
		}
		p.step = stepCreateIndexWhereOrEnd
	case stepCreateIndexWhereOrEnd:
		token := strings.ToUpper(p.peek())
		if token != "WHERE" {
			p.step = stepStatementEnd
			return nil
		}
		p.pop() // consume "WHERE"
		startPos := p.i
		node, err := p.parseCondExpr()
		if err != nil {
			return err
		}
		p.IndexWhereClause = strings.TrimSpace(p.sql[startPos:p.i])
		p.Conditions = node.ToDNF()
		p.step = stepStatementEnd
	}
	return nil
}

func (p *parserItem) doParseDropIndex() error {
	if p.step == stepDropIndexName {
		indexName := p.peek()
		if indexName == "" {
			return p.errorf("at DROP INDEX: expected index name")
		}
		p.IndexName = indexName
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}
