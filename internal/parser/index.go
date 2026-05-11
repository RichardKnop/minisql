package parser

import (
	"errors"
	"fmt"
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
		if p.peek() == "" {
			return p.wrapErr(errCreateIndexNoColumns)
		}
		startPos := p.i
		expr, err := p.parseExpr()
		if err != nil {
			return fmt.Errorf("at CREATE INDEX: failed to parse expression: %w", err)
		}
		// Simple column reference — not an expression index.
		if expr.Column != "" {
			p.Columns = append(p.Columns, minisql.Column{Name: expr.Column})
			p.step = stepCreateIndexCommaOrClosingParens
			return nil
		}
		// Complex expression (function call, arithmetic, JSON path, …).
		p.IndexExpression = expr
		p.IndexExpressionSQL = strings.TrimSpace(p.sql[startPos:p.i])
		p.Columns = append(p.Columns, minisql.Column{Name: "__expr__"})
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
		p.step = stepCreateIndexWithOrWhereOrEnd
	case stepCreateIndexWithOrWhereOrEnd:
		token := strings.ToUpper(p.peek())
		if token != "WITH" {
			p.step = stepCreateIndexWhereOrEnd
			return nil
		}
		if err := p.parseCreateIndexWithOptions(); err != nil {
			return err
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

func (p *parserItem) parseCreateIndexWithOptions() error {
	p.pop() // consume WITH
	if p.peek() != "(" {
		return p.errorf("at CREATE INDEX: expected opening parens after WITH")
	}
	p.pop()

	for {
		optionName := strings.ToUpper(p.peek())
		if optionName == "" || optionName == ")" {
			return p.errorf("at CREATE INDEX: expected WITH option name")
		}
		p.pop()

		if p.peek() != "=" {
			return p.errorf("at CREATE INDEX: expected '=' after WITH option name")
		}
		p.pop()

		optionValue := p.peek()
		if optionValue == "" {
			return p.errorf("at CREATE INDEX: expected WITH option value")
		}
		p.pop()

		switch optionName {
		case "TOKENIZER":
			p.IndexTokenizer = strings.ToLower(optionValue)
		default:
			return p.errorf("at CREATE INDEX: unknown WITH option %q", optionName)
		}

		switch p.peek() {
		case ",":
			p.pop()
			continue
		case ")":
			p.pop()
			return nil
		default:
			return p.errorf("at CREATE INDEX: expected comma or closing parens after WITH option")
		}
	}
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
