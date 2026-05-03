package parser

import (
	"fmt"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func (p *parserItem) parseExplain(analyze bool) error {
	p.Kind = minisql.Explain
	p.ExplainAnalyze = analyze
	p.pop()

	rest := &parserItem{
		sql:  p.sql[p.i:],
		step: stepBeginning,
	}
	statements, err := rest.doParse()
	if err != nil {
		return fmt.Errorf("EXPLAIN: %w", err)
	}
	if len(statements) != 1 {
		return p.errorf("at EXPLAIN: expected exactly one statement")
	}

	p.i += rest.i
	p.ExplainStatement = &statements[0]
	p.step = stepStatementEnd
	return nil
}
