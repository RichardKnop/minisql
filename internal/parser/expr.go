package parser

import (
	"fmt"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// parseExpr parses an arithmetic expression with correct operator precedence:
//
//	expr   := term   (('+' | '-') term)*
//	term   := factor (('*' | '/') factor)*
//	factor := '-' factor | '(' expr ')' | column_ref | numeric_literal
func (p *parserItem) parseExpr() (*minisql.Expr, error) {
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}
	for {
		op := p.peek()
		if op != "+" && op != "-" {
			break
		}
		p.pop()
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		arithOp := minisql.ArithAdd
		if op == "-" {
			arithOp = minisql.ArithSub
		}
		left = &minisql.Expr{Left: left, Right: right, Op: arithOp}
	}
	return left, nil
}

func (p *parserItem) parseTerm() (*minisql.Expr, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for {
		op := p.peek()
		if op != "*" && op != "/" {
			break
		}
		p.pop()
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		arithOp := minisql.ArithMul
		if op == "/" {
			arithOp = minisql.ArithDiv
		}
		left = &minisql.Expr{Left: left, Right: right, Op: arithOp}
	}
	return left, nil
}

func (p *parserItem) parseFactor() (*minisql.Expr, error) {
	token := p.peek()

	// Unary minus: wrap as (0 - inner)
	if token == "-" {
		p.pop()
		inner, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		return &minisql.Expr{
			Left:  &minisql.Expr{Literal: int64(0)},
			Right: inner,
			Op:    minisql.ArithSub,
		}, nil
	}

	// Parenthesised sub-expression
	if token == "(" {
		p.pop()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.peek() != ")" {
			return nil, fmt.Errorf("expected ')' in arithmetic expression")
		}
		p.pop()
		return expr, nil
	}

	// Numeric literal
	value, ln := p.peekValue()
	if ln > 0 {
		switch v := value.(type) {
		case int64:
			p.pop()
			return &minisql.Expr{Literal: v}, nil
		case float64:
			p.pop()
			return &minisql.Expr{Literal: v}, nil
		}
	}

	// Column reference (possibly alias-prefixed, e.g. "u.price")
	if isIdentifier(token) {
		p.pop()
		return &minisql.Expr{Column: token}, nil
	}

	return nil, fmt.Errorf("unexpected token %q in arithmetic expression", token)
}
