package parser

import (
	"fmt"
	"strings"

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

	// NULL literal
	if token == "NULL" {
		p.pop()
		return &minisql.Expr{IsNull: true}, nil
	}

	// Scalar literals: integer, float, string, boolean
	value, ln := p.peekValue()
	if ln > 0 {
		switch v := value.(type) {
		case int64:
			p.pop()
			return &minisql.Expr{Literal: v}, nil
		case float64:
			p.pop()
			return &minisql.Expr{Literal: v}, nil
		case string:
			p.pop()
			return &minisql.Expr{Literal: minisql.NewTextPointer([]byte(v))}, nil
		case bool:
			p.pop()
			return &minisql.Expr{Literal: v}, nil
		}
	}

	// Function call or column reference
	if isIdentifier(token) {
		upperToken := strings.ToUpper(token)
		if isBuiltinFunction(upperToken) {
			return p.parseFuncCall(upperToken)
		}
		p.pop()
		return &minisql.Expr{Column: token}, nil
	}

	return nil, fmt.Errorf("unexpected token %q in arithmetic expression", token)
}

// parseFuncCall parses FUNCNAME(arg, arg, ...) after the caller has confirmed
// the token is a known built-in function name (already upper-cased).
func (p *parserItem) parseFuncCall(funcName string) (*minisql.Expr, error) {
	p.pop() // consume function name
	if p.peek() != "(" {
		return nil, fmt.Errorf("expected '(' after %s", funcName)
	}
	p.pop() // consume "("

	var args []*minisql.Expr
	for {
		if p.peek() == ")" {
			p.pop()
			break
		}
		if len(args) > 0 {
			if p.peek() != "," {
				return nil, fmt.Errorf("expected ',' or ')' in %s() arguments", funcName)
			}
			p.pop() // consume ","
		}
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return &minisql.Expr{FuncName: funcName, Args: args}, nil
}

// isBuiltinFunction reports whether name (upper-cased) is a recognised scalar
// function that can appear inside an arithmetic expression.
func isBuiltinFunction(name string) bool {
	switch name {
	case "COALESCE", "NULLIF",
		"UPPER", "LOWER",
		"TRIM", "LTRIM", "RTRIM",
		"LENGTH",
		"SUBSTR",
		"REPLACE",
		"CONCAT",
		"ABS",
		"FLOOR", "CEIL",
		"ROUND",
		"MOD":
		return true
	}
	return false
}
