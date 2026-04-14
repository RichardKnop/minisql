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

	// CASE expression
	if strings.ToUpper(token) == "CASE" {
		p.pop()
		return p.parseCaseExpr()
	}

	// INTERVAL literal: INTERVAL 'n unit [n unit ...]'
	if strings.ToUpper(token) == "INTERVAL" {
		p.pop()
		return p.parseIntervalLiteral()
	}

	// CAST expression: CAST(expr AS type)
	if strings.ToUpper(token) == "CAST" {
		p.pop()
		if p.peek() != "(" {
			return nil, fmt.Errorf("CAST: expected '('")
		}
		p.pop() // consume "("
		return p.parseCastExpr()
	}

	// NOW() is tokenised as a single reserved-word — no argument list to parse.
	if token == "NOW()" {
		p.pop()
		return &minisql.Expr{FuncName: "NOW"}, nil
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

// parseCaseExpr parses a CASE expression after the CASE keyword has been consumed.
// Supports both forms:
//
//	Searched: CASE WHEN cond THEN result [WHEN ...] [ELSE result] END
//	Simple:   CASE expr WHEN value THEN result [WHEN ...] [ELSE result] END
func (p *parserItem) parseCaseExpr() (*minisql.Expr, error) {
	var caseInput *minisql.Expr

	// If the next token is not WHEN, this is a simple CASE with an input expression.
	if strings.ToUpper(p.peek()) != "WHEN" {
		var err error
		caseInput, err = p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("CASE: expected expression after CASE: %w", err)
		}
	}

	clauses := make([]minisql.CaseWhen, 0)
	for strings.ToUpper(p.peek()) == "WHEN" {
		p.pop() // consume WHEN

		var clause minisql.CaseWhen
		if caseInput == nil {
			// Searched CASE: parse a boolean condition.
			cond, err := p.parseCondExpr()
			if err != nil {
				return nil, fmt.Errorf("CASE: expected condition after WHEN: %w", err)
			}
			clause.Cond = cond
		} else {
			// Simple CASE: parse a value to compare against the input.
			val, err := p.parseExpr()
			if err != nil {
				return nil, fmt.Errorf("CASE: expected value after WHEN: %w", err)
			}
			clause.When = val
		}

		if strings.ToUpper(p.peek()) != "THEN" {
			return nil, fmt.Errorf("CASE: expected THEN after WHEN clause")
		}
		p.pop() // consume THEN

		then, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("CASE: expected expression after THEN: %w", err)
		}
		clause.Then = then
		clauses = append(clauses, clause)
	}

	if len(clauses) == 0 {
		return nil, fmt.Errorf("CASE expression requires at least one WHEN clause")
	}

	var caseElse *minisql.Expr
	if strings.ToUpper(p.peek()) == "ELSE" {
		p.pop() // consume ELSE
		var err error
		caseElse, err = p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("CASE: expected expression after ELSE: %w", err)
		}
	}

	if strings.ToUpper(p.peek()) != "END" {
		return nil, fmt.Errorf("CASE: expected END to close expression")
	}
	p.pop() // consume END

	return &minisql.Expr{
		CaseInput:   caseInput,
		CaseClauses: clauses,
		CaseElse:    caseElse,
	}, nil
}

// parseCastExpr parses the body of a CAST expression after "CAST(" has been consumed.
// Grammar: expr AS type_name ")"
// Supported type names: BOOLEAN, INT4, INT8, REAL, DOUBLE, TEXT, VARCHAR[(n)], TIMESTAMP.
func (p *parserItem) parseCastExpr() (*minisql.Expr, error) {
	inner, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("CAST: %w", err)
	}

	if strings.ToUpper(p.peek()) != "AS" {
		return nil, fmt.Errorf("CAST: expected AS, got %q", p.peek())
	}
	p.pop() // consume "AS"

	typeToken := strings.ToUpper(p.peek())
	var targetKind minisql.ColumnKind
	switch typeToken {
	case "BOOLEAN":
		targetKind = minisql.Boolean
		p.pop()
	case "INT4":
		targetKind = minisql.Int4
		p.pop()
	case "INT8":
		targetKind = minisql.Int8
		p.pop()
	case "REAL":
		targetKind = minisql.Real
		p.pop()
	case "DOUBLE":
		targetKind = minisql.Double
		p.pop()
	case "TEXT":
		targetKind = minisql.Text
		p.pop()
	case "TIMESTAMP":
		targetKind = minisql.Timestamp
		p.pop()
	case "VARCHAR(":
		// CAST(x AS VARCHAR(n)) — consume type token, optional length, and inner ")"
		targetKind = minisql.Varchar
		p.pop() // consume "VARCHAR("
		_, hasLen := p.peekIntWithLength()
		if hasLen > 0 {
			p.pop() // consume length number
		}
		if p.peek() == ")" {
			p.pop() // consume inner ")"
		}
	default:
		return nil, fmt.Errorf("CAST: unknown target type %q (want BOOLEAN, INT4, INT8, REAL, DOUBLE, TEXT, VARCHAR, TIMESTAMP)", typeToken)
	}

	if p.peek() != ")" {
		return nil, fmt.Errorf("CAST: expected ')', got %q", p.peek())
	}
	p.pop() // consume outer ")"

	return &minisql.Expr{CastExpr: inner, CastTargetType: targetKind}, nil
}

// parseIntervalLiteral parses the body of an INTERVAL expression after the
// INTERVAL keyword has been consumed.  The next token must be a quoted string
// containing one or more "value unit" pairs, e.g. '3 days' or '1 year 2 months'.
func (p *parserItem) parseIntervalLiteral() (*minisql.Expr, error) {
	value, ln := p.peekValue()
	if ln == 0 {
		return nil, fmt.Errorf("INTERVAL: expected quoted string")
	}
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("INTERVAL: expected quoted string, got %T", value)
	}
	p.pop()
	iv, err := minisql.ParseIntervalString(s)
	if err != nil {
		return nil, fmt.Errorf("INTERVAL: %w", err)
	}
	return &minisql.Expr{Literal: iv}, nil
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
		"MOD",
		"DATE_TRUNC",
		"EXTRACT", "DATE_PART",
		"TO_TIMESTAMP":
		return true
	}
	return false
}
