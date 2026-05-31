package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// parseExpr parses an arithmetic expression with correct operator precedence:
//
//	expr    := term    (('+' | '-') term)*
//	term    := jsonExpr (('*' | '/') jsonExpr)*
//	jsonExpr := factor (('->' | '->>') factor)*
//	factor  := '-' factor | '(' expr ')' | column_ref | numeric_literal
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
	left, err := p.parseJSONExpr()
	if err != nil {
		return nil, err
	}
	for {
		op := p.peek()
		if op != "*" && op != "/" {
			break
		}
		p.pop()
		right, err := p.parseJSONExpr()
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

// parseJSONExpr parses JSON path extraction operators -> and ->>.
// Binds tighter than + / - but looser than unary minus.
//
//	jsonExpr := factor ('->' factor | '->>' factor)*
func (p *parserItem) parseJSONExpr() (*minisql.Expr, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for {
		op := p.peek()
		if op != "->" && op != "->>" {
			break
		}
		p.pop()
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		arithOp := minisql.JSONArrow
		if op == "->>" {
			arithOp = minisql.JSONArrowArrow
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

	// Bind-parameter placeholder
	if token == "?" {
		p.pop()
		return &minisql.Expr{Literal: minisql.Placeholder{}}, nil
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

	// EXTRACT/DATE_PART support both:
	//   standard:  EXTRACT('year', col)
	//   SQL-std:   EXTRACT(year FROM col)
	// Detect the keyword-FROM form when the first token is an unquoted identifier.
	// We check p.sql[p.i] != '\'' to distinguish unquoted identifiers from quoted strings —
	// peek() strips quotes so 'year' and year both peek as "year".
	if funcName == "EXTRACT" || funcName == "DATE_PART" {
		first := p.peek()
		if first != ")" && p.i < len(p.sql) && p.sql[p.i] != '\'' && isIdentifier(first) {
			field := first
			p.pop() // consume field keyword (e.g. "year")
			if strings.ToUpper(p.peek()) == "FROM" {
				p.pop() // consume "FROM"
				tsExpr, err := p.parseExpr()
				if err != nil {
					return nil, fmt.Errorf("%s: %w", funcName, err)
				}
				if p.peek() != ")" {
					return nil, fmt.Errorf("%s: expected ')'", funcName)
				}
				p.pop() // consume ")"
				return &minisql.Expr{
					FuncName: funcName,
					Args: []*minisql.Expr{
						{Literal: minisql.NewTextPointer([]byte(strings.ToLower(field)))},
						tsExpr,
					},
				}, nil
			}
			// Not FROM — the identifier is a column reference; fall through with it
			// pre-parsed as the first arg.
			colExpr := &minisql.Expr{Column: field}
			var args []*minisql.Expr
			args = append(args, colExpr)
			for {
				if p.peek() == ")" {
					p.pop()
					break
				}
				if p.peek() != "," {
					return nil, fmt.Errorf("expected ',' or ')' in %s() arguments", funcName)
				}
				p.pop()
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
			}
			return &minisql.Expr{FuncName: funcName, Args: args}, nil
		}
	}

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

	// Detect OVER (...) — turns this call into a window function expression.
	if strings.ToUpper(p.peek()) == "OVER" {
		p.pop() // consume "OVER"
		spec, err := p.parseWindowSpec()
		if err != nil {
			return nil, fmt.Errorf("%s OVER: %w", funcName, err)
		}
		kind := windowFuncKind(funcName)
		if kind == 0 {
			return nil, fmt.Errorf("unknown window function %q", funcName)
		}
		wf := &minisql.WindowFunc{Kind: kind, Spec: spec}
		if len(args) > 0 {
			wf.Arg = args[0]
		}
		if len(args) > 1 {
			wf.Arg2 = args[1]
		}
		return &minisql.Expr{WindowFunc: wf}, nil
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
	case "JSON":
		targetKind = minisql.JSON
		p.pop()
	case "UUID":
		targetKind = minisql.UUID
		p.pop()
	default:
		return nil, fmt.Errorf("CAST: unknown target type %q (want BOOLEAN, INT4, INT8, REAL, DOUBLE, TEXT, VARCHAR, TIMESTAMP, JSON, UUID)", typeToken)
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
		"TO_TIMESTAMP",
		"JSON_EXTRACT", "JSON_VALID", "JSON_TYPE", "JSON_ARRAY_LENGTH", "JSON_CONTAINS",
		"MATCH", "TS_RANK",
		"VEC_L2", "VEC_COSINE":
		return true
	}
	return isWindowFunction(name)
}

// isWindowFunction reports whether name (upper-cased) is a window function.
func isWindowFunction(name string) bool {
	switch name {
	case "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
		"LAG", "LEAD",
		"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
		return true
	}
	return false
}

// windowFuncKind maps an upper-cased function name to its WindowFuncKind.
// Returns 0 if the name is not a dedicated window function (aggregate-as-window
// is handled separately via the token in the caller).
func windowFuncKind(name string) minisql.WindowFuncKind {
	switch name {
	case "ROW_NUMBER":
		return minisql.WindowRowNumber
	case "RANK":
		return minisql.WindowRank
	case "DENSE_RANK":
		return minisql.WindowDenseRank
	case "NTILE":
		return minisql.WindowNtile
	case "LAG":
		return minisql.WindowLag
	case "LEAD":
		return minisql.WindowLead
	case "FIRST_VALUE":
		return minisql.WindowFirstValue
	case "LAST_VALUE":
		return minisql.WindowLastValue
	case "NTH_VALUE":
		return minisql.WindowNthValue
	case "SUM":
		return minisql.WindowSum
	case "AVG":
		return minisql.WindowAvg
	case "COUNT":
		return minisql.WindowCount
	case "MIN":
		return minisql.WindowMin
	case "MAX":
		return minisql.WindowMax
	}
	return 0
}

// parseWindowSpec parses the content inside OVER (...).
// Grammar:
//
//	OVER ( [PARTITION BY col, ...] [ORDER BY col [ASC|DESC], ...] [frame_clause] )
func (p *parserItem) parseWindowSpec() (minisql.WindowSpec, error) {
	if p.peek() != "(" {
		return minisql.WindowSpec{}, fmt.Errorf("OVER: expected '('")
	}
	p.pop() // consume "("

	var spec minisql.WindowSpec

	// PARTITION BY
	if strings.ToUpper(p.peek()) == "PARTITION BY" {
		p.pop() // consume "PARTITION BY"
		for {
			col := p.peek()
			if !isIdentifier(col) {
				return minisql.WindowSpec{}, fmt.Errorf("OVER PARTITION BY: expected column name, got %q", col)
			}
			// Strip table qualifier if present (e.g. "t.col" → "col")
			if dot := strings.LastIndex(col, "."); dot >= 0 {
				col = col[dot+1:]
			}
			p.pop()
			spec.PartitionBy = append(spec.PartitionBy, col)
			if p.peek() != "," {
				break
			}
			p.pop() // consume ","
		}
	}

	// ORDER BY
	if strings.ToUpper(p.peek()) == "ORDER BY" {
		p.pop() // consume "ORDER BY"
		for {
			col := p.peek()
			if !isIdentifier(col) {
				return minisql.WindowSpec{}, fmt.Errorf("OVER ORDER BY: expected column name, got %q", col)
			}
			// Derive AliasPrefix from qualified name (e.g. "t.col")
			var aliasPrefix string
			name := col
			if dot := strings.LastIndex(col, "."); dot >= 0 {
				aliasPrefix = col[:dot]
				name = col[dot+1:]
			}
			p.pop()
			dir := minisql.Asc
			switch strings.ToUpper(p.peek()) {
			case "ASC":
				p.pop()
			case "DESC":
				dir = minisql.Desc
				p.pop()
			}
			spec.OrderBy = append(spec.OrderBy, minisql.OrderBy{
				Field:     minisql.Field{Name: name, AliasPrefix: aliasPrefix},
				Direction: dir,
			})
			if p.peek() != "," {
				break
			}
			p.pop() // consume ","
		}
	}

	// Optional frame clause: ROWS BETWEEN ... AND ... | RANGE BETWEEN ... AND ...
	upper := strings.ToUpper(p.peek())
	if upper == "ROWS BETWEEN" || upper == "RANGE BETWEEN" {
		frame, err := p.parseWindowFrame(upper)
		if err != nil {
			return minisql.WindowSpec{}, err
		}
		spec.Frame = &frame
	}

	if p.peek() != ")" {
		return minisql.WindowSpec{}, fmt.Errorf("OVER: expected ')', got %q", p.peek())
	}
	p.pop() // consume ")"

	return spec, nil
}

// parseWindowFrame parses the ROWS/RANGE BETWEEN ... AND ... clause.
// The modeToken is already peeked ("ROWS BETWEEN" or "RANGE BETWEEN").
func (p *parserItem) parseWindowFrame(modeToken string) (minisql.WindowFrame, error) {
	p.pop() // consume "ROWS BETWEEN" or "RANGE BETWEEN"

	mode := minisql.FrameRows
	if strings.HasPrefix(strings.ToUpper(modeToken), "RANGE") {
		mode = minisql.FrameRange
	}

	start, err := p.parseFrameBound()
	if err != nil {
		return minisql.WindowFrame{}, fmt.Errorf("frame start: %w", err)
	}

	// Consume the AND separator — do NOT rely on the reserved-words tokenizer
	// because "AND" is not in reservedWords (it is handled by the WHERE parser
	// via parseAndExpr).  Instead do a direct case-insensitive string compare.
	if strings.ToUpper(p.peek()) != "AND" {
		return minisql.WindowFrame{}, fmt.Errorf("frame: expected AND between bounds, got %q", p.peek())
	}
	p.pop()

	end, err := p.parseFrameBound()
	if err != nil {
		return minisql.WindowFrame{}, fmt.Errorf("frame end: %w", err)
	}

	return minisql.WindowFrame{Mode: mode, Start: start, End: end}, nil
}

// parseFrameBound parses one side of a frame specification.
func (p *parserItem) parseFrameBound() (minisql.FrameBound, error) {
	tok := strings.ToUpper(p.peek())

	switch tok {
	case "UNBOUNDED PRECEDING":
		p.pop()
		return minisql.FrameBound{Kind: minisql.FrameUnboundedPreceding}, nil
	case "UNBOUNDED FOLLOWING":
		p.pop()
		return minisql.FrameBound{Kind: minisql.FrameUnboundedFollowing}, nil
	case "CURRENT ROW":
		p.pop()
		return minisql.FrameBound{Kind: minisql.FrameCurrentRow}, nil
	}

	// N PRECEDING or N FOLLOWING
	n, ln := p.peekIntWithLength()
	if ln == 0 {
		return minisql.FrameBound{}, fmt.Errorf("frame bound: expected UNBOUNDED PRECEDING, CURRENT ROW, or integer offset, got %q", p.peek())
	}
	p.pop()
	direction := strings.ToUpper(p.peek())
	switch direction {
	case "PRECEDING":
		p.pop()
		return minisql.FrameBound{Kind: minisql.FramePreceding, Offset: int(n)}, nil
	case "FOLLOWING":
		p.pop()
		return minisql.FrameBound{Kind: minisql.FrameFollowing, Offset: int(n)}, nil
	default:
		return minisql.FrameBound{}, fmt.Errorf("frame bound: expected PRECEDING or FOLLOWING, got %q", p.peek())
	}
}
