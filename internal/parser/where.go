package parser

import (
	"context"
	"errors"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errEmptyWhereClause                          = errors.New("at WHERE: empty WHERE clause")
	errWhereWithoutOperator                      = errors.New("at WHERE: condition without operator")
	errWhereExpectedField                        = errors.New("at WHERE: expected field")
	errWhereExpectedPlaceholderOrValue           = errors.New("at WHERE: expected placeholder or value")
	errWhereExpectedIdentifierPlaceholderOrValue = errors.New("at WHERE: expected identifier, placeholder or value")
	errWhereUnknownOperator                      = errors.New("at WHERE: unknown operator")
)

// doParseWhere handles the stepWhere step. It parses the optional WHERE clause
// using a recursive-descent parser that builds a ConditionNode tree, then
// normalises the tree to DNF (OneOrMore) so all downstream code is unchanged.
func (p *parserItem) doParseWhere() error {
	whereOrEnd := p.peek()

	// No WHERE clause — move on.
	if whereOrEnd == ";" || whereOrEnd == "" {
		p.step = stepStatementEnd
		return nil
	}

	whereRWord := strings.ToUpper(whereOrEnd)

	// GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET / UNION / RETURNING appearing
	// before WHERE means no WHERE clause.
	switch whereRWord {
	case "GROUP BY":
		p.step = stepSelectGroupBy
		return nil
	case "HAVING":
		p.step = stepSelectHaving
		return nil
	case "ORDER BY", "LIMIT", "OFFSET":
		p.step = stepSelectOrderBy
		return nil
	case "UNION ALL", "UNION":
		p.step = stepStatementEnd
		return nil
	case "RETURNING":
		p.pop()
		p.step = stepReturningField
		return nil
	}

	if whereRWord != "WHERE" {
		return p.errorf("at WHERE: expected WHERE keyword")
	}
	if len(p.OrderBy) > 0 {
		return p.errorf("at WHERE: ORDER BY must be after WHERE clause")
	}
	if p.Offset.Valid || p.Limit.Valid {
		return p.errorf("at WHERE: OFFSET / LIMIT must be after WHERE clause")
	}
	if len(p.Conditions) > 0 {
		return p.errorf("at WHERE: multiple WHERE clauses are not supported")
	}

	p.pop() // consume "WHERE"

	node, err := p.parseCondExpr()
	if err != nil {
		return err
	}
	p.Conditions = node.ToDNF()

	// Determine the next parser step.
	next := strings.ToUpper(p.peek())
	switch next {
	case "GROUP BY":
		p.step = stepSelectGroupBy
	case "HAVING":
		p.step = stepSelectHaving
	case "ORDER BY", "LIMIT", "OFFSET":
		p.step = stepSelectOrderBy
	case "RETURNING":
		p.pop()
		p.step = stepReturningField
	default:
		p.step = stepStatementEnd
	}
	return nil
}

// parseCondExpr parses an OR expression (lowest precedence):
//
//	andExpr ('OR' andExpr)*
func (p *parserItem) parseCondExpr() (*minisql.ConditionNode, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for strings.ToUpper(p.peek()) == "OR" {
		p.pop()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &minisql.ConditionNode{
			Left:  left,
			Op:    minisql.LogicOpOr,
			Right: right,
		}
	}
	return left, nil
}

// parseAndExpr parses an AND expression:
//
//	primaryExpr ('AND' primaryExpr)*
//
// Note: the syntactic AND inside BETWEEN x AND y is consumed inside
// parseCondBetweenValues, so it is never seen by this loop.
func (p *parserItem) parseAndExpr() (*minisql.ConditionNode, error) {
	left, err := p.parsePrimaryCondExpr()
	if err != nil {
		return nil, err
	}
	for strings.ToUpper(p.peek()) == "AND" {
		p.pop()
		right, err := p.parsePrimaryCondExpr()
		if err != nil {
			return nil, err
		}
		left = &minisql.ConditionNode{
			Left:  left,
			Op:    minisql.LogicOpAnd,
			Right: right,
		}
	}
	return left, nil
}

// parsePrimaryCondExpr parses a parenthesised group or a single leaf condition.
func (p *parserItem) parsePrimaryCondExpr() (*minisql.ConditionNode, error) {
	if p.peek() == "(" {
		p.pop() // consume "("
		node, err := p.parseCondExpr()
		if err != nil {
			return nil, err
		}
		if p.peek() != ")" {
			return nil, p.errorf("at WHERE: expected closing parenthesis")
		}
		p.pop() // consume ")"
		return node, nil
	}
	return p.parseLeafCondition()
}

func nextTokenIsConditionOperator(token string) bool {
	switch strings.ToUpper(token) {
	case "IS NULL", "IS NOT NULL", "=", "!=", ">", ">=", "<", "<=", "LIKE", "NOT LIKE":
		return true
	default:
		return false
	}
}

// parseLeafCondition parses a single WHERE/HAVING condition: field op value.
// In HAVING clauses the left-side "field" may be a synthetic aggregate result
// column name such as "SUM(total_paid)" or "COUNT(*)".
func (p *parserItem) parseLeafCondition() (*minisql.ConditionNode, error) {
	identifier := p.peek()
	upperIdent := strings.ToUpper(identifier)

	// Built-in function call or CAST as WHERE left operand:
	//   LOWER(email) = ?, DATE_TRUNC('month', ts) = ?, CAST(x AS INT8) > 0, etc.
	if isBuiltinFunction(upperIdent) || upperIdent == "CAST" {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		cond := minisql.Condition{
			Operand1: minisql.Operand{Type: minisql.OperandExpr, Value: expr},
		}
		if (upperIdent == "MATCH" || upperIdent == "JSON_CONTAINS") && !nextTokenIsConditionOperator(p.peek()) {
			cond.Operator = minisql.Eq
			cond.Operand2 = minisql.Operand{Type: minisql.OperandBoolean, Value: true}
			return &minisql.ConditionNode{Leaf: &cond}, nil
		}
		return p.parseCondOperatorAndRHS(&cond)
	}

	// Handle aggregate function references (HAVING SUM(col) > x, etc.).
	if aggKind := aggregateKindFromToken(upperIdent); aggKind != 0 {
		p.pop() // consume e.g. "SUM("
		colName := p.peek()
		if !isIdentifier(colName) {
			return nil, p.errorf("at HAVING: expected column name in %s", strings.TrimSuffix(upperIdent, "("))
		}
		p.pop()
		if p.peek() != ")" {
			return nil, p.errorf("at HAVING: expected ')' after column name in %s", strings.TrimSuffix(upperIdent, "("))
		}
		p.pop()
		funcName := strings.TrimSuffix(upperIdent, "(")
		identifier = funcName + "(" + colName + ")" // e.g. "SUM(total_paid)"
	} else if upperIdent == "COUNT(*)" {
		p.pop()
		identifier = "COUNT(*)"
	} else {
		if !isIdentifier(identifier) {
			return nil, p.wrapErr(errWhereExpectedField)
		}
		// Parse as expression to support arithmetic (price * qty) and JSON path
		// (payload->>'key') starting with a plain column name.
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		// Complex expression (arithmetic, JSON path, etc.) — use OperandExpr.
		if expr.Column == "" {
			cond := minisql.Condition{
				Operand1: minisql.Operand{Type: minisql.OperandExpr, Value: expr},
			}
			return p.parseCondOperatorAndRHS(&cond)
		}
		// Bare column reference — fall through to OperandField handling below.
		identifier = expr.Column
	}

	cond := minisql.Condition{
		Operand1: minisql.Operand{
			Type:  minisql.OperandField,
			Value: fieldFromIdentifier(identifier),
		},
	}

	op := strings.ToUpper(p.peek())
	switch op {
	case "IS NULL":
		cond.Operator = minisql.Eq
		cond.Operand2 = minisql.Operand{Type: minisql.OperandNull}
		p.pop()
	case "IS NOT NULL":
		cond.Operator = minisql.Ne
		cond.Operand2 = minisql.Operand{Type: minisql.OperandNull}
		p.pop()
	case "IN (":
		cond.Operator = minisql.In
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondListValues(&cond); err != nil {
			return nil, err
		}
	case "NOT IN (":
		cond.Operator = minisql.NotIn
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondListValues(&cond); err != nil {
			return nil, err
		}
	case "BETWEEN":
		cond.Operator = minisql.Between
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondBetweenValues(&cond); err != nil {
			return nil, err
		}
	case "NOT BETWEEN":
		cond.Operator = minisql.NotBetween
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondBetweenValues(&cond); err != nil {
			return nil, err
		}
	case "=":
		cond.Operator = minisql.Eq
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case ">":
		cond.Operator = minisql.Gt
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case ">=":
		cond.Operator = minisql.Gte
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case "<":
		cond.Operator = minisql.Lt
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case "<=":
		cond.Operator = minisql.Lte
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case "!=":
		cond.Operator = minisql.Ne
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case "LIKE":
		cond.Operator = minisql.Like
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	case "NOT LIKE":
		cond.Operator = minisql.NotLike
		p.pop()
		if err := p.parseCondScalarValue(&cond); err != nil {
			return nil, err
		}
	default:
		return nil, p.wrapErr(errWhereUnknownOperator)
	}

	return &minisql.ConditionNode{Leaf: &cond}, nil
}

// parseCondOperatorAndRHS parses the operator and right-hand side of a condition
// whose left operand has already been set (expression or JSON path operands).
func (p *parserItem) parseCondOperatorAndRHS(cond *minisql.Condition) (*minisql.ConditionNode, error) {
	op := strings.ToUpper(p.peek())
	switch op {
	case "IS NULL":
		cond.Operator = minisql.Eq
		cond.Operand2 = minisql.Operand{Type: minisql.OperandNull}
		p.pop()
	case "IS NOT NULL":
		cond.Operator = minisql.Ne
		cond.Operand2 = minisql.Operand{Type: minisql.OperandNull}
		p.pop()
	case "=":
		cond.Operator = minisql.Eq
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case "!=":
		cond.Operator = minisql.Ne
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case ">":
		cond.Operator = minisql.Gt
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case ">=":
		cond.Operator = minisql.Gte
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case "<":
		cond.Operator = minisql.Lt
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case "<=":
		cond.Operator = minisql.Lte
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case "LIKE":
		cond.Operator = minisql.Like
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case "NOT LIKE":
		cond.Operator = minisql.NotLike
		p.pop()
		if err := p.parseCondScalarValue(cond); err != nil {
			return nil, err
		}
	case "IN (":
		cond.Operator = minisql.In
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondListValues(cond); err != nil {
			return nil, err
		}
	case "NOT IN (":
		cond.Operator = minisql.NotIn
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondListValues(cond); err != nil {
			return nil, err
		}
	case "BETWEEN":
		cond.Operator = minisql.Between
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondBetweenValues(cond); err != nil {
			return nil, err
		}
	case "NOT BETWEEN":
		cond.Operator = minisql.NotBetween
		cond.Operand2 = minisql.Operand{Type: minisql.OperandList, Value: []any{}}
		p.pop()
		if err := p.parseCondBetweenValues(cond); err != nil {
			return nil, err
		}
	default:
		return nil, p.wrapErr(errWhereUnknownOperator)
	}
	return &minisql.ConditionNode{Leaf: cond}, nil
}

// parseCondScalarValue parses a scalar value (literal, placeholder, field
// identifier, or scalar subquery) and assigns it to cond.Operand2.
func (p *parserItem) parseCondScalarValue(cond *minisql.Condition) error {
	value, ln := p.peekValue()
	if ln != 0 {
		cond.Operand2 = minisql.Operand{
			Type:  minisql.OperandQuotedString,
			Value: value,
		}
		switch v := value.(type) {
		case bool:
			cond.Operand2.Type = minisql.OperandBoolean
		case int64:
			cond.Operand2.Type = minisql.OperandInteger
		case float64:
			cond.Operand2.Type = minisql.OperandFloat
		case string:
			cond.Operand2.Value = minisql.NewTextPointer([]byte(v))
		}
		p.pop()
		return nil
	}
	if p.peek() == "?" {
		cond.Operand2 = minisql.Operand{Type: minisql.OperandPlaceholder}
		p.pop()
		return nil
	}
	// Scalar subquery: col = (SELECT ...)
	if p.peek() == "(" {
		p.pop() // consume "("
		if strings.ToUpper(p.peek()) != "SELECT" {
			return p.errorf("at WHERE: expected SELECT after '(' for scalar subquery")
		}
		subStmt, err := p.parseSubquery()
		if err != nil {
			return err
		}
		cond.Operand2 = minisql.Operand{Type: minisql.OperandSubquery, Value: subStmt}
		return nil
	}
	if identifier := p.peek(); isIdentifier(identifier) {
		cond.Operand2 = minisql.Operand{
			Type:  minisql.OperandField,
			Value: fieldFromIdentifier(identifier),
		}
		p.pop()
		return nil
	}
	return p.wrapErr(errWhereExpectedIdentifierPlaceholderOrValue)
}

// parseSubquery extracts the SQL from p.i to the matching closing paren,
// parses it as a SELECT statement, and returns the parsed *Statement.
// p.i must be positioned at the start of the SELECT keyword when called.
// The closing ")" is consumed before returning.
func (p *parserItem) parseSubquery() (*minisql.Statement, error) {
	// Find the matching ")" by scanning character-by-character, respecting
	// single-quoted strings and nested parentheses.
	var (
		depth   = 1
		scanI   = p.i
		inQuote = false
	)
	for scanI < len(p.sql) {
		ch := p.sql[scanI]
		if ch == '\'' {
			inQuote = !inQuote
		} else if !inQuote {
			if ch == '(' {
				depth += 1
			} else if ch == ')' {
				depth -= 1
				if depth == 0 {
					break
				}
			}
		}
		scanI += 1
	}
	if depth != 0 {
		return nil, p.errorf("at WHERE: unclosed parenthesis in subquery")
	}
	subSQL := strings.TrimSpace(p.sql[p.i:scanI])
	p.i = scanI + 1 // skip ")"
	p.popWhitespace()

	stmts, err := New().Parse(context.Background(), subSQL)
	if err != nil {
		return nil, p.errorf("at WHERE: subquery parse error: %v", err)
	}
	if len(stmts) != 1 || stmts[0].Kind != minisql.Select {
		return nil, p.errorf("at WHERE: subquery must be a single SELECT statement")
	}
	return &stmts[0], nil
}

// parseCondListValues parses the comma-separated values inside IN (...).
// The opening "(" is already consumed as part of the "IN (" token.
// If the first token is SELECT, the entire list is treated as a subquery.
func (p *parserItem) parseCondListValues(cond *minisql.Condition) error {
	// IN (SELECT ...) — list subquery
	if strings.ToUpper(p.peek()) == "SELECT" {
		subStmt, err := p.parseSubquery()
		if err != nil {
			return err
		}
		cond.Operand2 = minisql.Operand{Type: minisql.OperandSubquery, Value: subStmt}
		return nil
	}

	for {
		value, ln := p.peekValue()
		switch {
		case ln != 0:
			v := value
			if _, ok := v.(string); ok {
				v = minisql.NewTextPointer([]byte(v.(string)))
			}
			cond.Operand2.Value = append(cond.Operand2.Value.([]any), v)
			p.pop()
		case p.peek() == "?":
			cond.Operand2.Value = append(cond.Operand2.Value.([]any), minisql.Placeholder{})
			p.pop()
		default:
			return p.wrapErr(errWhereExpectedPlaceholderOrValue)
		}

		next := p.peek()
		if next == ")" {
			p.pop()
			return nil
		}
		if next == "," {
			p.pop()
			continue
		}
		return p.errorf("at WHERE IN (...): expected , or )")
	}
}

// parseCondBetweenValues parses "low AND high" for BETWEEN / NOT BETWEEN.
// Consumes the syntactic AND between the bounds so it is not treated as a
// logical AND by the outer parseAndExpr.
func (p *parserItem) parseCondBetweenValues(cond *minisql.Condition) error {
	// Parse low bound.
	value, ln := p.peekValue()
	switch {
	case ln != 0:
		v := value
		if _, ok := v.(string); ok {
			v = minisql.NewTextPointer([]byte(v.(string)))
		}
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), v)
		p.pop()
	case p.peek() == "?":
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), minisql.Placeholder{})
		p.pop()
	default:
		return p.errorf("at WHERE BETWEEN: expected value or placeholder for lower bound")
	}

	// Consume the syntactic AND.
	if strings.ToUpper(p.peek()) != "AND" {
		return p.errorf("at WHERE BETWEEN: expected AND between bounds")
	}
	p.pop()

	// Parse high bound.
	value, ln = p.peekValue()
	switch {
	case ln != 0:
		v := value
		if _, ok := v.(string); ok {
			v = minisql.NewTextPointer([]byte(v.(string)))
		}
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), v)
		p.pop()
	case p.peek() == "?":
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), minisql.Placeholder{})
		p.pop()
	default:
		return p.errorf("at WHERE BETWEEN: expected value or placeholder for upper bound")
	}

	return nil
}
