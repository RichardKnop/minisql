package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errEmptyWhereClause                          = fmt.Errorf("at WHERE: empty WHERE clause")
	errWhereWithoutOperator                      = fmt.Errorf("at WHERE: condition without operator")
	errWhereExpectedField                        = fmt.Errorf("at WHERE: expected field")
	errWhereExpectedPlaceholderOrValue           = fmt.Errorf("at WHERE: expected placeholder or value")
	errWhereExpectedIdentifierPlaceholderOrValue = fmt.Errorf("at WHERE: expected identifier, placeholder or value")
	errWhereUnknownOperator                      = fmt.Errorf("at WHERE: unknown operator")
)

// doParseWhere handles the stepWhere step. It parses the optional WHERE clause
// using a recursive-descent parser that builds a ConditionNode tree, then
// normalises the tree to DNF (OneOrMore) so all downstream code is unchanged.
func (p *parserItem) doParseWhere() error {
	whereOrEnd := p.peek()

	// No WHERE clause — move on.
	if whereOrEnd == ";" {
		p.step = stepStatementEnd
		return nil
	}

	whereRWord := strings.ToUpper(whereOrEnd)

	// ORDER BY / LIMIT / OFFSET appearing before WHERE (i.e. no WHERE clause).
	if whereRWord == "ORDER BY" || whereRWord == "LIMIT" || whereRWord == "OFFSET" {
		p.step = stepSelectOrderBy
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
	if next == "ORDER BY" || next == "LIMIT" || next == "OFFSET" {
		p.step = stepSelectOrderBy
	} else {
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

// parseLeafCondition parses a single WHERE condition: field op value.
func (p *parserItem) parseLeafCondition() (*minisql.ConditionNode, error) {
	identifier := p.peek()
	if !isIdentifier(identifier) {
		return nil, p.wrapErr(errWhereExpectedField)
	}
	p.pop()

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

// parseCondScalarValue parses a scalar value (literal, placeholder, or field identifier)
// and assigns it to cond.Operand2.
func (p *parserItem) parseCondScalarValue(cond *minisql.Condition) error {
	value, ln := p.peekValue()
	if ln != 0 {
		cond.Operand2 = minisql.Operand{
			Type:  minisql.OperandQuotedString,
			Value: value,
		}
		if _, ok := value.(bool); ok {
			cond.Operand2.Type = minisql.OperandBoolean
		} else if _, ok := value.(int64); ok {
			cond.Operand2.Type = minisql.OperandInteger
		} else if _, ok := value.(float64); ok {
			cond.Operand2.Type = minisql.OperandFloat
		} else if _, ok := value.(string); ok {
			cond.Operand2.Value = minisql.NewTextPointer([]byte(value.(string)))
		}
		p.pop()
		return nil
	}
	if p.peek() == "?" {
		cond.Operand2 = minisql.Operand{Type: minisql.OperandPlaceholder}
		p.pop()
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

// parseCondListValues parses the comma-separated values inside IN (...).
// The opening "(" is already consumed as part of the "IN (" token.
func (p *parserItem) parseCondListValues(cond *minisql.Condition) error {
	for {
		value, ln := p.peekValue()
		if ln != 0 {
			v := value
			if _, ok := v.(string); ok {
				v = minisql.NewTextPointer([]byte(v.(string)))
			}
			cond.Operand2.Value = append(cond.Operand2.Value.([]any), v)
			p.pop()
		} else if p.peek() == "?" {
			cond.Operand2.Value = append(cond.Operand2.Value.([]any), minisql.Placeholder{})
			p.pop()
		} else {
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
	if ln != 0 {
		v := value
		if _, ok := v.(string); ok {
			v = minisql.NewTextPointer([]byte(v.(string)))
		}
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), v)
		p.pop()
	} else if p.peek() == "?" {
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), minisql.Placeholder{})
		p.pop()
	} else {
		return p.errorf("at WHERE BETWEEN: expected value or placeholder for lower bound")
	}

	// Consume the syntactic AND.
	if strings.ToUpper(p.peek()) != "AND" {
		return p.errorf("at WHERE BETWEEN: expected AND between bounds")
	}
	p.pop()

	// Parse high bound.
	value, ln = p.peekValue()
	if ln != 0 {
		v := value
		if _, ok := v.(string); ok {
			v = minisql.NewTextPointer([]byte(v.(string)))
		}
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), v)
		p.pop()
	} else if p.peek() == "?" {
		cond.Operand2.Value = append(cond.Operand2.Value.([]any), minisql.Placeholder{})
		p.pop()
	} else {
		return p.errorf("at WHERE BETWEEN: expected value or placeholder for upper bound")
	}

	return nil
}
