package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errEmptyWhereClause                            = fmt.Errorf("at WHERE: empty WHERE clause")
	errWhereWithoutOperator                        = fmt.Errorf("at WHERE: condition without operator")
	errWhereExpectedField                          = fmt.Errorf("at WHERE: expected field")
	errWhereExpectedAndOr                          = fmt.Errorf("expected one of AND / OR")
	errWhereExpectedQuotedStringOrNumber           = fmt.Errorf("at WHERE: expected quoted string or number value")
	errWhereExpectedIdentifierQuotedStringOrNumber = fmt.Errorf("at WHERE: expected identifier, quoted string or number value")
	errWhereUnknownOperator                        = fmt.Errorf("at WHERE: unknown operator")
)

func (p *parser) doParseWhere() error {
	switch p.step {
	case stepWhere:
		whereOrEnd := p.peek()
		if whereOrEnd == ";" {
			p.step = stepStatementEnd
			return nil
		}
		whereRWord := strings.ToUpper(whereOrEnd)
		if whereRWord != "WHERE" {
			return fmt.Errorf("expected WHERE")
		}
		if len(p.OrderBy) > 0 {
			return fmt.Errorf("at WHERE: ORDER BY must be after WHERE clause")
		}
		if p.Offset.Valid || p.Limit.Valid {
			return fmt.Errorf("at WHERE: OFFSET / LIMIT must be after WHERE clause")
		}
		if len(p.Conditions) > 0 {
			return fmt.Errorf("at WHERE: multiple WHERE clauses are not supported")
		}
		p.pop()
		p.step = stepWhereConditionField
	case stepWhereConditionField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return errWhereExpectedField
		}
		p.Statement.Conditions = p.Statement.Conditions.Append(minisql.Condition{
			Operand1: minisql.Operand{
				Type:  minisql.OperandField,
				Value: identifier,
			},
		})
		p.pop()
		p.step = stepWhereConditionOperator
	case stepWhereConditionOperator:
		var (
			operatorOrNullComparison = p.peek()
			currentCondition, _      = p.Conditions.LastCondition()
		)
		switch strings.ToUpper(operatorOrNullComparison) {
		case "IS NULL":
			currentCondition.Operator = minisql.Eq
			currentCondition.Operand2 = minisql.Operand{
				Type: minisql.OperandNull,
			}
			p.step = stepWhereOperator
			p.Conditions.UpdateLast(currentCondition)
			p.pop()
			return nil
		case "IS NOT NULL":
			currentCondition.Operator = minisql.Ne
			currentCondition.Operand2 = minisql.Operand{
				Type: minisql.OperandNull,
			}
			p.step = stepWhereOperator
			p.Conditions.UpdateLast(currentCondition)
			p.pop()
			return nil
		case "IN (":
			currentCondition.Operator = minisql.In
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.OperandList,
				Value: []any{},
			}
		case "NOT IN (":
			currentCondition.Operator = minisql.NotIn
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.OperandList,
				Value: []any{},
			}
		case "=":
			currentCondition.Operator = minisql.Eq
		case ">":
			currentCondition.Operator = minisql.Gt
		case ">=":
			currentCondition.Operator = minisql.Gte
		case "<":
			currentCondition.Operator = minisql.Lt
		case "<=":
			currentCondition.Operator = minisql.Lte
		case "!=":
			currentCondition.Operator = minisql.Ne
		default:
			return errWhereUnknownOperator
		}
		p.Conditions.UpdateLast(currentCondition)
		p.pop()
		if currentCondition.Operator == minisql.In || currentCondition.Operator == minisql.NotIn {
			p.step = stepWhereConditionListValue
			return nil
		}
		p.step = stepWhereConditionValue
	case stepWhereConditionValue:
		var currentCondition, _ = p.Conditions.LastCondition()
		value, ln := p.peekValue()
		if ln == 0 {
			if identifier := p.peek(); isIdentifier(identifier) {
				currentCondition.Operand2 = minisql.Operand{
					Type:  minisql.OperandField,
					Value: identifier,
				}
			} else {
				return errWhereExpectedIdentifierQuotedStringOrNumber
			}
		} else {
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.OperandQuotedString,
				Value: value,
			}
			if _, ok := value.(bool); ok {
				currentCondition.Operand2.Type = minisql.OperandBoolean
			} else if _, ok := value.(int64); ok {
				currentCondition.Operand2.Type = minisql.OperandInteger
			} else if _, ok := value.(float64); ok {
				currentCondition.Operand2.Type = minisql.OperandFloat
			} else if _, ok := value.(string); ok {
				currentCondition.Operand2.Value = minisql.NewTextPointer([]byte(value.(string)))
			}
		}
		p.Conditions.UpdateLast(currentCondition)
		p.pop()
		p.step = stepWhereOperator
	case stepWhereConditionListValue:
		currentCondition, _ := p.Conditions.LastCondition()
		value, ln := p.peekValue()
		if ln == 0 {
			return errWhereExpectedQuotedStringOrNumber
		}
		if _, ok := value.(string); ok {
			value = minisql.NewTextPointer([]byte(value.(string)))
		}
		currentCondition.Operand2.Value = append(currentCondition.Operand2.Value.([]any), value)
		p.Conditions.UpdateLast(currentCondition)
		p.pop()
		p.step = stepWhereConditionListValueCommaOrEnd
	case stepWhereConditionListValueCommaOrEnd:
		switch strings.ToUpper(p.peek()) {
		case ",":
			p.pop()
			p.step = stepWhereConditionListValue
			return nil
		case ")":
			p.pop()
			p.step = stepWhereOperator
			return nil
		}
		return fmt.Errorf("at WHERE IN (...): expected , or )")
	case stepWhereOperator:
		rWord := strings.ToUpper(p.peek())
		lastCondition, ok := p.Conditions.LastCondition()
		if ok && minisql.IsValidCondition(lastCondition) {
			if rWord == ";" {
				p.step = stepStatementEnd
				return nil
			}
		}
		if rWord == "ORDER BY" || rWord == "LIMIT" || rWord == "OFFSET" {
			p.step = stepSelectOrderBy
			return nil
		}
		if rWord != "AND" && rWord != "OR" {
			return errWhereExpectedAndOr
		}
		if rWord == "OR" {
			p.Conditions = append(p.Conditions, make(minisql.Conditions, 0, 1))
		}
		p.pop()
		p.step = stepWhereConditionField
	}
	return nil
}
