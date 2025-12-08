package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errEmptyWhereClause                  = fmt.Errorf("at WHERE: empty WHERE clause")
	errWhereWithoutOperator              = fmt.Errorf("at WHERE: condition without operator")
	errWhereExpectedField                = fmt.Errorf("at WHERE: expected field")
	errWhereExpectedAndOr                = fmt.Errorf("expected one of AND / OR")
	errWhereExpectedQuotedStringOrNumber = fmt.Errorf("at WHERE: expected quoted string or number value")
	errWhereUnknownOperator              = fmt.Errorf("at WHERE: unknown operator")
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
		case "IS NOT NULL":
			currentCondition.Operator = minisql.Ne
			currentCondition.Operand2 = minisql.Operand{
				Type: minisql.OperandNull,
			}
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
		var (
			identifier          = p.peek()
			currentCondition, _ = p.Conditions.LastCondition()
		)
		if isIdentifier(identifier) {
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.OperandField,
				Value: identifier,
			}
		} else {
			value, ln := p.peekNumberOrQuotedStringWithLength()
			if ln == 0 {
				return errWhereExpectedQuotedStringOrNumber
			}
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.OperandQuotedString,
				Value: value,
			}
			if _, ok := value.(int64); ok {
				currentCondition.Operand2.Type = minisql.OperandInteger
			} else if _, ok := value.(float64); ok {
				currentCondition.Operand2.Type = minisql.OperandFloat
			}
		}
		p.Conditions.UpdateLast(currentCondition)
		p.pop()
		p.step = stepWhereOperator
	case stepWhereConditionListValue:
		currentCondition, _ := p.Conditions.LastCondition()
		value, ln := p.peekNumberOrQuotedStringWithLength()
		if ln == 0 {
			return errWhereExpectedQuotedStringOrNumber
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
		if rWord == "LIMIT" {
			p.step = stepWhereLimit
			return nil
		}
		if rWord == "OFFSET" {
			p.step = stepWhereOffset
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
	case stepWhereLimit:
		if strings.ToUpper(p.peek()) != "LIMIT" {
			return fmt.Errorf("at WHERE: expected LIMIT")
		}
		p.pop()
		limitValue, n := p.peekIntWithLength()
		if n == 0 {
			return fmt.Errorf("at WHERE: expected integer value for LIMIT")
		}
		p.Limit = minisql.OptionalValue{Value: limitValue, Valid: true}
		p.pop()
		p.step = stepWhereOffset
	case stepWhereOffset:
		rWord := p.peek()
		if rWord == ";" {
			p.step = stepStatementEnd
			return nil
		}
		if strings.ToUpper(rWord) != "OFFSET" {
			return fmt.Errorf("at WHERE: expected OFFSET")
		}
		p.pop()
		offsetValue, n := p.peekIntWithLength()
		if n == 0 {
			return fmt.Errorf("at WHERE: expected integer value for OFFSET")
		}
		p.Offset = minisql.OptionalValue{Value: offsetValue, Valid: true}
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}
