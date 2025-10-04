package parser

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	errEmptyWhereClause                  = fmt.Errorf("at WHERE: empty WHERE clause")
	errWhereWithoutOperator              = fmt.Errorf("at WHERE: condition without operator")
	errWhereRequiredForUpdateDelete      = fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	errWhereExpectedField                = fmt.Errorf("at WHERE: expected field")
	errWhereExpectedAndOr                = fmt.Errorf("expected one of AND / OR")
	errWhereExpectedQuotedStringOrNumber = fmt.Errorf("at WHERE: expected quoted string or number value")
	errWhereUnknownOperator              = fmt.Errorf("at WHERE: unknown operator")
)

func (p *parser) doParseWhere() error {
	switch p.step {
	case stepWhere:
		whereRWord := p.peek()
		if strings.ToUpper(whereRWord) != "WHERE" {
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
				Type:  minisql.Field,
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
		if strings.ToUpper(operatorOrNullComparison) == "IS NULL" {
			currentCondition.Operator = minisql.Eq
			currentCondition.Operand2 = minisql.Operand{
				Type: minisql.Null,
			}
		} else if strings.ToUpper(operatorOrNullComparison) == "IS NOT NULL" {
			currentCondition.Operator = minisql.Ne
			currentCondition.Operand2 = minisql.Operand{
				Type: minisql.Null,
			}
		} else {
			switch operatorOrNullComparison {
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
		}
		p.Conditions.UpdateLast(currentCondition)
		p.pop()
		p.step = stepWhereConditionValue
	case stepWhereConditionValue:
		var (
			identifier          = p.peek()
			currentCondition, _ = p.Conditions.LastCondition()
		)
		if isIdentifier(identifier) {
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.Field,
				Value: identifier,
			}
		} else {
			value, ln := p.peekNumberOrQuotedStringWithLength()
			if ln == 0 {
				return errWhereExpectedQuotedStringOrNumber
			}
			currentCondition.Operand2 = minisql.Operand{
				Type:  minisql.QuotedString,
				Value: value,
			}
			if _, ok := value.(int64); ok {
				currentCondition.Operand2.Type = minisql.Integer
			} else if _, ok := value.(float64); ok {
				currentCondition.Operand2.Type = minisql.Float
			}
		}
		p.Conditions.UpdateLast(currentCondition)
		p.pop()
		p.step = stepWhereOperator
	case stepWhereOperator:
		anOperator := strings.ToUpper(p.peek())
		if anOperator != "AND" && anOperator != "OR" {
			return errWhereExpectedAndOr
		}
		if anOperator == "OR" {
			p.Conditions = append(p.Conditions, make(minisql.Conditions, 0, 1))
		}
		p.pop()
		p.step = stepWhereConditionField
	}
	return nil
}
