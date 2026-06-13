package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errSelectWithoutFields     = errors.New("at SELECT: expected field to SELECT")
	errSelectExpectedTableName = errors.New("at SELECT: expected table name identifier")
	errCannotCombineAsterisk   = fmt.Errorf(`at SELECT: cannot combine "*" with other fields`)
	errExpectedFrom            = errors.New("at SELECT: expected FROM")
)

// aggregateKindFromToken maps the reserved-word token (e.g. "SUM(") to its AggregateKind.
func aggregateKindFromToken(upper string) minisql.AggregateKind {
	switch upper {
	case "SUM(":
		return minisql.AggregateSum
	case "AVG(":
		return minisql.AggregateAvg
	case "MIN(":
		return minisql.AggregateMin
	case "MAX(":
		return minisql.AggregateMax
	default:
		return 0
	}
}

/*
SELECT select_list

	    FROM table_expression
		[ WHERE ... ]
	    [ ORDER BY ... ]
	    [ LIMIT { count | ALL } ]
	    [ OFFSET start ]
*/
func (p *parserItem) doParseSelect() error {
	switch p.step {
	case stepSelectField:
		// Handle optional DISTINCT keyword before the field list
		if len(p.Fields) == 0 && strings.ToUpper(p.peek()) == "DISTINCT" {
			p.Distinct = true
			p.pop()
		}

		identifier := p.peek()
		upperIdent := strings.ToUpper(identifier)
		isAggFunc := aggregateKindFromToken(upperIdent) != 0

		if !isIdentifier(identifier) && identifier != "*" && upperIdent != "COUNT(*)" && !isAggFunc && upperIdent != "NOW()" && upperIdent != "GEN_RANDOM_UUID()" {
			return p.wrapErr(errSelectWithoutFields)
		}

		// Handle aggregate function calls: SUM(col), AVG(col), MIN(col), MAX(col)
		if isAggFunc {
			aggKind := aggregateKindFromToken(upperIdent)
			p.pop() // consume "SUM(" etc.
			colName := p.peek()
			if !isIdentifier(colName) {
				return p.errorf("at SELECT: expected column name in %s", strings.TrimSuffix(upperIdent, "("))
			}
			p.pop() // consume column name
			if p.peek() != ")" {
				return p.errorf("at SELECT: expected ')' after column name in %s", strings.TrimSuffix(upperIdent, "("))
			}
			p.pop() // consume ")"

			// SUM(col) OVER (...) — window aggregate, not a plain aggregate.
			if strings.ToUpper(p.peek()) == "OVER" {
				p.pop() // consume "OVER"
				spec, err := p.parseWindowSpec()
				if err != nil {
					return p.errorf("at SELECT %s OVER: %v", strings.TrimSuffix(upperIdent, "("), err)
				}
				funcName := strings.TrimSuffix(upperIdent, "(")
				kind := windowFuncKind(funcName)
				if kind == 0 {
					return p.errorf("at SELECT: unknown window function %q", funcName)
				}
				colExpr := &minisql.Expr{Column: colName}
				wfExpr := &minisql.Expr{WindowFunc: &minisql.WindowFunc{Kind: kind, Arg: colExpr, Spec: spec}}
				fieldName := funcName + "(" + colName + ") OVER (...)"
				field := minisql.Field{Name: fieldName, Expr: wfExpr}
				if strings.ToUpper(p.peek()) == "AS" {
					p.pop()
					alias := p.peek()
					if !isIdentifier(alias) {
						return p.errorf("at SELECT: expected alias after window function")
					}
					field.Alias = alias
					p.pop()
				}
				if len(p.Aggregates) > 0 {
					p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
				}
				p.Fields = append(p.Fields, field)
				if strings.ToUpper(p.peek()) == "FROM" {
					p.step = stepSelectFrom
					return nil
				}
				p.step = stepSelectComma
				return nil
			}

			// Build synthetic field name e.g. "SUM(price)"
			funcName := strings.TrimSuffix(upperIdent, "(")
			fieldName := funcName + "(" + colName + ")"

			// Keep Aggregates parallel to Fields.
			// If this is the first aggregate, backfill zeros for any regular fields already added.
			for len(p.Aggregates) < len(p.Fields) {
				p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
			}
			p.Fields = append(p.Fields, minisql.Field{Name: fieldName})
			p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{Kind: aggKind, Column: colName})

			// Optional alias: SUM(price) AS total
			if strings.ToUpper(p.peek()) == "AS" {
				p.pop()
				alias := p.peek()
				if !isIdentifier(alias) {
					return p.errorf("at SELECT: expected alias after aggregate function")
				}
				p.Fields[len(p.Fields)-1].Alias = alias
				p.pop()
			}

			maybeFrom := strings.ToUpper(p.peek())
			if maybeFrom == "FROM" {
				p.step = stepSelectFrom
				return nil
			}
			p.step = stepSelectComma
			return nil
		}

		// Handle * for selecting all rows — consume immediately before expression parsing.
		if identifier == "*" {
			p.Fields = append(p.Fields, fieldFromIdentifier(identifier))
			if len(p.Aggregates) > 0 {
				p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
			}
			p.pop()
			maybeFrom := strings.ToUpper(p.peek())
			if len(p.Fields) > 1 {
				return p.wrapErr(errCannotCombineAsterisk)
			}
			if maybeFrom != "FROM" {
				return p.wrapErr(errExpectedFrom)
			}
			p.step = stepSelectFrom
			return nil
		}

		// Handle COUNT(*) special case — consume immediately.
		if upperIdent == "COUNT(*)" {
			p.pop()

			// COUNT(*) OVER (...) — window count.
			if strings.ToUpper(p.peek()) == "OVER" {
				p.pop() // consume "OVER"
				spec, err := p.parseWindowSpec()
				if err != nil {
					return p.errorf("at SELECT COUNT(*) OVER: %v", err)
				}
				wfExpr := &minisql.Expr{WindowFunc: &minisql.WindowFunc{Kind: minisql.WindowCount, Spec: spec}}
				field := minisql.Field{Name: "COUNT(*) OVER (...)", Expr: wfExpr}
				if strings.ToUpper(p.peek()) == "AS" {
					p.pop()
					alias := p.peek()
					if !isIdentifier(alias) {
						return p.errorf("at SELECT: expected alias after COUNT(*) OVER")
					}
					field.Alias = alias
					p.pop()
				}
				if len(p.Aggregates) > 0 {
					p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
				}
				p.Fields = append(p.Fields, field)
				if strings.ToUpper(p.peek()) == "FROM" {
					p.step = stepSelectFrom
					return nil
				}
				p.step = stepSelectComma
				return nil
			}

			p.Fields = append(p.Fields, minisql.Field{Name: identifier})
			if len(p.Aggregates) > 0 {
				p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
			}
			if len(p.Fields) > 1 {
				if len(p.Aggregates) < len(p.Fields)-1 {
					for len(p.Aggregates) < len(p.Fields)-1 {
						p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
					}
				}
				p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{Kind: minisql.AggregateCount})
				// Optional alias: COUNT(*) AS cnt
				if strings.ToUpper(p.peek()) == "AS" {
					p.pop()
					alias := p.peek()
					if !isIdentifier(alias) {
						return p.errorf("at SELECT: expected alias after COUNT(*)")
					}
					p.Fields[len(p.Fields)-1].Alias = alias
					p.pop()
				}
				if strings.ToUpper(p.peek()) == "FROM" {
					p.step = stepSelectFrom
					return nil
				}
				p.step = stepSelectComma
				return nil
			}
			// Optional alias: COUNT(*) AS cnt (single-field case)
			if strings.ToUpper(p.peek()) == "AS" {
				p.pop()
				alias := p.peek()
				if !isIdentifier(alias) {
					return p.errorf("at SELECT: expected alias after COUNT(*)")
				}
				p.Fields[len(p.Fields)-1].Alias = alias
				p.pop()
			}
			if strings.ToUpper(p.peek()) != "FROM" {
				return p.wrapErr(errExpectedFrom)
			}
			p.step = stepSelectFrom
			return nil
		}

		// Normal field or arithmetic expression.
		// Do NOT pop the identifier first — parseExpr() consumes it and any
		// following operators so that "price * 1.1" is parsed as one expression.
		expr, err := p.parseExpr()
		if err != nil {
			return p.wrapErr(errSelectWithoutFields)
		}

		var field minisql.Field
		if expr.Column != "" && expr.Left == nil {
			// Plain column reference — preserve existing alias-prefix behaviour.
			field = fieldFromIdentifier(expr.Column)
		} else {
			// Computed expression — use the expression string as a placeholder name.
			field = minisql.Field{Name: expr.String(), Expr: expr}
		}

		if len(p.Aggregates) > 0 {
			p.Aggregates = append(p.Aggregates, minisql.AggregateExpr{})
		}
		p.Fields = append(p.Fields, field)

		maybeNext := strings.ToUpper(p.peek())
		switch maybeNext {
		case "AS":
			p.pop()
			alias := p.peek()
			if !isIdentifier(alias) {
				return p.errorf(`at SELECT: expected field alias for "identifier as"`)
			}
			// Store alias on the field itself (works for both plain and computed fields).
			p.Fields[len(p.Fields)-1].Alias = alias
			// Also keep the Aliases map for backward compatibility with plain fields.
			if field.Expr == nil {
				if p.Aliases == nil {
					p.Aliases = make(map[string]string)
				}
				p.Aliases[identifier] = alias
			}
			p.pop()
			// After consuming the alias, FROM may follow directly (last field in list).
			if strings.ToUpper(p.peek()) == "FROM" {
				p.step = stepSelectFrom
				return nil
			}
		case "FROM":
			p.step = stepSelectFrom
			return nil
		}

		p.step = stepSelectComma
	case stepSelectComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			return p.errorf("at SELECT: expected comma or FROM")
		}
		p.pop()
		p.step = stepSelectField
	case stepSelectFrom:
		from := strings.ToUpper(p.peek())
		if from != "FROM" {
			return p.wrapErr(errExpectedFrom)
		}
		p.pop()
		p.step = stepSelectFromTable
	case stepSelectFromTable:
		// Derived table: FROM (SELECT ...) alias
		if p.peek() == "(" {
			p.pop() // consume "("
			if strings.ToUpper(p.peek()) != "SELECT" {
				return p.errorf("at SELECT FROM: expected SELECT inside parentheses")
			}
			subStmt, err := p.parseSubquery()
			if err != nil {
				return err
			}
			p.FromSubquery = subStmt
			// Alias is required for derived tables.
			if strings.ToUpper(p.peek()) == "AS" {
				p.pop()
			}
			alias, _ := p.peekIdentifierWithLength()
			if !isIdentifier(alias) {
				return p.errorf("at SELECT FROM: expected alias after derived table subquery")
			}
			p.FromSubqueryAlias = alias
			p.pop()
			p.step = stepSelectJoin
			break
		}

		tableName, _ := p.peekIdentifierWithLength()
		if !isIdentifier(tableName) {
			return p.wrapErr(errSelectExpectedTableName)
		}
		p.TableName = tableName
		p.pop()

		// Check for optional table alias
		if strings.ToUpper(p.peek()) == "AS" {
			p.pop()
			tableAlias, _ := p.peekIdentifierWithLength()
			if !isIdentifier(tableAlias) {
				return p.errorf("at SELECT: expected table alias identifier")
			}
			p.TableAlias = tableAlias
			p.pop()
		}

		p.step = stepSelectJoin
	case stepSelectJoin:
		maybeJoin := strings.ToUpper(p.peek())
		switch maybeJoin {
		case "INNER JOIN":
			p.pop()
			p.joinInProgress.Type = minisql.Inner
			p.step = stepSelectJoinTable
		case "LEFT JOIN":
			p.joinInProgress.Type = minisql.Left
			p.step = stepSelectJoinTable
			p.pop()
		case "RIGHT JOIN":
			p.joinInProgress.Type = minisql.Right
			p.step = stepSelectJoinTable
			p.pop()
		case "FULL OUTER JOIN", "FULL JOIN":
			p.joinInProgress.Type = minisql.FullOuter
			p.step = stepSelectJoinTable
			p.pop()
		default:
			p.step = stepSelectGroupBy
			return nil
		}
	case stepSelectJoinTable:
		tableName, _ := p.peekIdentifierWithLength()
		if !isIdentifier(tableName) {
			return p.errorf("at JOIN: expected table name identifier")
		}
		p.joinInProgress.TableName = tableName
		p.pop()

		// Check for optional table alias
		if strings.ToUpper(p.peek()) == "AS" {
			p.pop()
			tableAlias, _ := p.peekIdentifierWithLength()
			if !isIdentifier(tableAlias) {
				return p.errorf("at JOIN: expected table alias identifier")
			}
			p.joinInProgress.TableAlias = tableAlias
			p.pop()
		}

		// Next we expect the JOIN condition
		if strings.ToUpper(p.peek()) != "ON" {
			return p.errorf("at JOIN: expected ON")
		}
		p.pop()

		p.step = stepSelectJoinConditionField
	case stepSelectJoinConditionField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.errorf("at JOIN: expected field")
		}
		p.joinInProgress.Conditions = append(p.joinInProgress.Conditions, minisql.Condition{
			Operand1: minisql.Operand{
				Type:  minisql.OperandField,
				Value: fieldFromIdentifier(identifier),
			},
		})
		p.pop()
		p.step = stepSelectJoinConditionOperator
	case stepSelectJoinConditionOperator:
		if p.peek() != "=" {
			return p.errorf("at JOIN condition: only '=' operator is supported")
		}
		p.joinInProgress.Conditions[len(p.joinInProgress.Conditions)-1].Operator = minisql.Eq
		p.pop()
		p.step = stepSelectJoinConditionValue
	case stepSelectJoinConditionValue:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return p.errorf("at JOIN: expected field")
		}
		p.joinInProgress.Conditions[len(p.joinInProgress.Conditions)-1].Operand2 = minisql.Operand{
			Type:  minisql.OperandField,
			Value: fieldFromIdentifier(identifier),
		}
		var err error
		p.Statement, err = p.AddJoin(
			p.joinInProgress.Type,
			p.joinInProgress.FromTableAlias(),
			p.joinInProgress.TableName,
			p.joinInProgress.TableAlias,
			p.joinInProgress.Conditions,
		)
		if err != nil {
			return err
		}
		p.joinInProgress = minisql.Join{}
		p.pop()
		p.step = stepSelectJoin
	case stepSelectGroupBy:
		if strings.ToUpper(p.peek()) != "GROUP BY" {
			p.step = stepSelectOrderBy
			return nil
		}
		p.pop() // consume "GROUP BY"
		p.step = stepSelectGroupByComma
	case stepSelectGroupByComma:
		// Each invocation parses one column name and then checks for a following comma.
		identifier := p.peek()
		if !isIdentifier(identifier) {
			if len(p.GroupBy) == 0 {
				return p.errorf("at GROUP BY: expected column name")
			}
			p.step = stepSelectHaving
			return nil
		}
		p.pop()
		p.GroupBy = append(p.GroupBy, minisql.Field{Name: identifier})
		if p.peek() == "," {
			p.pop() // consume ","
			// Stay in stepSelectGroupByComma to parse the next column.
		} else {
			p.step = stepSelectHaving
		}
	case stepSelectHaving:
		if strings.ToUpper(p.peek()) != "HAVING" {
			p.step = stepSelectOrderBy
			return nil
		}
		p.pop() // consume "HAVING"
		node, err := p.parseCondExpr()
		if err != nil {
			return err
		}
		p.Having = node.ToDNF()
		next := strings.ToUpper(p.peek())
		if next == "ORDER BY" || next == "LIMIT" || next == "OFFSET" {
			p.step = stepSelectOrderBy
		} else {
			p.step = stepStatementEnd
		}
	case stepSelectOrderBy:
		offsetRWord := p.peek()
		if strings.ToUpper(offsetRWord) != "ORDER BY" {
			p.step = stepSelectLimit
			return nil
		}
		p.pop()
		p.step = stepSelectOrderByField
	case stepSelectOrderByField:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			if len(p.OrderBy) == 0 {
				return p.errorf(`at ORDER BY: expected identifier`)
			}
			p.step = stepSelectLimit
			return nil
		}
		if identifier == "*" {
			return p.errorf(`at ORDER BY: cannot order by "*"`)
		}
		p.pop()

		// Parse field name and optional alias prefix (e.g., "u.name" -> prefix="u", name="name")
		var fieldName, aliasPrefix string
		if dotIndex := strings.Index(identifier, "."); dotIndex != -1 {
			aliasPrefix = identifier[:dotIndex]
			fieldName = identifier[dotIndex+1:]
		} else {
			fieldName = identifier
		}

		// Start with default direction as ASC
		theDirection := minisql.Asc
		if direction := strings.ToUpper(p.peek()); direction == "ASC" || direction == "DESC" {
			if direction == "DESC" {
				theDirection = minisql.Desc
			}
			p.pop()
		}
		p.OrderBy = append(p.OrderBy, minisql.OrderBy{
			Field:     minisql.Field{Name: fieldName, AliasPrefix: aliasPrefix},
			Direction: theDirection,
		})
		p.step = stepSelectOrderByComma
	case stepSelectOrderByComma:
		commaRWord := p.peek()
		if commaRWord != "," {
			p.step = stepSelectLimit
			return nil
		}
		p.pop()
		p.step = stepSelectOrderByField
	case stepSelectLimit:
		limitRWord := p.peek()
		if strings.ToUpper(limitRWord) != "LIMIT" {
			p.step = stepSelectOffset
			return nil
		}
		p.pop()
		limitValue, n := p.peekIntWithLength()
		if n == 0 {
			return p.errorf("at SELECT: expected integer value for LIMIT")
		}
		p.Limit = minisql.OptionalValue{Value: limitValue, Valid: true}
		p.pop()
		p.step = stepSelectOffset
	case stepSelectOffset:
		offsetRWord := p.peek()
		if strings.ToUpper(offsetRWord) != "OFFSET" {
			if !p.Offset.Valid {
				p.step = stepWhere
				return nil
			}
			p.step = stepStatementEnd
			return nil
		}
		p.pop()
		offsetValue, n := p.peekIntWithLength()
		if n == 0 {
			return p.errorf("at SELECT: expected integer value for OFFSET")
		}
		p.Offset = minisql.OptionalValue{Value: offsetValue, Valid: true}
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}

func fieldFromIdentifier(identifier string) minisql.Field {
	if parts := strings.SplitN(identifier, ".", 2); len(parts) == 2 {
		return minisql.Field{
			AliasPrefix: parts[0],
			Name:        parts[1],
		}
	}
	return minisql.Field{Name: identifier}
}
