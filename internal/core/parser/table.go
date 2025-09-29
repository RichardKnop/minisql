package parser

import (
	"fmt"
	"strconv"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

const (
	varcharMaxLength = 65535
)

var (
	errCreateTableNoColumns       = fmt.Errorf("at CREATE TABLE: no columns specified")
	errCreateTableInvalidColumDef = fmt.Errorf("at CREATE TABLE: invalid column definition")
)

func (p *parser) doParseCreateTable() (bool, error) {
	switch p.step {
	case stepCreateTableName:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at CREATE TABLE: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepCreateTableOpeningParens
	case stepCreateTableOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return false, fmt.Errorf("at CREATE TABLE: expected opening parens")
		}
		p.pop()
		p.step = stepCreateTableColumn
	case stepCreateTableColumn:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return false, errCreateTableNoColumns
		}
		p.Columns = append(p.Columns, minisql.Column{
			Name: identifier,
		})
		p.pop()
		p.step = stepCreateTableColumnDef
	case stepCreateTableColumnDef:
		columnDef := p.peek()
		aColumn, ok := isColumnDef(columnDef)
		if !ok {
			return false, errCreateTableInvalidColumDef
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Kind = aColumn.Kind
		if aColumn.Kind == minisql.Varchar {
			p.step = stepCreateTableVarcharLength
		} else {
			p.Columns[len(p.Columns)-1].Size = aColumn.Size
			p.step = stepCreateTableCommaOrClosingParens
		}
	case stepCreateTableVarcharLength:
		sizeToken := p.peek()
		size, err := strconv.Atoi(sizeToken)
		if err != nil {
			return false, fmt.Errorf("at CREATE TABLE: varchar size '%s' must be an integer", sizeToken)
		}
		if size <= 0 {
			return false, fmt.Errorf("at CREATE TABLE: varchar size must be a positive integer")
		}
		if size > varcharMaxLength {
			return false, fmt.Errorf("at CREATE TABLE: varchar size must be > 0 and <= %d", varcharMaxLength)
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Size = uint32(size)
		closingParens := p.peek()
		if closingParens != ")" {
			return false, fmt.Errorf("at CREATE TABLE: expecting closing parenthesis after varchar size")
		}
		p.pop()
		p.step = stepCreateTableCommaOrClosingParens
	case stepCreateTableCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return false, fmt.Errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateTableColumn
			return true, nil
		}
	}
	return false, nil
}

func (p *parser) doParseDropTable() (bool, error) {
	switch p.step {
	case stepDropTableName:
		tableName := p.peek()
		if len(tableName) == 0 {
			return false, fmt.Errorf("at DROP TABLE: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
	}
	return false, nil
}

func isColumnDef(token string) (minisql.Column, bool) {
	switch token {
	case "INT4":
		return minisql.Column{Kind: minisql.Int4, Size: 4}, true
	case "INT8":
		return minisql.Column{Kind: minisql.Int8, Size: 8}, true
	case "VARCHAR(":
		return minisql.Column{Kind: minisql.Varchar}, true
	default:
		return minisql.Column{}, false
	}
}
