package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

const (
	varcharMaxLength = 65535
)

var (
	errCreateTableNoColumns       = fmt.Errorf("at CREATE TABLE: no columns specified")
	errCreateTableInvalidColumDef = fmt.Errorf("at CREATE TABLE: invalid column definition")
)

func (p *parser) doParseCreateTable() error {
	switch p.step {
	case stepCreateTableIfNotExists:
		ifnotExists := p.peek()
		p.step = stepCreateTableName
		if strings.ToUpper(ifnotExists) != "IF NOT EXISTS" {
			return nil
		}
		p.IfNotExists = true
		p.pop()
		p.step = stepCreateTableName
	case stepCreateTableName:
		tableName := p.peek()
		if len(tableName) == 0 {
			return fmt.Errorf("at CREATE TABLE: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
		p.step = stepCreateTableOpeningParens
	case stepCreateTableOpeningParens:
		openingParens := p.peek()
		if len(openingParens) != 1 || openingParens != "(" {
			return fmt.Errorf("at CREATE TABLE: expected opening parens")
		}
		p.pop()
		p.step = stepCreateTableColumn
	case stepCreateTableColumn:
		identifier := p.peek()
		if !isIdentifier(identifier) {
			return errCreateTableNoColumns
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
			return errCreateTableInvalidColumDef
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Kind = aColumn.Kind
		if aColumn.Kind == minisql.Varchar {
			p.step = stepCreateTableVarcharLength
		} else {
			p.Columns[len(p.Columns)-1].Size = aColumn.Size
			p.step = stepCreateTableColumnNullNotNull
		}
	case stepCreateTableVarcharLength:
		sizeToken := p.peek()
		size, err := strconv.Atoi(sizeToken)
		if err != nil {
			return fmt.Errorf("at CREATE TABLE: varchar size '%s' must be an integer", sizeToken)
		}
		if size <= 0 {
			return fmt.Errorf("at CREATE TABLE: varchar size must be a positive integer")
		}
		if size > varcharMaxLength {
			return fmt.Errorf("at CREATE TABLE: varchar size must be > 0 and <= %d", varcharMaxLength)
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Size = uint32(size)
		closingParens := p.peek()
		if closingParens != ")" {
			return fmt.Errorf("at CREATE TABLE: expecting closing parenthesis after varchar size")
		}
		p.pop()
		p.step = stepCreateTableColumnNullNotNull
	case stepCreateTableColumnNullNotNull:
		nullNotNull := p.peek()
		p.step = stepCreateTableCommaOrClosingParens
		switch nullNotNull {
		case "NOT NULL":
			p.Columns[len(p.Columns)-1].Nullable = false
		case "NULL":
			p.Columns[len(p.Columns)-1].Nullable = true
		default:
			// Default to nullable if not specified
			p.Columns[len(p.Columns)-1].Nullable = true
			return nil
		}
		p.pop()
	case stepCreateTableCommaOrClosingParens:
		commaOrClosingParens := p.peek()
		if commaOrClosingParens != "," && commaOrClosingParens != ")" {
			return fmt.Errorf("at CREATE TABLE: expected comma or closing parens")
		}
		p.pop()
		if commaOrClosingParens == "," {
			p.step = stepCreateTableColumn
			return nil
		}
	}
	return nil
}

func (p *parser) doParseDropTable() error {
	switch p.step {
	case stepDropTableName:
		tableName := p.peek()
		if len(tableName) == 0 {
			return fmt.Errorf("at DROP TABLE: expected quoted table name")
		}
		p.TableName = tableName
		p.pop()
	}
	return nil
}

func isColumnDef(token string) (minisql.Column, bool) {
	switch strings.ToUpper(token) {
	case "BOOLEAN":
		return minisql.Column{Kind: minisql.Boolean, Size: 1}, true
	case "INT4":
		return minisql.Column{Kind: minisql.Int4, Size: 4}, true
	case "INT8":
		return minisql.Column{Kind: minisql.Int8, Size: 8}, true
	case "REAL":
		return minisql.Column{Kind: minisql.Real, Size: 4}, true
	case "DOUBLE":
		return minisql.Column{Kind: minisql.Double, Size: 8}, true
	case "VARCHAR(":
		return minisql.Column{Kind: minisql.Varchar}, true
	default:
		return minisql.Column{}, false
	}
}
