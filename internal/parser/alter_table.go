package parser

import (
	"errors"
	"strconv"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var (
	errAlterTableExpectedAction = errors.New("at ALTER TABLE: expected ADD COLUMN, DROP COLUMN, RENAME COLUMN, or RENAME TO")
)

func (p *parserItem) doParseAlterTable() error {
	switch p.step {
	case stepAlterTableName:
		name := p.peek()
		if !isIdentifier(name) {
			return p.errorf("at ALTER TABLE: expected table name, got %q", name)
		}
		p.TableName = name
		p.pop()
		p.step = stepAlterTableAction

	case stepAlterTableAction:
		action := strings.ToUpper(p.peek())
		switch action {
		case "ADD COLUMN":
			p.AlterTableAction = minisql.AlterTableAddColumn
			p.pop()
			p.step = stepAlterTableAddColumnName
		case "DROP COLUMN":
			p.AlterTableAction = minisql.AlterTableDropColumn
			p.pop()
			p.step = stepAlterTableDropColumnName
		case "RENAME COLUMN":
			p.AlterTableAction = minisql.AlterTableRenameColumn
			p.pop()
			p.step = stepAlterTableRenameColumnOldName
		case "RENAME TO":
			p.AlterTableAction = minisql.AlterTableRenameTo
			p.pop()
			p.step = stepAlterTableRenameTo
		default:
			return p.wrapErr(errAlterTableExpectedAction)
		}

	case stepAlterTableAddColumnName:
		name := p.peek()
		if !isIdentifier(name) {
			return p.errorf("at ALTER TABLE ADD COLUMN: expected column name, got %q", name)
		}
		p.Columns = append(p.Columns, minisql.Column{Name: name, Nullable: true})
		p.pop()
		p.step = stepAlterTableAddColumnType

	case stepAlterTableAddColumnType:
		typeTok := p.peek()
		col, ok := isColumnDef(typeTok)
		if !ok {
			return p.errorf("at ALTER TABLE ADD COLUMN: invalid column type %q", typeTok)
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Kind = col.Kind
		if col.Kind == minisql.Varchar {
			p.step = stepAlterTableAddColumnVarcharLen
		} else {
			p.Columns[len(p.Columns)-1].Size = col.Size
			p.step = stepAlterTableAddColumnConstraints
		}

	case stepAlterTableAddColumnVarcharLen:
		sizeToken := p.peek()
		size, err := strconv.ParseUint(sizeToken, 10, 32)
		if err != nil {
			return p.errorf("at ALTER TABLE ADD COLUMN: varchar size %q must be an integer", sizeToken)
		}
		if size == 0 {
			return p.errorf("at ALTER TABLE ADD COLUMN: varchar size must be a positive integer")
		}
		p.pop()
		p.Columns[len(p.Columns)-1].Size = uint32(size)
		closing := p.peek()
		if closing != ")" {
			return p.errorf("at ALTER TABLE ADD COLUMN: expected ')' after varchar size")
		}
		p.pop()
		p.step = stepAlterTableAddColumnConstraints

	case stepAlterTableAddColumnConstraints:
		tok := strings.ToUpper(p.peek())
		switch tok {
		case "NOT NULL":
			p.Columns[len(p.Columns)-1].Nullable = false
			p.pop()
		case "NULL":
			p.Columns[len(p.Columns)-1].Nullable = true
			p.pop()
		case "DEFAULT":
			p.pop()
			if strings.ToUpper(p.peek()) == "NOW()" {
				if p.Columns[len(p.Columns)-1].Kind != minisql.Timestamp {
					return p.errorf("at ALTER TABLE ADD COLUMN: NOW() default is only valid for TIMESTAMP columns")
				}
				p.Columns[len(p.Columns)-1].DefaultValueNow = true
				p.pop()
			} else {
				defaultValue, n := p.peekValue()
				if n == 0 {
					return p.errorf("at ALTER TABLE ADD COLUMN: expected default value after DEFAULT")
				}
				if err := isDefaultValueValid(p.Columns[len(p.Columns)-1], defaultValue); err != nil {
					return err
				}
				p.pop()
				if _, ok := defaultValue.(string); ok {
					defaultValue = minisql.NewTextPointer([]byte(defaultValue.(string)))
				}
				p.Columns[len(p.Columns)-1].DefaultValue = minisql.OptionalValue{
					Value: defaultValue,
					Valid: true,
				}
			}
		default:
			// No more column constraints.
			p.step = stepStatementEnd
			return nil
		}
		// Stay in this step to handle multiple constraints (e.g. NOT NULL DEFAULT val).

	case stepAlterTableDropColumnName:
		name := p.peek()
		if !isIdentifier(name) {
			return p.errorf("at ALTER TABLE DROP COLUMN: expected column name, got %q", name)
		}
		p.AlterColumnName = name
		p.pop()
		p.step = stepStatementEnd

	case stepAlterTableRenameColumnOldName:
		name := p.peek()
		if !isIdentifier(name) {
			return p.errorf("at ALTER TABLE RENAME COLUMN: expected column name, got %q", name)
		}
		p.AlterColumnName = name
		p.pop()
		p.step = stepAlterTableRenameColumnTo

	case stepAlterTableRenameColumnTo:
		tok := strings.ToUpper(p.peek())
		if tok != "TO" {
			return p.errorf("at ALTER TABLE RENAME COLUMN: expected TO, got %q", tok)
		}
		p.pop()
		p.step = stepAlterTableRenameColumnNewName

	case stepAlterTableRenameColumnNewName:
		name := p.peek()
		if !isIdentifier(name) {
			return p.errorf("at ALTER TABLE RENAME COLUMN: expected new column name, got %q", name)
		}
		p.NewColumnName = name
		p.pop()
		p.step = stepStatementEnd

	case stepAlterTableRenameTo:
		name := p.peek()
		if !isIdentifier(name) {
			return p.errorf("at ALTER TABLE RENAME TO: expected new table name, got %q", name)
		}
		p.NewTableName = name
		p.pop()
		p.step = stepStatementEnd
	}
	return nil
}
