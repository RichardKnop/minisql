package minisql

import (
	"context"
	"fmt"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

// executeAlterTable dispatches an ALTER TABLE statement to the appropriate
// sub-executor based on its AlterTableAction field.
func (d *Database) executeAlterTable(ctx context.Context, stmt Statement) error {
	_, exists, err := d.checkSchemaExists(ctx, SchemaTable, stmt.TableName)
	if err != nil {
		return err
	}
	if !exists {
		return minisqlErrors.ErrNoSuchTable{Name: stmt.TableName}
	}

	switch stmt.AlterTableAction {
	case AlterTableAddColumn:
		return d.alterTableAddColumn(ctx, stmt)
	case AlterTableDropColumn:
		return d.alterTableDropColumn(ctx, stmt)
	case AlterTableRenameColumn:
		return d.alterTableRenameColumn(ctx, stmt)
	case AlterTableRenameTo:
		return d.alterTableRenameTo(ctx, stmt)
	default:
		return fmt.Errorf("unknown ALTER TABLE action: %d", stmt.AlterTableAction)
	}
}

// alterTableAddColumn appends a new column to the table schema. Existing rows
// return the column's declared default via the lazy ADD COLUMN mechanism in RowView
// (rows written before the ADD COLUMN have a smaller ColumnCount; RowView returns
// the column's DefaultValue for positions >= ColumnCount).
func (d *Database) alterTableAddColumn(ctx context.Context, stmt Statement) error {
	table := d.tables[stmt.TableName]
	newCol := stmt.Columns[0]

	for _, col := range table.Columns {
		if !col.Deleted && col.Name == newCol.Name {
			return fmt.Errorf("column %q already exists in table %q", newCol.Name, stmt.TableName)
		}
	}

	live := 0
	for _, col := range table.Columns {
		if !col.Deleted {
			live += 1
		}
	}
	if live+1 > MaxColumns {
		return fmt.Errorf("table %q already has %d columns (max %d)", stmt.TableName, live, MaxColumns)
	}

	table.Columns = append(table.Columns, newCol)
	table.columnCache[newCol.Name] = len(table.Columns) - 1

	return d.updateTableSchema(ctx, table)
}

// alterTableDropColumn marks a column as Deleted=true (tombstone). Old cells still
// carry the column's bytes; TypeCode tells the decoder how to skip them. New cells
// written after the DROP write TypeCodeNull (0 bytes) for the dropped slot.
func (d *Database) alterTableDropColumn(ctx context.Context, stmt Statement) error {
	table := d.tables[stmt.TableName]

	colIdx := -1
	for i, col := range table.Columns {
		if !col.Deleted && col.Name == stmt.AlterColumnName {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("column %q not found in table %q", stmt.AlterColumnName, stmt.TableName)
	}

	col := table.Columns[colIdx]

	for _, pkCol := range table.PrimaryKey.Columns {
		if pkCol.Name == col.Name {
			return fmt.Errorf("cannot drop primary key column %q", col.Name)
		}
	}
	for _, si := range table.SecondaryIndexes {
		for _, siCol := range si.Columns {
			if siCol.Name == col.Name {
				return fmt.Errorf("cannot drop column %q: referenced by index %q", col.Name, si.Name)
			}
		}
	}
	for _, ui := range table.UniqueIndexes {
		for _, uiCol := range ui.Columns {
			if uiCol.Name == col.Name {
				return fmt.Errorf("cannot drop column %q: referenced by unique index %q", col.Name, ui.Name)
			}
		}
	}
	if table.referencedColumns[col.Name] {
		return fmt.Errorf("cannot drop column %q: referenced by a foreign key constraint", col.Name)
	}

	table.Columns[colIdx].Deleted = true
	delete(table.columnCache, col.Name)

	return d.updateTableSchema(ctx, table)
}

// alterTableRenameColumn renames a column in the schema. The B+ tree is untouched;
// only the schema DDL and in-memory column cache are updated.
func (d *Database) alterTableRenameColumn(ctx context.Context, stmt Statement) error {
	table := d.tables[stmt.TableName]

	colIdx := -1
	for i, col := range table.Columns {
		if !col.Deleted && col.Name == stmt.AlterColumnName {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("column %q not found in table %q", stmt.AlterColumnName, stmt.TableName)
	}
	for _, col := range table.Columns {
		if !col.Deleted && col.Name == stmt.NewColumnName {
			return fmt.Errorf("column %q already exists in table %q", stmt.NewColumnName, stmt.TableName)
		}
	}

	oldName := table.Columns[colIdx].Name
	table.Columns[colIdx].Name = stmt.NewColumnName
	delete(table.columnCache, oldName)
	table.columnCache[stmt.NewColumnName] = colIdx

	return d.updateTableSchema(ctx, table)
}

// alterTableRenameTo renames the table. It updates the table schema entry and all
// associated index schema entries (PK, unique, secondary) so they reference the new
// name. The in-memory d.tables map and Table.Name are updated accordingly.
func (d *Database) alterTableRenameTo(ctx context.Context, stmt Statement) error {
	oldName := stmt.TableName
	newName := stmt.NewTableName

	if _, newExists, err := d.checkSchemaExists(ctx, SchemaTable, newName); err != nil {
		return err
	} else if newExists {
		return fmt.Errorf("table %q already exists", newName)
	}

	table := d.tables[oldName]

	// Rename the table in-memory first so tableStatementFromTable picks up the new name.
	table.Name = newName

	// Update the table schema entry.
	if err := d.deleteSchema(ctx, SchemaTable, oldName); err != nil {
		table.Name = oldName
		return err
	}
	if err := d.insertSchema(ctx, Schema{
		Type:     SchemaTable,
		Name:     newName,
		RootPage: table.rootPageIdx,
		DDL:      tableStatementFromTable(table).DDL(),
	}); err != nil {
		table.Name = oldName
		return err
	}

	// Update primary key schema entry.
	if table.HasPrimaryKey() {
		if err := d.deleteSchema(ctx, SchemaPrimaryKey, table.PrimaryKey.Name); err != nil {
			return err
		}
		if err := d.insertSchema(ctx, Schema{
			Type:      SchemaPrimaryKey,
			Name:      table.PrimaryKey.Name,
			TableName: newName,
			RootPage:  table.PrimaryKey.Index.GetRootPageIdx(),
		}); err != nil {
			return err
		}
	}

	// Update unique index schema entries.
	for _, ui := range table.UniqueIndexes {
		if err := d.deleteSchema(ctx, SchemaUniqueIndex, ui.Name); err != nil {
			return err
		}
		if err := d.insertSchema(ctx, Schema{
			Type:      SchemaUniqueIndex,
			Name:      ui.Name,
			TableName: newName,
			RootPage:  ui.Index.GetRootPageIdx(),
		}); err != nil {
			return err
		}
	}

	// Update secondary index schema entries (DDL contains the old table name).
	for _, si := range table.SecondaryIndexes {
		updatedDDL := rewriteIndexDDLTableName(si, oldName, newName)
		if err := d.deleteSchema(ctx, SchemaSecondaryIndex, si.Name); err != nil {
			return err
		}
		if err := d.insertSchema(ctx, Schema{
			Type:      SchemaSecondaryIndex,
			Name:      si.Name,
			TableName: newName,
			DDL:       updatedDDL,
			RootPage:  si.Index.GetRootPageIdx(),
		}); err != nil {
			return err
		}
	}

	// Update in-memory tables map.
	delete(d.tables, oldName)
	d.tables[newName] = table

	// Update FK references: if other tables reference this table, update the map key.
	if inbounds, ok := d.referencedBy[oldName]; ok {
		d.referencedBy[newName] = inbounds
		delete(d.referencedBy, oldName)
	}
	// Update outgoing FK references from this table to match the new name.
	for targetTable, inbounds := range d.referencedBy {
		for i, inbound := range inbounds {
			if inbound.ChildTable == oldName {
				d.referencedBy[targetTable][i].ChildTable = newName
			}
		}
	}

	return nil
}

// updateTableSchema rebuilds the DDL for a table and replaces its schema entry.
func (d *Database) updateTableSchema(ctx context.Context, table *Table) error {
	if err := d.deleteSchema(ctx, SchemaTable, table.Name); err != nil {
		return err
	}
	return d.insertSchema(ctx, Schema{
		Type:     SchemaTable,
		Name:     table.Name,
		RootPage: table.rootPageIdx,
		DDL:      tableStatementFromTable(table).DDL(),
	})
}

// tableStatementFromTable constructs a Statement suitable for DDL generation from
// the current in-memory Table state (used by all ALTER TABLE sub-executors).
func tableStatementFromTable(t *Table) Statement {
	stmt := Statement{
		Kind:        CreateTable,
		TableName:   t.Name,
		Columns:     t.Columns,
		PrimaryKey:  t.PrimaryKey,
		ForeignKeys: t.ForeignKeys,
	}
	for _, ui := range t.UniqueIndexes {
		stmt.UniqueIndexes = append(stmt.UniqueIndexes, ui)
	}
	return stmt
}

// rewriteIndexDDLTableName rebuilds the CREATE INDEX DDL for si using the new table
// name in the ON clause.
func rewriteIndexDDLTableName(si SecondaryIndex, _, newTable string) string {
	s := Statement{
		Kind:               CreateIndex,
		IndexName:          si.Name,
		TableName:          newTable,
		IndexMethod:        si.Method,
		IndexTokenizer:     si.Tokenizer,
		IndexWhereClause:   si.WhereClause,
		IndexExpression:    si.Expression,
		IndexExpressionSQL: si.ExpressionSQL,
		Columns:            si.Columns,
	}
	return s.DDL()
}
