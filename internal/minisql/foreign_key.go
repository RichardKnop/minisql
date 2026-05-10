package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

// FKAction defines the referential action for ON DELETE / ON UPDATE.
type FKAction int

const (
	// FKActionRestrict rejects the operation if it would break the FK constraint.
	FKActionRestrict FKAction = iota + 1
	// FKActionNoAction is the same as RESTRICT in this implementation (no deferred checks).
	FKActionNoAction
	// FKActionSetNull sets the FK column(s) to NULL when the referenced row is deleted/updated.
	FKActionSetNull
	// FKActionCascade propagates the delete/update to child rows.
	FKActionCascade
)

func (a FKAction) String() string {
	switch a {
	case FKActionRestrict:
		return "restrict"
	case FKActionNoAction:
		return "no action"
	case FKActionSetNull:
		return "set null"
	case FKActionCascade:
		return "cascade"
	default:
		return "restrict"
	}
}

// ForeignKey describes a FOREIGN KEY constraint on one or more child table columns.
type ForeignKey struct {
	Name          string
	Columns       []string // local (child) columns
	TargetTable   string   // referenced (parent) table
	TargetColumns []string // referenced columns in parent table
	OnDelete      FKAction // default FKActionRestrict
	OnUpdate      FKAction // default FKActionRestrict
}

// inboundFK records a child-side FK pointing at a given parent table.
type inboundFK struct {
	ChildTable string
	FK         ForeignKey
}

// AutoFKName builds a deterministic FK constraint name from the involved tables and columns.
func AutoFKName(childTable, parentTable string, columns []string) string {
	return fmt.Sprintf("fk__%s__%s__%s", childTable, parentTable, strings.Join(columns, "_"))
}

// fkCascadeContextKey is used to signal that we are executing inside a FK cascade
// so that recursive parent-FK enforcement is suppressed for the cascading table.
type fkCascadeContextKey struct{}

// errFKScanDone is a sentinel returned by the scan callback to stop early.
var errFKScanDone = errors.New("fk: scan done")

// checkChildFK verifies all outgoing FK constraints for a new/updated row in childTable.
// Must be called while d.dbLock is held (write).
func (d *Database) checkChildFK(ctx context.Context, childTable *Table, row Row) error {
	if !d.foreignKeysEnabled {
		return nil
	}
	for _, fk := range childTable.ForeignKeys {
		// Collect FK column values from the row.
		values := make([]any, len(fk.Columns))
		anyNonNull := false
		for i, colName := range fk.Columns {
			idx := -1
			for j, col := range childTable.Columns {
				if col.Name == colName {
					idx = j
					break
				}
			}
			if idx < 0 || idx >= len(row.Values) {
				continue
			}
			if row.Values[idx].Valid {
				values[i] = row.Values[idx].Value
				anyNonNull = true
			}
		}
		if !anyNonNull {
			continue // all FK columns are NULL — permitted
		}

		parentTable, ok := d.tables[fk.TargetTable]
		if !ok {
			return fmt.Errorf("FK %q references unknown table %q", fk.Name, fk.TargetTable)
		}

		exists, err := d.fkValuesExistInParent(ctx, parentTable, fk.TargetColumns, values)
		if err != nil {
			return fmt.Errorf("FK check %q: %w", fk.Name, err)
		}
		if !exists {
			return minisqlErrors.ErrForeignKeyViolation{
				ChildTable:    childTable.Name,
				ChildColumns:  fk.Columns,
				ParentTable:   fk.TargetTable,
				ParentColumns: fk.TargetColumns,
			}
		}
	}
	return nil
}

// enforceParentFKOnDelete enforces all inbound FK constraints for a row being deleted
// from parentTable. Handles RESTRICT/NO ACTION (error), CASCADE (delete child rows),
// and SET NULL (null out FK columns in child rows).
// Must be called while d.dbLock is held (write).
func (d *Database) enforceParentFKOnDelete(ctx context.Context, parentTable *Table, oldRow Row) error {
	if !d.foreignKeysEnabled {
		return nil
	}
	// Suppress recursive parent enforcement during cascade operations.
	cascadeCtx := context.WithValue(ctx, fkCascadeContextKey{}, parentTable.Name)

	inbounds := d.referencedBy[parentTable.Name]
	for _, inbound := range inbounds {
		parentVals, anyNonNull := fkExtractValues(parentTable.Columns, inbound.FK.TargetColumns, oldRow)
		if !anyNonNull {
			continue
		}

		childTable, ok := d.tables[inbound.ChildTable]
		if !ok {
			continue
		}

		switch inbound.FK.OnDelete {
		case FKActionRestrict, FKActionNoAction:
			found, err := d.fkChildHasValues(ctx, childTable, inbound.FK.Columns, parentVals)
			if err != nil {
				return fmt.Errorf("FK parent check for %s.%v: %w", parentTable.Name, inbound.FK.TargetColumns, err)
			}
			if found {
				return minisqlErrors.ErrForeignKeyParentViolation{
					ParentTable:   parentTable.Name,
					ParentColumns: inbound.FK.TargetColumns,
					ChildTable:    inbound.ChildTable,
					ChildColumns:  inbound.FK.Columns,
				}
			}
		case FKActionCascade:
			if err := d.cascadeDeleteChildRows(cascadeCtx, childTable, inbound.FK.Columns, parentVals); err != nil {
				return fmt.Errorf("FK cascade delete on %s: %w", inbound.ChildTable, err)
			}
		case FKActionSetNull:
			if err := d.setNullChildRows(cascadeCtx, childTable, inbound.FK.Columns, parentVals); err != nil {
				return fmt.Errorf("FK set null on %s: %w", inbound.ChildTable, err)
			}
		}
	}
	return nil
}

// enforceParentFKOnUpdate enforces all inbound FK constraints for a row being updated
// in parentTable. Handles RESTRICT/NO ACTION (error), CASCADE (update FK value in child
// rows), and SET NULL (null out FK columns in child rows).
// Must be called while d.dbLock is held (write).
func (d *Database) enforceParentFKOnUpdate(ctx context.Context, parentTable *Table, oldRow, newRow Row) error {
	if !d.foreignKeysEnabled {
		return nil
	}
	cascadeCtx := context.WithValue(ctx, fkCascadeContextKey{}, parentTable.Name)

	inbounds := d.referencedBy[parentTable.Name]
	for _, inbound := range inbounds {
		oldVals, anyNonNull := fkExtractValues(parentTable.Columns, inbound.FK.TargetColumns, oldRow)
		if !anyNonNull {
			continue
		}
		newVals, _ := fkExtractValues(parentTable.Columns, inbound.FK.TargetColumns, newRow)

		// Only act if the referenced column values actually changed.
		if fkSliceEqual(oldVals, newVals) {
			continue
		}

		childTable, ok := d.tables[inbound.ChildTable]
		if !ok {
			continue
		}

		switch inbound.FK.OnUpdate {
		case FKActionRestrict, FKActionNoAction:
			found, err := d.fkChildHasValues(ctx, childTable, inbound.FK.Columns, oldVals)
			if err != nil {
				return fmt.Errorf("FK parent update check for %s.%v: %w", parentTable.Name, inbound.FK.TargetColumns, err)
			}
			if found {
				return minisqlErrors.ErrForeignKeyParentViolation{
					ParentTable:   parentTable.Name,
					ParentColumns: inbound.FK.TargetColumns,
					ChildTable:    inbound.ChildTable,
					ChildColumns:  inbound.FK.Columns,
				}
			}
		case FKActionCascade:
			if err := d.cascadeUpdateChildRows(cascadeCtx, childTable, inbound.FK.Columns, oldVals, newVals); err != nil {
				return fmt.Errorf("FK cascade update on %s: %w", inbound.ChildTable, err)
			}
		case FKActionSetNull:
			if err := d.setNullChildRows(cascadeCtx, childTable, inbound.FK.Columns, oldVals); err != nil {
				return fmt.Errorf("FK set null on %s: %w", inbound.ChildTable, err)
			}
		}
	}
	return nil
}

// cascadeDeleteChildRows deletes all rows in childTable where colNames == values.
func (d *Database) cascadeDeleteChildRows(ctx context.Context, childTable *Table, colNames []string, values []any) error {
	rows, err := d.fkChildFindRows(ctx, childTable, colNames, values)
	if err != nil {
		return err
	}
	for _, row := range rows {
		// Enforce inbound FKs on the child being deleted (the child may itself be a parent).
		if err := d.enforceParentFKOnDelete(ctx, childTable, row); err != nil {
			return err
		}
		cursor, err := childTable.Seek(ctx, row.Key)
		if err != nil {
			return err
		}
		if err := cursor.delete(ctx, row); err != nil {
			return err
		}
		if childTable.getRowCount != nil {
			if tx := TxFromContext(ctx); tx != nil {
				tx.AddRowCountDelta(childTable.Name, -1)
			}
		}
	}
	return nil
}

// cascadeUpdateChildRows updates FK column(s) in all matching child rows from oldValues
// to newValues.
func (d *Database) cascadeUpdateChildRows(ctx context.Context, childTable *Table, colNames []string, oldValues, newValues []any) error {
	rows, err := d.fkChildFindRows(ctx, childTable, colNames, oldValues)
	if err != nil {
		return err
	}
	for _, row := range rows {
		updates := make(map[string]OptionalValue, len(colNames))
		for i, colName := range colNames {
			if newValues[i] == nil {
				updates[colName] = OptionalValue{}
			} else {
				updates[colName] = OptionalValue{Value: newValues[i], Valid: true}
			}
		}
		stmt := Statement{
			Kind:      Update,
			TableName: childTable.Name,
			Columns:   childTable.Columns,
			Fields:    fieldsFromColumns(childTable.Columns...),
			Updates:   updates,
		}
		cursor, err := childTable.Seek(ctx, row.Key)
		if err != nil {
			return err
		}
		// Temporarily detach parent FK callback to avoid re-checking the parent
		// we are already cascading from.
		orig := childTable.checkParentFK
		childTable.checkParentFK = nil
		_, updateErr := cursor.update(ctx, stmt, row)
		childTable.checkParentFK = orig
		if updateErr != nil {
			return updateErr
		}
	}
	return nil
}

// setNullChildRows sets the FK column(s) to NULL in all matching child rows.
func (d *Database) setNullChildRows(ctx context.Context, childTable *Table, colNames []string, values []any) error {
	rows, err := d.fkChildFindRows(ctx, childTable, colNames, values)
	if err != nil {
		return err
	}
	for _, row := range rows {
		updates := make(map[string]OptionalValue, len(colNames))
		for _, colName := range colNames {
			updates[colName] = OptionalValue{} // NULL
		}
		stmt := Statement{
			Kind:      Update,
			TableName: childTable.Name,
			Columns:   childTable.Columns,
			Fields:    fieldsFromColumns(childTable.Columns...),
			Updates:   updates,
		}
		cursor, err := childTable.Seek(ctx, row.Key)
		if err != nil {
			return err
		}
		orig := childTable.checkParentFK
		childTable.checkParentFK = nil
		_, updateErr := cursor.update(ctx, stmt, row)
		childTable.checkParentFK = orig
		if updateErr != nil {
			return updateErr
		}
	}
	return nil
}

// fkValuesExistInParent checks whether the given values exist in the target columns
// of parentTable. Uses PK or unique index for O(log n) lookup for single-column FKs.
func (d *Database) fkValuesExistInParent(ctx context.Context, parentTable *Table, targetColumns []string, values []any) (bool, error) {
	if len(targetColumns) == 1 {
		return d.fkSingleValueExistsInParent(ctx, parentTable, targetColumns[0], values[0])
	}

	// Multi-column: sequential scan (composite FK index lookup is complex; scan is correct).
	found := false
	err := parentTable.sequentialScan(ctx, Scan{}, fieldsFromColumns(parentTable.Columns...), func(row Row) error {
		for i, colName := range targetColumns {
			ov, ok := row.GetValue(colName)
			if !ok || !ov.Valid {
				return nil
			}
			if !fkValuesEqual(ov.Value, values[i]) {
				return nil
			}
		}
		found = true
		return errFKScanDone
	})
	if err != nil && !errors.Is(err, errFKScanDone) {
		return false, err
	}
	return found, nil
}

// fkSingleValueExistsInParent is the fast-path for single-column FK lookups.
func (d *Database) fkSingleValueExistsInParent(ctx context.Context, parentTable *Table, targetColumn string, value any) (bool, error) {
	// Fast path: primary key lookup
	if parentTable.HasPrimaryKey() &&
		len(parentTable.PrimaryKey.Columns) == 1 &&
		parentTable.PrimaryKey.Columns[0].Name == targetColumn &&
		parentTable.PrimaryKey.Index != nil {

		col := parentTable.PrimaryKey.Columns[0]
		key, err := castKeyValue(col, value)
		if err != nil {
			return false, err
		}
		rowIDs, err := parentTable.PrimaryKey.Index.FindRowIDs(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, err
		}
		return len(rowIDs) > 0, nil
	}

	// Try unique indexes.
	for _, uniqueIndex := range parentTable.UniqueIndexes {
		if len(uniqueIndex.Columns) != 1 || uniqueIndex.Columns[0].Name != targetColumn {
			continue
		}
		if uniqueIndex.Index == nil {
			continue
		}
		col := uniqueIndex.Columns[0]
		key, err := castKeyValue(col, value)
		if err != nil {
			return false, err
		}
		rowIDs, err := uniqueIndex.Index.FindRowIDs(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, err
		}
		return len(rowIDs) > 0, nil
	}

	return false, fmt.Errorf("FK target column %q in table %q has no usable index", targetColumn, parentTable.Name)
}

// fkChildHasValues returns true if any row in childTable has colNames == values.
func (d *Database) fkChildHasValues(ctx context.Context, childTable *Table, colNames []string, values []any) (bool, error) {
	rows, err := d.fkChildFindRows(ctx, childTable, colNames, values)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// fkChildFindRows returns all rows in childTable where colNames == values.
func (d *Database) fkChildFindRows(ctx context.Context, childTable *Table, colNames []string, values []any) ([]Row, error) {
	var rows []Row
	err := childTable.sequentialScan(ctx, Scan{}, fieldsFromColumns(childTable.Columns...), func(row Row) error {
		for i, colName := range colNames {
			ov, ok := row.GetValue(colName)
			if !ok || !ov.Valid {
				return nil
			}
			if !fkValuesEqual(ov.Value, values[i]) {
				return nil
			}
		}
		rows = append(rows, row.Clone())
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// fkExtractValues collects the values of targetColNames from row.
// Returns the value slice and whether any value is non-NULL.
func fkExtractValues(tableCols []Column, targetColNames []string, row Row) ([]any, bool) {
	values := make([]any, len(targetColNames))
	anyNonNull := false
	for i, colName := range targetColNames {
		for j, col := range tableCols {
			if col.Name == colName {
				if j < len(row.Values) && row.Values[j].Valid {
					values[i] = row.Values[j].Value
					anyNonNull = true
				}
				break
			}
		}
	}
	return values, anyNonNull
}

// fkValuesEqual compares two FK values for equality, handling int32/int64 cross-type.
func fkValuesEqual(a, b any) bool {
	if a == b {
		return true
	}
	ai, aok := fkToInt64(a)
	bi, bok := fkToInt64(b)
	if aok && bok {
		return ai == bi
	}
	return false
}

func fkToInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int32:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

// fkSliceEqual returns true if two value slices are element-wise equal.
func fkSliceEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !fkValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
