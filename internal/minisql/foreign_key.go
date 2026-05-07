package minisql

import (
	"context"
	"errors"
	"fmt"
)

// FKAction defines the referential action for ON DELETE / ON UPDATE.
type FKAction int

const (
	// FKActionRestrict rejects the operation if it would break the FK constraint.
	FKActionRestrict FKAction = iota + 1
	// FKActionNoAction is the same as RESTRICT in this implementation (no deferred checks).
	FKActionNoAction
	// FKActionSetNull sets the FK column to NULL when the referenced row is deleted/updated.
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

// ForeignKey describes a single FOREIGN KEY constraint on a child table column.
type ForeignKey struct {
	Name         string
	Column       string   // local (child) column
	TargetTable  string   // referenced (parent) table
	TargetColumn string   // referenced column in parent table
	OnDelete     FKAction // default FKActionRestrict
	OnUpdate     FKAction // default FKActionRestrict
}

// ErrForeignKeyViolation is returned when a child row's FK value does not exist
// in the parent table.
type ErrForeignKeyViolation struct {
	ChildTable   string
	ChildColumn  string
	ParentTable  string
	ParentColumn string
}

func (e ErrForeignKeyViolation) Error() string {
	return fmt.Sprintf(
		"foreign key constraint violation: %s.%s references non-existent value in %s.%s",
		e.ChildTable, e.ChildColumn, e.ParentTable, e.ParentColumn,
	)
}

// ErrForeignKeyParentViolation is returned when deleting or updating a parent row
// that is still referenced by one or more child rows.
type ErrForeignKeyParentViolation struct {
	ParentTable  string
	ParentColumn string
	ChildTable   string
	ChildColumn  string
}

func (e ErrForeignKeyParentViolation) Error() string {
	return fmt.Sprintf(
		"foreign key constraint violation: %s.%s is still referenced by %s.%s",
		e.ParentTable, e.ParentColumn, e.ChildTable, e.ChildColumn,
	)
}

// ErrDropTableReferencedByFK is the base error for dropping a table that is
// still referenced by a foreign key in another table.
var ErrDropTableReferencedByFK = errors.New("cannot drop table: it is still referenced by a foreign key constraint")

// ErrMultiColumnFKNotSupported is returned when a multi-column FK is specified.
var ErrMultiColumnFKNotSupported = errors.New("multi-column foreign keys are not yet supported")

// ErrFKActionNotSupported is returned when CASCADE or SET NULL action is specified.
var ErrFKActionNotSupported = errors.New("only RESTRICT and NO ACTION are supported for foreign key actions in this version")

// inboundFK records a child-side FK pointing at a given parent table.
type inboundFK struct {
	ChildTable string
	FK         ForeignKey
}

// AutoFKName builds a deterministic FK constraint name from the involved tables/column.
func AutoFKName(childTable, parentTable, column string) string {
	return fmt.Sprintf("fk__%s__%s__%s", childTable, parentTable, column)
}

// errFKScanDone is a sentinel returned by the scan callback to stop early.
var errFKScanDone = errors.New("fk: scan done")

// checkChildFK verifies all outgoing FK constraints for a new row in childTable.
// Must be called while d.dbLock is held (write).
func (d *Database) checkChildFK(ctx context.Context, childTable *Table, row Row) error {
	if !d.foreignKeysEnabled {
		return nil
	}
	for _, fk := range childTable.ForeignKeys {
		colIdx := -1
		for i, col := range childTable.Columns {
			if col.Name == fk.Column {
				colIdx = i
				break
			}
		}
		if colIdx < 0 || colIdx >= len(row.Values) {
			continue
		}
		val := row.Values[colIdx]
		if !val.Valid {
			continue // NULL FK value is permitted
		}

		parentTable, ok := d.tables[fk.TargetTable]
		if !ok {
			return fmt.Errorf("FK %q references unknown table %q", fk.Name, fk.TargetTable)
		}

		exists, err := d.fkValueExistsInParent(ctx, parentTable, fk.TargetColumn, val.Value)
		if err != nil {
			return fmt.Errorf("FK check %q: %w", fk.Name, err)
		}
		if !exists {
			return ErrForeignKeyViolation{
				ChildTable:   childTable.Name,
				ChildColumn:  fk.Column,
				ParentTable:  fk.TargetTable,
				ParentColumn: fk.TargetColumn,
			}
		}
	}
	return nil
}

// checkParentFK verifies that deleting or updating oldRow in parentTable does not
// break any inbound FK constraints (RESTRICT / NO ACTION).
// Must be called while d.dbLock is held (write).
func (d *Database) checkParentFK(ctx context.Context, parentTable *Table, oldRow Row) error {
	if !d.foreignKeysEnabled {
		return nil
	}
	inbounds := d.referencedBy[parentTable.Name]
	for _, inbound := range inbounds {
		parentColIdx := -1
		for i, col := range parentTable.Columns {
			if col.Name == inbound.FK.TargetColumn {
				parentColIdx = i
				break
			}
		}
		if parentColIdx < 0 || parentColIdx >= len(oldRow.Values) {
			continue
		}
		parentVal := oldRow.Values[parentColIdx]
		if !parentVal.Valid {
			continue // NULL parent value cannot be referenced
		}

		childTable, ok := d.tables[inbound.ChildTable]
		if !ok {
			continue
		}

		referenced, err := d.fkChildHasValue(ctx, childTable, inbound.FK.Column, parentVal.Value)
		if err != nil {
			return fmt.Errorf("FK parent check for %s.%s: %w", parentTable.Name, inbound.FK.TargetColumn, err)
		}
		if referenced {
			return ErrForeignKeyParentViolation{
				ParentTable:  parentTable.Name,
				ParentColumn: inbound.FK.TargetColumn,
				ChildTable:   inbound.ChildTable,
				ChildColumn:  inbound.FK.Column,
			}
		}
	}
	return nil
}

// fkValueExistsInParent checks if value exists in the target column of parentTable.
// Uses PK or unique index for O(log n) lookup.
func (d *Database) fkValueExistsInParent(ctx context.Context, parentTable *Table, targetColumn string, value any) (bool, error) {
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

	// Try unique indexes
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

// fkChildHasValue returns true if any row in childTable has colName == value.
func (d *Database) fkChildHasValue(ctx context.Context, childTable *Table, colName string, value any) (bool, error) {
	var (
		found  bool
		colIdx = -1
	)
	for i, col := range childTable.Columns {
		if col.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return false, nil
	}

	err := childTable.sequentialScan(ctx, Scan{}, []Field{{Name: colName}}, func(row Row) error {
		if colIdx >= len(row.Values) {
			return nil
		}
		v := row.Values[colIdx]
		if v.Valid && fkValuesEqual(v.Value, value) {
			found = true
			return errFKScanDone
		}
		return nil
	})
	if err != nil && !errors.Is(err, errFKScanDone) {
		return false, err
	}
	return found, nil
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
