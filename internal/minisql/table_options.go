package minisql

import (
	"context"
)

// TableOption is a functional option applied to a Table during construction via NewTable.
type TableOption func(*Table)

// WithPrimaryKey sets the primary key index on the table being constructed.
func WithPrimaryKey(pk PrimaryKey) TableOption {
	return func(t *Table) {
		t.PrimaryKey = pk
	}
}

// WithUniqueIndex registers a unique index on the table being constructed.
func WithUniqueIndex(index UniqueIndex) TableOption {
	return func(t *Table) {
		t.UniqueIndexes[index.Name] = index
	}
}

// WithSecondaryIndex registers a secondary (non-unique) index on the table being constructed.
func WithSecondaryIndex(index SecondaryIndex) TableOption {
	return func(t *Table) {
		t.SecondaryIndexes[index.Name] = index
	}
}

// WithParallelScan enables or disables concurrent leaf-page scanning for this table.
func WithParallelScan(enabled bool) TableOption {
	return func(t *Table) {
		t.parallelScan = enabled
	}
}

// withSortMemLimit sets the sort-spill threshold for this table (package-internal).
func withSortMemLimit(n int64) TableOption {
	return func(t *Table) {
		t.sortMemLimit = n
	}
}

// WithForeignKeys sets the outgoing FK constraints on the table.
func WithForeignKeys(fks []ForeignKey) TableOption {
	return func(t *Table) {
		t.ForeignKeys = fks
		t.fkColumnSet = make(map[string]bool)
		for _, fk := range fks {
			for _, col := range fk.Columns {
				t.fkColumnSet[col] = true
			}
		}
	}
}

// WithChildFKChecker wires up a callback that checks outgoing FK constraints.
// Called by *Database; must only be invoked while d.dbLock is held (write).
func WithChildFKChecker(fn func(context.Context, Row) error) TableOption {
	return func(t *Table) { t.checkChildFK = fn }
}

// WithParentFKChecker wires up a callback that enforces inbound FK constraints on DELETE.
// Called by *Database; must only be invoked while d.dbLock is held (write).
func WithParentFKChecker(fn func(context.Context, Row) error) TableOption {
	return func(t *Table) { t.checkParentFK = fn }
}

// WithParentFKUpdateEnforcer wires up a callback that enforces inbound FK constraints on UPDATE.
// Called by *Database; must only be invoked while d.dbLock is held (write).
func WithParentFKUpdateEnforcer(fn func(context.Context, Row, Row) error) TableOption {
	return func(t *Table) { t.enforceParentFKOnUpdate = fn }
}

// WithReferencedColumns marks which columns of this table are FK targets in other tables.
func WithReferencedColumns(cols map[string]bool) TableOption {
	return func(t *Table) { t.referencedColumns = cols }
}

// WithRowCountGetter sets an O(1) row-count accessor on the table.
// When set, COUNT(*) with no WHERE clause returns the value directly
// instead of performing a leaf-page walk.
func WithRowCountGetter(fn func() int64) TableOption {
	return func(t *Table) {
		t.getRowCount = fn
	}
}

// WithPlanCache wires the shared plan cache into the table so that PlanQuery
// can skip re-planning on repeated executions of the same prepared statement.
func WithPlanCache(cache LRUCache[string]) TableOption {
	return func(t *Table) {
		t.planCache = cache
	}
}
