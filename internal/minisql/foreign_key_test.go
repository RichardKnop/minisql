package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFKAction_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action FKAction
		want   string
	}{
		{FKActionRestrict, "restrict"},
		{FKActionNoAction, "no action"},
		{FKActionSetNull, "set null"},
		{FKActionCascade, "cascade"},
		{FKAction(0), "restrict"}, // zero value falls back to restrict
		{FKAction(99), "restrict"}, // unknown value falls back to restrict
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.action.String())
	}
}

func TestErrForeignKeyViolation_Error(t *testing.T) {
	t.Parallel()

	err := ErrForeignKeyViolation{
		ChildTable:   "orders",
		ChildColumn:  "user_id",
		ParentTable:  "users",
		ParentColumn: "id",
	}
	msg := err.Error()
	assert.Contains(t, msg, "orders")
	assert.Contains(t, msg, "user_id")
	assert.Contains(t, msg, "users")
	assert.Contains(t, msg, "id")
}

func TestErrForeignKeyParentViolation_Error(t *testing.T) {
	t.Parallel()

	err := ErrForeignKeyParentViolation{
		ParentTable:  "users",
		ParentColumn: "id",
		ChildTable:   "orders",
		ChildColumn:  "user_id",
	}
	msg := err.Error()
	assert.Contains(t, msg, "users")
	assert.Contains(t, msg, "id")
	assert.Contains(t, msg, "orders")
	assert.Contains(t, msg, "user_id")
}

func TestAutoFKName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "fk__orders__users__user_id", AutoFKName("orders", "users", "user_id"))
	assert.Equal(t, "fk__order_items__products__product_id", AutoFKName("order_items", "products", "product_id"))
}

func TestFKValuesEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"identical int64", int64(1), int64(1), true},
		{"different int64", int64(1), int64(2), false},
		{"int32 vs int64 equal", int32(5), int64(5), true},
		{"int64 vs int32 equal", int64(5), int32(5), true},
		{"int32 vs int64 unequal", int32(3), int64(4), false},
		{"identical int32", int32(7), int32(7), true},
		{"string equal", "abc", "abc", true},
		{"string unequal", "abc", "def", false},
		{"nil both", nil, nil, true},
		{"nil vs int64", nil, int64(1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, fkValuesEqual(tt.a, tt.b))
		})
	}
}

func TestFKToInt64(t *testing.T) {
	t.Parallel()

	v, ok := fkToInt64(int64(42))
	require.True(t, ok)
	assert.Equal(t, int64(42), v)

	v, ok = fkToInt64(int32(7))
	require.True(t, ok)
	assert.Equal(t, int64(7), v)

	_, ok = fkToInt64("not a number")
	assert.False(t, ok)

	_, ok = fkToInt64(nil)
	assert.False(t, ok)
}

func TestStmtHasIndexOnColumn(t *testing.T) {
	t.Parallel()

	db := &Database{}
	cols := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: 100, Name: "email"},
		{Kind: Varchar, Size: 100, Name: "name"},
	}
	stmt := Statement{
		Kind:       CreateTable,
		TableName:  "users",
		Columns:    cols,
		PrimaryKey: NewPrimaryKey("pk__users", cols[0:1], true),
		UniqueIndexes: []UniqueIndex{
			{IndexInfo: IndexInfo{Name: "unique__users__email", Columns: cols[1:2]}},
		},
	}

	assert.True(t, db.stmtHasIndexOnColumn(stmt, "id"), "PK column")
	assert.True(t, db.stmtHasIndexOnColumn(stmt, "email"), "unique column")
	assert.False(t, db.stmtHasIndexOnColumn(stmt, "name"), "plain column")
	assert.False(t, db.stmtHasIndexOnColumn(stmt, "nonexistent"), "missing column")
}

func TestTableHasIndexOnColumn(t *testing.T) {
	t.Parallel()

	db := &Database{}
	cols := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: 100, Name: "email"},
		{Kind: Varchar, Size: 100, Name: "name"},
	}
	table := &Table{
		PrimaryKey: PrimaryKey{
			IndexInfo: IndexInfo{
				Name:    "pk__users",
				Columns: cols[0:1],
			},
		},
		UniqueIndexes: map[string]UniqueIndex{
			"unique__users__email": {
				IndexInfo: IndexInfo{
					Name:    "unique__users__email",
					Columns: cols[1:2],
				},
			},
		},
	}

	assert.True(t, db.tableHasIndexOnColumn(table, "id"), "PK column")
	assert.True(t, db.tableHasIndexOnColumn(table, "email"), "unique column")
	assert.False(t, db.tableHasIndexOnColumn(table, "name"), "plain column")
}

func TestWithForeignKeys(t *testing.T) {
	t.Parallel()

	fks := []ForeignKey{
		{Name: "fk1", Column: "user_id", TargetTable: "users", TargetColumn: "id"},
		{Name: "fk2", Column: "product_id", TargetTable: "products", TargetColumn: "id"},
	}
	table := &Table{}
	WithForeignKeys(fks)(table)

	assert.Equal(t, fks, table.ForeignKeys)
	assert.True(t, table.fkColumnSet["user_id"])
	assert.True(t, table.fkColumnSet["product_id"])
	assert.False(t, table.fkColumnSet["name"])
}

func TestWithChildFKChecker(t *testing.T) {
	t.Parallel()

	called := false
	fn := func(_ context.Context, _ Row) error { called = true; return nil }
	table := &Table{}
	WithChildFKChecker(fn)(table)
	require.NotNil(t, table.checkChildFK)
	_ = table.checkChildFK(context.Background(), Row{})
	assert.True(t, called)
}

func TestWithParentFKChecker(t *testing.T) {
	t.Parallel()

	called := false
	fn := func(_ context.Context, _ Row) error { called = true; return nil }
	table := &Table{}
	WithParentFKChecker(fn)(table)
	require.NotNil(t, table.checkParentFK)
	_ = table.checkParentFK(context.Background(), Row{})
	assert.True(t, called)
}

func TestWithReferencedColumns(t *testing.T) {
	t.Parallel()

	cols := map[string]bool{"id": true}
	table := &Table{}
	WithReferencedColumns(cols)(table)
	assert.Equal(t, cols, table.referencedColumns)
}

func TestRemoveFromReferencedBy(t *testing.T) {
	t.Parallel()

	db := &Database{referencedBy: make(map[string][]inboundFK)}
	db.referencedBy["users"] = []inboundFK{
		{ChildTable: "orders", FK: ForeignKey{Column: "user_id", TargetColumn: "id"}},
		{ChildTable: "profiles", FK: ForeignKey{Column: "user_id", TargetColumn: "id"}},
	}

	// Remove one child; the other must remain.
	db.removeFromReferencedBy("users", "orders")
	remaining := db.referencedBy["users"]
	require.Len(t, remaining, 1)
	assert.Equal(t, "profiles", remaining[0].ChildTable)

	// Remove the last child; the key must be deleted from the map.
	db.removeFromReferencedBy("users", "profiles")
	_, exists := db.referencedBy["users"]
	assert.False(t, exists, "map key should be removed when no entries remain")

	// Removing a non-existent child must not panic.
	db.removeFromReferencedBy("users", "nonexistent")
}

func TestWithParallelScanEnabled(t *testing.T) {
	t.Parallel()

	db := &Database{}
	WithParallelScanEnabled()(db)
	assert.True(t, db.parallelScan)
}
