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
		{FKAction(0), "restrict"},  // zero value falls back to restrict
		{FKAction(99), "restrict"}, // unknown value falls back to restrict
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.action.String())
	}
}

func TestErrForeignKeyViolation_Error(t *testing.T) {
	t.Parallel()

	err := ErrForeignKeyViolation{
		ChildTable:    "orders",
		ChildColumns:  []string{"user_id"},
		ParentTable:   "users",
		ParentColumns: []string{"id"},
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
		ParentTable:   "users",
		ParentColumns: []string{"id"},
		ChildTable:    "orders",
		ChildColumns:  []string{"user_id"},
	}
	msg := err.Error()
	assert.Contains(t, msg, "users")
	assert.Contains(t, msg, "id")
	assert.Contains(t, msg, "orders")
	assert.Contains(t, msg, "user_id")
}

func TestAutoFKName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "fk__orders__users__user_id", AutoFKName("orders", "users", []string{"user_id"}))
	assert.Equal(t, "fk__order_items__products__product_id", AutoFKName("order_items", "products", []string{"product_id"}))
	assert.Equal(t, "fk__order_items__orders__order_id_user_id", AutoFKName("order_items", "orders", []string{"order_id", "user_id"}))
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

func TestColumnsMatchSet(t *testing.T) {
	t.Parallel()

	cols := func(names ...string) []Column {
		out := make([]Column, len(names))
		for i, n := range names {
			out[i] = Column{Name: n, Kind: Int8, Size: 8}
		}
		return out
	}

	assert.True(t, columnsMatchSet(cols("a", "b"), []string{"a", "b"}))
	assert.True(t, columnsMatchSet(cols("a", "b"), []string{"b", "a"}), "order-independent")
	assert.False(t, columnsMatchSet(cols("a", "b"), []string{"a"}), "subset")
	assert.False(t, columnsMatchSet(cols("a"), []string{"a", "b"}), "superset")
	assert.False(t, columnsMatchSet(cols("a", "b"), []string{"a", "c"}), "different name")
	assert.True(t, columnsMatchSet(cols(), []string{}), "both empty")
}

func TestStmtHasCompositeUniqueConstraint(t *testing.T) {
	t.Parallel()

	db := &Database{}
	cols := []Column{
		{Kind: Int8, Size: 8, Name: "order_id"},
		{Kind: Int8, Size: 8, Name: "product_id"},
		{Kind: Int8, Size: 8, Name: "qty"},
	}
	stmt := Statement{
		Kind:       CreateTable,
		TableName:  "order_lines",
		Columns:    cols,
		PrimaryKey: NewPrimaryKey("pk__order_lines", cols[0:2], false),
	}

	assert.True(t, db.stmtHasCompositeUniqueConstraint(stmt, []string{"order_id", "product_id"}))
	assert.True(t, db.stmtHasCompositeUniqueConstraint(stmt, []string{"product_id", "order_id"}), "order-independent")
	assert.False(t, db.stmtHasCompositeUniqueConstraint(stmt, []string{"order_id", "qty"}))
	assert.False(t, db.stmtHasCompositeUniqueConstraint(stmt, []string{"order_id"}), "subset of PK")

	// Via unique index (no PK).
	stmt2 := Statement{
		Kind:      CreateTable,
		TableName: "foo",
		Columns:   cols,
		UniqueIndexes: []UniqueIndex{
			{IndexInfo: IndexInfo{Name: "uq_foo", Columns: cols[0:2]}},
		},
	}
	assert.True(t, db.stmtHasCompositeUniqueConstraint(stmt2, []string{"order_id", "product_id"}))
	assert.False(t, db.stmtHasCompositeUniqueConstraint(stmt2, []string{"order_id", "qty"}))
}

func TestTableHasCompositeUniqueConstraint(t *testing.T) {
	t.Parallel()

	db := &Database{}
	cols := []Column{
		{Kind: Int8, Size: 8, Name: "order_id"},
		{Kind: Int8, Size: 8, Name: "product_id"},
		{Kind: Int8, Size: 8, Name: "qty"},
	}
	table := &Table{
		PrimaryKey: PrimaryKey{
			IndexInfo: IndexInfo{Name: "pk__order_lines", Columns: cols[0:2]},
		},
	}

	assert.True(t, db.tableHasCompositeUniqueConstraint(table, []string{"order_id", "product_id"}))
	assert.True(t, db.tableHasCompositeUniqueConstraint(table, []string{"product_id", "order_id"}), "order-independent")
	assert.False(t, db.tableHasCompositeUniqueConstraint(table, []string{"order_id", "qty"}))

	// Via unique index (no PK).
	table2 := &Table{
		UniqueIndexes: map[string]UniqueIndex{
			"uq_foo": {IndexInfo: IndexInfo{Name: "uq_foo", Columns: cols[0:2]}},
		},
	}
	assert.True(t, db.tableHasCompositeUniqueConstraint(table2, []string{"order_id", "product_id"}))
	assert.False(t, db.tableHasCompositeUniqueConstraint(table2, []string{"order_id", "qty"}))
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
		{Name: "fk1", Columns: []string{"user_id"}, TargetTable: "users", TargetColumns: []string{"id"}},
		{Name: "fk2", Columns: []string{"product_id"}, TargetTable: "products", TargetColumns: []string{"id"}},
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
		{ChildTable: "orders", FK: ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}}},
		{ChildTable: "profiles", FK: ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}}},
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

func TestFKExtractValues(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "email", Kind: Varchar, Size: 100},
		{Name: "status", Kind: Varchar, Size: 50},
	}

	t.Run("extracts single column value", func(t *testing.T) {
		row := Row{
			Columns: cols,
			Values: []OptionalValue{
				{Value: int64(42), Valid: true},
				{Value: NewTextPointer([]byte("alice@example.com")), Valid: true},
				{Value: NewTextPointer([]byte("active")), Valid: true},
			},
		}
		vals, anyNonNull := fkExtractValues(cols, []string{"id"}, row)
		require.True(t, anyNonNull)
		require.Len(t, vals, 1)
		assert.Equal(t, int64(42), vals[0])
	})

	t.Run("extracts multiple column values", func(t *testing.T) {
		row := Row{
			Columns: cols,
			Values: []OptionalValue{
				{Value: int64(1), Valid: true},
				{Value: NewTextPointer([]byte("alice@example.com")), Valid: true},
				{Value: NewTextPointer([]byte("active")), Valid: true},
			},
		}
		vals, anyNonNull := fkExtractValues(cols, []string{"id", "email"}, row)
		require.True(t, anyNonNull)
		require.Len(t, vals, 2)
		assert.Equal(t, int64(1), vals[0])
	})

	t.Run("null value returns anyNonNull=false", func(t *testing.T) {
		row := Row{
			Columns: cols,
			Values: []OptionalValue{
				{Valid: false}, // NULL id
				{Value: NewTextPointer([]byte("alice@example.com")), Valid: true},
				{Value: NewTextPointer([]byte("active")), Valid: true},
			},
		}
		vals, anyNonNull := fkExtractValues(cols, []string{"id"}, row)
		assert.False(t, anyNonNull)
		assert.Nil(t, vals[0])
	})

	t.Run("unknown column returns anyNonNull=false", func(t *testing.T) {
		row := Row{
			Columns: cols,
			Values: []OptionalValue{
				{Value: int64(1), Valid: true},
			},
		}
		_, anyNonNull := fkExtractValues(cols, []string{"nonexistent"}, row)
		assert.False(t, anyNonNull)
	})
}

func TestFKSliceEqual(t *testing.T) {
	t.Parallel()

	assert.True(t, fkSliceEqual([]any{int64(1), "foo"}, []any{int64(1), "foo"}))
	assert.False(t, fkSliceEqual([]any{int64(1)}, []any{int64(2)}))
	assert.False(t, fkSliceEqual([]any{int64(1)}, []any{int64(1), int64(2)}))
	assert.True(t, fkSliceEqual([]any{}, []any{}))
	assert.True(t, fkSliceEqual([]any{nil}, []any{nil}))
	assert.False(t, fkSliceEqual(nil, []any{int64(1)}))
	assert.True(t, fkSliceEqual(nil, nil))
}

func TestFKSingleValueExistsInParent_NoIndex(t *testing.T) {
	t.Parallel()

	// A table with no PK and no unique indexes — should return an error.
	table := &Table{
		Name:    "foo",
		Columns: []Column{{Name: "bar", Kind: Int8, Size: 8}},
	}
	db := &Database{}
	_, err := db.fkSingleValueExistsInParent(context.Background(), table, "bar", int64(1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no usable index")
}

func TestFKValuesExistInParent_MultiColumn(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "order_id", Kind: Int8, Size: 8},
		{Name: "product_id", Kind: Int8, Size: 8},
	}
	// Use virtualRows so sequentialScan doesn't require a real pager.
	table := &Table{
		Name:    "order_lines",
		Columns: cols,
		virtualRows: []Row{
			{
				Columns: cols,
				Values: []OptionalValue{
					{Value: int64(1), Valid: true},
					{Value: int64(2), Valid: true},
				},
			},
		},
	}
	db := &Database{}

	exists, err := db.fkValuesExistInParent(context.Background(), table, []string{"order_id", "product_id"}, []any{int64(1), int64(2)})
	require.NoError(t, err)
	assert.True(t, exists)

	notExists, err := db.fkValuesExistInParent(context.Background(), table, []string{"order_id", "product_id"}, []any{int64(1), int64(99)})
	require.NoError(t, err)
	assert.False(t, notExists)
}

func TestFKChildFindRows_VirtualTable(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	table := &Table{
		Name:    "orders",
		Columns: cols,
		virtualRows: []Row{
			{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}},
			{Columns: cols, Values: []OptionalValue{{Value: int64(2), Valid: true}}},
			{Columns: cols, Values: []OptionalValue{{Valid: false}}}, // NULL
		},
	}
	db := &Database{}

	rows, err := db.fkChildFindRows(context.Background(), table, []string{"user_id"}, []any{int64(1)})
	require.NoError(t, err)
	assert.Len(t, rows, 1)

	rows, err = db.fkChildFindRows(context.Background(), table, []string{"user_id"}, []any{int64(99)})
	require.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestCheckChildFK_FKDisabled(t *testing.T) {
	t.Parallel()

	db := &Database{foreignKeysEnabled: false}
	err := db.checkChildFK(context.Background(), &Table{}, Row{})
	require.NoError(t, err)
}

func TestCheckChildFK_AllNullValues(t *testing.T) {
	t.Parallel()

	db := &Database{foreignKeysEnabled: true}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{
		Name:    "orders",
		Columns: childCols,
		ForeignKeys: []ForeignKey{{
			Columns:       []string{"user_id"},
			TargetTable:   "users",
			TargetColumns: []string{"id"},
		}},
	}
	// NULL FK column — FK check skipped.
	row := Row{
		Columns: childCols,
		Values:  []OptionalValue{{Valid: false}},
	}
	err := db.checkChildFK(context.Background(), childTable, row)
	require.NoError(t, err)
}

func TestCheckChildFK_ParentTableMissing(t *testing.T) {
	t.Parallel()

	db := &Database{
		foreignKeysEnabled: true,
		tables:             map[string]*Table{},
	}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{
		Name:    "orders",
		Columns: childCols,
		ForeignKeys: []ForeignKey{{
			Columns:       []string{"user_id"},
			TargetTable:   "users",
			TargetColumns: []string{"id"},
		}},
	}
	row := Row{
		Columns: childCols,
		Values:  []OptionalValue{{Value: int64(1), Valid: true}},
	}
	err := db.checkChildFK(context.Background(), childTable, row)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown table")
}

func TestCheckChildFK_MultiColumn_VirtualParent(t *testing.T) {
	t.Parallel()

	parentCols := []Column{
		{Name: "a", Kind: Int8, Size: 8},
		{Name: "b", Kind: Int8, Size: 8},
	}
	parentTable := &Table{
		Name:    "parent",
		Columns: parentCols,
		virtualRows: []Row{
			{
				Columns: parentCols,
				Values: []OptionalValue{
					{Value: int64(1), Valid: true},
					{Value: int64(2), Valid: true},
				},
			},
		},
	}
	db := &Database{
		foreignKeysEnabled: true,
		tables:             map[string]*Table{"parent": parentTable},
	}
	childCols := []Column{
		{Name: "a_ref", Kind: Int8, Size: 8},
		{Name: "b_ref", Kind: Int8, Size: 8},
	}
	childTable := &Table{
		Name:    "child",
		Columns: childCols,
		ForeignKeys: []ForeignKey{{
			Columns:       []string{"a_ref", "b_ref"},
			TargetTable:   "parent",
			TargetColumns: []string{"a", "b"},
		}},
	}

	// Matching row — no error.
	row := Row{
		Columns: childCols,
		Values: []OptionalValue{
			{Value: int64(1), Valid: true},
			{Value: int64(2), Valid: true},
		},
	}
	require.NoError(t, db.checkChildFK(context.Background(), childTable, row))

	// Non-matching row — ErrForeignKeyViolation.
	badRow := Row{
		Columns: childCols,
		Values: []OptionalValue{
			{Value: int64(1), Valid: true},
			{Value: int64(99), Valid: true},
		},
	}
	err := db.checkChildFK(context.Background(), childTable, badRow)
	require.Error(t, err)
	var fkErr ErrForeignKeyViolation
	require.ErrorAs(t, err, &fkErr)
}

func TestEnforceParentFKOnDelete_FKDisabled(t *testing.T) {
	t.Parallel()

	db := &Database{}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{}, Row{})
	require.NoError(t, err)
}

func TestEnforceParentFKOnDelete_NoInbounds(t *testing.T) {
	t.Parallel()

	db := &Database{
		foreignKeysEnabled: true,
		referencedBy:       map[string][]inboundFK{},
	}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{Name: "users"}, Row{})
	require.NoError(t, err)
}

func TestEnforceParentFKOnDelete_ChildTableMissing(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnDelete: FKActionRestrict},
			}},
		},
		tables: map[string]*Table{},
	}
	row := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{Name: "users", Columns: cols}, row)
	require.NoError(t, err) // missing child table → continue → no error
}

func TestEnforceParentFKOnDelete_RESTRICT_NoChildRows(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{Name: "orders", Columns: childCols, virtualRows: []Row{}}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnDelete: FKActionRestrict},
			}},
		},
		tables: map[string]*Table{"orders": childTable},
	}
	row := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{Name: "users", Columns: cols}, row)
	require.NoError(t, err)
}

func TestEnforceParentFKOnDelete_RESTRICT_ChildExists(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{
		Name:    "orders",
		Columns: childCols,
		virtualRows: []Row{
			{Columns: childCols, Values: []OptionalValue{{Value: int64(1), Valid: true}}},
		},
	}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnDelete: FKActionRestrict},
			}},
		},
		tables: map[string]*Table{"orders": childTable},
	}
	row := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{Name: "users", Columns: cols}, row)
	require.Error(t, err)
	var fkErr ErrForeignKeyParentViolation
	require.ErrorAs(t, err, &fkErr)
}

func TestEnforceParentFKOnDelete_CASCADE_NoChildRows(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{Name: "orders", Columns: childCols, virtualRows: []Row{}}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnDelete: FKActionCascade},
			}},
		},
		tables: map[string]*Table{"orders": childTable},
	}
	row := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{Name: "users", Columns: cols}, row)
	require.NoError(t, err)
}

func TestEnforceParentFKOnDelete_SETNULL_NoChildRows(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{Name: "orders", Columns: childCols, virtualRows: []Row{}}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnDelete: FKActionSetNull},
			}},
		},
		tables: map[string]*Table{"orders": childTable},
	}
	row := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	err := db.enforceParentFKOnDelete(context.Background(), &Table{Name: "users", Columns: cols}, row)
	require.NoError(t, err)
}

func TestEnforceParentFKOnUpdate_FKDisabled(t *testing.T) {
	t.Parallel()

	db := &Database{}
	err := db.enforceParentFKOnUpdate(context.Background(), &Table{}, Row{}, Row{})
	require.NoError(t, err)
}

func TestEnforceParentFKOnUpdate_NoInbounds(t *testing.T) {
	t.Parallel()

	db := &Database{
		foreignKeysEnabled: true,
		referencedBy:       map[string][]inboundFK{},
	}
	err := db.enforceParentFKOnUpdate(context.Background(), &Table{Name: "users"}, Row{}, Row{})
	require.NoError(t, err)
}

func TestEnforceParentFKOnUpdate_ColumnsUnchanged(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnUpdate: FKActionRestrict},
			}},
		},
	}
	row := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	// Same old/new values → no action needed.
	err := db.enforceParentFKOnUpdate(context.Background(), &Table{Name: "users", Columns: cols}, row, row)
	require.NoError(t, err)
}

func TestEnforceParentFKOnUpdate_RESTRICT_NoChildRows(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{Name: "orders", Columns: childCols, virtualRows: []Row{}}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnUpdate: FKActionRestrict},
			}},
		},
		tables: map[string]*Table{"orders": childTable},
	}
	oldRow := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	newRow := Row{Columns: cols, Values: []OptionalValue{{Value: int64(2), Valid: true}}}
	err := db.enforceParentFKOnUpdate(context.Background(), &Table{Name: "users", Columns: cols}, oldRow, newRow)
	require.NoError(t, err)
}

func TestEnforceParentFKOnUpdate_RESTRICT_ChildExists(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	childCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	childTable := &Table{
		Name:    "orders",
		Columns: childCols,
		virtualRows: []Row{
			{Columns: childCols, Values: []OptionalValue{{Value: int64(1), Valid: true}}},
		},
	}
	db := &Database{
		foreignKeysEnabled: true,
		referencedBy: map[string][]inboundFK{
			"users": {{
				ChildTable: "orders",
				FK:         ForeignKey{Columns: []string{"user_id"}, TargetColumns: []string{"id"}, OnUpdate: FKActionRestrict},
			}},
		},
		tables: map[string]*Table{"orders": childTable},
	}
	oldRow := Row{Columns: cols, Values: []OptionalValue{{Value: int64(1), Valid: true}}}
	newRow := Row{Columns: cols, Values: []OptionalValue{{Value: int64(2), Valid: true}}}
	err := db.enforceParentFKOnUpdate(context.Background(), &Table{Name: "users", Columns: cols}, oldRow, newRow)
	require.Error(t, err)
	var fkErr ErrForeignKeyParentViolation
	require.ErrorAs(t, err, &fkErr)
}

func TestCascadeDeleteChildRows_Empty(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	table := &Table{Name: "orders", Columns: cols, virtualRows: []Row{}}
	db := &Database{}
	err := db.cascadeDeleteChildRows(context.Background(), table, []string{"user_id"}, []any{int64(1)})
	require.NoError(t, err)
}

func TestCascadeUpdateChildRows_Empty(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	table := &Table{Name: "orders", Columns: cols, virtualRows: []Row{}}
	db := &Database{}
	err := db.cascadeUpdateChildRows(context.Background(), table, []string{"user_id"}, []any{int64(1)}, []any{int64(2)})
	require.NoError(t, err)
}

func TestSetNullChildRows_Empty(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	table := &Table{Name: "orders", Columns: cols, virtualRows: []Row{}}
	db := &Database{}
	err := db.setNullChildRows(context.Background(), table, []string{"user_id"}, []any{int64(1)})
	require.NoError(t, err)
}
