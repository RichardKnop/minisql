package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// buildProjectionSchema / projectFast
// ---------------------------------------------------------------------------

func TestBuildProjectionSchema(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "name", Kind: Varchar},
		{Name: "age", Kind: Int8},
	}

	t.Run("projects subset in requested order", func(t *testing.T) {
		t.Parallel()
		fields := []Field{{Name: "name"}, {Name: "id"}}
		projCols, srcIdxs := buildProjectionSchema(cols, fields)
		require.NotNil(t, projCols)
		assert.Equal(t, []Column{{Name: "name", Kind: Varchar}, {Name: "id", Kind: Int8}}, projCols)
		assert.Equal(t, []int{1, 0}, srcIdxs)
	})

	t.Run("projects all columns in order", func(t *testing.T) {
		t.Parallel()
		fields := fieldsFromColumns(cols...)
		projCols, srcIdxs := buildProjectionSchema(cols, fields)
		require.NotNil(t, projCols)
		assert.Equal(t, cols, projCols)
		assert.Equal(t, []int{0, 1, 2}, srcIdxs)
	})

	t.Run("returns nil when any field has Expr", func(t *testing.T) {
		t.Parallel()
		fields := []Field{{Name: "id"}, {Name: "computed", Expr: &Expr{Column: "expr"}}}
		projCols, srcIdxs := buildProjectionSchema(cols, fields)
		assert.Nil(t, projCols)
		assert.Nil(t, srcIdxs)
	})

	t.Run("qualified alias prefix resolves correctly", func(t *testing.T) {
		t.Parallel()
		qualifiedCols := []Column{
			{Name: "u.id", Kind: Int8},
			{Name: "u.name", Kind: Varchar},
		}
		fields := []Field{{AliasPrefix: "u", Name: "name"}}
		projCols, srcIdxs := buildProjectionSchema(qualifiedCols, fields)
		require.NotNil(t, projCols)
		assert.Len(t, projCols, 1)
		assert.Equal(t, []int{1}, srcIdxs)
	})
}

func TestRow_projectFast(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "name", Kind: Varchar},
		{Name: "age", Kind: Int8},
	}
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: NewTextPointer([]byte("alice")), Valid: true},
		{Value: int64(30), Valid: true},
	})

	t.Run("projects two columns in swapped order", func(t *testing.T) {
		t.Parallel()
		projCols := []Column{{Name: "age", Kind: Int8}, {Name: "id", Kind: Int8}}
		srcIdxs := []int{2, 0}
		projected := row.projectFast(projCols, srcIdxs)
		assert.Equal(t, projCols, projected.Columns)
		assert.Equal(t, int64(30), projected.Values[0].Value)
		assert.Equal(t, int64(1), projected.Values[1].Value)
	})

	t.Run("projects single column", func(t *testing.T) {
		t.Parallel()
		projCols := []Column{{Name: "name", Kind: Varchar}}
		srcIdxs := []int{1}
		projected := row.projectFast(projCols, srcIdxs)
		assert.Equal(t, projCols, projected.Columns)
		assert.Equal(t, NewTextPointer([]byte("alice")), projected.Values[0].Value)
	})
}

// ---------------------------------------------------------------------------
// Two-table join helpers
// ---------------------------------------------------------------------------

// joinColumns are the columns for a small "orders" inner table used in join tests.
var joinColumns = []Column{
	{Name: "order_id", Kind: Int8, Size: 8},
	{Name: "user_id", Kind: Int8, Size: 8},
	{Name: "amount", Kind: Int8, Size: 8},
}

// newNamedTestTable creates a Table with the given name wired to a fresh pager.
// Unlike newTestTable it does NOT call t.Parallel(), so it can be called
// multiple times within the same test function without panicking.
func newNamedTestTable(t *testing.T, name string, columns []Column) (*Table, *TransactionManager) {
	t.Helper()
	tempFile, err := os.CreateTemp("", "test_db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(tempFile.Name()) })

	pager, err := NewPager(tempFile, PageSize, 1000)
	require.NoError(t, err)

	tablePager := pager.ForTable(columns)
	txManager := NewTransactionManager(zap.NewNop(), tempFile.Name(), mockPagerFactory(tablePager), pager, nil)
	txPager := NewTransactionalPager(tablePager, txManager, name, "")
	table := NewTable(testLogger, txPager, txManager, name, columns, 0, nil)
	return table, txManager
}

// insertRows is a thin wrapper around mustInsert that accepts [][]OptionalValue.
func insertRowValues(ctx context.Context, t *testing.T, table *Table, txManager *TransactionManager, vals [][]OptionalValue) {
	t.Helper()
	stmt := Statement{Kind: Insert, Fields: fieldsFromColumns(table.Columns...), Inserts: vals}
	mustInsert(ctx, t, table, txManager, stmt)
}

// ---------------------------------------------------------------------------
// selectStreamingJoin
// ---------------------------------------------------------------------------

func TestTable_Select_StreamingInnerJoin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// users table: (id INT8, name VARCHAR)
	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
		{Name: "username", Kind: Varchar, Size: MaxInlineVarchar, Nullable: true},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}, {Value: NewTextPointer([]byte("alice")), Valid: true}},
		{{Value: int64(2), Valid: true}, {Value: NewTextPointer([]byte("bob")), Valid: true}},
		{{Value: int64(3), Valid: true}, {Value: NewTextPointer([]byte("carol")), Valid: true}},
	})

	// orders table: (order_id INT8, user_id INT8, amount INT8)
	orders, ordersTx := newNamedTestTable(t, "orders", joinColumns)
	insertRowValues(ctx, t, orders, ordersTx, [][]OptionalValue{
		{{Value: int64(10), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(100), Valid: true}},
		{{Value: int64(11), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(200), Valid: true}},
		{{Value: int64(12), Valid: true}, {Value: int64(2), Valid: true}, {Value: int64(150), Valid: true}},
		// user_id=3 has no orders — excluded by INNER JOIN
	})

	// Wire the table provider so users can look up orders.
	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":  users,
		"orders": orders,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "o", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Fields: []Field{
			{AliasPrefix: "u", Name: "user_id"},
			{AliasPrefix: "o", Name: "order_id"},
			{AliasPrefix: "o", Name: "amount"},
		},
		Joins: []Join{{
			TableName:  "orders",
			TableAlias: "o",
			Type:       Inner,
			Conditions: Conditions{onCond},
		}},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	// 3 orders match: user 1 → 2 orders, user 2 → 1 order
	assert.Len(t, rows, 3)

	// Verify user_id values are only 1 and 2 (user 3 excluded).
	// Combined join columns are alias-qualified: "u.user_id".
	var userIDs []int64
	for _, row := range rows {
		uid, ok := row.GetValue("u.user_id")
		require.True(t, ok, "column u.user_id not found in row %v", row.Columns)
		userIDs = append(userIDs, uid.Value.(int64))
	}
	assert.Contains(t, userIDs, int64(1))
	assert.Contains(t, userIDs, int64(2))
	assert.NotContains(t, userIDs, int64(3))
}

func TestTable_Select_StreamingInnerJoin_WithLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}},
		{{Value: int64(2), Valid: true}},
	})

	orders, ordersTx := newNamedTestTable(t, "orders", joinColumns)
	insertRowValues(ctx, t, orders, ordersTx, [][]OptionalValue{
		{{Value: int64(10), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(100), Valid: true}},
		{{Value: int64(11), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(200), Valid: true}},
		{{Value: int64(12), Valid: true}, {Value: int64(2), Valid: true}, {Value: int64(150), Valid: true}},
	})

	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":  users,
		"orders": orders,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "o", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Fields:     []Field{{AliasPrefix: "u", Name: "user_id"}, {AliasPrefix: "o", Name: "order_id"}},
		Joins: []Join{{
			TableName:  "orders",
			TableAlias: "o",
			Type:       Inner,
			Conditions: Conditions{onCond},
		}},
		Limit: OptionalValue{Value: int64(2), Valid: true},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	assert.Len(t, rows, 2)
}

func TestTable_Select_StreamingLeftJoin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
		{Name: "username", Kind: Varchar, Size: MaxInlineVarchar, Nullable: true},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}, {Value: NewTextPointer([]byte("alice")), Valid: true}},
		{{Value: int64(2), Valid: true}, {Value: NewTextPointer([]byte("bob")), Valid: true}},
	})

	orders, ordersTx := newNamedTestTable(t, "orders", joinColumns)
	insertRowValues(ctx, t, orders, ordersTx, [][]OptionalValue{
		{{Value: int64(10), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(100), Valid: true}},
	})

	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":  users,
		"orders": orders,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "o", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Fields: []Field{
			{AliasPrefix: "u", Name: "user_id"},
			{AliasPrefix: "o", Name: "order_id"},
		},
		Joins: []Join{{
			TableName:  "orders",
			TableAlias: "o",
			Type:       Left,
			Conditions: Conditions{onCond},
		}},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	// 2 rows: alice+order10, bob+NULL
	assert.Len(t, rows, 2)
}

// ---------------------------------------------------------------------------
// selectSemiJoinDirectRowView (IN-subquery semi-join path)
// ---------------------------------------------------------------------------

func TestTable_Select_SemiJoinDirectRowView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// users table
	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
		{Name: "username", Kind: Varchar, Size: MaxInlineVarchar, Nullable: true},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}, {Value: NewTextPointer([]byte("alice")), Valid: true}},
		{{Value: int64(2), Valid: true}, {Value: NewTextPointer([]byte("bob")), Valid: true}},
		{{Value: int64(3), Valid: true}, {Value: NewTextPointer([]byte("carol")), Valid: true}},
	})

	// active_users inner table (semi-join build side)
	activeCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	active, activeTx := newNamedTestTable(t, "active_users", activeCols)
	insertRowValues(ctx, t, active, activeTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}},
		{{Value: int64(3), Valid: true}},
	})

	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":        users,
		"active_users": active,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "au", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Fields:     []Field{{AliasPrefix: "u", Name: "user_id"}, {AliasPrefix: "u", Name: "username"}},
		Joins: []Join{{
			TableName:  "active_users",
			TableAlias: "au",
			Type:       Semi,
			Conditions: Conditions{onCond},
		}},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	// users 1 and 3 are in active_users, user 2 is not
	assert.Len(t, rows, 2)

	var ids []int64
	for _, row := range rows {
		v, ok := row.GetValue("user_id")
		require.True(t, ok, "column user_id not found in row %v", row.Columns)
		ids = append(ids, v.Value.(int64))
	}
	assert.ElementsMatch(t, []int64{1, 3}, ids)
}

func TestTable_Select_AntiSemiJoin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}},
		{{Value: int64(2), Valid: true}},
		{{Value: int64(3), Valid: true}},
	})

	bannedCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}
	banned, bannedTx := newNamedTestTable(t, "banned_users", bannedCols)
	insertRowValues(ctx, t, banned, bannedTx, [][]OptionalValue{
		{{Value: int64(2), Valid: true}},
	})

	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":        users,
		"banned_users": banned,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "b", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Fields:     []Field{{AliasPrefix: "u", Name: "user_id"}},
		Joins: []Join{{
			TableName:  "banned_users",
			TableAlias: "b",
			Type:       AntiSemi,
			Conditions: Conditions{onCond},
		}},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	// user 2 is banned — only 1 and 3 pass
	assert.Len(t, rows, 2)
	var ids []int64
	for _, row := range rows {
		v, ok := row.GetValue("u.user_id")
		require.True(t, ok, "column u.user_id not found in row %v", row.Columns)
		ids = append(ids, v.Value.(int64))
	}
	assert.ElementsMatch(t, []int64{1, 3}, ids)
}

func TestTable_Select_StreamingInnerJoin_Distinct(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
		{Name: "country", Kind: Varchar, Size: 32},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}, {Value: NewTextPointer([]byte("US")), Valid: true}},
		{{Value: int64(2), Valid: true}, {Value: NewTextPointer([]byte("UK")), Valid: true}},
		{{Value: int64(3), Valid: true}, {Value: NewTextPointer([]byte("US")), Valid: true}},
	})

	orders, ordersTx := newNamedTestTable(t, "orders", joinColumns)
	insertRowValues(ctx, t, orders, ordersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(100), Valid: true}},
		{{Value: int64(2), Valid: true}, {Value: int64(2), Valid: true}, {Value: int64(200), Valid: true}},
		{{Value: int64(3), Valid: true}, {Value: int64(3), Valid: true}, {Value: int64(300), Valid: true}},
	})

	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":  users,
		"orders": orders,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "o", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Distinct:   true,
		Fields:     []Field{{AliasPrefix: "u", Name: "country"}},
		Joins: []Join{{
			TableName:  "orders",
			TableAlias: "o",
			Type:       Inner,
			Conditions: Conditions{onCond},
		}},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	// 3 joined rows, but "US" appears twice (users 1 and 3); DISTINCT yields 2 unique countries
	assert.Len(t, rows, 2)

	var countries []string
	for _, row := range rows {
		v, ok := row.GetValue("u.country")
		require.True(t, ok, "column u.country not found in row %v", row.Columns)
		countries = append(countries, v.Value.(TextPointer).String())
	}
	assert.ElementsMatch(t, []string{"US", "UK"}, countries)
}

func TestTable_Select_JoinWithOrderByLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	userCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
	}
	users, usersTx := newNamedTestTable(t, "users", userCols)
	insertRowValues(ctx, t, users, usersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}},
		{{Value: int64(2), Valid: true}},
		{{Value: int64(3), Valid: true}},
	})

	orders, ordersTx := newNamedTestTable(t, "orders", joinColumns)
	// order_id, user_id, amount — amounts are 300, 100, 200 to test sort
	insertRowValues(ctx, t, orders, ordersTx, [][]OptionalValue{
		{{Value: int64(1), Valid: true}, {Value: int64(1), Valid: true}, {Value: int64(300), Valid: true}},
		{{Value: int64(2), Valid: true}, {Value: int64(2), Valid: true}, {Value: int64(100), Valid: true}},
		{{Value: int64(3), Valid: true}, {Value: int64(3), Valid: true}, {Value: int64(200), Valid: true}},
	})

	users.provider = &mapTableProvider{tables: map[string]*Table{
		"users":  users,
		"orders": orders,
	}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "u", Name: "user_id"},
		OperandField,
		Field{AliasPrefix: "o", Name: "user_id"},
	)
	stmt := Statement{
		Kind:       Select,
		TableAlias: "u",
		Fields:     []Field{{AliasPrefix: "o", Name: "amount"}},
		Joins: []Join{{
			TableName:  "orders",
			TableAlias: "o",
			Type:       Inner,
			Conditions: Conditions{onCond},
		}},
		Limit: OptionalValue{Value: int64(2), Valid: true},
		OrderBy: []OrderBy{{
			Field: Field{AliasPrefix: "o", Name: "amount"},
		}},
	}

	result, err := users.Select(ctx, stmt)
	require.NoError(t, err)

	rows := collectRows(ctx, result)
	// Sorted ASC by amount: 100, 200, 300 — LIMIT 2 yields [100, 200]
	require.Len(t, rows, 2)

	amounts := make([]int64, 2)
	for i, row := range rows {
		v, ok := row.GetValue("o.amount")
		require.True(t, ok, "column o.amount not found in row %v", row.Columns)
		amounts[i] = v.Value.(int64)
	}
	assert.Equal(t, []int64{100, 200}, amounts)
}
