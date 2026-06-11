package minisql

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/pkg/lrucache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDatabase_PrepareStatementCaching tests the database-level statement caching
func TestDatabase_PrepareStatementCaching(t *testing.T) {
	t.Parallel()

	var (
		ctx        = context.Background()
		mockParser = new(MockParser)
		db         = &Database{
			parser:    mockParser,
			stmtCache: lrucache.New[string](100),
		}
	)

	// Mock the parser to return different statement instances
	query1 := `SELECT * FROM users WHERE id = ?"`
	stmt1Mock := Statement{
		Kind:      Select,
		TableName: "users",
		Conditions: OneOrMore{
			{
				FieldIsEqual(Field{Name: "id"}, OperandPlaceholder, nil),
			},
		},
	}
	stmt1Want := stmt1Mock
	stmt1Want.CacheKey = query1
	stmt1Want.cachedSelectedFields = []Field{{Name: "id"}}

	query2 := `SELECT * FROM posts WHERE user_id = ?`
	stmt2Mock := Statement{
		Kind:      Select,
		TableName: "posts",
		Conditions: OneOrMore{
			{
				FieldIsEqual(Field{Name: "user_id"}, OperandPlaceholder, nil),
			},
		},
	}
	stmt2Want := stmt2Mock
	stmt2Want.CacheKey = query2
	stmt2Want.cachedSelectedFields = []Field{{Name: "user_id"}}

	mockParser.On("Parse", ctx, query1).Return([]Statement{stmt1Mock}, nil).Once()

	// First call should parse and set CacheKey.
	stmt1, err := db.PrepareStatement(ctx, query1)
	require.NoError(t, err)
	assert.Equal(t, stmt1Want, stmt1)

	// Second call with same query should return cached statement (CacheKey preserved).
	stmt2, err := db.PrepareStatement(ctx, query1)
	require.NoError(t, err)
	assert.Equal(t, stmt1Want, stmt2)

	mockParser.On("Parse", ctx, query2).Return([]Statement{stmt2Mock}, nil).Once()

	// Different query should parse separately.
	stmt3, err := db.PrepareStatement(ctx, query2)
	require.NoError(t, err)
	assert.NotEqual(t, stmt1, stmt3, "Different queries should have different statements")
	assert.Equal(t, stmt2Want, stmt3)
}

// TestPlanCache_OrderByQuery verifies that the plan cache is used for no-condition
// queries (ORDER BY-only), and that DDL invalidation clears it.
func TestPlanCache_OrderByQuery(t *testing.T) {
	planCache := lrucache.New[string](100)
	table, _, _ := newTestTable(t, testColumns, WithPlanCache(planCache))

	ctx := context.Background()

	const query = `SELECT * FROM test_table ORDER BY id;`
	stmt := Statement{
		Kind:      Select,
		CacheKey:  query,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
		OrderBy:   []OrderBy{{Field: Field{Name: "id"}, Direction: Asc}},
	}

	// First call: cache miss — plan is derived and stored.
	plan1, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)

	// Second call: should hit the cache.
	plan2, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)
	assert.Equal(t, plan1, plan2, "cached plan should be returned on second call")

	// Purge simulates a DDL change; next call must re-derive.
	planCache.Purge()
	plan3, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)
	assert.Equal(t, plan1, plan3, "re-derived plan after purge should match original")
}

// TestPlanCache_ConditionQueryNotCached verifies that queries with WHERE conditions
// are NOT cached (their plans contain bound values that differ per execution).
func TestPlanCache_ConditionQueryNotCached(t *testing.T) {
	planCache := lrucache.New[string](100)
	table, _, _ := newTestTable(t, testColumns, WithPlanCache(planCache))

	ctx := context.Background()

	const query = `SELECT * FROM test_table WHERE id = ?;`
	stmt := Statement{
		Kind:      Select,
		CacheKey:  query,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
		Conditions: OneOrMore{
			{FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(42))},
		},
	}

	_, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)

	// The plan cache must remain empty — condition-based plans are not cached.
	_, cached := planCache.Get(query)
	assert.False(t, cached, "condition-based plans must not be stored in the plan cache")
}

// TestScanLimit_SimpleLimit verifies that PlanQuery attaches ScanLimit = LIMIT when
// a LIMIT is present, there is no in-memory sort, and no DISTINCT.
func TestScanLimit_SimpleLimit(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns)
	ctx := context.Background()

	stmt := Statement{
		Kind:      Select,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
		Limit:     OptionalValue{Value: int64(10), Valid: true},
	}

	plan, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)
	require.Len(t, plan.Scans, 1)
	assert.EqualValues(t, 10, plan.Scans[0].ScanLimit, "ScanLimit should equal LIMIT")
}

// TestScanLimit_OffsetPlusLimit verifies that ScanLimit = OFFSET + LIMIT so that
// skipped (offset) rows are counted in the early-stop total.
func TestScanLimit_OffsetPlusLimit(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns)
	ctx := context.Background()

	stmt := Statement{
		Kind:      Select,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
		Limit:     OptionalValue{Value: int64(20), Valid: true},
		Offset:    OptionalValue{Value: int64(5), Valid: true},
	}

	plan, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)
	require.Len(t, plan.Scans, 1)
	assert.EqualValues(t, 25, plan.Scans[0].ScanLimit, "ScanLimit should equal OFFSET + LIMIT")
}

// TestScanLimit_SortInMemory_NoScanLimit verifies that ScanLimit is NOT set when the
// query requires an in-memory sort (ORDER BY on a column with no index).
func TestScanLimit_SortInMemory_NoScanLimit(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns)
	ctx := context.Background()

	// "email" has no secondary index in testColumns, so ORDER BY email forces an in-memory sort.
	stmt := Statement{
		Kind:      Select,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
		OrderBy:   []OrderBy{{Field: Field{Name: "email"}, Direction: Asc}},
		Limit:     OptionalValue{Value: int64(10), Valid: true},
	}

	plan, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)
	assert.True(t, plan.SortInMemory, "expected in-memory sort for non-indexed ORDER BY")
	for _, scan := range plan.Scans {
		assert.EqualValues(t, 0, scan.ScanLimit, "ScanLimit must be 0 when SortInMemory is true")
	}
}

// TestScanLimit_Distinct_NoScanLimit verifies that ScanLimit is NOT set when DISTINCT
// is requested (we cannot stop early because later rows might be the distinct ones).
func TestScanLimit_Distinct_NoScanLimit(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns)
	ctx := context.Background()

	stmt := Statement{
		Kind:      Select,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
		Distinct:  true,
		Limit:     OptionalValue{Value: int64(10), Valid: true},
	}

	plan, err := table.PlanQuery(ctx, stmt)
	require.NoError(t, err)
	for _, scan := range plan.Scans {
		assert.EqualValues(t, 0, scan.ScanLimit, "ScanLimit must be 0 when DISTINCT is set")
	}
}

// TestScanLimit_CachedPlanNotMutated verifies that two calls with different LIMIT values
// each receive the correct ScanLimit without corrupting the shared cached plan.
// The test uses a no-condition query (cache-eligible) so the plan cache is exercised.
func TestScanLimit_CachedPlanNotMutated(t *testing.T) {
	planCache := lrucache.New[string](100)
	table, _, _ := newTestTable(t, testColumns, WithPlanCache(planCache))
	ctx := context.Background()

	// No WHERE clause → plan is cache-eligible; no ORDER BY → SortInMemory = false.
	const query = `SELECT * FROM test_table;`
	baseStmt := Statement{
		Kind:      Select,
		CacheKey:  query,
		TableName: testTableName,
		Columns:   testColumns,
		Fields:    []Field{{Name: "*"}},
	}

	stmt10 := baseStmt
	stmt10.Limit = OptionalValue{Value: int64(10), Valid: true}

	stmt50 := baseStmt
	stmt50.Limit = OptionalValue{Value: int64(50), Valid: true}

	plan10, err := table.PlanQuery(ctx, stmt10)
	require.NoError(t, err)
	require.Len(t, plan10.Scans, 1)
	assert.EqualValues(t, 10, plan10.Scans[0].ScanLimit, "first call: ScanLimit should be 10")

	plan50, err := table.PlanQuery(ctx, stmt50)
	require.NoError(t, err)
	require.Len(t, plan50.Scans, 1)
	assert.EqualValues(t, 50, plan50.Scans[0].ScanLimit, "second call: ScanLimit should be 50")

	// The cached plan must not have been mutated — ScanLimit must remain 0 in the cache.
	cached, ok := planCache.Get(query)
	require.True(t, ok, "plan should still be in cache")
	cachedPlan := cached.(QueryPlan)
	for _, scan := range cachedPlan.Scans {
		assert.EqualValues(t, 0, scan.ScanLimit, "cached plan must not have ScanLimit set")
	}
}
