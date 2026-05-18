package minisql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDatabase_Analyze(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		mockParser    = new(MockParser)
		ctx           = context.Background()
		rows          = gen.Rows(100)
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, pager, pager, nil)
	require.NoError(t, err)

	// Create a test table
	stmt := Statement{
		Kind:       CreateTable,
		TableName:  testTableName,
		Columns:    testColumns,
		PrimaryKey: NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true),
		UniqueIndexes: []UniqueIndex{
			{
				IndexInfo: IndexInfo{
					Name:    UniqueIndexName(testTableName, "email"),
					Columns: testColumns[1:2],
				},
			},
		},
	}

	mockParser.On("Parse", mock.Anything, stmt.DDL()).Return([]Statement{stmt}, nil)

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	// Create a secondary index on created column
	createIndexStmt := Statement{
		Kind:      CreateIndex,
		IndexName: "idx_created",
		TableName: testTableName,
		Columns:   []Column{{Name: "created"}},
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, createIndexStmt)
		return err
	})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))
	assert.Len(t, aDatabase.tables[testTableName].UniqueIndexes, 1)
	assert.Len(t, aDatabase.tables[testTableName].SecondaryIndexes, 1)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testBigColumns...),
		Inserts: [][]OptionalValue{},
	}
	// Make each 10 rows have the same timestamp so we can test distinct keys
	now := time.Now().Add(-time.Hour)
	for i, row := range rows {
		if i > 0 && i%10 == 0 {
			now = now.Add(time.Minute)
		}
		row.Values[5] = MakeTimestamp(MustParseTimestampMicros(now.Format(timestampFormat)))
		insertStmt.Inserts = append(insertStmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, aDatabase.tables[testTableName], aDatabase.txManager, insertStmt)

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aDatabase.Analyze(ctx, testTableName)
	})
	require.NoError(t, err)

	stats, err := aDatabase.listStats(ctx, "")
	require.NoError(t, err)

	assert.Len(t, stats, 4)

	// Table-level stat has no index name and just the row count.
	assert.Equal(t, Stats{TableName: testTableName, StatValue: "100"}, stats[0])

	// PK on id (Int8) — numeric, so a histogram suffix is appended.
	assert.Equal(t, testTableName, stats[1].TableName)
	assert.Equal(t, "pkey__test_table", stats[1].IndexName)
	assert.True(t, strings.HasPrefix(stats[1].StatValue, "100 100"), "pk stat should start with '100 100'")
	pkStats, err := parseIndexStats(stats[1].StatValue)
	require.NoError(t, err)
	assert.Equal(t, int64(100), pkStats.NEntry)
	assert.Equal(t, []int64{100}, pkStats.NDistinct)
	assert.NotNil(t, pkStats.Hist, "PK on Int8 column should have a histogram")
	assert.Equal(t, histogramBuckets+1, len(pkStats.Hist.Bounds))

	// Unique index on email (Varchar) — no histogram for text columns.
	assert.Equal(t, Stats{
		TableName: testTableName,
		IndexName: "key__test_table__email",
		StatValue: "100 100",
	}, stats[2])

	// Secondary index on created (Timestamp) — numeric, so a histogram suffix is appended.
	// Also MCV (non-unique index) with |mcv= suffix.
	assert.Equal(t, testTableName, stats[3].TableName)
	assert.Equal(t, "idx_created", stats[3].IndexName)
	assert.True(t, strings.HasPrefix(stats[3].StatValue, "100 10"), "created stat should start with '100 10'")
	createdStats, err := parseIndexStats(stats[3].StatValue)
	require.NoError(t, err)
	assert.Equal(t, int64(100), createdStats.NEntry)
	assert.Equal(t, []int64{10}, createdStats.NDistinct)
	assert.NotNil(t, createdStats.Hist, "Timestamp index should have a histogram")
	// 10 distinct timestamps — histogram has at most 10 distinct boundaries.
	assert.GreaterOrEqual(t, len(createdStats.Hist.Bounds), 2)
	// Non-unique secondary index collects MCV.
	assert.NotEmpty(t, createdStats.MCV, "non-unique secondary index should have MCV entries")

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_Analyze_MCVGuidedPlanSelection(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		mockParser    = new(MockParser)
		ctx           = context.Background()
		indexName     = "idx_status"
		statusColumns = []Column{
			{Kind: Int8, Size: 8, Name: "id"},
			{Kind: Varchar, Size: 50, Name: "status", Nullable: true},
		}
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, pager, pager, nil)
	require.NoError(t, err)

	stmt := Statement{
		Kind:       CreateTable,
		TableName:  testTableName,
		Columns:    statusColumns,
		PrimaryKey: NewPrimaryKey(PrimaryKeyName(testTableName), statusColumns[0:1], true),
	}
	mockParser.On("Parse", mock.Anything, stmt.DDL()).Return([]Statement{stmt}, nil)
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	createIndexStmt := Statement{
		Kind:      CreateIndex,
		IndexName: indexName,
		TableName: testTableName,
		Columns:   statusColumns[1:2],
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, createIndexStmt)
		return err
	})
	require.NoError(t, err)

	// Insert 1000 rows: 950 "active", 50 "inactive".
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(statusColumns...),
		Inserts: [][]OptionalValue{},
	}
	for i := range 1000 {
		status := "active"
		if i < 50 {
			status = "inactive"
		}
		insertStmt.Inserts = append(insertStmt.Inserts, []OptionalValue{
			MakeInt8(int64(i + 1)),
			MakeVarchar(NewTextPointer([]byte(status))),
		})
	}
	mustInsert(ctx, t, aDatabase.tables[testTableName], aDatabase.txManager, insertStmt)

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aDatabase.Analyze(ctx, testTableName)
	})
	require.NoError(t, err)

	// Verify MCV was collected.
	statusStats := aDatabase.tables[testTableName].indexStats[indexName]
	assert.Equal(t, int64(1000), statusStats.NEntry)
	require.NotEmpty(t, statusStats.MCV)
	// Dominant value ("active") should be first (sorted desc by count).
	assert.Equal(t, "active", statusStats.MCV[0].Value)
	assert.Equal(t, int64(950), statusStats.MCV[0].Count)

	// Dominant value: 95% selectivity → should fall back to sequential scan.
	activeQuery := Statement{
		Kind: Select,
		Conditions: OneOrMore{
			{FieldIsEqual(Field{Name: "status"}, OperandQuotedString, NewTextPointer([]byte("active")))},
		},
	}
	activePlan, err := aDatabase.tables[testTableName].PlanQuery(ctx, activeQuery)
	require.NoError(t, err)
	require.Len(t, activePlan.Scans, 1)
	assert.Equal(t, ScanTypeSequential, activePlan.Scans[0].Type,
		"dominant value should trigger sequential scan fallback")

	// Rare value: 5% selectivity → should use index point scan.
	inactiveQuery := Statement{
		Kind: Select,
		Conditions: OneOrMore{
			{FieldIsEqual(Field{Name: "status"}, OperandQuotedString, NewTextPointer([]byte("inactive")))},
		},
	}
	inactivePlan, err := aDatabase.tables[testTableName].PlanQuery(ctx, inactiveQuery)
	require.NoError(t, err)
	require.Len(t, inactivePlan.Scans, 1)
	assert.Equal(t, ScanTypeIndexPoint, inactivePlan.Scans[0].Type,
		"rare value should use index point scan")

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_Analyze_CompositeIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		mockParser    = new(MockParser)
		ctx           = context.Background()
		columns       = []Column{
			{Kind: Varchar, Size: 50, Name: "country"},
			{Kind: Varchar, Size: 100, Name: "city"},
			{Kind: Varchar, Size: 100, Name: "street"},
		}
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, pager, pager, nil)
	require.NoError(t, err)

	// Create a test table
	stmt := Statement{
		Kind:       CreateTable,
		TableName:  testTableName,
		Columns:    columns,
		PrimaryKey: NewPrimaryKey(PrimaryKeyName(testTableName), columns, false),
	}

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))

	// Batch insert test rows
	var (
		countries  = gen.UniqueCountries(2)
		cities     = gen.UniqueCities(10)
		streets    = gen.UniqueStreets(100)
		insertStmt = Statement{
			Kind:      Insert,
			TableName: testTableName,
			Fields:    fieldsFromColumns(columns...),
			Inserts:   [][]OptionalValue{},
		}
		cityIdx = 0
	)
	for i := range 100 {
		values := make([]OptionalValue, 0, len(columns))
		if i < 50 {
			values = append(values, MakeVarchar(NewTextPointer([]byte(countries[0]))))
		} else {
			values = append(values, MakeVarchar(NewTextPointer([]byte(countries[1]))))
		}
		if i > 0 && i%10 == 0 {
			cityIdx += 1
		}
		values = append(values, MakeVarchar(NewTextPointer([]byte(cities[cityIdx]))))
		values = append(values, MakeVarchar(NewTextPointer([]byte(streets[i]))))
		insertStmt.Inserts = append(insertStmt.Inserts, values)
	}

	mustInsert(ctx, t, aDatabase.tables[testTableName], aDatabase.txManager, insertStmt)

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aDatabase.Analyze(ctx, testTableName)
	})
	require.NoError(t, err)

	stats, err := aDatabase.listStats(ctx, "")
	require.NoError(t, err)

	assert.Len(t, stats, 2)
	assert.Equal(t, Stats{
		TableName: testTableName,
		StatValue: "100",
	}, stats[0])
	// Expected: 100 rows, 2 distinct countries, 10 distinct (country,city) pairs, 100 distinct (country,city,street) combinations
	assert.Equal(t, Stats{
		TableName: testTableName,
		IndexName: "pkey__test_table",
		StatValue: "100 2 10 100",
	}, stats[1])

	mock.AssertExpectationsForObjects(t, mockParser)
}
