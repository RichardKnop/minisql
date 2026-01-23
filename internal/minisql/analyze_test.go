package minisql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDatabase_Analyze(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		mockParser     = new(MockParser)
		ctx            = context.Background()
		rows           = gen.Rows(100)
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, aPager, aPager)
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
	// Make each 10 rows have the same timestmap so we can test distinct keys
	now := time.Now().Add(-time.Hour)
	for i, aRow := range rows {
		if i > 0 && i%10 == 0 {
			now = now.Add(time.Minute)
		}
		aRow.Values[5].Value = MustParseTimestamp(now.Format(timestampFormat))
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	mustInsert(t, ctx, aDatabase.tables[testTableName], aDatabase.txManager, insertStmt)

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aDatabase.Analyze(ctx, testTableName)
	})
	require.NoError(t, err)

	stats, err := aDatabase.listStats(ctx, "")
	require.NoError(t, err)

	assert.Len(t, stats, 4)
	assert.Equal(t, Stats{
		TableName: testTableName,
		StatValue: "100",
	}, stats[0])
	assert.Equal(t, Stats{
		TableName: testTableName,
		IndexName: "pkey__test_table",
		StatValue: "100 100",
	}, stats[1])
	assert.Equal(t, Stats{
		TableName: testTableName,
		IndexName: "key__test_table__email",
		StatValue: "100 100",
	}, stats[2])
	assert.Equal(t, Stats{
		TableName: testTableName,
		IndexName: "idx_created",
		StatValue: "100 10",
	}, stats[3])

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_Analyze_CompositeIndex(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		mockParser     = new(MockParser)
		ctx            = context.Background()
		columns        = []Column{
			{Kind: Varchar, Size: 50, Name: "country"},
			{Kind: Varchar, Size: 100, Name: "city"},
			{Kind: Varchar, Size: 100, Name: "street"},
		}
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, aPager, aPager)
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
			values = append(values, OptionalValue{Value: NewTextPointer([]byte(countries[0])), Valid: true})
		} else {
			values = append(values, OptionalValue{Value: NewTextPointer([]byte(countries[1])), Valid: true})
		}
		if i > 0 && i%10 == 0 {
			cityIdx += 1
		}
		values = append(values, OptionalValue{Value: NewTextPointer([]byte(cities[cityIdx])), Valid: true})
		values = append(values, OptionalValue{Value: NewTextPointer([]byte(streets[i])), Valid: true})
		insertStmt.Inserts = append(insertStmt.Inserts, values)
	}

	mustInsert(t, ctx, aDatabase.tables[testTableName], aDatabase.txManager, insertStmt)

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
