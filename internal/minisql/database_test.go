package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testDbName = "test_db"
)

func TestNewDatabase(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Contains(t, aDatabase.ListTableNames(ctx), SchemaTableName)
}

func TestNewDatabase_WithExistingTableAndPrimaryKey(t *testing.T) {
	var (
		aPager     = initTest(t)
		mockParser = new(MockParser)
		ctx        = context.Background()
	)

	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager, aPager)
	require.NoError(t, err)

	// Let's create 2 tables, one without and one with primary key
	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	stmt2 := Statement{
		Kind:      CreateTable,
		TableName: testTableName2,
		Columns:   testColumnsWithPrimaryKey,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt, stmt2)
	require.NoError(t, err)

	// Now, let's flush and re-initialize the database to load existing tables
	require.NoError(t, aDatabase.Flush(ctx))

	mockParser.On("Parse", mock.Anything, stmt.CreateTableDDL()).Return([]Statement{stmt}, nil).Once()
	mockParser.On("Parse", mock.Anything, stmt2.CreateTableDDL()).Return([]Statement{stmt2}, nil).Once()

	aDatabase, err = NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 3)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
		testTableName2,
	}, aDatabase.ListTableNames(ctx))
	assert.Len(t, aDatabase.primaryKeys, 1)

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumns, aDatabase.tables[testTableName].Columns)
	assert.Equal(t, testTableName2, aDatabase.tables[testTableName2].Name)
	assert.Equal(t, testColumnsWithPrimaryKey, aDatabase.tables[testTableName2].Columns)

	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName2].GetRootPageIdx())
	assert.Equal(t, PageIndex(3), aDatabase.primaryKeys[testTableName2].GetRootPageIdx())

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_CreateTable(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))
	assert.Empty(t, aDatabase.primaryKeys)

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumns, aDatabase.tables[testTableName].Columns)
	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())

	// Root page plus a new page for table, should be 2 in total
	assert.Len(t, aPager.pages, 2)
}

func TestDatabase_CreateTable_WithPrimaryKey(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumnsWithPrimaryKey,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))
	assert.Len(t, aDatabase.primaryKeys, 1)

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumnsWithPrimaryKey, aDatabase.tables[testTableName].Columns)

	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.primaryKeys[testTableName].GetRootPageIdx())

	// Root page plus a new page for table and index, should be 3 in total
	assert.Len(t, aPager.pages, 3)
}

func TestDatabase_DropTable(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	tablePager := aDatabase.factory.ForTable(testColumns)
	assertFreePages(t, tablePager, []PageIndex{1})
}

func TestDatabase_DropTable_WithPrimaryKey(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumnsWithPrimaryKey,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.Len(t, aDatabase.primaryKeys, 1)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	_, err = aDatabase.NewConnection(1, nil).ExecuteStatements(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))
	assert.Empty(t, aDatabase.primaryKeys)

	tablePager := aDatabase.factory.ForTable(testColumns)
	assertFreePages(t, tablePager, []PageIndex{2, 1})
}

func initTest(t *testing.T) *pagerImpl {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(tempFile.Name()) })

	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)

	return aPager
}
