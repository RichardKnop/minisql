package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var testDbName = "test_db"

func TestNewDatabase(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	mockParser := new(MockParser)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, uint32(0), aDatabase.tables[SchemaTableName].RootPageIdx)
	assert.Contains(t, aDatabase.ListTableNames(ctx), SchemaTableName)

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_CreateTable(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	mockParser := new(MockParser)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	_, err = aDatabase.ExecuteInTransaction(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumns, aDatabase.tables[testTableName].Columns)
	assert.Equal(t, uint32(3), aDatabase.tables[testTableName].RootPageIdx)
	assert.Equal(t, uint32(0), aDatabase.tables[SchemaTableName].RootPageIdx)
	assert.ElementsMatch(t, []string{SchemaTableName, testTableName}, aDatabase.ListTableNames(ctx))

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_DropTable(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	mockParser := new(MockParser)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	_, err = aDatabase.ExecuteInTransaction(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	_, err = aDatabase.ExecuteInTransaction(ctx, stmt)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, uint32(0), aDatabase.tables[SchemaTableName].RootPageIdx)
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	mock.AssertExpectationsForObjects(t, mockParser)
}
