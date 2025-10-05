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
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)
	mockParser := new(MockParser)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, uint32(0), aDatabase.tables[SchemaTableName].RootPageIdx)
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_CreateTable(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)
	mockParser := new(MockParser)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	aTable, err := aDatabase.CreateTable(ctx, stmt)
	require.NoError(t, err)
	assert.Equal(t, testTableName, aTable.Name)
	assert.Equal(t, testColumns, aTable.Columns)

	assert.Len(t, aDatabase.tables, 2)
	assert.Equal(t, uint32(0), aDatabase.tables[SchemaTableName].RootPageIdx)
	assert.Equal(t, uint32(3), aDatabase.tables[testTableName].RootPageIdx)
	assert.Equal(t, []string{SchemaTableName, testTableName}, aDatabase.ListTableNames(ctx))

	mock.AssertExpectationsForObjects(t, mockParser)
}
