package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabase_CreateTable(t *testing.T) {
	t.Parallel()

	aDatabase, err := NewDatabase("db", nil, nil)
	require.NoError(t, err)

	aTable, err := aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.NoError(t, err)
	assert.Equal(t, "foo", aTable.Name)
	assert.Equal(t, testColumns, aTable.Columns)
	assert.Empty(t, aTable.Pages)
	assert.Equal(t, uint32(267), aTable.rowSize)
	assert.Equal(t, 0, aTable.numRows)
	assert.Len(t, aDatabase.tables, 1)

	aTable, err = aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTableAlreadyExists)
	assert.Len(t, aDatabase.tables, 1)
}

func TestDatabase_DropTable(t *testing.T) {
	t.Parallel()

	aDatabase, err := NewDatabase("db", nil, nil)
	require.NoError(t, err)

	err = aDatabase.DropTable(context.Background(), "foo")
	require.Error(t, err)
	assert.ErrorIs(t, err, errTableDoesNotExist)

	_, err = aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.NoError(t, err)
	assert.Len(t, aDatabase.tables, 1)

	err = aDatabase.DropTable(context.Background(), "foo")
	require.NoError(t, err)
	assert.Len(t, aDatabase.tables, 0)
}
