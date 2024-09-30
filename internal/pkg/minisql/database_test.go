package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:generate mockery --name=Pager --structname=MockPager --inpackage --case=snake --testonly

func TestDatabase_CreateTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, "db", nil, nil)
	require.NoError(t, err)

	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)
	assert.Equal(t, "foo", aTable.Name)
	assert.Equal(t, testColumns, aTable.Columns)
	assert.Equal(t, uint32(267), aTable.rowSize)
	assert.Equal(t, 0, aTable.numRows)
	assert.Len(t, aDatabase.tables, 1)

	// aTable, err = aDatabase.CreateTable(ctx, "foo", testColumns)
	// require.Error(t, err)
	// assert.ErrorIs(t, err, errTableAlreadyExists)
	// assert.Len(t, aDatabase.tables, 1)
}

func TestDatabase_DropTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, "db", nil, nil)
	require.NoError(t, err)

	err = aDatabase.DropTable(ctx, "foo")
	require.Error(t, err)
	assert.ErrorIs(t, err, errTableDoesNotExist)

	_, err = aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)
	assert.Len(t, aDatabase.tables, 1)

	err = aDatabase.DropTable(ctx, "foo")
	require.NoError(t, err)
	assert.Len(t, aDatabase.tables, 0)
}
