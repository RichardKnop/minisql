package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabase_ExecuteInsert(t *testing.T) {
	t.Parallel()

	aDatabase, err := NewDatabase("db")
	require.NoError(t, err)

	insertStmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
		Inserts:   [][]any{{gen.Int64(), gen.Email(), int32(gen.IntRange(18, 100))}},
	}

	_, err = aDatabase.executeInsert(context.Background(), insertStmt)
	require.Error(t, errTableDoesNotExist)

	_, err = aDatabase.CreateTable(context.Background(), "foo", testColumns)
	require.NoError(t, err)

	_, err = aDatabase.executeInsert(context.Background(), insertStmt)
	require.NoError(t, err)
}
