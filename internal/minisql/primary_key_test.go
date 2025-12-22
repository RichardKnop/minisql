package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimaryKeyName(t *testing.T) {
	t.Parallel()

	indexName := primaryKeyName("table_name")
	assert.Equal(t, "pkey__table_name", indexName)

	tableName := tableNameFromPrimaryKey(indexName)
	assert.Equal(t, "table_name", tableName)
}
