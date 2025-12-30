package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUniqueIndexName(t *testing.T) {
	t.Parallel()

	indexName := UniqueIndexName("table_name", "column_name")
	assert.Equal(t, "key__table_name__column_name", indexName)

	tableName := tableNameFromUniqueIndex(indexName)
	assert.Equal(t, "table_name", tableName)
}
