package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecondaryIndexName(t *testing.T) {
	t.Parallel()

	indexName := secondaryIndexName("table_name", "column_name")
	assert.Equal(t, "idx__table_name__column_name", indexName)

	tableName := tableNameFromSecondaryIndex(indexName)
	assert.Equal(t, "table_name", tableName)
}
