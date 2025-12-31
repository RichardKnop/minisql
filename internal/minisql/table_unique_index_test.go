package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUniqueIndexName(t *testing.T) {
	t.Parallel()

	indexName := UniqueIndexName("table_name", "a_b", "b_c")
	assert.Equal(t, "key__table_name__a_b__b_c", indexName)

	tableName := tableNameFromUniqueIndex(indexName)
	assert.Equal(t, "table_name", tableName)
}
