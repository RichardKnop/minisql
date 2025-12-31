package minisql

import (
	"context"
	"fmt"
	"strings"
)

type UniqueIndex struct {
	IndexInfo
	Index BTreeIndex
}

func UniqueIndexName(tableName string, columns ...string) string {
	return fmt.Sprintf(
		"key__%s__%s",
		tableName,
		strings.Join(columns, "__"),
	)
}

func tableNameFromUniqueIndex(indexName string) string {
	return strings.Split(indexName, "__")[1]
}

func (t *Table) insertUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, key OptionalValue, rowID RowID) error {
	// We only need to insert into the unique index if the key is not NULL
	if !key.Valid {
		return nil
	}

	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	castedKey, err := castKeyValue(uniqueIndex.Columns[0], key.Value)
	if err != nil {
		return fmt.Errorf("failed to cast key value for unique index  %s: %w", uniqueIndex.Name, err)
	}

	t.logger.Sugar().With(
		"index", uniqueIndex.Name,
		"key", castedKey,
	).Debug("inserting unique index key")

	if err := uniqueIndex.Index.Insert(ctx, castedKey, rowID); err != nil {
		return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
	}

	return nil
}

func (t *Table) updateUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, oldKey OptionalValue, aRow Row) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	castedOldKey, err := castKeyValue(uniqueIndex.Columns[0], oldKey.Value)
	if err != nil {
		return fmt.Errorf("failed to cast old unique index value for %s: %w", uniqueIndex.Name, err)
	}

	newKey, ok := aRow.GetValue(uniqueIndex.Columns[0].Name)
	if !ok {
		return nil
	}
	rowID := aRow.Key

	// We only need to insert into the index index if the key is not NULL
	if newKey.Valid {
		castedKey, err := castKeyValue(uniqueIndex.Columns[0], newKey.Value)
		if err != nil {
			return fmt.Errorf("failed to cast unique index key for %s: %w", uniqueIndex.Name, err)
		}
		// We try to insert new index key first to avoid leaving table in inconsistent state
		// If the new index key is already taken, we return an error without modifying the existing row
		if err := uniqueIndex.Index.Insert(ctx, castedKey, rowID); err != nil {
			return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
		}
	}

	if err := uniqueIndex.Index.Delete(ctx, castedOldKey, rowID); err != nil {
		return fmt.Errorf("failed to delete key for unique index %s: %w", uniqueIndex.Name, err)
	}

	return nil
}
