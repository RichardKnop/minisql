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

func (t *Table) insertUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, keyParts []OptionalValue, rowID RowID) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	if len(keyParts) == 0 {
		return fmt.Errorf("no keys provided for unique index %s", uniqueIndex.Name)
	}

	if len(uniqueIndex.Columns) > 1 {
		return t.insertUniqueCompositeIndexKey(ctx, uniqueIndex, keyParts, rowID)
	}

	key := keyParts[0]

	// We only need to insert into the unique index if the key is not NULL
	if !key.Valid {
		return nil
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

func (t *Table) insertUniqueCompositeIndexKey(ctx context.Context, uniqueIndex UniqueIndex, keyParts []OptionalValue, rowID RowID) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	// For composite unique indexes, if ANY column is NULL, we don't insert into the index
	// This follows standard SQL behavior where NULL != NULL and unique constraints
	// don't apply when any indexed column contains NULL
	for _, key := range keyParts {
		if !key.Valid {
			return nil
		}
	}

	keyValues := make([]any, 0, len(keyParts))
	for i, key := range keyParts {
		castedKey, err := castKeyValue(uniqueIndex.Columns[i], key.Value)
		if err != nil {
			return fmt.Errorf("failed to cast unique index value for %s: %w", uniqueIndex.Name, err)
		}
		keyValues = append(keyValues, castedKey)
	}

	ck := NewCompositeKey(uniqueIndex.Columns[0:len(keyValues)], keyValues...)

	t.logger.Sugar().With(
		"index", uniqueIndex.Name,
		"key", ck,
	).Debug("inserting unique index key")

	if err := uniqueIndex.Index.Insert(ctx, ck, rowID); err != nil {
		return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
	}

	return nil
}

func (t *Table) updateUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, oldKeyParts []OptionalValue, aRow Row) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	if len(oldKeyParts) == 0 {
		return fmt.Errorf("no old keys provided for old index %s", uniqueIndex.Name)
	}

	if len(uniqueIndex.Columns) > 1 {
		return t.updateCompositeUniqueIndexKey(ctx, uniqueIndex, oldKeyParts, aRow)
	}

	oldKey := oldKeyParts[0]

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

func (t *Table) updateCompositeUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, oldKeyParts []OptionalValue, aRow Row) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	// Check if old key should have been in the index (all columns non-NULL)
	oldKeyInIndex := true
	oldKeyValues := make([]any, 0, len(oldKeyParts))
	for i, key := range oldKeyParts {
		if !key.Valid {
			oldKeyInIndex = false
			break
		}
		castedKey, err := castKeyValue(uniqueIndex.Columns[i], key.Value)
		if err != nil {
			return fmt.Errorf("failed to cast old unique index value for %s: %w", uniqueIndex.Name, err)
		}
		oldKeyValues = append(oldKeyValues, castedKey)
	}

	// Check if new key should be in the index (all columns non-NULL)
	newKeyInIndex := true
	newKeyValues := make([]any, 0, len(oldKeyParts))
	for _, aColumn := range uniqueIndex.Columns {
		keyValue, ok := aRow.GetValue(aColumn.Name)
		if !ok {
			return fmt.Errorf("failed to get value for new unique index %s", uniqueIndex.Name)
		}
		if !keyValue.Valid {
			newKeyInIndex = false
			break
		}
		castedKey, err := castKeyValue(aColumn, keyValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast new unique index value for %s: %w", uniqueIndex.Name, err)
		}
		newKeyValues = append(newKeyValues, castedKey)
	}

	rowID := aRow.Key

	// Insert new key if all columns are non-NULL
	if newKeyInIndex {
		// We try to insert new index key first to avoid leaving table in inconsistent state
		// If the new index key is already taken, we return an error without modifying the existing row
		ck := NewCompositeKey(uniqueIndex.Columns, newKeyValues...)
		if err := uniqueIndex.Index.Insert(ctx, ck, rowID); err != nil {
			return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
		}
	}

	// Delete old key if it was in the index (all old columns were non-NULL)
	if oldKeyInIndex {
		oldCK := NewCompositeKey(uniqueIndex.Columns, oldKeyValues...)
		if err := uniqueIndex.Index.Delete(ctx, oldCK, rowID); err != nil {
			return fmt.Errorf("failed to delete key for unique index %s: %w", uniqueIndex.Name, err)
		}
	}

	return nil
}
