package minisql

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// UniqueIndex associates a unique-enforcing B+ tree index with its metadata.
// Attempting to insert a duplicate key into the underlying Index returns ErrDuplicateKey.
type UniqueIndex struct {
	Index BTreeIndex
	IndexInfo
}

// UniqueIndexName returns the canonical internal name for a unique index,
// following the convention "key__<tableName>__<col1>__<col2>...".
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

	if ce := t.logger.Check(zap.DebugLevel, "inserting unique index key"); ce != nil {
		ce.Write(zap.String("index", uniqueIndex.Name), zap.Any("key", castedKey))
	}

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

	if ce := t.logger.Check(zap.DebugLevel, "inserting unique index key"); ce != nil {
		ce.Write(zap.String("index", uniqueIndex.Name), zap.Any("key", ck))
	}

	if err := uniqueIndex.Index.Insert(ctx, ck, rowID); err != nil {
		return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
	}

	return nil
}

func (t *Table) updateUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, oldKeyParts []OptionalValue, row Row) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no Btree index instance", t.Name, uniqueIndex.Name)
	}

	if len(oldKeyParts) == 0 {
		return fmt.Errorf("no old keys provided for old index %s", uniqueIndex.Name)
	}

	if len(uniqueIndex.Columns) > 1 {
		return t.updateCompositeUniqueIndexKey(ctx, uniqueIndex, oldKeyParts, row)
	}

	oldKey := oldKeyParts[0]

	castedOldKey, err := castKeyValue(uniqueIndex.Columns[0], oldKey.Value)
	if err != nil {
		return fmt.Errorf("failed to cast old unique index value for %s: %w", uniqueIndex.Name, err)
	}

	newKey, ok := row.GetValue(uniqueIndex.Columns[0].Name)
	if !ok {
		return nil
	}
	rowID := row.Key

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

func (t *Table) updateCompositeUniqueIndexKey(ctx context.Context, uniqueIndex UniqueIndex, oldKeyParts []OptionalValue, row Row) error {
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
	for _, col := range uniqueIndex.Columns {
		keyValue, ok := row.GetValue(col.Name)
		if !ok {
			return fmt.Errorf("failed to get value for new unique index %s", uniqueIndex.Name)
		}
		if !keyValue.Valid {
			newKeyInIndex = false
			break
		}
		castedKey, err := castKeyValue(col, keyValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast new unique index value for %s: %w", uniqueIndex.Name, err)
		}
		newKeyValues = append(newKeyValues, castedKey)
	}

	rowID := row.Key

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
