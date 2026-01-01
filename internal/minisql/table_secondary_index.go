package minisql

import (
	"context"
	"fmt"
)

type SecondaryIndex struct {
	IndexInfo
	Index BTreeIndex
}

func (t *Table) insertSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, keys []OptionalValue, rowID RowID) error {
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	if len(keys) == 0 {
		return fmt.Errorf("no keys provided for secondary index %s", secondaryIndex.Name)
	}

	if len(keys) > 1 {
		return fmt.Errorf("composite unique indexes are not supported yet")
	}

	key := keys[0]

	// We only need to insert into the unique index if the key is not NULL
	if !key.Valid {
		return nil
	}

	castedKey, err := castKeyValue(secondaryIndex.Columns[0], key.Value)
	if err != nil {
		return fmt.Errorf("failed to cast key value for secondary index  %s: %w", secondaryIndex.Name, err)
	}

	t.logger.Sugar().With(
		"index", secondaryIndex.Name,
		"key", castedKey,
	).Debug("inserting secondary index key")

	if err := secondaryIndex.Index.Insert(ctx, castedKey, rowID); err != nil {
		return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
	}

	return nil
}

func (t *Table) updateSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, oldKeyParts []OptionalValue, aRow Row) error {
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	if len(oldKeyParts) == 0 {
		return fmt.Errorf("no old keys provided for secondary index %s", secondaryIndex.Name)
	}

	if len(oldKeyParts) > 1 {
		return t.updateCompositeSecondaryIndexKey(ctx, secondaryIndex, oldKeyParts, aRow)
	}

	oldKey := oldKeyParts[0]

	castedOldKey, err := castKeyValue(secondaryIndex.Columns[0], oldKey.Value)
	if err != nil {
		return fmt.Errorf("failed to cast old secondary index value for %s: %w", secondaryIndex.Name, err)
	}

	newKey, ok := aRow.GetValue(secondaryIndex.Columns[0].Name)
	if !ok {
		return nil
	}
	rowID := aRow.Key

	// We only need to insert into the index index if the key is not NULL
	if newKey.Valid {
		castedKey, err := castKeyValue(secondaryIndex.Columns[0], newKey.Value)
		if err != nil {
			return fmt.Errorf("failed to cast secondary index key for %s: %w", secondaryIndex.Name, err)
		}
		// We try to insert new index key first to avoid leaving table in inconsistent state
		// If the new index key is already taken, we return an error without modifying the existing row
		if err := secondaryIndex.Index.Insert(ctx, castedKey, rowID); err != nil {
			return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	if err := secondaryIndex.Index.Delete(ctx, castedOldKey, rowID); err != nil {
		return fmt.Errorf("failed to delete key for secondary index %s: %w", secondaryIndex.Name, err)
	}

	return nil
}

func (t *Table) updateCompositeSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, oldKeyParts []OptionalValue, aRow Row) error {
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	// Check if old key should have been in the index (all columns non-NULL)
	// Note: minisql doesn't index NULL values even for secondary indexes
	oldKeyInIndex := true
	oldKeyValues := make([]any, 0, len(oldKeyParts))
	for i, key := range oldKeyParts {
		if !key.Valid {
			oldKeyInIndex = false
			break
		}
		castedKey, err := castKeyValue(secondaryIndex.Columns[i], key.Value)
		if err != nil {
			return fmt.Errorf("failed to cast old secondary index value for %s: %w", secondaryIndex.Name, err)
		}
		oldKeyValues = append(oldKeyValues, castedKey)
	}

	// Check if new key should be in the index (all columns non-NULL)
	newKeyInIndex := true
	newKeyValues := make([]any, 0, len(oldKeyParts))
	for _, aColumn := range secondaryIndex.Columns {
		keyValue, ok := aRow.GetValue(aColumn.Name)
		if !ok {
			return fmt.Errorf("failed to get value for new secondary index %s", secondaryIndex.Name)
		}
		if !keyValue.Valid {
			newKeyInIndex = false
			break
		}
		castedKey, err := castKeyValue(aColumn, keyValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast new secondary index value for %s: %w", secondaryIndex.Name, err)
		}
		newKeyValues = append(newKeyValues, castedKey)
	}

	rowID := aRow.Key

	// Insert new key if all columns are non-NULL
	if newKeyInIndex {
		ck := NewCompositeKey(secondaryIndex.Columns, newKeyValues...)
		if err := secondaryIndex.Index.Insert(ctx, ck, rowID); err != nil {
			return fmt.Errorf("failed to insert key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	// Delete old key if it was in the index (all old columns were non-NULL)
	if oldKeyInIndex {
		oldCK := NewCompositeKey(secondaryIndex.Columns, oldKeyValues...)
		if err := secondaryIndex.Index.Delete(ctx, oldCK, rowID); err != nil {
			return fmt.Errorf("failed to delete key for secondary index %s: %w", secondaryIndex.Name, err)
		}
	}

	return nil
}
