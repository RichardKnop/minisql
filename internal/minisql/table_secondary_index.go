package minisql

import (
	"context"
	"fmt"
)

type SecondaryIndex struct {
	IndexInfo
	Index BTreeIndex
}

func (t *Table) insertSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, key OptionalValue, rowID RowID) error {
	// We only need to insert into the unique index if the key is not NULL
	if !key.Valid {
		return nil
	}

	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
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

func (t *Table) updateSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, oldKey OptionalValue, aRow Row) error {
	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

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
