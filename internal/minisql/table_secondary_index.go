package minisql

import (
	"context"
	"fmt"
	"strings"
)

type SecondaryIndex struct {
	IndexInfo
	Index BTreeIndex
}

func secondaryIndexName(tableName, columnName string) string {
	return fmt.Sprintf(
		"idx__%s__%s",
		tableName,
		columnName,
	)
}

func tableNameFromSecondaryIndex(indexName string) string {
	return strings.Split(indexName, "__")[1]
}

func (t *Table) insertSecondaryIndexKey(ctx context.Context, secondaryIndex SecondaryIndex, key OptionalValue, rowID RowID) error {
	// We only need to insert into the unique index if the key is not NULL
	if !key.Valid {
		return nil
	}

	if secondaryIndex.Index == nil {
		return fmt.Errorf("table %s has secondary index %s but no Btree index instance", t.Name, secondaryIndex.Name)
	}

	castedKey, err := castKeyValue(secondaryIndex.Column, key.Value)
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

	castedOldKey, err := castKeyValue(secondaryIndex.Column, oldKey.Value)
	if err != nil {
		return fmt.Errorf("failed to cast old secondary index value for %s: %w", secondaryIndex.Name, err)
	}

	newKey, ok := aRow.GetValue(secondaryIndex.Column.Name)
	if !ok {
		return nil
	}
	rowID := aRow.Key

	// We only need to insert into the index index if the key is not NULL
	if newKey.Valid {
		castedKey, err := castKeyValue(secondaryIndex.Column, newKey.Value)
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
