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

func uniqueIndexName(tableName, columnName string) string {
	return fmt.Sprintf(
		"key__%s__%s",
		tableName,
		columnName,
	)
}

func tableNameFromUniqueIndex(indexName string) string {
	return strings.Split(indexName, "__")[1]
}

func (t *Table) insertUniqueKey(ctx context.Context, uniqueIndex UniqueIndex, uniqueValue OptionalValue, rowID RowID) error {
	// We only need to insert into the unique index if the value is not NULL
	if !uniqueValue.Valid {
		return nil
	}

	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no index", t.Name, uniqueIndex.Name)
	}

	castedValue, err := castKeyValue(uniqueIndex.Column, uniqueValue.Value)
	if err != nil {
		return fmt.Errorf("failed to cast key value for unique index  %s: %w", uniqueIndex.Name, err)
	}

	t.logger.Sugar().With(
		"index", uniqueIndex.Name,
		"key", castedValue,
	).Debug("inserting unique index key")

	if err := uniqueIndex.Index.Insert(ctx, castedValue, rowID); err != nil {
		return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
	}

	return nil
}

func (t *Table) updateUniqueKey(ctx context.Context, uniqueIndex UniqueIndex, oldUniqueValue OptionalValue, aRow Row) error {
	if uniqueIndex.Index == nil {
		return fmt.Errorf("table %s has unique index %s but no index", t.Name, uniqueIndex.Name)
	}

	oldKeyValue, err := castKeyValue(uniqueIndex.Column, oldUniqueValue.Value)
	if err != nil {
		return fmt.Errorf("failed to cast old unique index value for %s: %w", uniqueIndex.Name, err)
	}

	uniqueValue, ok := aRow.GetValue(uniqueIndex.Column.Name)
	if !ok {
		return nil
	}
	rowID := aRow.Key

	// We only need to insert into the unique index if the value is not NULL
	if uniqueValue.Valid {
		castedValue, err := castKeyValue(uniqueIndex.Column, uniqueValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast unique index value for %s: %w", uniqueIndex.Name, err)
		}
		// We try to insert new unique key first to avoid leaving table in inconsistent state
		// If the new unique key is already taken, we return an error without modifying the existing row
		if err := uniqueIndex.Index.Insert(ctx, castedValue, rowID); err != nil {
			return fmt.Errorf("failed to insert key for unique index %s: %w", uniqueIndex.Name, err)
		}
	}

	if err := uniqueIndex.Index.Delete(ctx, oldKeyValue, rowID); err != nil {
		return fmt.Errorf("failed to delete key for unique index %s: %w", uniqueIndex.Name, err)
	}

	return nil
}
