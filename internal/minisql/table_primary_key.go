package minisql

import (
	"context"
	"fmt"
	"strings"
)

type IndexInfo struct {
	Name    string
	Columns []Column
}

type PrimaryKey struct {
	IndexInfo
	Autoincrement bool
	Index         BTreeIndex
}

func NewPrimaryKey(indexName string, columns []Column, autoincrement bool) PrimaryKey {
	return PrimaryKey{
		IndexInfo: IndexInfo{
			Name:    indexName,
			Columns: columns,
		},
		Autoincrement: autoincrement,
	}
}

func PrimaryKeyName(tableName string) string {
	return fmt.Sprintf(
		"pkey__%s",
		tableName,
	)
}

func tableNameFromPrimaryKey(indexName string) string {
	return strings.TrimPrefix(indexName, "pkey__")
}

func (t *Table) insertPrimaryKey(ctx context.Context, keyParts []OptionalValue, rowID RowID) (any, error) {
	if t.PrimaryKey.Index == nil {
		return nil, fmt.Errorf("table %s has primary key but no index", t.Name)
	}

	if len(keyParts) == 0 {
		return nil, fmt.Errorf("no keys provided for primary key %s", t.PrimaryKey.Name)
	}

	if len(keyParts) > 1 {
		return t.insertCompositePrimaryKey(ctx, keyParts, rowID)
	}

	key := keyParts[0]

	if !key.Valid {
		if !t.PrimaryKey.Autoincrement {
			return 0, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
		}
		newPrimaryKey, err := t.insertAutoincrementedPrimaryKey(ctx, rowID)
		if err != nil {
			return 0, err
		}
		return newPrimaryKey, nil
	}
	castedKey, err := castKeyValue(t.PrimaryKey.Columns[0], key.Value)
	if err != nil {
		return 0, fmt.Errorf("failed to cast primary key value for %s: %w", t.PrimaryKey.Name, err)
	}

	t.logger.Sugar().With(
		"index", t.PrimaryKey.Name,
		"key", castedKey,
	).Debug("inserting primary key")

	if err := t.PrimaryKey.Index.Insert(ctx, castedKey, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}

	return castedKey, nil
}

func (t *Table) insertAutoincrementedPrimaryKey(ctx context.Context, rowID RowID) (int64, error) {
	if t.PrimaryKey.Autoincrement && t.PrimaryKey.Columns[0].Kind != Int8 {
		return 0, fmt.Errorf("autoincrement primary key %s must be of type INT8", t.PrimaryKey.Name)
	}

	lastKey, err := t.PrimaryKey.Index.SeekLastKey(ctx, t.PrimaryKey.Index.GetRootPageIdx())
	if err != nil {
		return 0, err
	}
	lastPrimaryKey, ok := lastKey.(int64)
	if !ok {
		return 0, fmt.Errorf("failed to cast last primary key value for autoincrement")
	}
	newPrimaryKey := lastPrimaryKey + 1

	t.logger.Sugar().With(
		"index", t.PrimaryKey.Name,
		"key", int(newPrimaryKey),
	).Debug("inserting autoincremented primary key")

	if err := t.PrimaryKey.Index.Insert(ctx, newPrimaryKey, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}

	return newPrimaryKey, nil
}

func (t *Table) insertCompositePrimaryKey(ctx context.Context, keyParts []OptionalValue, rowID RowID) (any, error) {
	if t.PrimaryKey.Index == nil {
		return nil, fmt.Errorf("table %s has primary key but no index", t.Name)
	}

	keyValues := make([]any, 0, len(keyParts))
	for i, key := range keyParts {
		if !key.Valid {
			return 0, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
		}
		castedKey, err := castKeyValue(t.PrimaryKey.Columns[i], key.Value)
		if err != nil {
			return 0, fmt.Errorf("failed to cast primary key value for %s: %w", t.PrimaryKey.Name, err)
		}
		keyValues = append(keyValues, castedKey)
	}

	ck := NewCompositeKey(t.PrimaryKey.Columns, keyValues...)

	t.logger.Sugar().With(
		"index", t.PrimaryKey.Name,
	).Debug("inserting composite primary key")

	if err := t.PrimaryKey.Index.Insert(ctx, ck, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}

	return ck, nil
}

func (t *Table) updatePrimaryKey(ctx context.Context, oldKeyParts []OptionalValue, aRow Row) error {
	if t.PrimaryKey.Index == nil {
		return fmt.Errorf("table %s has primary key but no Btree index instance", t.Name)
	}

	if len(oldKeyParts) == 0 {
		return fmt.Errorf("no old keys provided for primary key %s", t.PrimaryKey.Name)
	}

	if len(t.PrimaryKey.Columns) > 1 {
		return t.updateCompositePrimaryKey(ctx, oldKeyParts, aRow)
	}

	oldKey := oldKeyParts[0]

	castedOldKey, err := castKeyValue(t.PrimaryKey.Columns[0], oldKey.Value)
	if err != nil {
		return fmt.Errorf("failed to cast old primary key value for %s: %w", t.PrimaryKey.Name, err)
	}

	newKey, ok := aRow.GetValue(t.PrimaryKey.Columns[0].Name)
	if !ok {
		return nil
	}
	if !newKey.Valid {
		return fmt.Errorf("cannot update primary key %s to NULL", t.PrimaryKey.Name)
	}
	castedKey, err := castKeyValue(t.PrimaryKey.Columns[0], newKey.Value)
	if err != nil {
		return fmt.Errorf("failed to cast new primary key value for %s: %w", t.PrimaryKey.Name, err)
	}
	rowID := aRow.Key

	// We try to insert new primary key first to avoid leaving table in inconsistent state
	// If the new primary key is already taken, we return an error without modifying the existing row
	if err := t.PrimaryKey.Index.Insert(ctx, castedKey, rowID); err != nil {
		return fmt.Errorf("failed to insert new primary key %s: %w", t.PrimaryKey.Name, err)
	}
	if err := t.PrimaryKey.Index.Delete(ctx, castedOldKey, rowID); err != nil {
		return fmt.Errorf("failed to delete old primary key %s: %w", t.PrimaryKey.Name, err)
	}

	return nil
}

func (t *Table) updateCompositePrimaryKey(ctx context.Context, oldKeyParts []OptionalValue, aRow Row) error {
	if t.PrimaryKey.Index == nil {
		return fmt.Errorf("table %s has primary key but no index", t.Name)
	}

	oldKeyValues := make([]any, 0, len(oldKeyParts))
	for i, keyPart := range oldKeyParts {
		if !keyPart.Valid {
			return fmt.Errorf("failed to get value for old composite primary key %s", t.PrimaryKey.Name)
		}
		castedKey, err := castKeyValue(t.PrimaryKey.Columns[i], keyPart.Value)
		if err != nil {
			return fmt.Errorf("failed to cast old primary key value for %s: %w", t.PrimaryKey.Name, err)
		}
		oldKeyValues = append(oldKeyValues, castedKey)
	}

	newKeyValues := make([]any, 0, len(oldKeyParts))
	for _, aColumn := range t.PrimaryKey.Columns {
		keyValue, ok := aRow.GetValue(aColumn.Name)
		if !ok {
			return fmt.Errorf("failed to get value for new composite primary key %s", t.PrimaryKey.Name)
		}
		if !keyValue.Valid {
			return fmt.Errorf("cannot update composite primary key %s to part NULL", t.PrimaryKey.Name)
		}
		castedKey, err := castKeyValue(aColumn, keyValue.Value)
		if err != nil {
			return fmt.Errorf("failed to cast new composite primary key value for %s: %w", t.PrimaryKey.Name, err)
		}
		newKeyValues = append(newKeyValues, castedKey)
	}

	var (
		oldCK = NewCompositeKey(t.PrimaryKey.Columns, oldKeyValues...)
		ck    = NewCompositeKey(t.PrimaryKey.Columns, newKeyValues...)
		rowID = aRow.Key
	)

	// We try to insert new primary key first to avoid leaving table in inconsistent state
	// If the new primary key is already taken, we return an error without modifying the existing row
	if err := t.PrimaryKey.Index.Insert(ctx, ck, rowID); err != nil {
		return fmt.Errorf("failed to insert new composite primary key %s: %w", t.PrimaryKey.Name, err)
	}
	if err := t.PrimaryKey.Index.Delete(ctx, oldCK, rowID); err != nil {
		return fmt.Errorf("failed to delete old composite primary key %s: %w", t.PrimaryKey.Name, err)
	}

	return nil
}

// castKeyValue casts an index key value to the appropriate type based on the column kind
// parser returns all numbers as int64 or float64, but index keys can be int4 (int32) or real (float32)
func castKeyValue(aColumn Column, aValue any) (any, error) {
	switch aColumn.Kind {
	case Int4:
		value, ok := aValue.(int32)
		if !ok {
			_, ok = aValue.(int64)
			if !ok {
				return nil, fmt.Errorf("could not cast value for column %s to either int64 or int32", aColumn.Name)
			}
			value = int32(aValue.(int64))
		}
		return value, nil
	case Real:
		value, ok := aValue.(float32)
		if !ok {
			_, ok = aValue.(float64)
			if !ok {
				return nil, fmt.Errorf("could not cast value for column %s to either float64 or float32", aColumn.Name)
			}
			value = float32(aValue.(float64))
		}
		return value, nil
	case Varchar:
		tp, ok := aValue.(TextPointer)
		if !ok {
			return nil, fmt.Errorf("could not cast value for column %s to TextPointer", aColumn.Name)
		}
		return tp.String(), nil
	case Timestamp:
		timestamp, ok := aValue.(Time)
		if !ok {
			return nil, fmt.Errorf("could not cast value for column %s to Time", aColumn.Name)
		}
		return timestamp.TotalMicroseconds(), nil
	default:
		return aValue, nil
	}
}
