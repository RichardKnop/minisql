package minisql

import (
	"context"
	"fmt"
)

type PrimaryKey struct {
	Name          string
	Column        Column
	Autoincrement bool
	Index         BTreeIndex
}

func (t *Table) insertPrimaryKey(ctx context.Context, pkValue OptionalValue, rowID RowID) (any, error) {
	if !pkValue.Valid {
		if !t.PrimaryKey.Autoincrement {
			return 0, fmt.Errorf("failed to get value for primary key %s", t.PrimaryKey.Name)
		}
		newPrimaryKey, err := t.insertAutoincrementedPrimaryKey(ctx, rowID)
		if err != nil {
			return 0, err
		}
		return newPrimaryKey, nil
	}
	castedValue, err := castPrimaryKeyValue(t.PrimaryKey.Column, pkValue.Value)
	if err != nil {
		return 0, fmt.Errorf("failed to cast primary key value for %s: %w", t.PrimaryKey.Name, err)
	}

	t.logger.Sugar().With(
		"name", t.PrimaryKey.Name,
		"key", castedValue,
	).Debug("inserting primary key")

	if err := t.PrimaryKey.Index.Insert(ctx, castedValue, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}
	return castedValue, nil
}

func (t *Table) insertAutoincrementedPrimaryKey(ctx context.Context, rowID RowID) (int64, error) {
	if t.PrimaryKey.Autoincrement && t.PrimaryKey.Column.Kind != Int8 {
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
		"name", t.PrimaryKey.Name,
		"key", int(newPrimaryKey),
	).Debug("inserting autoincremented primary key")

	if err := t.PrimaryKey.Index.Insert(ctx, newPrimaryKey, rowID); err != nil {
		return 0, fmt.Errorf("failed to insert primary key %s: %w", t.PrimaryKey.Name, err)
	}
	return newPrimaryKey, nil
}

func (t *Table) updatePrimaryKey(ctx context.Context, oldPkValue OptionalValue, aRow *Row) error {
	if t.PrimaryKey.Index == nil {
		return fmt.Errorf("table %s has primary key but no index", t.Name)
	}
	pkValue, ok := aRow.GetValue(t.PrimaryKey.Column.Name)
	if !ok {
		return nil
	}
	if !pkValue.Valid {
		return fmt.Errorf("cannot update primary key %s to NULL", t.PrimaryKey.Name)
	}
	castedValue, err := castPrimaryKeyValue(t.PrimaryKey.Column, pkValue.Value)
	if err != nil {
		return fmt.Errorf("failed to cast primary key value for %s: %w", t.PrimaryKey.Name, err)
	}
	rowID := aRow.Key
	// We try to insert new primary key first to avoid leaving table in inconsistent state
	// If the new primary key is already taken, we return an error without modifying the existing row
	if err := t.PrimaryKey.Index.Insert(ctx, castedValue, rowID); err != nil {
		return fmt.Errorf("failed to insert new primary key %s: %w", t.PrimaryKey.Name, err)
	}
	if err := t.PrimaryKey.Index.Delete(ctx, oldPkValue.Value); err != nil {
		return fmt.Errorf("failed to delete old primary key %s: %w", t.PrimaryKey.Name, err)
	}

	return nil
}

// castPrimaryKeyValue casts the primary key value to the appropriate type based on the column kind
// parser returns all numbers as int64 or float64, but primary keys can be int4 (int32) or real (float32)
func castPrimaryKeyValue(aColumn Column, aValue any) (any, error) {
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
	default:
		return aValue, nil
	}
}
