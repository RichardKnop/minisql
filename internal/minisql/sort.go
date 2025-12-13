package minisql

import (
	"sort"
	"strings"
)

func (t *Table) sortRows(rows []Row, orderBy []OrderBy) error {
	if len(orderBy) == 0 {
		return nil
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, clause := range orderBy {
			valI, foundI := rows[i].GetValue(clause.Field.Name)
			valJ, foundJ := rows[j].GetValue(clause.Field.Name)

			if !foundI || !foundJ {
				continue
			}

			cmp := compareValues(valI, valJ)

			if cmp == 0 {
				continue // Equal, check next ORDER BY column
			}

			if clause.Direction == Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	return nil
}

func compareValues(a, b OptionalValue) int {
	if !a.Valid && !b.Valid {
		return 0
	}
	if !a.Valid {
		return -1 // NULL is less than any value
	}
	if !b.Valid {
		return 1
	}

	return compareAny(a.Value, b.Value)
}

func compareAny(a, b any) int {
	switch aVal := a.(type) {
	case bool:
		bVal := b.(bool)
		if aVal == bVal {
			return 0
		}
		if aVal {
			return 1
		}
		return -1

	case int32:
		bVal := b.(int32)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0

	case int64:
		bVal := b.(int64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0

	case float32:
		bVal := b.(float32)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0

	case float64:
		bVal := b.(float64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0

	case string:
		bVal := b.(string)
		return strings.Compare(aVal, bVal)

	case TextPointer:
		bVal := b.(TextPointer)
		return strings.Compare(aVal.String(), bVal.String())

	default:
		return 0
	}
}
