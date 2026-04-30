package minisql

import (
	"bytes"
	"strings"
)

// equalAny returns true if a and b are equal, using type-safe comparison.
// Returns false for nil inputs (SQL NULL != NULL semantics) and type mismatches.
func equalAny(a, b any) bool {
	if a == nil || b == nil {
		return false
	}
	return compareAny(a, b) == 0
}

// compareAny compares two non-NULL values of the same type and returns -1, 0, or 1.
func compareAny(a, b any) int {
	switch val := a.(type) {
	case bool:
		bVal := b.(bool)
		if val == bVal {
			return 0
		}
		if val {
			return 1
		}
		return -1

	case int32:
		switch bVal := b.(type) {
		case int32:
			if val < bVal {
				return -1
			} else if val > bVal {
				return 1
			}
			return 0
		case int64:
			if int64(val) < bVal {
				return -1
			} else if int64(val) > bVal {
				return 1
			}
			return 0
		}
		return -1

	case int64:
		switch bVal := b.(type) {
		case int64:
			if val < bVal {
				return -1
			} else if val > bVal {
				return 1
			}
			return 0
		case int32:
			if val < int64(bVal) {
				return -1
			} else if val > int64(bVal) {
				return 1
			}
			return 0
		}
		return -1

	case float32:
		switch bVal := b.(type) {
		case float32:
			if val < bVal {
				return -1
			} else if val > bVal {
				return 1
			}
			return 0
		case float64:
			if float64(val) < bVal {
				return -1
			} else if float64(val) > bVal {
				return 1
			}
			return 0
		}
		return -1

	case float64:
		switch bVal := b.(type) {
		case float64:
			if val < bVal {
				return -1
			} else if val > bVal {
				return 1
			}
			return 0
		case float32:
			if val < float64(bVal) {
				return -1
			} else if val > float64(bVal) {
				return 1
			}
			return 0
		}
		return -1

	case string:
		bVal := b.(string)
		return strings.Compare(val, bVal)

	case TextPointer:
		bVal := b.(TextPointer)
		return strings.Compare(val.String(), bVal.String())

	case TimestampMicros:
		bVal := b.(TimestampMicros)
		if val < bVal {
			return -1
		} else if val > bVal {
			return 1
		}
		return 0

	case CompositeKey:
		bVal := b.(CompositeKey)
		return bytes.Compare(val.Comparison, bVal.Comparison)
	}

	return 0
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
