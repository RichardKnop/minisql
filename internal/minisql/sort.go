package minisql

import (
	"sort"
)

func (t *Table) sortRows(rows []Row, orderBy []OrderBy) error {
	if len(orderBy) == 0 {
		return nil
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, clause := range orderBy {
			// Use alias prefix for JOIN queries where columns are prefixed (e.g., "u.name")
			colName := clause.Field.Name
			if clause.Field.AliasPrefix != "" {
				colName = clause.Field.AliasPrefix + "." + clause.Field.Name
			}
			valI, foundI := rows[i].GetValue(colName)
			valJ, foundJ := rows[j].GetValue(colName)

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
