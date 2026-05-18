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
			valI, foundI := rows[i].getValueQualified(clause.Field.AliasPrefix, clause.Field.Name)
			valJ, foundJ := rows[j].getValueQualified(clause.Field.AliasPrefix, clause.Field.Name)

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
