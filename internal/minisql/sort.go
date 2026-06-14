package minisql

import (
	"sort"
)

// evalOrderByValue returns the sort key for a row given an ORDER BY clause.
// If the clause has an Expr (e.g. NATURAL_SORT(col)), it is evaluated;
// otherwise the named column value is looked up directly.
func evalOrderByValue(clause OrderBy, row Row) (OptionalValue, bool, error) {
	if clause.Field.Expr != nil {
		v, err := clause.Field.Expr.Eval(row)
		if err != nil {
			return OptionalValue{}, false, err
		}
		if v == nil {
			return OptionalValue{}, true, nil
		}
		return OptionalValue{Value: v, Valid: true}, true, nil
	}
	val, found := row.getValueQualified(clause.Field.AliasPrefix, clause.Field.Name)
	return val, found, nil
}

func (t *Table) sortRows(rows []Row, orderBy []OrderBy) error {
	if len(orderBy) == 0 {
		return nil
	}

	var sortErr error
	sort.Slice(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for _, clause := range orderBy {
			valI, foundI, err := evalOrderByValue(clause, rows[i])
			if err != nil {
				sortErr = err
				return false
			}
			valJ, foundJ, err := evalOrderByValue(clause, rows[j])
			if err != nil {
				sortErr = err
				return false
			}
			if !foundI || !foundJ {
				continue
			}

			cmp := compareValues(valI, valJ)
			if cmp == 0 {
				continue
			}
			if clause.Direction == Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	return sortErr
}
