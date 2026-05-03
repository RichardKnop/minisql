package minisql

import "fmt"

// ErrCheckConstraintViolation is returned when an INSERT or UPDATE row fails a CHECK constraint.
type ErrCheckConstraintViolation struct {
	ColumnName string
	Expr       string
}

func (e ErrCheckConstraintViolation) Error() string {
	return fmt.Sprintf("check constraint violation on column %q: %s", e.ColumnName, e.Expr)
}

// validateCheckConstraints verifies that row satisfies every column-level CHECK
// constraint defined in columns.  Returns ErrCheckConstraintViolation on the
// first failure, nil if all constraints pass.
func validateCheckConstraints(columns []Column, row Row) error {
	for _, col := range columns {
		if col.CheckCond == nil {
			continue
		}
		dnf := col.CheckCond.ToDNF()
		ok, err := row.CheckOneOrMore(dnf)
		if err != nil {
			return fmt.Errorf("check constraint on column %q: %w", col.Name, err)
		}
		if !ok {
			return ErrCheckConstraintViolation{ColumnName: col.Name, Expr: col.Check}
		}
	}
	return nil
}
