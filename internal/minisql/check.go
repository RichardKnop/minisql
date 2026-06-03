package minisql

import (
	"fmt"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

// ErrCheckConstraintViolation aliases the public type so internal and e2e
// tests can reference it via either package without an import cycle.
type ErrCheckConstraintViolation = minisqlErrors.ErrCheckConstraintViolation

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
