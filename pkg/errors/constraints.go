package errors

import (
	"fmt"
	"strings"
)

// ErrUniqueViolation is returned when an INSERT or UPDATE would create a
// duplicate value in a unique index or primary key.
type ErrUniqueViolation struct {
	Table   string
	Index   string
	Columns []string
	cause   error // typically ErrDuplicateKey; preserved for errors.Is chains
}

// NewUniqueViolation constructs an ErrUniqueViolation. Pass the raw
// ErrDuplicateKey sentinel as cause so that errors.Is(err, ErrDuplicateKey)
// continues to work for callers who already rely on that check.
func NewUniqueViolation(table, index string, columns []string, cause error) ErrUniqueViolation {
	return ErrUniqueViolation{Table: table, Index: index, Columns: columns, cause: cause}
}

func (e ErrUniqueViolation) Error() string {
	return fmt.Sprintf(
		"unique constraint violation on table %q index %q: duplicate value in column(s) (%s)",
		e.Table, e.Index, strings.Join(e.Columns, ", "),
	)
}

// Unwrap lets errors.Is(err, ErrDuplicateKey) traverse the chain.
func (e ErrUniqueViolation) Unwrap() error { return e.cause }

// ErrNotNullViolation is returned when an INSERT or UPDATE would store NULL
// in a NOT NULL column.
type ErrNotNullViolation struct {
	Table  string
	Column string
}

func (e ErrNotNullViolation) Error() string {
	return fmt.Sprintf("not null constraint violation on table %q: field %q cannot be NULL", e.Table, e.Column)
}

// ErrTypeMismatch is returned when a supplied value cannot be stored in the
// target column due to an incompatible type.
type ErrTypeMismatch struct {
	Table    string
	Column   string
	Expected string
	Detail   string // optional human-readable detail from the underlying validator
}

func (e ErrTypeMismatch) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("type mismatch on table %q column %q: %s", e.Table, e.Column, e.Detail)
	}
	return fmt.Sprintf("type mismatch on table %q column %q: expected %s", e.Table, e.Column, e.Expected)
}

// ErrCheckConstraintViolation is returned when an INSERT or UPDATE row fails
// a CHECK constraint.
type ErrCheckConstraintViolation struct {
	Table      string
	ColumnName string
	Expr       string
}

func (e ErrCheckConstraintViolation) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("check constraint violation on table %q column %q: %s", e.Table, e.ColumnName, e.Expr)
	}
	return fmt.Sprintf("check constraint violation on column %q: %s", e.ColumnName, e.Expr)
}
