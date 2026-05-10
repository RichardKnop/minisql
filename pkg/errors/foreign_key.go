package errors

import (
	"errors"
	"fmt"
	"strings"
)

// ErrForeignKeyViolation is returned when a child row's FK value does not exist
// in the parent table.
type ErrForeignKeyViolation struct {
	ChildTable    string
	ChildColumns  []string
	ParentTable   string
	ParentColumns []string
}

func (e ErrForeignKeyViolation) Error() string {
	return fmt.Sprintf(
		"foreign key constraint violation: %s.(%s) references non-existent value in %s.(%s)",
		e.ChildTable, strings.Join(e.ChildColumns, ", "),
		e.ParentTable, strings.Join(e.ParentColumns, ", "),
	)
}

// ErrForeignKeyParentViolation is returned when deleting or updating a parent row
// that is still referenced by one or more child rows.
type ErrForeignKeyParentViolation struct {
	ParentTable   string
	ParentColumns []string
	ChildTable    string
	ChildColumns  []string
}

func (e ErrForeignKeyParentViolation) Error() string {
	return fmt.Sprintf(
		"foreign key constraint violation: %s.(%s) is still referenced by %s.(%s)",
		e.ParentTable, strings.Join(e.ParentColumns, ", "),
		e.ChildTable, strings.Join(e.ChildColumns, ", "),
	)
}

// ErrDropTableReferencedByFK is the base error for dropping a table that is
// still referenced by a foreign key in another table.
var ErrDropTableReferencedByFK = errors.New("cannot drop table: it is still referenced by a foreign key constraint")
