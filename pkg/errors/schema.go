package errors

import (
	"fmt"
)

// ErrNoSuchTable is returned when a statement references a table that does not
// exist in the database schema.
type ErrNoSuchTable struct {
	Name string
}

func (e ErrNoSuchTable) Error() string {
	return fmt.Sprintf("table %q does not exist", e.Name)
}

// ErrTableAlreadyExists is returned when CREATE TABLE is called for a table
// whose name already exists in the schema.
type ErrTableAlreadyExists struct {
	Name string
}

func (e ErrTableAlreadyExists) Error() string {
	return fmt.Sprintf("table %q already exists", e.Name)
}

// ErrNoSuchIndex is returned when a statement references an index that does not
// exist in the database schema.
type ErrNoSuchIndex struct {
	Name string
}

func (e ErrNoSuchIndex) Error() string {
	return fmt.Sprintf("index %q does not exist", e.Name)
}

// ErrIndexAlreadyExists is returned when CREATE INDEX is called for an index
// whose name already exists in the schema.
type ErrIndexAlreadyExists struct {
	Name string
}

func (e ErrIndexAlreadyExists) Error() string {
	return fmt.Sprintf("index %q already exists", e.Name)
}
