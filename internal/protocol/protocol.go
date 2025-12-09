package protocol

import (
	"github.com/RichardKnop/minisql/internal/minisql"
)

// This is very simplistic TCP protocol using JSON messages terminated by newlines.
// This will need to be rewritten later with a proper wire protocol for production
// use. But for now, I just need something very quick so I can focus on the database
// internals.

type Request struct {
	Type string `json:"type"` // "sql", "ping", "list_tables"
	SQL  string `json:"sql"`
}

type Response struct {
	Kind         minisql.StatementKind     `json:"kind,omitempty"`
	Success      bool                      `json:"success"`
	Error        string                    `json:"error,omitempty"`
	Columns      []minisql.Column          `json:"columns,omitempty"`
	Rows         [][]minisql.OptionalValue `json:"rows,omitempty"`
	RowsAffected int                       `json:"rows_affected,omitempty"`
	Message      string                    `json:"message,omitempty"`
}
