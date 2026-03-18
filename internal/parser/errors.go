package parser

import (
	"fmt"
)

// ParseError is returned by Parse when the SQL input is malformed.
// It carries the byte offset in the normalised (whitespace-collapsed) SQL
// string where the error was detected, a short snippet of the surrounding
// text for context, and the human-readable message.
type ParseError struct {
	Pos  int    // byte offset in the normalised SQL string
	Near string // up to 20 chars of SQL starting at Pos
	Msg  string // human-readable description
	err  error  // underlying sentinel error (nil for inline errors)
}

func (e *ParseError) Error() string {
	if e.Near != "" {
		return fmt.Sprintf("%s (near %q, position %d)", e.Msg, e.Near, e.Pos)
	}
	return fmt.Sprintf("%s (position %d)", e.Msg, e.Pos)
}

// Unwrap allows errors.Is / errors.As to match against the underlying
// sentinel error, preserving backwards-compatible error checking.
func (e *ParseError) Unwrap() error {
	return e.err
}

// errorf creates a ParseError at the current position with a formatted message.
// Use for inline errors that have no corresponding package-level sentinel.
func (p *parserItem) errorf(format string, args ...any) error {
	return &ParseError{
		Pos:  p.i,
		Near: p.near(),
		Msg:  fmt.Sprintf(format, args...),
	}
}

// wrapErr wraps a sentinel error in a ParseError at the current position.
// The sentinel is accessible via errors.Is / errors.As through Unwrap,
// so existing error checks remain valid.
func (p *parserItem) wrapErr(err error) error {
	return &ParseError{
		Pos:  p.i,
		Near: p.near(),
		Msg:  err.Error(),
		err:  err,
	}
}

// near returns up to 20 characters of the normalised SQL starting at the
// current parse position, used to give context in error messages.
func (p *parserItem) near() string {
	if p.i >= len(p.sql) {
		return ""
	}
	end := min(p.i+20, len(p.sql))
	return p.sql[p.i:end]
}
