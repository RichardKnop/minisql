package errors

import (
	"errors"
)

// ErrConcurrentWriter is returned when a second write transaction is attempted
// while one is already active. Only one write transaction may exist at a time.
var ErrConcurrentWriter = errors.New("concurrent writer: only one write transaction allowed at a time")
