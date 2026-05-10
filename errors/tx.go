package errors

import (
	"errors"
)

// ErrTxConflict is returned when an optimistic concurrency check fails at commit time.
var ErrTxConflict = errors.New("transaction conflict detected")
