package errors

import (
	"errors"
	"fmt"
)

// ErrPageChecksumMismatch is returned when the CRC32 checksum stored in the
// last four bytes of a database page does not match the computed checksum.
// This indicates on-disk corruption for the affected page.
var ErrPageChecksumMismatch = errors.New("page checksum mismatch: possible corruption")

// PageChecksumError carries the page index alongside the sentinel so callers
// can identify which page is corrupt.
type PageChecksumError struct {
	PageIndex uint32
}

// Error returns a string representation of the PageChecksumError, including the page index.
func (e PageChecksumError) Error() string {
	return fmt.Sprintf("page %d: checksum mismatch (possible corruption)", e.PageIndex)
}

// Is allows errors.Is to recognize PageChecksumError as an ErrPageChecksumMismatch.
func (e PageChecksumError) Is(target error) bool {
	return target == ErrPageChecksumMismatch
}
