//go:build windows

package minisql

import "os"

// syscallFsync on Windows delegates to f.Sync(), which calls FlushFileBuffers.
// The POSIX fsync(2) bypass in sync_file_unix.go is macOS-specific (avoiding
// F_FULLFSYNC); it does not apply on Windows.
func syscallFsync(f *os.File) error {
	return f.Sync()
}

// fastSync calls syscallFsync when the DBFile is a plain *os.File, and falls
// back to the interface's Sync() method for any other implementation (mocks,
// in-memory files used in tests, etc.).
func fastSync(f DBFile) error {
	if osf, ok := f.(*os.File); ok {
		return syscallFsync(osf)
	}
	return f.Sync()
}
