package minisql

import (
	"os"
	"syscall"
)

// syscallFsync calls the POSIX fsync(2) syscall directly via SyscallConn,
// bypassing os.File.Sync() which on macOS uses fcntl(F_FULLFSYNC) instead of
// plain fsync(2). F_FULLFSYNC guarantees a physical media write (~3-5 ms on
// Apple SSDs), whereas POSIX fsync(2) only guarantees the data is in the OS
// page cache (~50 µs). For a non-safety-critical database this matches the
// behaviour of modernc.org/sqlite (which also calls plain fsync(2)).
//
// Falls back to f.Sync() if SyscallConn is unavailable.
func syscallFsync(f *os.File) error {
	raw, err := f.SyscallConn()
	if err != nil {
		return f.Sync()
	}
	var syncErr error
	if ctlErr := raw.Control(func(fd uintptr) {
		syncErr = syscall.Fsync(int(fd))
	}); ctlErr != nil {
		return f.Sync()
	}
	return syncErr
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
