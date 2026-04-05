//go:build !windows

package cacheevict

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

// openCacheSizesFile creates or opens a persistent file-backed mmap region.
// Returns the mmap'd data, whether the file was newly created (or had a
// size mismatch), and any error.
func openCacheSizesFile(path string, size int) ([]byte, bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}

	isNew := info.Size() == 0
	if isNew || info.Size() != int64(size) {
		if err := f.Truncate(int64(size)); err != nil {
			return nil, false, err
		}
		isNew = true
	}

	data, err := syscall.Mmap(
		int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, false, err
	}

	return data, isNew, nil
}

func closeCacheSizesFile(data []byte) error {
	return syscall.Munmap(data)
}

func syncCacheSizesFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		uintptr(syscall.MS_SYNC),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// processAlive checks whether a process with the given PID is still running.
//
// kill(pid, 0) returns:
//   - nil:    process exists and we have permission to signal it
//   - ESRCH:  process does not exist
//   - EPERM:  process exists but we lack permission (different user)
//
// We treat EPERM as alive to avoid incorrectly reclaiming the eviction
// lock from a process running as a different user.
func processAlive(pid int32) bool {
	err := syscall.Kill(int(pid), 0)
	if err == nil {
		return true
	}
	return !errors.Is(err, syscall.ESRCH)
}
