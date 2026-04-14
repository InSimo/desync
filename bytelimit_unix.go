//go:build !windows

package desync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// shmName returns the shared memory file name, scoped to the current user
// to avoid cross-user permission issues.
func shmName() string {
	return fmt.Sprintf("desync-inflight-%d", os.Getuid())
}

// shmDir returns the directory for the shared memory file.
// On Linux, /dev/shm is a tmpfs mount.  On macOS and other systems,
// fall back to os.TempDir().
func shmDir() string {
	if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
		return "/dev/shm"
	}
	return os.TempDir()
}

// openSharedMem creates or opens the shared memory file and returns
// the mmap'd byte slice.  The file is created with mode 0666 so that
// processes running as different users (e.g. git user on a server) can
// share the same region.
func openSharedMem() ([]byte, error) {
	path := filepath.Join(shmDir(), shmName())

	// Try to open an existing file first (works even if we're not the
	// owner, as long as it was created with 0666).
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Ensure the file is the right size.  Truncate is a no-op if
	// already the correct size.
	if err := f.Truncate(shmSize); err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(
		int(f.Fd()), 0, shmSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// closeSharedMem unmaps the shared memory region.
func closeSharedMem(data []byte) error {
	return syscall.Munmap(data)
}

// processAlive checks whether a process with the given PID is still running.
//
// kill(pid, 0) returns:
//   - nil:    process exists and we have permission to signal it
//   - ESRCH:  process does not exist
//   - EPERM:  process exists but we lack permission (different user)
//
// We treat EPERM as alive to avoid incorrectly reaping slots owned by
// processes running as a different user.
func processAlive(pid int32) bool {
	err := syscall.Kill(int(pid), 0)
	if err == nil {
		return true
	}
	// ESRCH = no such process → dead.  Any other error (EPERM) → assume alive.
	return !errors.Is(err, syscall.ESRCH)
}
