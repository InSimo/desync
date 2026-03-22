//go:build !windows

package bytelimit

import (
	"os"
	"path/filepath"
	"syscall"
)

// shmName is the shared memory file name.
const shmName = "desync-inflight"

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
	path := filepath.Join(shmDir(), shmName)
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
func processAlive(pid int32) bool {
	return syscall.Kill(int(pid), 0) == nil
}
