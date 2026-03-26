//go:build !windows

package desync

import (
	"os"

	"golang.org/x/sys/unix"
)

// FadviseNoNeed tells the kernel that the given file range is no longer
// needed and its page cache entries can be reclaimed.  This is advisory —
// errors are silently ignored.
func FadviseNoNeed(f *os.File, offset, length int64) {
	unix.Fadvise(int(f.Fd()), offset, length, unix.FADV_DONTNEED)
}
