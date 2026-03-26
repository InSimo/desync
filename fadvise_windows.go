package desync

import "os"

// FadviseNoNeed is a no-op on Windows (no fadvise support).
func FadviseNoNeed(f *os.File, offset, length int64) {}
