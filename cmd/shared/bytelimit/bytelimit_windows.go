//go:build windows

package bytelimit

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32              = syscall.NewLazyDLL("kernel32.dll")
	procCreateFileMapping = kernel32.NewProc("CreateFileMappingW")
	procOpenFileMapping   = kernel32.NewProc("OpenFileMappingW")
	procMapViewOfFile     = kernel32.NewProc("MapViewOfFile")
	procUnmapViewOfFile   = kernel32.NewProc("UnmapViewOfFile")
	procCloseHandle       = kernel32.NewProc("CloseHandle")
	procOpenProcess       = kernel32.NewProc("OpenProcess")
)

const (
	invalidHandleValue = ^uintptr(0) // INVALID_HANDLE_VALUE
	pageReadWrite      = 0x04        // PAGE_READWRITE
	fileMapAllAccess   = 0x000F001F  // FILE_MAP_ALL_ACCESS
	processQueryInfo   = 0x0400      // PROCESS_QUERY_LIMITED_INFORMATION
)

// shmMapName is the name of the shared memory mapping object.
// "Local\" prefix scopes it to the current session — sufficient for
// git operations which all run in the same user session.
const shmMapName = "Local\\desync-inflight"

// windowsMapping holds the handle for cleanup.
var windowsMapping struct {
	handle uintptr
}

func openSharedMem() ([]byte, error) {
	namePtr, err := syscall.UTF16PtrFromString(shmMapName)
	if err != nil {
		return nil, err
	}

	// Try to open an existing mapping first.
	handle, _, _ := procOpenFileMapping.Call(
		fileMapAllAccess,
		0,
		uintptr(unsafe.Pointer(namePtr)),
	)
	if handle == 0 {
		// Create a new mapping backed by the page file.
		handle, _, err = procCreateFileMapping.Call(
			invalidHandleValue,
			0,
			pageReadWrite,
			0,
			uintptr(shmSize),
			uintptr(unsafe.Pointer(namePtr)),
		)
		if handle == 0 {
			return nil, fmt.Errorf("CreateFileMapping: %w", err)
		}
	}
	windowsMapping.handle = handle

	ptr, _, err := procMapViewOfFile.Call(
		handle,
		fileMapAllAccess,
		0, 0,
		uintptr(shmSize),
	)
	if ptr == 0 {
		procCloseHandle.Call(handle)
		return nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), shmSize)
	return data, nil
}

func closeSharedMem(data []byte) error {
	if len(data) > 0 {
		procUnmapViewOfFile.Call(uintptr(unsafe.Pointer(&data[0])))
	}
	if windowsMapping.handle != 0 {
		procCloseHandle.Call(windowsMapping.handle)
		windowsMapping.handle = 0
	}
	return nil
}

// processAlive checks whether a process with the given PID is still running.
//
// OpenProcess with PROCESS_QUERY_LIMITED_INFORMATION returns:
//   - a valid handle: process exists (we have permission or it's ours)
//   - 0 with ERROR_INVALID_PARAMETER: process does not exist
//   - 0 with ERROR_ACCESS_DENIED: process exists but belongs to another user
//
// We treat ERROR_ACCESS_DENIED as alive to avoid incorrectly reaping slots
// owned by processes running as a different user.
func processAlive(pid int32) bool {
	handle, _, err := procOpenProcess.Call(processQueryInfo, 0, uintptr(pid))
	if handle != 0 {
		procCloseHandle.Call(handle)
		return true
	}
	// ERROR_ACCESS_DENIED (5) means the process exists but we can't open it.
	if errno, ok := err.(syscall.Errno); ok && errno == 5 {
		return true
	}
	return false
}
