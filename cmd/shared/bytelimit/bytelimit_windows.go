//go:build windows

package bytelimit

import (
	"fmt"
	"os"
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
const shmMapName = "Local\\desync-inflight"

// windowsMapping holds the handle and mapped view pointer for cleanup.
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

	// Convert the pointer to a byte slice.
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
func processAlive(pid int32) bool {
	handle, _, _ := procOpenProcess.Call(processQueryInfo, 0, uintptr(pid))
	if handle == 0 {
		return false
	}
	procCloseHandle.Call(handle)
	return true
}

// Ensure os is used (for potential future use).
var _ = os.Getpid
