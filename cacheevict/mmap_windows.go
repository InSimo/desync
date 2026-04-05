//go:build windows

package cacheevict

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32                = syscall.NewLazyDLL("kernel32.dll")
	procCreateFileMappingW  = kernel32.NewProc("CreateFileMappingW")
	procMapViewOfFile       = kernel32.NewProc("MapViewOfFile")
	procUnmapViewOfFile     = kernel32.NewProc("UnmapViewOfFile")
	procFlushViewOfFile     = kernel32.NewProc("FlushViewOfFile")
	procFlushFileBuffers    = kernel32.NewProc("FlushFileBuffers")
	procCloseHandle         = kernel32.NewProc("CloseHandle")
	procOpenProcess         = kernel32.NewProc("OpenProcess")
)

const (
	pageReadWrite           = 0x04
	fileMapAllAccess        = 0x000F001F
	processQueryLimitedInfo = 0x1000
)

var windowsCacheSizesState struct {
	fileHandle    syscall.Handle
	mappingHandle uintptr
}

func openCacheSizesFile(path string, size int) ([]byte, bool, error) {
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, false, err
	}

	handle, err := syscall.CreateFile(
		pathp,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, false, fmt.Errorf("CreateFile %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		syscall.CloseHandle(handle)
		return nil, false, err
	}

	isNew := info.Size() == 0
	if isNew || info.Size() != int64(size) {
		_, err := syscall.Seek(handle, int64(size), 0)
		if err != nil {
			syscall.CloseHandle(handle)
			return nil, false, fmt.Errorf("Seek: %w", err)
		}
		if err := syscall.SetEndOfFile(handle); err != nil {
			syscall.CloseHandle(handle)
			return nil, false, fmt.Errorf("SetEndOfFile: %w", err)
		}
		isNew = true
	}

	mapping, _, err := procCreateFileMappingW.Call(
		uintptr(handle),
		0,
		pageReadWrite,
		0,
		uintptr(size),
		0,
	)
	if mapping == 0 {
		syscall.CloseHandle(handle)
		return nil, false, fmt.Errorf("CreateFileMappingW: %w", err)
	}

	ptr, _, err := procMapViewOfFile.Call(
		mapping,
		fileMapAllAccess,
		0, 0,
		uintptr(size),
	)
	if ptr == 0 {
		procCloseHandle.Call(mapping)
		syscall.CloseHandle(handle)
		return nil, false, fmt.Errorf("MapViewOfFile: %w", err)
	}

	windowsCacheSizesState.fileHandle = handle
	windowsCacheSizesState.mappingHandle = mapping

	data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), size)
	return data, isNew, nil
}

func closeCacheSizesFile(data []byte) error {
	if len(data) > 0 {
		procUnmapViewOfFile.Call(uintptr(unsafe.Pointer(&data[0])))
	}
	if windowsCacheSizesState.mappingHandle != 0 {
		procCloseHandle.Call(windowsCacheSizesState.mappingHandle)
		windowsCacheSizesState.mappingHandle = 0
	}
	if windowsCacheSizesState.fileHandle != 0 {
		syscall.CloseHandle(windowsCacheSizesState.fileHandle)
		windowsCacheSizesState.fileHandle = 0
	}
	return nil
}

func syncCacheSizesFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	ret, _, err := procFlushViewOfFile.Call(
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
	)
	if ret == 0 {
		return fmt.Errorf("FlushViewOfFile: %w", err)
	}
	if windowsCacheSizesState.fileHandle != 0 {
		ret, _, err = procFlushFileBuffers.Call(uintptr(windowsCacheSizesState.fileHandle))
		if ret == 0 {
			return fmt.Errorf("FlushFileBuffers: %w", err)
		}
	}
	return nil
}

// processAlive checks whether a process with the given PID is still running.
//
// OpenProcess with PROCESS_QUERY_LIMITED_INFORMATION returns:
//   - a valid handle: process exists
//   - 0 with ERROR_INVALID_PARAMETER: process does not exist
//   - 0 with ERROR_ACCESS_DENIED (5): process exists but belongs to another user
//
// We treat ERROR_ACCESS_DENIED as alive to avoid incorrectly reclaiming
// the eviction lock from a process running as a different user.
func processAlive(pid int32) bool {
	handle, _, err := procOpenProcess.Call(processQueryLimitedInfo, 0, uintptr(pid))
	if handle != 0 {
		procCloseHandle.Call(handle)
		return true
	}
	if errno, ok := err.(syscall.Errno); ok && errno == 5 {
		return true
	}
	return false
}
