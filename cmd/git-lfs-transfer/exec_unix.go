//go:build !windows

package main

import "syscall"

// execDelegate replaces the current process with path using execve(2).
// The SSH stdin/stdout/stderr are inherited automatically.
// If it returns, the exec failed.
func execDelegate(path string, args []string, env []string) error {
	return syscall.Exec(path, args, env)
}
