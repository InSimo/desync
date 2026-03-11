//go:build windows

package main

import (
	"os"
	"os/exec"
)

// execDelegate starts path as a child process with stdin/stdout/stderr
// inherited from the current process, waits for it to finish, and exits
// with the same exit code. This emulates Unix execve on Windows.
// It only returns if the process could not be started.
func execDelegate(path string, args []string, env []string) error {
	cmd := exec.Command(path, args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		return err
	}
	os.Exit(0)
	return nil // unreachable
}
