//go:build !windows

package appserver

import "os/exec"

func configureCommand(_ *exec.Cmd) {}

func configureBatchCommand(_ *exec.Cmd, _ string) {}

func preferredCodexPaths() []string { return nil }

func terminateOwnedProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
