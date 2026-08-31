//go:build !windows

package gitquery

import "os/exec"

func prepareCommand(_ *exec.Cmd) {}
