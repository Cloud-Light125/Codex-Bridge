//go:build windows

package appserver

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// preferredCodexPaths returns native Codex installations before PATH shims.
// Windows can have an older npm codex.cmd earlier on PATH; that binary may
// report a valid version while not implementing the paginated history RPCs.
func preferredCodexPaths() []string {
	localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
	if localAppData == "" {
		return nil
	}

	var result []codexPathCandidate
	for _, directory := range []string{
		filepath.Join(localAppData, "OpenAI", "Codex", "bin"),
		filepath.Join(localAppData, "Programs", "OpenAI", "Codex", "bin"),
	} {
		result = append(result, codexPathCandidates(directory)...)
	}
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].modified.Equal(result[right].modified) {
			return result[left].path < result[right].path
		}
		return result[left].modified.After(result[right].modified)
	})
	paths := make([]string, 0, len(result))
	for _, candidate := range result {
		paths = append(paths, candidate.path)
	}
	return paths
}

type codexPathCandidate struct {
	path     string
	modified time.Time
}

func codexPathCandidates(directory string) []codexPathCandidate {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil
	}

	result := make([]codexPathCandidate, 0)
	add := func(path string, modified time.Time) {
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			return
		}
		result = append(result, codexPathCandidate{path: path, modified: modified})
	}
	for _, entry := range entries {
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			add(filepath.Join(directory, entry.Name(), "codex.exe"), info.ModTime())
			continue
		}
		if !strings.EqualFold(entry.Name(), "codex.exe") && !strings.EqualFold(entry.Name(), "codex.cmd") && !strings.EqualFold(entry.Name(), "codex.bat") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		add(filepath.Join(directory, entry.Name()), info.ModTime())
	}
	return result
}

func configureCommand(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
}

func configureBatchCommand(cmd *exec.Cmd, commandLine string) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CmdLine: commandLine}
}

func terminateOwnedProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	kill := exec.Command("taskkill.exe", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F")
	kill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := kill.Run(); err == nil {
		return nil
	}
	return cmd.Process.Kill()
}
