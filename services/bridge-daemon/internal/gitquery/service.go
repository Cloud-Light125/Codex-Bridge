package gitquery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultTimeout       = 10 * time.Second
	DefaultMaxOutputSize = 200 * 1024
)

var (
	ErrInvalidWorkingDirectory = errors.New("工作目录无效")
	ErrNotGitRepository        = errors.New("工作目录不是 Git 仓库")
)

// CommandError contains only bounded stderr from one of the fixed Git
// commands. It deliberately does not expose a command string assembled from
// user input.
type CommandError struct {
	Args    []string
	Stderr  string
	Cause   error
	Timeout bool
}

func (e *CommandError) Error() string {
	if e == nil {
		return "Git 命令失败"
	}
	if e.Timeout {
		return "Git 查询超时"
	}
	if strings.TrimSpace(e.Stderr) != "" {
		return strings.TrimSpace(e.Stderr)
	}
	if e.Cause != nil {
		return fmt.Sprintf("Git 查询失败：%v", e.Cause)
	}
	return "Git 查询失败"
}

func (e *CommandError) Unwrap() error { return e.Cause }

type commandResult struct {
	stdout    string
	stderr    string
	truncated bool
}

type commandRunner func(context.Context, string, []string) (commandResult, error)

// Service is a deliberately narrow, read-only Git query surface. There is no
// method accepting an arbitrary command or argument list from callers.
type Service struct {
	GitPath       string
	Timeout       time.Duration
	MaxOutputSize int
	runner        commandRunner
}

type DiffSummary struct {
	Stat         string `json:"stat"`
	ChangedFiles string `json:"changedFiles"`
	Truncated    bool   `json:"truncated"`
}

type Snapshot struct {
	WorkingDirectory string `json:"workingDirectory"`
	Repository       bool   `json:"repository"`
	Branch           string `json:"branch,omitempty"`
	LatestCommit     string `json:"latestCommit,omitempty"`
	Status           string `json:"status,omitempty"`
	DiffStat         string `json:"diffStat,omitempty"`
	ChangedFiles     string `json:"changedFiles,omitempty"`
	Diff             string `json:"diff,omitempty"`
	Truncated        bool   `json:"truncated"`
}

func New() *Service {
	return &Service{GitPath: "git", Timeout: DefaultTimeout, MaxOutputSize: DefaultMaxOutputSize}
}

func (s *Service) normalize() {
	if strings.TrimSpace(s.GitPath) == "" {
		s.GitPath = "git"
	}
	if s.Timeout <= 0 {
		s.Timeout = DefaultTimeout
	}
	if s.MaxOutputSize <= 0 {
		s.MaxOutputSize = DefaultMaxOutputSize
	}
}

func (s *Service) IsWorkingDirectoryValid(workingDirectory string) bool {
	path, err := normalizeDirectory(workingDirectory)
	return err == nil && path != ""
}

func (s *Service) IsRepository(ctx context.Context, workingDirectory string) (bool, error) {
	path, err := normalizeDirectory(workingDirectory)
	if err != nil {
		return false, err
	}
	result, err := s.run(ctx, path, "rev-parse", "--is-inside-work-tree")
	if err != nil {
		var commandErr *CommandError
		if errors.As(err, &commandErr) && !commandErr.Timeout {
			return false, nil
		}
		return false, err
	}
	return strings.EqualFold(strings.TrimSpace(result.stdout), "true"), nil
}

func (s *Service) GetStatus(ctx context.Context, workingDirectory string) (string, error) {
	path, err := s.repositoryDirectory(ctx, workingDirectory)
	if err != nil {
		return "", err
	}
	result, err := s.run(ctx, path, "status", "--short")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(result.stdout), nil
}

func (s *Service) GetDiffSummary(ctx context.Context, workingDirectory string) (DiffSummary, error) {
	path, err := s.repositoryDirectory(ctx, workingDirectory)
	if err != nil {
		return DiffSummary{}, err
	}
	stat, err := s.run(ctx, path, "diff", "--stat")
	if err != nil {
		return DiffSummary{}, err
	}
	files, err := s.run(ctx, path, "diff", "--name-only")
	if err != nil {
		return DiffSummary{}, err
	}
	return DiffSummary{Stat: strings.TrimSpace(stat.stdout), ChangedFiles: strings.TrimSpace(files.stdout), Truncated: stat.truncated || files.truncated}, nil
}

func (s *Service) GetDiff(ctx context.Context, workingDirectory string) (string, bool, error) {
	path, err := s.repositoryDirectory(ctx, workingDirectory)
	if err != nil {
		return "", false, err
	}
	result, err := s.run(ctx, path, "diff")
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(result.stdout), result.truncated, nil
}

func (s *Service) GetBranch(ctx context.Context, workingDirectory string) (string, error) {
	path, err := s.repositoryDirectory(ctx, workingDirectory)
	if err != nil {
		return "", err
	}
	result, err := s.run(ctx, path, "branch", "--show-current")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(result.stdout), nil
}

func (s *Service) GetLatestCommit(ctx context.Context, workingDirectory string) (string, error) {
	path, err := s.repositoryDirectory(ctx, workingDirectory)
	if err != nil {
		return "", err
	}
	result, err := s.run(ctx, path, "log", "-1", "--oneline")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(result.stdout), nil
}

func (s *Service) Inspect(ctx context.Context, workingDirectory string, includeDiff bool) (Snapshot, error) {
	path, err := s.repositoryDirectory(ctx, workingDirectory)
	if err != nil {
		return Snapshot{}, err
	}
	status, err := s.run(ctx, path, "status", "--short")
	if err != nil {
		return Snapshot{}, err
	}
	branch, err := s.run(ctx, path, "branch", "--show-current")
	if err != nil {
		return Snapshot{}, err
	}
	latest, err := s.run(ctx, path, "log", "-1", "--oneline")
	if err != nil {
		return Snapshot{}, err
	}
	summary, err := s.GetDiffSummary(ctx, path)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		WorkingDirectory: path,
		Repository:       true,
		Branch:           strings.TrimSpace(branch.stdout),
		LatestCommit:     strings.TrimSpace(latest.stdout),
		Status:           strings.TrimSpace(status.stdout),
		DiffStat:         summary.Stat,
		ChangedFiles:     summary.ChangedFiles,
		Truncated:        status.truncated || branch.truncated || latest.truncated || summary.Truncated,
	}
	if includeDiff {
		diff, truncated, err := s.GetDiff(ctx, path)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.Diff, snapshot.Truncated = diff, snapshot.Truncated || truncated
	}
	return snapshot, nil
}

func (s *Service) repositoryDirectory(ctx context.Context, workingDirectory string) (string, error) {
	path, err := normalizeDirectory(workingDirectory)
	if err != nil {
		return "", err
	}
	repository, err := s.IsRepository(ctx, path)
	if err != nil {
		return "", err
	}
	if !repository {
		return "", ErrNotGitRepository
	}
	return path, nil
}

func (s *Service) run(ctx context.Context, workingDirectory string, args ...string) (commandResult, error) {
	s.normalize()
	commandContext, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	if s.runner != nil {
		return s.runner(commandContext, workingDirectory, append([]string(nil), args...))
	}
	return s.runCommand(commandContext, workingDirectory, args)
}

func (s *Service) runCommand(ctx context.Context, workingDirectory string, args []string) (commandResult, error) {
	command := exec.CommandContext(ctx, s.GitPath, args...)
	command.Dir = workingDirectory
	prepareCommand(command)
	stdout := &boundedBuffer{limit: s.MaxOutputSize}
	stderr := &boundedBuffer{limit: s.MaxOutputSize}
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if ctx.Err() != nil {
		return commandResult{stdout: stdout.String(), stderr: stderr.String(), truncated: stdout.truncated || stderr.truncated}, &CommandError{Args: append([]string(nil), args...), Stderr: stderr.String(), Cause: ctx.Err(), Timeout: errors.Is(ctx.Err(), context.DeadlineExceeded)}
	}
	if err != nil {
		return commandResult{stdout: stdout.String(), stderr: stderr.String(), truncated: stdout.truncated || stderr.truncated}, &CommandError{Args: append([]string(nil), args...), Stderr: stderr.String(), Cause: err}
	}
	return commandResult{stdout: stdout.String(), stderr: stderr.String(), truncated: stdout.truncated || stderr.truncated}, nil
}

func normalizeDirectory(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ErrInvalidWorkingDirectory
	}
	path, err := filepath.Abs(filepath.Clean(value))
	if err != nil {
		return "", ErrInvalidWorkingDirectory
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return "", ErrInvalidWorkingDirectory
	}
	return path, nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
	if b.limit <= 0 {
		return len(value), nil
	}
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.truncated = true
		return len(value), nil
	}
	if len(value) > remaining {
		_, _ = b.Buffer.Write(value[:remaining])
		b.truncated = true
		return len(value), nil
	}
	return b.Buffer.Write(value)
}
