package gitquery

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadOnlyQueriesAgainstTemporaryRepository(t *testing.T) {
	directory := t.TempDir()
	runGit(t, directory, "init")
	runGit(t, directory, "config", "user.email", "action-test@example.invalid")
	runGit(t, directory, "config", "user.name", "Action Test")
	file := filepath.Join(directory, "test.txt")
	if err := os.WriteFile(file, []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, directory, "add", "test.txt")
	runGit(t, directory, "commit", "-m", "initial")
	if err := os.WriteFile(file, []byte("before\nafter\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	service := New()
	status, err := service.GetStatus(context.Background(), directory)
	if err != nil || !strings.Contains(status, "M") {
		t.Fatalf("status=%q err=%v", status, err)
	}
	branch, err := service.GetBranch(context.Background(), directory)
	if err != nil || strings.TrimSpace(branch) == "" {
		t.Fatalf("branch=%q err=%v", branch, err)
	}
	latest, err := service.GetLatestCommit(context.Background(), directory)
	if err != nil || !strings.Contains(latest, "initial") {
		t.Fatalf("latest=%q err=%v", latest, err)
	}
	summary, err := service.GetDiffSummary(context.Background(), directory)
	if err != nil || !strings.Contains(summary.ChangedFiles, "test.txt") || !strings.Contains(summary.Stat, "test.txt") {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
	diff, truncated, err := service.GetDiff(context.Background(), directory)
	if err != nil || truncated || !strings.Contains(diff, "+after") {
		t.Fatalf("diff=%q truncated=%v err=%v", diff, truncated, err)
	}
}

func TestInvalidAndNonRepositoryDirectoriesAreRejected(t *testing.T) {
	service := New()
	if service.IsWorkingDirectoryValid(filepath.Join(t.TempDir(), "missing")) {
		t.Fatal("missing directory reported as valid")
	}
	if _, err := service.GetStatus(context.Background(), filepath.Join(t.TempDir(), "missing")); !errors.Is(err, ErrInvalidWorkingDirectory) {
		t.Fatalf("invalid directory error=%v", err)
	}
	directory := t.TempDir()
	if repository, err := service.IsRepository(context.Background(), directory); err != nil || repository {
		t.Fatalf("repository=%v err=%v", repository, err)
	}
	if _, _, err := service.GetDiff(context.Background(), directory); !errors.Is(err, ErrNotGitRepository) {
		t.Fatalf("non-repository error=%v", err)
	}
}

func TestTimeoutAndOutputLimits(t *testing.T) {
	service := New()
	service.Timeout = 5 * time.Millisecond
	service.runner = func(ctx context.Context, _ string, _ []string) (commandResult, error) {
		<-ctx.Done()
		return commandResult{}, &CommandError{Cause: ctx.Err(), Timeout: true}
	}
	if _, err := service.GetStatus(context.Background(), t.TempDir()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error=%v", err)
	}

	buffer := &boundedBuffer{limit: 8}
	if _, err := buffer.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if buffer.String() != "01234567" || !buffer.truncated {
		t.Fatalf("bounded buffer=%q truncated=%v", buffer.String(), buffer.truncated)
	}
}

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
