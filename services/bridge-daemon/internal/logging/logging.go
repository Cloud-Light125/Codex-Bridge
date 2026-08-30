package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

var credentialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(/bot)[0-9]+:[A-Za-z0-9_-]+`),
	regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)[^\s,;]+`),
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+\-/=]+`),
	regexp.MustCompile(`(?i)((?:api[_-]?key|bot[_-]?token|access[_-]?token|refresh[_-]?token|app[_-]?secret|client[_-]?secret|secret|token|password)["']?\s*[=:]\s*["']?)[^\s,;"']+`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{12,}\b`),
}

var messageContentPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)((?:"|')?(?:prompt|input|text|content)(?:"|')?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;]+)`),
}

type SafeLogger struct {
	mu     sync.Mutex
	logger *log.Logger
	file   *os.File
}

func New(path string) (*SafeLogger, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	w := io.MultiWriter(file, os.Stderr)
	return &SafeLogger{
		logger: log.New(w, "", log.Ldate|log.Ltime|log.LUTC),
		file:   file,
	}, nil
}

func Redact(value string) string {
	result := value
	for _, pattern := range credentialPatterns {
		result = pattern.ReplaceAllString(result, "${1}[REDACTED]")
	}
	for _, pattern := range messageContentPatterns {
		result = pattern.ReplaceAllString(result, "${1}[CONTENT REDACTED]")
	}
	return result
}

func (l *SafeLogger) Printf(format string, args ...any) {
	if l == nil {
		return
	}
	message := Redact(fmt.Sprintf(format, args...))
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Print(strings.TrimSpace(message))
}

func (l *SafeLogger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
