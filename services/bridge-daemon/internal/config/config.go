package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

type Options struct {
	Listen      string
	Token       string
	CodexPath   string
	SandboxMode string
	Version     string
}

type Paths struct {
	DataDir                    string
	LogDir                     string
	LogFile                    string
	BindingsFile               string
	ConversationNumbersFile    string
	ThreadNumbersFile          string
	OpenClawSessionNumbersFile string
	MirrorFile                 string
	CommandsFile               string
}

func (o Options) Validate() error {
	if strings.TrimSpace(o.Token) == "" {
		return errors.New("--token is required")
	}
	host, _, err := net.SplitHostPort(o.Listen)
	if err != nil {
		return fmt.Errorf("invalid --listen address: %w", err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("refusing non-loopback listen host %q; only 127.0.0.1 is allowed", host)
	}
	if _, err := security.ParseSandboxMode(o.SandboxMode); err != nil {
		return fmt.Errorf("invalid --sandbox %q; expected read-only or workspace-write", o.SandboxMode)
	}
	return nil
}

func UserPaths() (Paths, error) {
	root := strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
	if root == "" {
		userConfig, err := os.UserConfigDir()
		if err != nil {
			return Paths{}, fmt.Errorf("resolve user data directory: %w", err)
		}
		root = userConfig
	}
	dataDir := filepath.Join(root, "CloudLight", "CodexBridge")
	stateDir := filepath.Join(dataDir, "data")
	logDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return Paths{}, fmt.Errorf("create log directory: %w", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return Paths{}, fmt.Errorf("create state directory: %w", err)
	}
	return Paths{
		DataDir:                    dataDir,
		LogDir:                     logDir,
		LogFile:                    filepath.Join(logDir, "bridge-daemon.log"),
		BindingsFile:               filepath.Join(dataDir, "bindings.json"),
		ConversationNumbersFile:    filepath.Join(stateDir, "conversation-numbers.json"),
		ThreadNumbersFile:          filepath.Join(stateDir, "thread-numbers.json"),
		OpenClawSessionNumbersFile: filepath.Join(stateDir, "openclaw-session-numbers.json"),
		MirrorFile:                 filepath.Join(stateDir, "mirror-state.json"),
		CommandsFile:               filepath.Join(stateDir, "commands.json"),
	}, nil
}
