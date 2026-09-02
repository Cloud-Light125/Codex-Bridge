package appserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

type Detection struct {
	Path      string
	Version   string
	Available bool
	Error     string
}

type Event struct {
	Channel string
	Method  string
	Params  map[string]any
	ID      string
}

type TurnStartOptions struct {
	CWD               string
	CollaborationMode string
	Model             string
	ReasoningEffort   string
	ApprovalPolicy    security.ApprovalPolicy
	SandboxMode       security.SandboxMode
}

type RPCError struct {
	Code    int
	Message string
	Detail  string
}

func (e *RPCError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("Codex app-server RPC error (%d)", e.Code)
	}
	return fmt.Sprintf("Codex app-server RPC error (%d): %s", e.Code, e.Message)
}

type response struct {
	result json.RawMessage
	err    error
}

type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type Client struct {
	codexPath string
	cwd       string
	version   string
	logger    *bridgelog.SafeLogger
	onEvent   func(Event)
	history   *HistoryReader

	mu             sync.RWMutex
	writeMu        sync.Mutex
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	pending        map[uint64]chan response
	serverRequests map[string]json.RawMessage
	running        bool
	closing        bool
	pid            int
	exit           chan error
	nextID         atomic.Uint64
}

func Detect(customPath string) Detection {
	candidate := strings.TrimSpace(customPath)
	if candidate != "" {
		if resolved, err := exec.LookPath(candidate); err == nil {
			return detectedCLI(resolved)
		}
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			absolute, _ := filepath.Abs(candidate)
			return detectedCLI(absolute)
		}
		return Detection{Path: candidate, Error: "configured Codex CLI path does not exist or is not executable"}
	}
	resolved, err := exec.LookPath("codex")
	if err != nil {
		return Detection{Error: "Codex CLI was not found on PATH; install Codex CLI or configure a custom path"}
	}
	return detectedCLI(resolved)
}

func detectedCLI(path string) Detection {
	detection := Detection{Path: path}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	extension := strings.ToLower(filepath.Ext(path))
	var command *exec.Cmd
	if runtime.GOOS == "windows" && (extension == ".cmd" || extension == ".bat") {
		comspec := os.Getenv("ComSpec")
		if comspec == "" {
			comspec = "cmd.exe"
		}
		command = exec.CommandContext(ctx, comspec)
		configureBatchCommand(command, fmt.Sprintf(`/d /s /c ""%s" --version"`, path))
	} else {
		command = exec.CommandContext(ctx, path, "--version")
	}
	output, err := command.CombinedOutput()
	if err != nil {
		detection.Error = "Codex CLI failed --version validation"
		return detection
	}
	version := strings.TrimSpace(string(output))
	if newline := strings.IndexAny(version, "\r\n"); newline >= 0 {
		version = version[:newline]
	}
	if len(version) > 128 {
		version = version[:128]
	}
	if version == "" {
		detection.Error = "Codex CLI returned an empty --version response"
		return detection
	}
	detection.Available = true
	detection.Version = bridgelog.Redact(version)
	return detection
}

func NewClient(codexPath, cwd, version string, logger *bridgelog.SafeLogger, onEvent func(Event)) *Client {
	client := &Client{
		codexPath:      codexPath,
		cwd:            cwd,
		version:        version,
		logger:         logger,
		onEvent:        onEvent,
		pending:        make(map[uint64]chan response),
		serverRequests: make(map[string]json.RawMessage),
	}
	client.history = NewHistoryReader(client, logger)
	return client
}

func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	cmd := c.buildCommand()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("open app-server stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("open app-server stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("open app-server stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("start Codex app-server: %w", err)
	}
	c.cmd = cmd
	c.stdin = stdin
	c.pending = make(map[uint64]chan response)
	c.serverRequests = make(map[string]json.RawMessage)
	c.running = true
	c.closing = false
	c.pid = cmd.Process.Pid
	c.exit = make(chan error, 1)
	exit := c.exit
	c.mu.Unlock()
	if c.history != nil {
		c.history.Reset()
	}

	go c.readStdout(stdout)
	go c.readStderr(stderr)
	go c.waitProcess(cmd, exit)

	c.logger.Printf("rpcTrace stage=initialize-request cliPath=%s cwd=%s pid=%d", bridgelog.Redact(c.codexPath), bridgelog.Redact(c.cwd), c.pid)
	result, err := c.request(ctx, "initialize", map[string]any{
		"capabilities": map[string]any{"experimentalApi": true},
		"clientInfo": map[string]any{
			"name":    "cloudlight-codex-bridge",
			"title":   "CloudLight Codex Bridge",
			"version": c.version,
		},
	})
	if err != nil {
		_ = c.Close()
		return fmt.Errorf("initialize Codex app-server: %w", err)
	}
	_ = result
	c.logger.Printf("rpcTrace stage=initialize-completed pid=%d", c.pid)
	if err := c.notify(ctx, "initialized", nil); err != nil {
		_ = c.Close()
		return fmt.Errorf("notify Codex app-server initialized: %w", err)
	}
	c.logger.Printf("rpcTrace stage=initialized-notification pid=%d", c.pid)
	return nil
}

func (c *Client) ThreadList(ctx context.Context, limit int, cursor string) (map[string]any, error) {
	params := map[string]any{"limit": limit, "sortKey": "updated_at"}
	if strings.TrimSpace(cursor) != "" {
		params["cursor"] = cursor
	}
	return c.request(ctx, "thread/list", params)
}

func (c *Client) ThreadRead(ctx context.Context, threadID string, includeTurns bool) (map[string]any, error) {
	return c.request(ctx, "thread/read", map[string]any{
		"threadId":     threadID,
		"includeTurns": includeTurns,
	})
}

// ThreadTurnsList pages a stored thread's turns. The options mirror the
// app-server protocol, including its opaque cursor and itemsView fields.
func (c *Client) ThreadTurnsList(ctx context.Context, options ThreadTurnsListOptions) (map[string]any, error) {
	params := map[string]any{"threadId": strings.TrimSpace(options.ThreadID)}
	if cursor := strings.TrimSpace(options.Cursor); cursor != "" {
		params["cursor"] = cursor
	}
	if options.Limit > 0 {
		params["limit"] = options.Limit
	}
	if direction := strings.TrimSpace(options.SortDirection); direction != "" {
		params["sortDirection"] = direction
	}
	if view := strings.TrimSpace(options.ItemsView); view != "" {
		params["itemsView"] = view
	}
	return c.request(ctx, "thread/turns/list", params)
}

// ThreadItemsList pages persisted items. The protocol accepts an optional
// turnId; it is intentionally omitted when empty rather than sent as an
// empty selector.
func (c *Client) ThreadItemsList(ctx context.Context, options ThreadItemsListOptions) (map[string]any, error) {
	params := map[string]any{"threadId": strings.TrimSpace(options.ThreadID)}
	if turnID := strings.TrimSpace(options.TurnID); turnID != "" {
		params["turnId"] = turnID
	}
	if cursor := strings.TrimSpace(options.Cursor); cursor != "" {
		params["cursor"] = cursor
	}
	if options.Limit > 0 {
		params["limit"] = options.Limit
	}
	if direction := strings.TrimSpace(options.SortDirection); direction != "" {
		params["sortDirection"] = direction
	}
	return c.request(ctx, "thread/items/list", params)
}

// ThreadReadHistory is the safe history entry point for callers that need
// message bodies. It never sends includeTurns=true after a paginated thread
// has been identified; the HistoryReader selects the legacy path only for
// legacy threads or an unknown-mode compatibility probe.
func (c *Client) ThreadReadHistory(ctx context.Context, threadID string, limit int) (map[string]any, error) {
	return c.history.ReadThread(ctx, threadID, limit)
}

// ThreadReadActivity reads metadata and, for paginated threads, only the
// newest Turn without Items. It is intended for list/status refreshes.
func (c *Client) ThreadReadActivity(ctx context.Context, threadID string) (map[string]any, error) {
	return c.history.ReadActivity(ctx, threadID)
}

// ThreadReadActivityHistory reads only a bounded set of recent Turn status
// records and never hydrates their Items.
func (c *Client) ThreadReadActivityHistory(ctx context.Context, threadID string, limit int) (map[string]any, error) {
	return c.history.ReadActivityHistory(ctx, threadID, limit)
}

func (c *Client) HistoryCapability() HistoryCapability {
	return c.history.Capability()
}

// AccountRateLimits reads the official Codex account rate-limit snapshot.
// The protocol requires an explicit null params value for this read-only RPC.
func (c *Client) AccountRateLimits(ctx context.Context) (map[string]any, error) {
	return c.requestValue(ctx, "account/rateLimits/read", nil, true)
}

func (c *Client) ThreadResume(ctx context.Context, threadID, cwd string) (map[string]any, error) {
	params := map[string]any{"threadId": threadID, "persistExtendedHistory": true}
	if strings.TrimSpace(cwd) != "" {
		params["cwd"] = strings.TrimSpace(cwd)
	}
	return c.request(ctx, "thread/resume", params)
}

func (c *Client) TurnStart(ctx context.Context, threadID, text string, options TurnStartOptions) (map[string]any, error) {
	params, diagnostic, err := buildTurnStartParams(threadID, text, options)
	if err != nil {
		return nil, err
	}
	c.logger.Printf(
		"Codex turn/start protocol: thread=%s approvalPolicy=%s sandboxType=%s networkAccess=%t model=%s",
		shortThreadID(threadID), diagnostic.ApprovalPolicy, diagnostic.SandboxType, diagnostic.NetworkAccess, diagnostic.Model,
	)
	return c.request(ctx, "turn/start", params)
}

func (c *Client) TurnInterrupt(ctx context.Context, threadID, turnID string) error {
	_, err := c.request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID})
	return err
}

func (c *Client) RespondServerRequest(ctx context.Context, requestID string, result map[string]any) error {
	c.mu.Lock()
	rawID, ok := c.serverRequests[requestID]
	if ok {
		delete(c.serverRequests, requestID)
	}
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("server request %q is no longer pending", requestID)
	}
	payload := map[string]any{"id": rawID, "result": result}
	if err := c.write(payload); err != nil {
		c.mu.Lock()
		c.serverRequests[requestID] = rawID
		c.mu.Unlock()
		return err
	}
	return nil
}

func (c *Client) request(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	return c.requestValue(ctx, method, params, params != nil)
}

func (c *Client) requestValue(ctx context.Context, method string, params any, includeParams bool) (map[string]any, error) {
	id := c.nextID.Add(1)
	reply := make(chan response, 1)
	c.mu.Lock()
	if !c.running || c.stdin == nil {
		c.mu.Unlock()
		return nil, errors.New("Codex app-server is not running")
	}
	c.pending[id] = reply
	c.mu.Unlock()

	payload := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if includeParams {
		payload["params"] = params
	}
	if err := c.write(payload); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case received := <-reply:
		if received.err != nil {
			return nil, received.err
		}
		var result map[string]any
		if len(received.result) == 0 || string(received.result) == "null" {
			return map[string]any{}, nil
		}
		if err := json.Unmarshal(received.result, &result); err != nil {
			return nil, fmt.Errorf("decode %s result: %w", method, err)
		}
		return result, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (c *Client) notify(_ context.Context, method string, params map[string]any) error {
	payload := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		payload["params"] = params
	}
	return c.write(payload)
}

func (c *Client) write(payload map[string]any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c.mu.RLock()
	stdin := c.stdin
	running := c.running
	c.mu.RUnlock()
	if !running || stdin == nil {
		return errors.New("Codex app-server is not running")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := stdin.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write Codex app-server request: %w", err)
	}
	return nil
}

func (c *Client) readStdout(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var envelope rpcEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			c.emit(Event{Channel: "transport_error", Params: map[string]any{"message": "invalid JSON from Codex app-server"}})
			continue
		}
		c.handleEnvelope(envelope)
	}
	if err := scanner.Err(); err != nil {
		c.emit(Event{Channel: "transport_error", Params: map[string]any{"message": err.Error()}})
	}
}

func (c *Client) readStderr(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 4*1024), 256*1024)
	for scanner.Scan() {
		diagnostic := classifyStderr(scanner.Text())
		if !diagnostic.Warning && !diagnostic.PersistenceError {
			continue
		}
		c.logger.Printf("codex app-server stderr: category=%s persistenceError=%t summary=%s", diagnostic.Category, diagnostic.PersistenceError, diagnostic.Message)
		c.emit(Event{Channel: "stderr", Params: map[string]any{
			"message":          diagnostic.Message,
			"category":         diagnostic.Category,
			"warning":          diagnostic.Warning,
			"persistenceError": diagnostic.PersistenceError,
		}})
	}
	if err := scanner.Err(); err != nil {
		c.emit(Event{Channel: "transport_error", Params: map[string]any{"message": "read Codex app-server stderr: " + bridgelog.Redact(err.Error())}})
	}
}

func (c *Client) handleEnvelope(envelope rpcEnvelope) {
	if len(envelope.ID) > 0 && (len(envelope.Result) > 0 || len(envelope.Error) > 0) {
		var id uint64
		if err := json.Unmarshal(envelope.ID, &id); err != nil {
			return
		}
		c.mu.Lock()
		reply := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if reply == nil {
			return
		}
		if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
			var rpcError struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(envelope.Error, &rpcError); err != nil {
				reply <- response{err: fmt.Errorf("Codex app-server returned an invalid RPC error: %s", bridgelog.Redact(string(envelope.Error)))}
			} else {
				reply <- response{err: &RPCError{
					Code: rpcError.Code, Message: bridgelog.Redact(rpcError.Message), Detail: bridgelog.Redact(string(envelope.Error)),
				}}
			}
		} else {
			reply <- response{result: envelope.Result}
		}
		close(reply)
		return
	}
	if envelope.Method == "" {
		return
	}
	params := map[string]any{}
	if len(envelope.Params) > 0 {
		_ = json.Unmarshal(envelope.Params, &params)
	}
	if len(envelope.ID) > 0 {
		requestID, ok := canonicalRPCID(envelope.ID)
		if !ok {
			c.emit(Event{Channel: "transport_error", Params: map[string]any{"message": "unsupported server request id"}})
			return
		}
		c.mu.Lock()
		c.serverRequests[requestID] = append(json.RawMessage(nil), envelope.ID...)
		c.mu.Unlock()
		c.emit(Event{Channel: "server_request", Method: envelope.Method, Params: params, ID: requestID})
		return
	}
	c.emit(Event{Channel: "notification", Method: envelope.Method, Params: params})
}

func canonicalRPCID(raw json.RawMessage) (string, bool) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return "s:" + text, true
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return "", false
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return "n:" + number.String(), true
	}
	return "", false
}

func (c *Client) emit(event Event) {
	if c.onEvent != nil {
		c.onEvent(event)
	}
}

func (c *Client) waitProcess(cmd *exec.Cmd, exit chan error) {
	err := cmd.Wait()
	c.mu.Lock()
	if c.cmd != cmd {
		c.mu.Unlock()
		return
	}
	wasClosing := c.closing
	c.running = false
	c.pid = 0
	c.stdin = nil
	for id, pending := range c.pending {
		pending <- response{err: errors.New("Codex app-server exited before responding")}
		close(pending)
		delete(c.pending, id)
	}
	c.serverRequests = make(map[string]json.RawMessage)
	c.mu.Unlock()
	if err == nil && !wasClosing {
		err = errors.New("Codex app-server exited unexpectedly")
	}
	exit <- err
	close(exit)
}

func (c *Client) buildCommand() *exec.Cmd {
	extension := strings.ToLower(filepath.Ext(c.codexPath))
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" && (extension == ".cmd" || extension == ".bat") {
		comspec := os.Getenv("ComSpec")
		if comspec == "" {
			comspec = "cmd.exe"
		}
		cmd = exec.Command(comspec)
		configureBatchCommand(cmd, fmt.Sprintf(`/d /s /c ""%s" app-server --listen stdio://"`, c.codexPath))
	} else {
		cmd = exec.Command(c.codexPath, "app-server", "--listen", "stdio://")
	}
	cmd.Dir = c.cwd
	configureCommand(cmd)
	return cmd
}

func (c *Client) PID() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pid
}

func (c *Client) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

func (c *Client) Wait(ctx context.Context) error {
	c.mu.RLock()
	exit := c.exit
	c.mu.RUnlock()
	if exit == nil {
		return errors.New("Codex app-server was not started")
	}
	select {
	case err := <-exit:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.cmd == nil || !c.running {
		c.mu.Unlock()
		return nil
	}
	c.closing = true
	cmd := c.cmd
	stdin := c.stdin
	exit := c.exit
	c.mu.Unlock()
	if stdin != nil {
		_ = stdin.Close()
	}
	select {
	case <-exit:
		return nil
	case <-time.After(1200 * time.Millisecond):
	}
	terminationErr := terminateOwnedProcess(cmd)
	select {
	case <-exit:
		// The process can exit between taskkill's failed lookup and Process.Kill.
		// In that case Windows may report ERROR_INVALID_PARAMETER even though the
		// owned process has already shut down successfully.
		return nil
	case <-time.After(3 * time.Second):
		if terminationErr != nil && !errors.Is(terminationErr, os.ErrProcessDone) {
			return terminationErr
		}
		return errors.New("timed out waiting for owned Codex app-server to exit")
	}
}
