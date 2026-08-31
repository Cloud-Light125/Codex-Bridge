package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/appserver"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

const (
	StateIdle                = "idle"
	StateAccepted            = "accepted"
	StateRunning             = "running"
	StateRunningLocal        = StateRunning
	StateRunningExternal     = "running-external"
	StateWaitingApproval     = "waiting-approval"
	StateWaitingUserInput    = "waiting-user-input"
	StateInterrupting        = "interrupting"
	StateCompletedUnverified = "completed-unverified"
	StatePersisted           = "persisted"
	StatePersistenceFailed   = "persistence-failed"
	StateThreadMismatch      = "thread-mismatch"
	StateCompleted           = StatePersisted
	StateFailed              = "failed"
	StateUnknown             = "unknown"
)

type Status struct {
	Version                   string `json:"version"`
	CodexCLIPath              string `json:"codexCliPath"`
	CodexCLIVersion           string `json:"codexCliVersion,omitempty"`
	CodexCLIAvailable         bool   `json:"codexCliAvailable"`
	CodexCLIPathSource        string `json:"codexCliPathSource,omitempty"`
	CodexCLIValidationStatus  string `json:"codexCliValidationStatus"`
	CodexCLIConnectionStatus  string `json:"codexCliConnectionStatus"`
	AppServerRunning          bool   `json:"appServerRunning"`
	AppServerPID              int    `json:"appServerPid"`
	LastError                 string `json:"lastError,omitempty"`
	StartedAt                 string `json:"startedAt"`
	ListenAddress             string `json:"listenAddress"`
	SandboxMode               string `json:"sandboxMode"`
	ApprovalPolicy            string `json:"approvalPolicy"`
	DangerFullAccess          bool   `json:"dangerFullAccess"`
	RemoteApproval            bool   `json:"remoteApproval"`
	AutomaticPersistenceProbe bool   `json:"automaticPersistenceProbe"`
}

type deltaBuffer struct {
	ThreadID string
	TurnID   string
	ItemID   string
	Text     strings.Builder
	FlushAt  time.Time
}

type Manager struct {
	mu           sync.RWMutex
	status       Status
	detection    appserver.Detection
	codexPath    string
	cwd          string
	client       *appserver.Client
	broker       *events.Broker
	logger       *bridgelog.SafeLogger
	ctx          context.Context
	cancel       context.CancelFunc
	interactions *interactions.Store
	registry     any

	stateMu  sync.RWMutex
	states   map[string]control.RuntimeState
	starting map[string]bool

	threadLocksMu sync.Mutex
	threadLocks   map[string]*sync.Mutex

	deltaMu sync.Mutex
	deltas  map[string]*deltaBuffer

	traceMu              sync.RWMutex
	traces               map[string]*turnTrace
	lastVerifications    map[string]control.PersistenceVerification
	autoPersistenceProbe bool
	configChanged        chan struct{}
	configGeneration     uint64
	connectionCancel     context.CancelFunc
}

func NewManager(version, listenAddress, codexPath, sandboxMode string, broker *events.Broker, logger *bridgelog.SafeLogger, registry any) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	parsedSandboxMode, err := security.ParseSandboxMode(sandboxMode)
	if err != nil {
		cancel()
		return nil, err
	}
	detection := appserver.Detect(codexPath)
	pathSource := ""
	validationStatus := "failed"
	connectionStatus := "not-found"
	if detection.Available {
		validationStatus = "succeeded"
		connectionStatus = "connecting"
		if strings.TrimSpace(codexPath) == "" {
			pathSource = "PATH"
		} else {
			pathSource = "SavedPath"
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		cwd = filepath.Dir(os.Args[0])
	}
	persistenceProbeSetting := strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_BRIDGE_PERSISTENCE_PROBE")))
	automaticPersistenceProbe := persistenceProbeSetting != "0" && persistenceProbeSetting != "false" && persistenceProbeSetting != "off"
	status := Status{
		Version: version, CodexCLIPath: detection.Path, CodexCLIVersion: detection.Version, CodexCLIAvailable: detection.Available,
		CodexCLIPathSource: pathSource, CodexCLIValidationStatus: validationStatus, CodexCLIConnectionStatus: connectionStatus,
		StartedAt: time.Now().UTC().Format(time.RFC3339), ListenAddress: listenAddress,
		SandboxMode: string(parsedSandboxMode), ApprovalPolicy: string(security.ApprovalOnRequest),
		DangerFullAccess: false, RemoteApproval: false,
		AutomaticPersistenceProbe: automaticPersistenceProbe,
	}
	if detection.Error != "" {
		status.LastError = detection.Error
	}
	return &Manager{
		status: status, detection: detection, codexPath: codexPath, cwd: cwd,
		broker: broker, logger: logger, ctx: ctx, cancel: cancel,
		interactions: interactions.NewStore(), registry: registry, states: make(map[string]control.RuntimeState),
		starting: make(map[string]bool), threadLocks: make(map[string]*sync.Mutex),
		deltas: make(map[string]*deltaBuffer), traces: make(map[string]*turnTrace),
		lastVerifications:    make(map[string]control.PersistenceVerification),
		autoPersistenceProbe: status.AutomaticPersistenceProbe,
		configChanged:        make(chan struct{}, 1), configGeneration: 1,
	}, nil
}

func (m *Manager) Start() {
	go m.run()
	go m.expirationLoop()
	go m.deltaLoop()
}

func (m *Manager) run() {
	for {
		if m.ctx.Err() != nil {
			return
		}
		detection, generation := m.connectionConfig()
		if !detection.Available {
			m.logger.Printf("Codex CLI unavailable: %s", detection.Error)
			m.broker.Publish(events.Error, map[string]any{"message": detection.Error})
			if !m.waitForConfigChange() {
				return
			}
			continue
		}

		client := appserver.NewClient(detection.Path, m.cwd, m.status.Version, m.logger, m.handleAppServerEvent)
		m.mu.Lock()
		if generation != m.configGeneration {
			m.mu.Unlock()
			continue
		}
		m.client = client
		m.status.AppServerRunning = false
		m.status.AppServerPID = 0
		if m.status.LastError == "" {
			m.status.CodexCLIConnectionStatus = "connecting"
		} else {
			m.status.CodexCLIConnectionStatus = "reconnecting"
		}
		m.mu.Unlock()

		startContext, cancel := context.WithTimeout(m.ctx, 20*time.Second)
		m.mu.Lock()
		if generation == m.configGeneration {
			m.connectionCancel = cancel
		}
		m.mu.Unlock()
		err := client.Start(startContext)
		cancel()
		m.mu.Lock()
		if generation == m.configGeneration {
			m.connectionCancel = nil
		}
		m.mu.Unlock()
		if err != nil {
			if !m.isCurrentGeneration(generation) {
				m.clearClient(client)
				continue
			}
			m.setDisconnected(generation, err, "reconnecting")
			m.broker.Publish(events.Error, map[string]any{"message": bridgelog.Redact(err.Error())})
			m.logger.Printf("Codex app-server connection failed: %v", err)
			if !m.waitRetry(5 * time.Second) {
				return
			}
			continue
		}

		m.mu.Lock()
		if generation != m.configGeneration {
			m.mu.Unlock()
			_ = client.Close()
			continue
		}
		m.status.AppServerRunning = true
		m.status.AppServerPID = client.PID()
		m.status.LastError = ""
		m.status.CodexCLIConnectionStatus = "connected"
		m.mu.Unlock()
		m.initializeThreadRegistry(client)
		if !m.isCurrentGeneration(generation) {
			m.clearClient(client)
			continue
		}
		m.logger.Printf("[codex-app-server] connected path=%s pid=%d cliVersion=%s", detection.Path, client.PID(), detection.Version)
		m.broker.Publish(events.CodexConnected, map[string]any{"pid": client.PID()})

		pollContext, stopPolling := context.WithCancel(m.ctx)
		go m.pollThreads(pollContext, client)
		err = client.Wait(m.ctx)
		stopPolling()
		if m.ctx.Err() != nil {
			return
		}
		if !m.isCurrentGeneration(generation) {
			m.clearClient(client)
			continue
		}
		if err == nil {
			err = errors.New("Codex app-server disconnected")
		}
		m.setDisconnected(generation, err, "reconnecting")
		m.markControlLost()
		m.logger.Printf("Codex app-server disconnected: %v", err)
		m.broker.Publish(events.CodexDisconnected, map[string]any{"message": bridgelog.Redact(err.Error())})
		if !m.waitRetry(3 * time.Second) {
			return
		}
	}
}

func (m *Manager) initializeThreadRegistry(client *appserver.Client) {
	store := conversationregistry.ForCodex(m.registry)
	if store == nil {
		return
	}
	cursor := ""
	metadata := []conversationregistry.Metadata{}
	for page := 0; page < 100; page++ {
		ctx, cancel := context.WithTimeout(m.ctx, 8*time.Second)
		raw, err := client.ThreadList(ctx, 100, cursor)
		cancel()
		if err != nil {
			m.logger.Printf("thread number initialization deferred: %v", err)
			return
		}
		list := control.NormalizeThreadList(raw)
		for _, thread := range list.Threads {
			metadata = append(metadata, conversationregistry.Metadata{Backend: conversationregistry.BackendCodex, TargetID: thread.ThreadID, Title: thread.Title, CWD: thread.CWD, CreatedAt: thread.CreatedAt, LastSeenAt: thread.UpdatedAt})
		}
		if strings.TrimSpace(list.NextCursor) == "" || list.NextCursor == cursor {
			break
		}
		cursor = list.NextCursor
	}
	if _, err := store.EnsureBatchBackend(conversationregistry.BackendCodex, metadata); err != nil {
		m.logger.Printf("persist thread numbers: %v", err)
	}
}

func (m *Manager) waitRetry(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-m.configChanged:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *Manager) waitForConfigChange() bool {
	select {
	case <-m.configChanged:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *Manager) connectionConfig() (appserver.Detection, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.detection, m.configGeneration
}

func (m *Manager) isCurrentGeneration(generation uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return generation == m.configGeneration
}

func (m *Manager) clearClient(client *appserver.Client) {
	m.mu.Lock()
	if m.client == client {
		m.client = nil
	}
	m.mu.Unlock()
}

func (m *Manager) setDisconnected(generation uint64, err error, connectionStatus string) {
	message := ""
	if err != nil {
		message = bridgelog.Redact(err.Error())
	}
	m.mu.Lock()
	if generation != m.configGeneration {
		m.mu.Unlock()
		return
	}
	m.status.AppServerRunning = false
	m.status.AppServerPID = 0
	m.status.LastError = message
	m.status.CodexCLIConnectionStatus = connectionStatus
	m.mu.Unlock()
}

func (m *Manager) markControlLost() {
	resolved := m.interactions.ExpireAll("expired")
	for _, item := range resolved {
		m.broker.PublishScoped(events.InteractionResolved, item.ThreadID, item.TurnID, item.ItemID, map[string]any{"interaction": item})
	}
	m.stateMu.Lock()
	for threadID, state := range m.states {
		if controllableOrigin(state.Origin) && isActiveState(state.State) {
			state.State = StateUnknown
			state.Origin = "unknown"
			state.CanInterrupt = false
			state.CanSend = false
			state.Error = "后端已失去该 Turn 的控制信息"
			state.PendingInteractionCount = 0
			state.LastActivityAt = nowText()
			m.states[threadID] = state
		}
	}
	m.stateMu.Unlock()
}

func (m *Manager) pollThreads(ctx context.Context, client *appserver.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	previous := map[string]string{}
	initialized := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			requestContext, cancel := context.WithTimeout(ctx, 8*time.Second)
			raw, err := client.ThreadList(requestContext, 100, "")
			cancel()
			if err != nil {
				continue
			}
			current, items := threadVersions(raw)
			for id, item := range items {
				m.reconcileActivity(control.ActivityFromThreadRead(map[string]any{"thread": item}))
				if initialized {
					if old, ok := previous[id]; !ok || old != current[id] {
						m.broker.PublishScoped(events.ThreadUpdated, id, "", "", map[string]any{"source": "poll"})
					}
				}
			}
			previous = current
			initialized = true
		}
	}
}

func threadVersions(raw map[string]any) (map[string]string, map[string]map[string]any) {
	result := map[string]string{}
	items := map[string]map[string]any{}
	var values []any
	for _, key := range []string{"data", "items", "threads", "results"} {
		if found, ok := raw[key].([]any); ok {
			values = found
			break
		}
	}
	for _, value := range values {
		item, _ := value.(map[string]any)
		if nested, ok := item["thread"].(map[string]any); ok {
			item = nested
		}
		id := textValue(item["id"])
		if id == "" {
			continue
		}
		result[id] = fmt.Sprintf("%v|%v", item["updatedAt"], item["status"])
		items[id] = item
	}
	return result, items
}

func (m *Manager) ThreadList(ctx context.Context, limit int, cursor string) (map[string]any, error) {
	client, err := m.runningClient()
	if err != nil {
		return nil, err
	}
	raw, err := client.ThreadList(ctx, limit, cursor)
	if err == nil {
		_, items := threadVersions(raw)
		for _, item := range items {
			m.reconcileActivity(control.ActivityFromThreadRead(map[string]any{"thread": item}))
		}
	}
	return raw, err
}

func (m *Manager) ThreadRead(ctx context.Context, threadID string, includeTurns bool) (map[string]any, error) {
	client, err := m.runningClient()
	if err != nil {
		return nil, err
	}
	raw, err := client.ThreadRead(ctx, threadID, includeTurns)
	if err == nil {
		m.reconcileActivity(control.ActivityFromThreadRead(raw))
	}
	return raw, err
}

// AccountRateLimits returns the official read-only app-server snapshot. It
// does not derive quota from token usage and does not start or modify a Turn.
func (m *Manager) AccountRateLimits(ctx context.Context) (map[string]any, error) {
	client, err := m.runningClient()
	if err != nil {
		return nil, err
	}
	return client.AccountRateLimits(ctx)
}

func (m *Manager) runningClient() (*appserver.Client, error) {
	m.mu.RLock()
	client := m.client
	running := m.status.AppServerRunning
	lastError := m.status.LastError
	m.mu.RUnlock()
	if !running || client == nil {
		if lastError != "" {
			return nil, fmt.Errorf("Codex app-server is unavailable: %s", lastError)
		}
		return nil, errors.New("Codex app-server is unavailable")
	}
	return client, nil
}

func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *Manager) ApplyCodexPath(path, source string) (Status, error) {
	detection := appserver.Detect(strings.TrimSpace(path))
	if !detection.Available {
		m.logger.Printf("[codex-discovery] validation failed path=%s error=%s", detection.Path, detection.Error)
		return m.Status(), errors.New(detection.Error)
	}
	source = normalizeCodexPathSource(source)

	m.mu.Lock()
	pathChanged := !m.detection.Available || !samePath(m.detection.Path, detection.Path)
	m.codexPath = detection.Path
	m.detection = detection
	m.status.CodexCLIPath = detection.Path
	m.status.CodexCLIVersion = detection.Version
	m.status.CodexCLIAvailable = true
	m.status.CodexCLIPathSource = source
	m.status.CodexCLIValidationStatus = "succeeded"
	m.status.LastError = ""
	client := m.client
	cancel := m.connectionCancel
	reconnectRequested := pathChanged || !m.status.AppServerRunning
	if pathChanged {
		m.configGeneration++
		m.status.AppServerRunning = false
		m.status.AppServerPID = 0
		if client == nil {
			m.status.CodexCLIConnectionStatus = "connecting"
		} else {
			m.status.CodexCLIConnectionStatus = "reconnecting"
		}
	}
	status := m.status
	m.mu.Unlock()

	m.logger.Printf("[codex-daemon] applying new Codex path path=%s source=%s", detection.Path, source)
	m.logger.Printf("[codex-config] runtime path updated path=%s source=%s", detection.Path, source)
	if reconnectRequested {
		m.logger.Printf("[codex-app-server] reconnecting with discovered path path=%s", detection.Path)
		select {
		case m.configChanged <- struct{}{}:
		default:
		}
	}
	if pathChanged {
		if cancel != nil {
			cancel()
		}
		if client != nil {
			if err := client.Close(); err != nil {
				m.logger.Printf("close previous Codex app-server during path update: %v", err)
			}
		}
		m.markControlLost()
	}
	m.broker.Publish(events.CodexConfigUpdated, map[string]any{"path": detection.Path, "source": source})
	return status, nil
}

func normalizeCodexPathSource(source string) string {
	switch strings.TrimSpace(source) {
	case "SavedPath", "PATH", "RunningCodex", "RunningChatGPT", "Manual":
		return strings.TrimSpace(source)
	default:
		return "Manual"
	}
}

func (m *Manager) SetSandboxMode(mode string) (Status, error) {
	parsed, err := security.ParseSandboxMode(mode)
	if err != nil {
		return Status{}, errors.New("sandboxMode must be read-only or workspace-write")
	}
	m.mu.Lock()
	m.status.SandboxMode = string(parsed)
	status := m.status
	m.mu.Unlock()
	return status, nil
}

func (m *Manager) Close() error {
	m.expirePendingOnShutdown()
	m.cancel()
	m.flushDeltas()
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client != nil {
		if err := client.Close(); err != nil {
			m.logger.Printf("close Codex app-server: %v", err)
			return err
		}
	}
	return nil
}

func (m *Manager) threadLock(threadID string) *sync.Mutex {
	m.threadLocksMu.Lock()
	defer m.threadLocksMu.Unlock()
	lock := m.threadLocks[threadID]
	if lock == nil {
		lock = &sync.Mutex{}
		m.threadLocks[threadID] = lock
	}
	return lock
}

func textValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func nestedMap(value map[string]any, key string) map[string]any {
	result, _ := value[key].(map[string]any)
	return result
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func isActiveState(state string) bool {
	switch state {
	case StateAccepted, StateRunning, StateRunningExternal, StateWaitingApproval, StateWaitingUserInput, StateInterrupting, StateCompletedUnverified:
		return true
	default:
		return false
	}
}

func controllableOrigin(origin string) bool {
	switch strings.ToLower(strings.TrimSpace(origin)) {
	case "local", "bridge", control.TurnOriginTelegram, control.TurnOriginQQ:
		return true
	default:
		return false
	}
}
