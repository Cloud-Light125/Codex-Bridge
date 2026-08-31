// Package channelprofiles owns logical channel profiles and the physical
// Telegram/QQ transports beneath them.  A profile is a user-facing routing
// choice; a resource is the one poller or gateway established for a set of
// identical credentials.  Keeping those concepts separate is what permits
// Codex and OpenClaw to share a bot without creating competing consumers.
package channelprofiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/qqbot"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/telegram"
)

const (
	PlatformTelegram = "telegram"
	PlatformQQ       = "qqbot"

	DefaultTelegramProfileID = "telegram-default"
	DefaultQQProfileID       = "qq-default"
)

var (
	ErrProfileNotFound  = errors.New("channel profile was not found")
	ErrPlatformMismatch = errors.New("channel profile platform does not match binding")
)

// ConfigureRequest contains one logical profile.  The token/AppSecret are
// write-only inputs: they are used to configure a live resource and are never
// retained in a status response, event payload, binding, or daemon log.
type ConfigureRequest struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Platform string          `json:"platform"`
	Enabled  bool            `json:"enabled"`
	Telegram *TelegramConfig `json:"telegram,omitempty"`
	QQ       *QQConfig       `json:"qq,omitempty"`
}

type TelegramConfig struct {
	Token                 *string `json:"token,omitempty"`
	AllowedUserIDs        []int64 `json:"allowedUserIds"`
	PollingTimeoutSeconds int     `json:"pollingTimeoutSeconds"`
	SendProgressUpdates   bool    `json:"sendProgressUpdates"`
	AutoStart             bool    `json:"autoStart"`
	ProxyMode             string  `json:"proxyMode,omitempty"`
	ProxyURL              string  `json:"proxyUrl,omitempty"`
}

type QQConfig struct {
	AppSecret                 *string  `json:"appSecret,omitempty"`
	Enabled                   bool     `json:"enabled"`
	AutoStart                 bool     `json:"autoStart"`
	AppID                     string   `json:"appId"`
	Environment               string   `json:"environment"`
	AllowedUserOpenIDs        []string `json:"allowedUserOpenIds"`
	AllowedGroupOpenIDs       []string `json:"allowedGroupOpenIds"`
	AllowedGroupMemberOpenIDs []string `json:"allowedGroupMemberOpenIds"`
	GroupTriggerMode          string   `json:"groupTriggerMode"`
	CommandPrefix             string   `json:"commandPrefix"`
	SendProgressUpdates       bool     `json:"sendProgressUpdates"`
	GatewayReconnectEnabled   bool     `json:"gatewayReconnectEnabled"`
	ProxyMode                 string   `json:"proxyMode"`
	ProxyURL                  string   `json:"proxyUrl"`
}

type BackendRoute struct {
	TelegramProfileIDs []string `json:"telegramProfileIds"`
	QQProfileIDs       []string `json:"qqProfileIds"`
}

// BackendRouting intentionally allows a backend to select more than one
// profile.  The binding remains the final, explicit platform/profile/
// conversation/backend/target dispatch decision.
type BackendRouting struct {
	Codex    BackendRoute `json:"codex"`
	OpenClaw BackendRoute `json:"openclaw"`
}

type ProfileStatus struct {
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	Platform            string   `json:"platform"`
	Enabled             bool     `json:"enabled"`
	ResourceID          string   `json:"resourceId"`
	SharedWithProfileID string   `json:"sharedWithProfileId,omitempty"`
	AssignedBackends    []string `json:"assignedBackends"`
	Configured          bool     `json:"configured"`
	Running             bool     `json:"running"`
	Connected           bool     `json:"connected"`
	State               string   `json:"state"`
	TokenSet            bool     `json:"tokenSet,omitempty"`
	SecretConfigured    bool     `json:"secretConfigured,omitempty"`
	AccountID           string   `json:"accountId,omitempty"`
	BotUsername         string   `json:"botUsername,omitempty"`
	LastConnectedAt     string   `json:"lastConnectedAt,omitempty"`
	LastUpdateAt        string   `json:"lastUpdateAt,omitempty"`
	ReconnectCount      int      `json:"reconnectCount"`
	LastError           string   `json:"lastError,omitempty"`
	ProxyMode           string   `json:"proxyMode,omitempty"`
	MaskedProxyAddress  string   `json:"maskedProxyAddress,omitempty"`
	BindingCount        int      `json:"bindingCount"`
}

type ListResponse struct {
	Profiles []ProfileStatus `json:"profiles"`
	Routing  BackendRouting  `json:"routing"`
}

type profile struct {
	request  ConfigureRequest
	resource *resource
}

type resource struct {
	id          string
	platform    string
	key         string
	canonicalID string
	profiles    map[string]struct{}
	startedBy   map[string]struct{}
	telegram    *telegram.Service
	qq          *qqbot.Service
}

type Manager struct {
	mu        sync.RWMutex
	control   *control.Service
	runtime   *bridgeruntime.Manager
	bindings  *bindings.Repository
	broker    *events.Broker
	logger    *bridgelog.SafeLogger
	registry  any
	commands  *commandregistry.Registry
	openclaw  conversation.IConversationBackend
	profiles  map[string]*profile
	resources map[string]*resource // credential key -> resource
	byID      map[string]*resource // canonical physical resource id -> resource
	routing   BackendRouting
}

func NewManager(controlService *control.Service, runtimeManager *bridgeruntime.Manager, repository *bindings.Repository, broker *events.Broker, logger *bridgelog.SafeLogger, registry any, commands *commandregistry.Registry, openclaw conversation.IConversationBackend) *Manager {
	return &Manager{
		control: controlService, runtime: runtimeManager, bindings: repository, broker: broker, logger: logger, registry: registry,
		commands: commands, openclaw: openclaw, profiles: make(map[string]*profile), resources: make(map[string]*resource), byID: make(map[string]*resource),
	}
}

func (m *Manager) SetOpenClawBackend(backend conversation.IConversationBackend) {
	m.mu.Lock()
	m.openclaw = backend
	for _, item := range m.resources {
		if item.telegram != nil {
			item.telegram.SetOpenClawBackend(backend)
		}
		if item.qq != nil {
			item.qq.SetOpenClawBackend(backend)
		}
	}
	m.mu.Unlock()
}

func (m *Manager) SetCommandRegistry(commands *commandregistry.Registry) {
	if commands == nil {
		return
	}
	m.mu.Lock()
	m.commands = commands
	for _, item := range m.resources {
		if item.telegram != nil {
			item.telegram.SetCommandRegistry(commands)
		}
		if item.qq != nil {
			item.qq.SetCommandRegistry(commands)
		}
	}
	m.mu.Unlock()
}

func (m *Manager) Configure(request ConfigureRequest) (ProfileStatus, error) {
	request, err := normalizeRequest(request)
	if err != nil {
		return ProfileStatus{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	previous := m.profiles[request.ID]
	key, err := m.resourceKeyLocked(request, previous)
	if err != nil {
		return ProfileStatus{}, err
	}
	item := m.resources[key]
	if item == nil {
		item, err = m.newResourceLocked(request, key)
		if err != nil {
			return ProfileStatus{}, err
		}
		m.resources[key] = item
		m.byID[item.id] = item
	}
	if previous != nil && previous.resource != item {
		delete(previous.resource.profiles, request.ID)
		delete(previous.resource.startedBy, request.ID)
		if err := m.releaseIfUnusedLocked(previous.resource); err != nil {
			return ProfileStatus{}, err
		}
	}
	item.profiles[request.ID] = struct{}{}
	// Keep write-only credential inputs out of the manager's logical Profile
	// metadata. The live adapter necessarily owns an active credential, but
	// profile status, route data, diagnostics, and any future metadata dump
	// must not retain a Token/AppSecret pointer.
	storedRequest := cloneConfigureRequest(request)
	if storedRequest.Telegram != nil {
		storedRequest.Telegram.Token = nil
	}
	if storedRequest.QQ != nil {
		storedRequest.QQ.AppSecret = nil
	}
	m.profiles[request.ID] = &profile{request: storedRequest, resource: item}

	// Only the canonical physical resource normally applies transport
	// configuration. An alias with the same credentials intentionally cannot
	// spin up or reconfigure a second poller/gateway. The exception repairs a
	// resource whose legacy credential was explicitly cleared: the first alias
	// to supply the matching credential can configure that single resource.
	if item.canonicalID == request.ID || !resourceConfigured(item) {
		if err := m.configureResourceLocked(item, request); err != nil {
			return ProfileStatus{}, err
		}
	}
	return m.statusLocked(m.profiles[request.ID]), nil
}

func (m *Manager) Start(ctx context.Context, profileID string) (ProfileStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.profiles[strings.TrimSpace(profileID)]
	if !ok {
		return ProfileStatus{}, ErrProfileNotFound
	}
	if !item.request.Enabled {
		return m.statusLocked(item), errors.New("channel profile is disabled")
	}
	if _, already := item.resource.startedBy[profileID]; already {
		return m.statusLocked(item), nil
	}
	wasEmpty := len(item.resource.startedBy) == 0
	item.resource.startedBy[profileID] = struct{}{}
	if wasEmpty {
		if err := startResource(ctx, item.resource); err != nil {
			delete(item.resource.startedBy, profileID)
			return m.statusLocked(item), err
		}
	}
	return m.statusLocked(item), nil
}

func (m *Manager) Stop(ctx context.Context, profileID string) (ProfileStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.profiles[strings.TrimSpace(profileID)]
	if !ok {
		return ProfileStatus{}, ErrProfileNotFound
	}
	delete(item.resource.startedBy, profileID)
	if len(item.resource.startedBy) == 0 {
		if err := stopResource(ctx, item.resource); err != nil {
			return m.statusLocked(item), err
		}
	}
	return m.statusLocked(item), nil
}

func (m *Manager) Delete(ctx context.Context, profileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	profileID = strings.TrimSpace(profileID)
	item, ok := m.profiles[profileID]
	if !ok {
		return ErrProfileNotFound
	}
	if m.bindings != nil {
		for _, binding := range m.bindings.List() {
			if binding.ChannelProfileID == profileID {
				return errors.New("remove or rebind this profile's conversations before deleting it")
			}
		}
	}
	delete(m.profiles, profileID)
	delete(item.resource.profiles, profileID)
	delete(item.resource.startedBy, profileID)
	m.removeProfileFromRoutingLocked(profileID)
	return m.releaseIfUnusedLocked(item.resource)
}

func (m *Manager) List() ListResponse {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := ListResponse{Profiles: make([]ProfileStatus, 0, len(m.profiles)), Routing: cloneRouting(m.routing)}
	for _, item := range m.profiles {
		result.Profiles = append(result.Profiles, m.statusLocked(item))
	}
	sort.Slice(result.Profiles, func(i, j int) bool {
		if result.Profiles[i].Platform != result.Profiles[j].Platform {
			return result.Profiles[i].Platform < result.Profiles[j].Platform
		}
		return result.Profiles[i].Name < result.Profiles[j].Name
	})
	return result
}

func (m *Manager) Status(profileID string) (ProfileStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.profiles[strings.TrimSpace(profileID)]
	if !ok {
		return ProfileStatus{}, ErrProfileNotFound
	}
	return m.statusLocked(item), nil
}

func (m *Manager) SetRouting(routing BackendRouting) (BackendRouting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	routing = normalizeRouting(routing)
	if err := m.validateRouteLocked(routing.Codex); err != nil {
		return BackendRouting{}, fmt.Errorf("codex routing: %w", err)
	}
	if err := m.validateRouteLocked(routing.OpenClaw); err != nil {
		return BackendRouting{}, fmt.Errorf("openclaw routing: %w", err)
	}
	m.routing = routing
	return cloneRouting(m.routing), nil
}

func (m *Manager) Routing() BackendRouting {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneRouting(m.routing)
}

// CurrentBackend returns the default backend for an unbound conversation on a
// logical channel profile. A binding always wins over this fallback. When a
// profile is assigned to both backends, retain Codex as the legacy default so
// existing channels do not change behavior until they are explicitly bound.
func (m *Manager) CurrentBackend(profileID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	profileID = strings.TrimSpace(profileID)
	if routeContains(m.routing.OpenClaw, profileID) && !routeContains(m.routing.Codex, profileID) {
		return conversation.BackendOpenClaw
	}
	return conversation.BackendCodex
}

// PrepareBinding resolves a user-facing profile ID into its physical resource
// ID.  This makes the repository key deterministic even when profiles share
// one token/AppSecret.
func (m *Manager) PrepareBinding(request bindings.CreateRequest) (bindings.CreateRequest, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	profileID := strings.TrimSpace(request.ChannelProfileID)
	if profileID == "" {
		return bindings.CreateRequest{}, errors.New("channelProfileId is required")
	}
	item, ok := m.profiles[profileID]
	if !ok {
		return bindings.CreateRequest{}, ErrProfileNotFound
	}
	if !platformMatchesChannel(item.request.Platform, request.ChannelType) {
		return bindings.CreateRequest{}, ErrPlatformMismatch
	}
	backend := strings.ToLower(strings.TrimSpace(request.Backend))
	if backend != conversation.BackendCodex && backend != conversation.BackendOpenClaw {
		return bindings.CreateRequest{}, errors.New("binding backend must be codex or openclaw")
	}
	// A shared transport reports its canonical physical profile. If that
	// profile is only assigned to the other backend, choose a logical alias on
	// the same resource that is assigned to this backend. The stored binding
	// still owns the physical ResourceID, so one chat on a shared bot cannot
	// accidentally deliver to two backends.
	if !m.profileAllowsBackendLocked(item.request.ID, backend) {
		candidates := make([]string, 0, len(item.resource.profiles))
		for candidate := range item.resource.profiles {
			candidates = append(candidates, candidate)
		}
		sort.Strings(candidates)
		for _, candidate := range candidates {
			if m.profileAllowsBackendLocked(candidate, backend) {
				item = m.profiles[candidate]
				break
			}
		}
		if !m.profileAllowsBackendLocked(item.request.ID, backend) {
			return bindings.CreateRequest{}, fmt.Errorf("channel profile %q is not assigned to %s", profileID, backend)
		}
	}
	if !item.request.Enabled {
		return bindings.CreateRequest{}, errors.New("channel profile is disabled")
	}
	request.ChannelProfileID = item.request.ID
	request.ResourceID = item.resource.id
	if strings.TrimSpace(request.AccountID) == "" {
		request.AccountID = resourceAccountID(item.resource)
	}
	return request, nil
}

func (m *Manager) profileAllowsBackendLocked(profileID, backend string) bool {
	if m.profiles[profileID] == nil {
		return false
	}
	switch backend {
	case conversation.BackendCodex:
		return routeContains(m.routing.Codex, profileID)
	case conversation.BackendOpenClaw:
		return routeContains(m.routing.OpenClaw, profileID)
	default:
		return false
	}
}

func (m *Manager) BindingCreated(binding bindings.Binding) {
	m.mu.RLock()
	item := m.byID[binding.ResourceID]
	m.mu.RUnlock()
	if item == nil {
		return
	}
	if item.telegram != nil {
		item.telegram.BindingCreated(binding)
	}
	if item.qq != nil {
		item.qq.BindingCreated(binding)
	}
}

func (m *Manager) BindingDeleted(binding bindings.Binding) {
	m.mu.RLock()
	item := m.byID[binding.ResourceID]
	m.mu.RUnlock()
	if item == nil {
		return
	}
	if item.telegram != nil {
		item.telegram.BindingDeleted(binding)
	}
	if item.qq != nil {
		item.qq.BindingDeleted(binding)
	}
}

func (m *Manager) TelegramStatus() telegram.AdapterStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item := m.profileOrFirstLocked(DefaultTelegramProfileID, PlatformTelegram)
	if item == nil || item.resource.telegram == nil {
		return telegram.AdapterStatus{Type: "telegram", ChannelType: "telegram", State: "not-configured", PollingState: "stopped"}
	}
	return item.resource.telegram.Adapter().TelegramStatus()
}

func (m *Manager) QQStatus() qqbot.AdapterStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item := m.profileOrFirstLocked(DefaultQQProfileID, PlatformQQ)
	if item == nil || item.resource.qq == nil {
		return qqbot.AdapterStatus{Type: "qqbot", ChannelType: "qqbot", Environment: "production", GatewayState: "not-configured", ConnectionState: "not-configured"}
	}
	return item.resource.qq.Adapter().QQBotStatus()
}

// Legacy API adapters retain existing desktop clients and old automation. All
// calls target a migrated default profile, never a parallel hidden adapter.
func (m *Manager) ConfigureDefaultTelegram(input telegram.ConfigureRequest) (telegram.AdapterStatus, error) {
	request := ConfigureRequest{ID: DefaultTelegramProfileID, Name: "Telegram-1", Platform: PlatformTelegram, Enabled: true, Telegram: telegramConfigFromAdapter(input)}
	if _, err := m.Configure(request); err != nil {
		return telegram.AdapterStatus{}, err
	}
	m.ensureLegacyDefaultRouting(DefaultTelegramProfileID, PlatformTelegram)
	return m.TelegramStatus(), nil
}

func (m *Manager) StartDefaultTelegram(ctx context.Context) (telegram.AdapterStatus, error) {
	if _, err := m.Start(ctx, DefaultTelegramProfileID); err != nil {
		return m.TelegramStatus(), err
	}
	return m.TelegramStatus(), nil
}

func (m *Manager) StopDefaultTelegram(ctx context.Context) (telegram.AdapterStatus, error) {
	if _, err := m.Stop(ctx, DefaultTelegramProfileID); err != nil {
		return m.TelegramStatus(), err
	}
	return m.TelegramStatus(), nil
}

func (m *Manager) DeleteDefaultTelegramToken(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	item := m.profiles[DefaultTelegramProfileID]
	if item == nil || item.resource.telegram == nil {
		return nil
	}
	delete(item.resource.startedBy, DefaultTelegramProfileID)
	return item.resource.telegram.DeleteToken(ctx)
}

func (m *Manager) TestDefaultTelegram(ctx context.Context, input telegram.TestRequest) telegram.TestResult {
	m.mu.RLock()
	item := m.profileOrFirstLocked(DefaultTelegramProfileID, PlatformTelegram)
	m.mu.RUnlock()
	if item == nil || item.resource.telegram == nil {
		return telegram.TestResult{Category: "invalid-token", Message: "Telegram token is required"}
	}
	return item.resource.telegram.Adapter().Test(ctx, input)
}

func (m *Manager) TestDefaultTelegramProxy(ctx context.Context, input telegram.ProxyTestRequest) telegram.ProxyTestResult {
	m.mu.RLock()
	item := m.profileOrFirstLocked(DefaultTelegramProfileID, PlatformTelegram)
	m.mu.RUnlock()
	if item == nil || item.resource.telegram == nil {
		return telegram.ProxyTestResult{Category: "not-configured", Message: "Telegram profile is not configured"}
	}
	return item.resource.telegram.Adapter().TestProxy(ctx, input)
}

func (m *Manager) ConfigureDefaultQQ(input qqbot.ConfigureRequest, secret *string) (qqbot.AdapterStatus, error) {
	request := ConfigureRequest{ID: DefaultQQProfileID, Name: "QQ-1", Platform: PlatformQQ, Enabled: input.Enabled, QQ: qqConfigFromAdapter(input, secret)}
	if _, err := m.Configure(request); err != nil {
		return qqbot.AdapterStatus{}, err
	}
	m.ensureLegacyDefaultRouting(DefaultQQProfileID, PlatformQQ)
	return m.QQStatus(), nil
}

// Legacy channel endpoints predate the routing API. Keep their historical
// single-bot behavior usable by assigning the migrated default profile to both
// backends until the Profile UI or routing API explicitly changes it.
func (m *Manager) ensureLegacyDefaultRouting(profileID, platform string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if platform == PlatformTelegram {
		if len(m.routing.Codex.TelegramProfileIDs) == 0 {
			m.routing.Codex.TelegramProfileIDs = []string{profileID}
		}
		if len(m.routing.OpenClaw.TelegramProfileIDs) == 0 {
			m.routing.OpenClaw.TelegramProfileIDs = []string{profileID}
		}
		return
	}
	if len(m.routing.Codex.QQProfileIDs) == 0 {
		m.routing.Codex.QQProfileIDs = []string{profileID}
	}
	if len(m.routing.OpenClaw.QQProfileIDs) == 0 {
		m.routing.OpenClaw.QQProfileIDs = []string{profileID}
	}
}

func (m *Manager) SetDefaultQQSecret(secret string) (qqbot.AdapterStatus, error) {
	if strings.TrimSpace(secret) == "" {
		return qqbot.AdapterStatus{}, errors.New("QQ Bot AppSecret is required")
	}
	m.mu.RLock()
	item := m.profiles[DefaultQQProfileID]
	if item == nil || item.request.QQ == nil {
		m.mu.RUnlock()
		return qqbot.AdapterStatus{}, errors.New("configure QQ AppID before setting its AppSecret")
	}
	request := cloneConfigureRequest(item.request)
	m.mu.RUnlock()
	request.QQ.AppSecret = &secret
	// Reconfigure through the normal resource-key path instead of merely
	// assigning the secret to an already-unconfigured adapter.  That upgrades
	// old /channels/qqbot callers into the same credential fingerprint used by
	// Profile API callers, so a subsequent matching profile cannot create a
	// second Gateway.
	if _, err := m.Configure(request); err != nil {
		return qqbot.AdapterStatus{}, err
	}
	return m.QQStatus(), nil
}

func (m *Manager) StartDefaultQQ(ctx context.Context) (qqbot.AdapterStatus, error) {
	if _, err := m.Start(ctx, DefaultQQProfileID); err != nil {
		return m.QQStatus(), err
	}
	return m.QQStatus(), nil
}

func (m *Manager) StopDefaultQQ(ctx context.Context) (qqbot.AdapterStatus, error) {
	if _, err := m.Stop(ctx, DefaultQQProfileID); err != nil {
		return m.QQStatus(), err
	}
	return m.QQStatus(), nil
}

func (m *Manager) DeleteDefaultQQSecret(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	item := m.profiles[DefaultQQProfileID]
	if item == nil || item.resource.qq == nil {
		return nil
	}
	delete(item.resource.startedBy, DefaultQQProfileID)
	return item.resource.qq.DeleteSecret(ctx)
}

func (m *Manager) TestDefaultQQ(ctx context.Context, input qqbot.TestRequest) qqbot.TestResult {
	m.mu.RLock()
	item := m.profileOrFirstLocked(DefaultQQProfileID, PlatformQQ)
	m.mu.RUnlock()
	if item == nil || item.resource.qq == nil {
		return qqbot.TestResult{Code: "qqbot_credentials_missing", Message: "请先配置 QQ Bot。"}
	}
	return item.resource.qq.Test(ctx, input)
}

func (m *Manager) TestDefaultQQNetwork(ctx context.Context) qqbot.NetworkTestResult {
	m.mu.RLock()
	item := m.profileOrFirstLocked(DefaultQQProfileID, PlatformQQ)
	m.mu.RUnlock()
	if item == nil || item.resource.qq == nil {
		return qqbot.NetworkTestResult{Code: "not-configured", Message: "请先配置 QQ Bot。"}
	}
	return item.resource.qq.Adapter().TestNetwork(ctx)
}

func (m *Manager) DefaultQQIdentities() []qqbot.DiscoveredIdentity {
	m.mu.RLock()
	item := m.profileOrFirstLocked(DefaultQQProfileID, PlatformQQ)
	m.mu.RUnlock()
	if item == nil || item.resource.qq == nil {
		return []qqbot.DiscoveredIdentity{}
	}
	return item.resource.qq.Adapter().DiscoveredIdentities()
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	resources := make([]*resource, 0, len(m.byID))
	for _, item := range m.byID {
		resources = append(resources, item)
	}
	m.profiles = make(map[string]*profile)
	m.resources = make(map[string]*resource)
	m.byID = make(map[string]*resource)
	m.mu.Unlock()
	var failures []error
	for _, item := range resources {
		if err := closeResource(ctx, item); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (m *Manager) TelegramMirrorTarget() (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item := m.profileForBackendLocked(m.routing.Codex.TelegramProfileIDs, PlatformTelegram, DefaultTelegramProfileID)
	if item == nil || item.resource.telegram == nil {
		return "", false
	}
	status := item.resource.telegram.Adapter().TelegramStatus()
	return status.BotID, status.Running && status.Connected
}

func (m *Manager) SendTelegramMirror(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
	m.mu.RLock()
	item := m.profileForBackendLocked(m.routing.Codex.TelegramProfileIDs, PlatformTelegram, DefaultTelegramProfileID)
	m.mu.RUnlock()
	if item == nil || item.resource.telegram == nil {
		return channels.OutboundResult{}, errors.New("Telegram profile is not configured")
	}
	return item.resource.telegram.Adapter().SendMessage(ctx, message)
}

func (m *Manager) QQMirrorTarget() (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item := m.profileForBackendLocked(m.routing.Codex.QQProfileIDs, PlatformQQ, DefaultQQProfileID)
	if item == nil || item.resource.qq == nil {
		return "", false
	}
	status := item.resource.qq.Adapter().QQBotStatus()
	return status.AppID, status.Running && status.Connected
}

func (m *Manager) SendQQMirror(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
	m.mu.RLock()
	item := m.profileForBackendLocked(m.routing.Codex.QQProfileIDs, PlatformQQ, DefaultQQProfileID)
	m.mu.RUnlock()
	if item == nil || item.resource.qq == nil {
		return channels.OutboundResult{}, errors.New("QQ profile is not configured")
	}
	return item.resource.qq.Adapter().SendMessage(ctx, message)
}

func (m *Manager) newResourceLocked(request ConfigureRequest, key string) (*resource, error) {
	item := &resource{id: request.ID, canonicalID: request.ID, platform: request.Platform, key: key, profiles: make(map[string]struct{}), startedBy: make(map[string]struct{})}
	switch request.Platform {
	case PlatformTelegram:
		item.telegram = telegram.NewService(m.control, m.runtime, m.bindings, m.broker, m.logger, m.registry)
		item.telegram.SetChannelProfileID(item.id)
		item.telegram.SetBindingPreparer(m.PrepareBinding)
		item.telegram.SetBackendResolver(m.CurrentBackend)
		item.telegram.SetOpenClawBackend(m.openclaw)
		if m.commands != nil {
			item.telegram.SetCommandRegistry(m.commands)
		}
	case PlatformQQ:
		item.qq = qqbot.NewService(m.control, m.runtime, m.bindings, m.broker, m.logger, m.registry)
		item.qq.SetChannelProfileID(item.id)
		item.qq.SetBindingPreparer(m.PrepareBinding)
		item.qq.SetBackendResolver(m.CurrentBackend)
		item.qq.SetOpenClawBackend(m.openclaw)
		if m.commands != nil {
			item.qq.SetCommandRegistry(m.commands)
		}
	default:
		return nil, errors.New("unsupported channel platform")
	}
	return item, nil
}

func (m *Manager) configureResourceLocked(item *resource, request ConfigureRequest) error {
	switch item.platform {
	case PlatformTelegram:
		if request.Telegram == nil {
			return errors.New("telegram configuration is required")
		}
		_, err := item.telegram.Configure(telegram.ConfigureRequest{
			Token: request.Telegram.Token, AllowedUserIDs: append([]int64(nil), request.Telegram.AllowedUserIDs...),
			PollingTimeoutSeconds: request.Telegram.PollingTimeoutSeconds, SendProgressUpdates: request.Telegram.SendProgressUpdates,
			AutoStart: request.Telegram.AutoStart, ProxyMode: request.Telegram.ProxyMode, ProxyURL: request.Telegram.ProxyURL,
		})
		return err
	case PlatformQQ:
		if request.QQ == nil {
			return errors.New("QQ configuration is required")
		}
		if request.QQ.AppSecret != nil {
			if _, err := item.qq.Adapter().SetSecret(*request.QQ.AppSecret); err != nil {
				return err
			}
		}
		_, err := item.qq.Configure(qqbot.ConfigureRequest{
			Enabled: request.QQ.Enabled, AutoStart: request.QQ.AutoStart, AppID: request.QQ.AppID, Environment: request.QQ.Environment,
			AllowedUserOpenIDs: append([]string(nil), request.QQ.AllowedUserOpenIDs...), AllowedGroupOpenIDs: append([]string(nil), request.QQ.AllowedGroupOpenIDs...),
			AllowedGroupMemberOpenIDs: append([]string(nil), request.QQ.AllowedGroupMemberOpenIDs...), GroupTriggerMode: request.QQ.GroupTriggerMode,
			CommandPrefix: request.QQ.CommandPrefix, SendProgressUpdates: request.QQ.SendProgressUpdates,
			GatewayReconnectEnabled: request.QQ.GatewayReconnectEnabled, ProxyMode: request.QQ.ProxyMode, ProxyURL: request.QQ.ProxyURL,
		})
		return err
	}
	return nil
}

func (m *Manager) resourceKeyLocked(request ConfigureRequest, previous *profile) (string, error) {
	switch request.Platform {
	case PlatformTelegram:
		if request.Telegram == nil {
			return "", errors.New("telegram configuration is required")
		}
		if request.Telegram.Token != nil {
			if token := strings.TrimSpace(*request.Telegram.Token); token != "" {
				return "telegram:" + fingerprint(token), nil
			}
		}
	case PlatformQQ:
		if request.QQ == nil {
			return "", errors.New("QQ configuration is required")
		}
		if request.QQ.AppSecret != nil {
			if secret := *request.QQ.AppSecret; strings.TrimSpace(secret) != "" {
				return "qqbot:" + fingerprint(strings.TrimSpace(request.QQ.AppID)+"\x00"+secret), nil
			}
		}
	}
	if previous != nil && previous.resource != nil && previous.resource.platform == request.Platform {
		return previous.resource.key, nil
	}
	return request.Platform + ":unconfigured:" + request.ID, nil
}

func (m *Manager) releaseIfUnusedLocked(item *resource) error {
	if len(item.profiles) != 0 {
		return nil
	}
	// A profile may be reconfigured with new credentials while retaining its
	// stable profile ID. In that case the replacement resource has already
	// claimed the same canonical ID; never remove the replacement from byID
	// while retiring the old transport.
	if current := m.resources[item.key]; current == item {
		delete(m.resources, item.key)
	}
	if current := m.byID[item.id]; current == item {
		delete(m.byID, item.id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return closeResource(ctx, item)
}

func (m *Manager) statusLocked(item *profile) ProfileStatus {
	status := ProfileStatus{ID: item.request.ID, Name: item.request.Name, Platform: item.request.Platform, Enabled: item.request.Enabled, ResourceID: item.resource.id, AssignedBackends: m.assignedBackendsLocked(item.request.ID)}
	if item.resource.canonicalID != item.request.ID {
		status.SharedWithProfileID = item.resource.canonicalID
	}
	if item.resource.telegram != nil {
		adapter := item.resource.telegram.Adapter().TelegramStatus()
		status.Configured, status.Running, status.Connected, status.State = adapter.Configured, adapter.Running, adapter.Connected, adapter.State
		status.TokenSet, status.AccountID, status.BotUsername = adapter.TokenSet, adapter.BotID, adapter.BotUsername
		status.LastUpdateAt, status.LastError, status.ProxyMode, status.MaskedProxyAddress, status.BindingCount = adapter.LastUpdateAt, adapter.LastError, adapter.ProxyMode, adapter.MaskedProxyAddress, adapter.BindingCount
	}
	if item.resource.qq != nil {
		adapter := item.resource.qq.Adapter().QQBotStatus()
		status.Configured, status.Running, status.Connected, status.State = adapter.Configured, adapter.Running, adapter.Connected, adapter.ConnectionState
		if status.State == "" {
			status.State = adapter.GatewayState
		}
		status.SecretConfigured, status.AccountID = adapter.SecretConfigured, adapter.AppID
		status.LastConnectedAt, status.ReconnectCount, status.LastError, status.ProxyMode, status.MaskedProxyAddress, status.BindingCount = adapter.LastConnectedAt, adapter.ReconnectCount, adapter.LastErrorMessage, adapter.ProxyMode, adapter.MaskedProxyAddress, adapter.BindingCount
	}
	return status
}

func (m *Manager) assignedBackendsLocked(profileID string) []string {
	result := []string{}
	if routeContains(m.routing.Codex, profileID) {
		result = append(result, conversation.BackendCodex)
	}
	if routeContains(m.routing.OpenClaw, profileID) {
		result = append(result, conversation.BackendOpenClaw)
	}
	return result
}

func (m *Manager) validateRouteLocked(route BackendRoute) error {
	for _, id := range route.TelegramProfileIDs {
		item, ok := m.profiles[id]
		if !ok {
			return fmt.Errorf("profile %q does not exist", id)
		}
		if item.request.Platform != PlatformTelegram {
			return fmt.Errorf("profile %q is not Telegram", id)
		}
	}
	for _, id := range route.QQProfileIDs {
		item, ok := m.profiles[id]
		if !ok {
			return fmt.Errorf("profile %q does not exist", id)
		}
		if item.request.Platform != PlatformQQ {
			return fmt.Errorf("profile %q is not QQ", id)
		}
	}
	return nil
}

func (m *Manager) removeProfileFromRoutingLocked(profileID string) {
	m.routing.Codex.TelegramProfileIDs = removeID(m.routing.Codex.TelegramProfileIDs, profileID)
	m.routing.Codex.QQProfileIDs = removeID(m.routing.Codex.QQProfileIDs, profileID)
	m.routing.OpenClaw.TelegramProfileIDs = removeID(m.routing.OpenClaw.TelegramProfileIDs, profileID)
	m.routing.OpenClaw.QQProfileIDs = removeID(m.routing.OpenClaw.QQProfileIDs, profileID)
}

func (m *Manager) profileOrFirstLocked(preferred, platform string) *profile {
	if item := m.profiles[preferred]; item != nil && item.request.Platform == platform {
		return item
	}
	return m.profileForBackendLocked(nil, platform, "")
}

func (m *Manager) profileForBackendLocked(ids []string, platform, fallback string) *profile {
	for _, id := range ids {
		if item := m.profiles[id]; item != nil && item.request.Platform == platform {
			return item
		}
	}
	if fallback != "" {
		if item := m.profiles[fallback]; item != nil && item.request.Platform == platform {
			return item
		}
	}
	keys := make([]string, 0, len(m.profiles))
	for id := range m.profiles {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		if item := m.profiles[id]; item.request.Platform == platform {
			return item
		}
	}
	return nil
}

func normalizeRequest(request ConfigureRequest) (ConfigureRequest, error) {
	request.ID = strings.TrimSpace(request.ID)
	request.Name = strings.TrimSpace(request.Name)
	request.Platform = strings.ToLower(strings.TrimSpace(request.Platform))
	if request.ID == "" {
		return ConfigureRequest{}, errors.New("profile id is required")
	}
	if len(request.ID) > 128 {
		return ConfigureRequest{}, errors.New("profile id is too long")
	}
	if request.Name == "" {
		request.Name = request.ID
	}
	if request.Platform != PlatformTelegram && request.Platform != PlatformQQ {
		return ConfigureRequest{}, errors.New("platform must be telegram or qqbot")
	}
	if request.Platform == PlatformTelegram && request.Telegram == nil {
		return ConfigureRequest{}, errors.New("telegram configuration is required")
	}
	if request.Platform == PlatformQQ && request.QQ == nil {
		return ConfigureRequest{}, errors.New("QQ configuration is required")
	}
	return request, nil
}

func normalizeRouting(routing BackendRouting) BackendRouting {
	routing.Codex.TelegramProfileIDs = normalizeIDs(routing.Codex.TelegramProfileIDs)
	routing.Codex.QQProfileIDs = normalizeIDs(routing.Codex.QQProfileIDs)
	routing.OpenClaw.TelegramProfileIDs = normalizeIDs(routing.OpenClaw.TelegramProfileIDs)
	routing.OpenClaw.QQProfileIDs = normalizeIDs(routing.OpenClaw.QQProfileIDs)
	return routing
}

func normalizeIDs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func cloneRouting(value BackendRouting) BackendRouting {
	value.Codex.TelegramProfileIDs = cloneIDs(value.Codex.TelegramProfileIDs)
	value.Codex.QQProfileIDs = cloneIDs(value.Codex.QQProfileIDs)
	value.OpenClaw.TelegramProfileIDs = cloneIDs(value.OpenClaw.TelegramProfileIDs)
	value.OpenClaw.QQProfileIDs = cloneIDs(value.OpenClaw.QQProfileIDs)
	return value
}

func cloneIDs(values []string) []string {
	result := make([]string, len(values))
	copy(result, values)
	return result
}

func cloneConfigureRequest(value ConfigureRequest) ConfigureRequest {
	result := value
	if value.Telegram != nil {
		copy := *value.Telegram
		copy.AllowedUserIDs = append([]int64(nil), value.Telegram.AllowedUserIDs...)
		if value.Telegram.Token != nil {
			token := *value.Telegram.Token
			copy.Token = &token
		}
		result.Telegram = &copy
	}
	if value.QQ != nil {
		copy := *value.QQ
		copy.AllowedUserOpenIDs = append([]string(nil), value.QQ.AllowedUserOpenIDs...)
		copy.AllowedGroupOpenIDs = append([]string(nil), value.QQ.AllowedGroupOpenIDs...)
		copy.AllowedGroupMemberOpenIDs = append([]string(nil), value.QQ.AllowedGroupMemberOpenIDs...)
		if value.QQ.AppSecret != nil {
			secret := *value.QQ.AppSecret
			copy.AppSecret = &secret
		}
		result.QQ = &copy
	}
	return result
}

func routeContains(route BackendRoute, profileID string) bool {
	for _, candidate := range append(append([]string(nil), route.TelegramProfileIDs...), route.QQProfileIDs...) {
		if candidate == profileID {
			return true
		}
	}
	return false
}

func removeID(values []string, value string) []string {
	result := values[:0]
	for _, candidate := range values {
		if candidate != value {
			result = append(result, candidate)
		}
	}
	return result
}

func platformMatchesChannel(platform, channelType string) bool {
	platform, channelType = strings.ToLower(strings.TrimSpace(platform)), strings.ToLower(strings.TrimSpace(channelType))
	return (platform == PlatformTelegram && channelType == "telegram") || (platform == PlatformQQ && channelType == "qqbot")
}

func resourceAccountID(item *resource) string {
	if item == nil {
		return ""
	}
	if item.telegram != nil {
		return item.telegram.Adapter().TelegramStatus().BotID
	}
	if item.qq != nil {
		return item.qq.Adapter().QQBotStatus().AppID
	}
	return ""
}

func resourceConfigured(item *resource) bool {
	if item == nil {
		return false
	}
	if item.telegram != nil {
		return item.telegram.Adapter().TelegramStatus().TokenSet
	}
	if item.qq != nil {
		return item.qq.Adapter().QQBotStatus().SecretConfigured
	}
	return false
}

func startResource(ctx context.Context, item *resource) error {
	if item.telegram != nil {
		return item.telegram.Start(ctx)
	}
	if item.qq != nil {
		return item.qq.Start(ctx)
	}
	return errors.New("channel resource is unavailable")
}

func stopResource(ctx context.Context, item *resource) error {
	if item.telegram != nil {
		return item.telegram.Stop(ctx)
	}
	if item.qq != nil {
		return item.qq.Stop(ctx)
	}
	return nil
}

func closeResource(ctx context.Context, item *resource) error {
	if item.telegram != nil {
		return item.telegram.Close(ctx)
	}
	if item.qq != nil {
		return item.qq.Close(ctx)
	}
	return nil
}

func telegramConfigFromAdapter(input telegram.ConfigureRequest) *TelegramConfig {
	return &TelegramConfig{Token: input.Token, AllowedUserIDs: append([]int64(nil), input.AllowedUserIDs...), PollingTimeoutSeconds: input.PollingTimeoutSeconds, SendProgressUpdates: input.SendProgressUpdates, AutoStart: input.AutoStart, ProxyMode: input.ProxyMode, ProxyURL: input.ProxyURL}
}

func qqConfigFromAdapter(input qqbot.ConfigureRequest, secret *string) *QQConfig {
	return &QQConfig{AppSecret: secret, Enabled: input.Enabled, AutoStart: input.AutoStart, AppID: input.AppID, Environment: input.Environment, AllowedUserOpenIDs: append([]string(nil), input.AllowedUserOpenIDs...), AllowedGroupOpenIDs: append([]string(nil), input.AllowedGroupOpenIDs...), AllowedGroupMemberOpenIDs: append([]string(nil), input.AllowedGroupMemberOpenIDs...), GroupTriggerMode: input.GroupTriggerMode, CommandPrefix: input.CommandPrefix, SendProgressUpdates: input.SendProgressUpdates, GatewayReconnectEnabled: input.GatewayReconnectEnabled, ProxyMode: input.ProxyMode, ProxyURL: input.ProxyURL}
}

func fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// ResourceCount is intentionally exposed for focused integration tests and
// diagnostics. It reveals no credential material.
func (m *Manager) ResourceCount(platform string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, item := range m.byID {
		if item.platform == strings.ToLower(strings.TrimSpace(platform)) {
			count++
		}
	}
	return count
}

func (m *Manager) WaitForIdle(_ time.Duration) {}
