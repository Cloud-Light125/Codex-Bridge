package bindings

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrDuplicate = errors.New("channel address is already bound")
	ErrNotFound  = errors.New("binding was not found")
)

type Binding struct {
	ID string `json:"id"`
	// Backend and TargetID are deliberately explicit.  A target value alone
	// must never be used to infer whether it is a Codex Thread or an OpenClaw
	// Session.
	Backend          string `json:"backend"`
	TargetID         string `json:"targetId"`
	ChannelType      string `json:"channelType"`
	ChannelProfileID string `json:"channelProfileId,omitempty"`
	// ResourceID is the canonical physical channel resource.  It is not a
	// credential and lets several logical profiles share exactly one poller or
	// gateway without making an inbound message ambiguous.
	ResourceID       string `json:"resourceId,omitempty"`
	AccountID        string `json:"accountId"`
	ConversationType string `json:"conversationType"`
	ConversationID   string `json:"conversationId,omitempty"`
	ChatID           string `json:"chatId"`
	TopicID          string `json:"topicId,omitempty"`
	ThreadID         string `json:"threadId"`
	SessionKey       string `json:"sessionKey,omitempty"`
	Enabled          bool   `json:"enabled"`
	Legacy           bool   `json:"legacy,omitempty"`
	CreatedAt        string `json:"createdAt"`
	UpdatedAt        string `json:"updatedAt"`
}

type CreateRequest struct {
	Backend          string `json:"backend"`
	TargetID         string `json:"targetId"`
	ChannelType      string `json:"channelType"`
	ChannelProfileID string `json:"channelProfileId"`
	ResourceID       string `json:"resourceId,omitempty"`
	AccountID        string `json:"accountId"`
	ConversationType string `json:"conversationType"`
	ConversationID   string `json:"conversationId"`
	ChatID           string `json:"chatId"`
	TopicID          string `json:"topicId"`
	ThreadID         string `json:"threadId"`
	SessionKey       string `json:"sessionKey"`
	Enabled          *bool  `json:"enabled"`
}

type diskModel struct {
	Version  int       `json:"version"`
	Bindings []Binding `json:"bindings"`
}

type Repository struct {
	mu    sync.RWMutex
	path  string
	items map[string]Binding
}

func NewRepository(path string) (*Repository, error) {
	repository := &Repository{path: path, items: make(map[string]Binding)}
	if err := repository.load(); err != nil {
		return nil, err
	}
	return repository, nil
}

func (r *Repository) List() []Binding {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Binding, 0, len(r.items))
	for _, binding := range r.items {
		result = append(result, binding)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result
}

func (r *Repository) Create(request CreateRequest) (Binding, error) {
	request = normalizeRequest(request)
	if err := validateRequest(request); err != nil {
		return Binding{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	address := addressKey(request.ChannelType, requestAddressOwner(request), request.ConversationType, request.ChatID, request.TopicID)
	for _, existing := range r.items {
		if bindingAddressKey(existing) == address {
			return Binding{}, ErrDuplicate
		}
	}
	enabled := true
	if request.Enabled != nil {
		enabled = *request.Enabled
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	binding := Binding{
		ID: newID(), Backend: request.Backend, TargetID: request.TargetID, ChannelType: request.ChannelType,
		ChannelProfileID: request.ChannelProfileID, ResourceID: request.ResourceID, AccountID: request.AccountID,
		ConversationType: request.ConversationType, ConversationID: request.ConversationID,
		ChatID: request.ChatID, TopicID: request.TopicID, ThreadID: request.ThreadID, SessionKey: request.SessionKey,
		Enabled: enabled, CreatedAt: now, UpdatedAt: now,
	}
	r.items[binding.ID] = binding
	if err := r.saveLocked(); err != nil {
		delete(r.items, binding.ID)
		return Binding{}, err
	}
	return binding, nil
}

// FindAddress resolves the single binding for a channel address. The legacy
// form accepts chatID, topicID; the conversation-aware form accepts
// conversationType, chatID, topicID.
func (r *Repository) FindAddress(channelType, accountID string, address ...string) (Binding, bool) {
	conversationType, chatID, topicID, ok := addressParts(channelType, address)
	if !ok {
		return Binding{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := addressKey(channelType, strings.TrimSpace(accountID), conversationType, chatID, topicID)
	for _, binding := range r.items {
		if bindingAddressKey(binding) == key || legacyAccountAddressKey(binding) == key {
			return binding, true
		}
	}
	return Binding{}, false
}

// UpsertAddress atomically creates or replaces the Thread target for an address.
func (r *Repository) UpsertAddress(request CreateRequest) (Binding, *Binding, error) {
	request = normalizeRequest(request)
	if err := validateRequest(request); err != nil {
		return Binding{}, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := addressKey(request.ChannelType, requestAddressOwner(request), request.ConversationType, request.ChatID, request.TopicID)
	var previous *Binding
	for id, existing := range r.items {
		if bindingAddressKey(existing) != key {
			continue
		}
		copy := existing
		previous = &copy
		existing.ThreadID = request.ThreadID
		existing.Backend = request.Backend
		existing.TargetID = request.TargetID
		existing.ChannelProfileID = request.ChannelProfileID
		existing.ResourceID = request.ResourceID
		existing.ConversationID = request.ConversationID
		existing.SessionKey = request.SessionKey
		existing.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if request.Enabled != nil {
			existing.Enabled = *request.Enabled
		}
		r.items[id] = existing
		if err := r.saveLocked(); err != nil {
			r.items[id] = copy
			return Binding{}, nil, err
		}
		return existing, previous, nil
	}
	enabled := true
	if request.Enabled != nil {
		enabled = *request.Enabled
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	binding := Binding{ID: newID(), Backend: request.Backend, TargetID: request.TargetID, ChannelType: request.ChannelType, ChannelProfileID: request.ChannelProfileID, ResourceID: request.ResourceID, AccountID: request.AccountID, ConversationType: request.ConversationType, ConversationID: request.ConversationID, ChatID: request.ChatID, TopicID: request.TopicID, ThreadID: request.ThreadID, SessionKey: request.SessionKey, Enabled: enabled, CreatedAt: now, UpdatedAt: now}
	r.items[binding.ID] = binding
	if err := r.saveLocked(); err != nil {
		delete(r.items, binding.ID)
		return Binding{}, nil, err
	}
	return binding, nil, nil
}

func (r *Repository) ListChannelAccount(channelType, accountID string) []Binding {
	r.mu.RLock()
	defer r.mu.RUnlock()
	channelType, accountID = strings.ToLower(strings.TrimSpace(channelType)), strings.TrimSpace(accountID)
	result := []Binding{}
	for _, binding := range r.items {
		if strings.EqualFold(binding.ChannelType, channelType) && binding.AccountID == accountID {
			result = append(result, binding)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result
}

func (r *Repository) CountChannelAccount(channelType, accountID string) int {
	return len(r.ListChannelAccount(channelType, accountID))
}

// DeleteAddress accepts the same legacy and conversation-aware forms as
// FindAddress.
func (r *Repository) DeleteAddress(channelType, accountID string, address ...string) (Binding, error) {
	conversationType, chatID, topicID, ok := addressParts(channelType, address)
	if !ok {
		return Binding{}, ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := addressKey(channelType, strings.TrimSpace(accountID), conversationType, chatID, topicID)
	for id, binding := range r.items {
		if bindingAddressKey(binding) != key && legacyAccountAddressKey(binding) != key {
			continue
		}
		delete(r.items, id)
		if err := r.saveLocked(); err != nil {
			r.items[id] = binding
			return Binding{}, err
		}
		return binding, nil
	}
	return Binding{}, ErrNotFound
}

func (r *Repository) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	id = strings.TrimSpace(id)
	existing, ok := r.items[id]
	if !ok {
		return ErrNotFound
	}
	delete(r.items, id)
	if err := r.saveLocked(); err != nil {
		r.items[id] = existing
		return err
	}
	return nil
}

func (r *Repository) load() error {
	data, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read bindings: %w", err)
	}
	var model diskModel
	if err := json.Unmarshal(data, &model); err != nil {
		return fmt.Errorf("decode bindings: %w", err)
	}
	if model.Version != 1 && model.Version != 2 && model.Version != 3 && model.Version != 4 {
		return fmt.Errorf("decode bindings: unsupported version %d", model.Version)
	}
	addresses := make(map[string]string, len(model.Bindings))
	for index, binding := range model.Bindings {
		if model.Version == 1 {
			switch strings.ToLower(strings.TrimSpace(binding.ChannelType)) {
			case "telegram":
				binding.ConversationType = "default"
			}
		}
		if model.Version <= 2 && strings.EqualFold(strings.TrimSpace(binding.ChannelType), "qq") {
			binding.Enabled = false
			binding.Legacy = true
			if model.Version == 1 {
				binding.ConversationType = "legacy"
			}
		}
		if model.Version <= 3 {
			// Prior files had a single physical bot per platform.  Preserve that
			// behavior by assigning a stable default profile/resource during the
			// upgrade; the desktop then configures that profile from the existing
			// DPAPI secret.
			if binding.ChannelProfileID == "" {
				binding.ChannelProfileID = defaultProfileID(binding.ChannelType)
			}
			if binding.ResourceID == "" {
				binding.ResourceID = binding.ChannelProfileID
			}
			if binding.ConversationID == "" {
				binding.ConversationID = binding.ChatID
			}
			if binding.TargetID == "" {
				if strings.EqualFold(binding.Backend, "openclaw") {
					binding.TargetID = firstNonEmpty(binding.SessionKey, binding.ThreadID)
				} else {
					binding.TargetID = binding.ThreadID
				}
			}
		}
		binding = normalizeBinding(binding)
		if err := validateStoredBinding(binding, model.Version); err != nil {
			return fmt.Errorf("decode bindings: item %d: %w", index, err)
		}
		if _, duplicate := r.items[binding.ID]; duplicate {
			return fmt.Errorf("decode bindings: duplicate binding id %q", binding.ID)
		}
		key := bindingAddressKey(binding)
		if prior, duplicate := addresses[key]; duplicate {
			return fmt.Errorf("decode bindings: duplicate channel address in %q and %q", prior, binding.ID)
		}
		addresses[key] = binding.ID
		r.items[binding.ID] = binding
	}
	if model.Version <= 3 {
		if err := r.saveLocked(); err != nil {
			return fmt.Errorf("migrate bindings to version 4: %w", err)
		}
	}
	return nil
}

func (r *Repository) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	model := diskModel{Version: 4, Bindings: make([]Binding, 0, len(r.items))}
	for _, binding := range r.items {
		model.Bindings = append(model.Bindings, binding)
	}
	sort.Slice(model.Bindings, func(i, j int) bool { return model.Bindings[i].ID < model.Bindings[j].ID })
	data, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		return err
	}
	temporary := r.path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return err
	}
	backup := r.path + ".bak"
	_ = os.Remove(backup)
	if _, err := os.Stat(r.path); err == nil {
		if err := os.Rename(r.path, backup); err != nil {
			_ = os.Remove(temporary)
			return err
		}
	}
	if err := os.Rename(temporary, r.path); err != nil {
		_ = os.Rename(backup, r.path)
		return err
	}
	_ = os.Remove(backup)
	return nil
}

func addressKey(channelType, accountID, conversationType, chatID, topicID string) string {
	return strings.Join([]string{strings.ToLower(strings.TrimSpace(channelType)), strings.TrimSpace(accountID), strings.ToLower(strings.TrimSpace(conversationType)), strings.TrimSpace(chatID), strings.TrimSpace(topicID)}, "\x00")
}

func bindingAddressKey(binding Binding) string {
	return addressKey(binding.ChannelType, bindingAddressOwner(binding), binding.ConversationType, binding.ChatID, binding.TopicID)
}

func legacyAccountAddressKey(binding Binding) string {
	return addressKey(binding.ChannelType, binding.AccountID, binding.ConversationType, binding.ChatID, binding.TopicID)
}

func bindingAddressOwner(binding Binding) string {
	if value := strings.TrimSpace(binding.ResourceID); value != "" {
		return value
	}
	if value := strings.TrimSpace(binding.ChannelProfileID); value != "" {
		return value
	}
	return binding.AccountID
}

func requestAddressOwner(request CreateRequest) string {
	if value := strings.TrimSpace(request.ResourceID); value != "" {
		return value
	}
	if value := strings.TrimSpace(request.ChannelProfileID); value != "" {
		return value
	}
	return request.AccountID
}

func normalizeRequest(request CreateRequest) CreateRequest {
	request.Backend = normalizeBackend(request.Backend)
	request.TargetID = strings.TrimSpace(request.TargetID)
	request.ChannelType = strings.ToLower(strings.TrimSpace(request.ChannelType))
	request.ChannelProfileID = strings.TrimSpace(request.ChannelProfileID)
	request.ResourceID = strings.TrimSpace(request.ResourceID)
	request.AccountID = strings.TrimSpace(request.AccountID)
	request.ConversationType = strings.ToLower(strings.TrimSpace(request.ConversationType))
	if request.ChannelType == "telegram" && request.ConversationType == "" {
		request.ConversationType = "default"
	}
	request.ConversationID = strings.TrimSpace(request.ConversationID)
	request.ChatID = strings.TrimSpace(request.ChatID)
	if request.ChatID == "" {
		request.ChatID = request.ConversationID
	}
	if request.ConversationID == "" {
		request.ConversationID = request.ChatID
	}
	request.TopicID = strings.TrimSpace(request.TopicID)
	request.ThreadID = strings.TrimSpace(request.ThreadID)
	request.SessionKey = strings.TrimSpace(request.SessionKey)
	if request.Backend == "openclaw" && request.SessionKey == "" {
		request.SessionKey = request.ThreadID
	}
	if request.Backend == "openclaw" && request.ThreadID == "" {
		request.ThreadID = request.SessionKey
	}
	if request.TargetID == "" {
		if request.Backend == "openclaw" {
			request.TargetID = firstNonEmpty(request.SessionKey, request.ThreadID)
		} else {
			request.TargetID = request.ThreadID
		}
	}
	return request
}

func normalizeBinding(binding Binding) Binding {
	binding.ID = strings.TrimSpace(binding.ID)
	binding.ChannelType = strings.ToLower(strings.TrimSpace(binding.ChannelType))
	binding.Backend = normalizeBackend(binding.Backend)
	binding.TargetID = strings.TrimSpace(binding.TargetID)
	binding.ChannelProfileID = strings.TrimSpace(binding.ChannelProfileID)
	binding.ResourceID = strings.TrimSpace(binding.ResourceID)
	binding.AccountID = strings.TrimSpace(binding.AccountID)
	binding.ConversationType = strings.ToLower(strings.TrimSpace(binding.ConversationType))
	binding.ConversationID = strings.TrimSpace(binding.ConversationID)
	binding.ChatID = strings.TrimSpace(binding.ChatID)
	if binding.ChatID == "" {
		binding.ChatID = binding.ConversationID
	}
	if binding.ConversationID == "" {
		binding.ConversationID = binding.ChatID
	}
	binding.TopicID = strings.TrimSpace(binding.TopicID)
	binding.ThreadID = strings.TrimSpace(binding.ThreadID)
	binding.SessionKey = strings.TrimSpace(binding.SessionKey)
	if binding.Backend == "openclaw" && binding.SessionKey == "" {
		binding.SessionKey = binding.ThreadID
	}
	if binding.Backend == "openclaw" && binding.ThreadID == "" {
		binding.ThreadID = binding.SessionKey
	}
	if binding.TargetID == "" {
		if binding.Backend == "openclaw" {
			binding.TargetID = firstNonEmpty(binding.SessionKey, binding.ThreadID)
		} else {
			binding.TargetID = binding.ThreadID
		}
	}
	binding.CreatedAt = strings.TrimSpace(binding.CreatedAt)
	binding.UpdatedAt = strings.TrimSpace(binding.UpdatedAt)
	return binding
}

func validateRequest(request CreateRequest) error {
	request.Backend = normalizeBackend(request.Backend)
	if request.Backend != "codex" && request.Backend != "openclaw" {
		return errors.New("backend must be codex or openclaw")
	}
	if request.ChannelType != "telegram" && request.ChannelType != "qqbot" && request.ChannelType != "qq" {
		return errors.New("channelType must be telegram, qqbot, or qq")
	}
	if request.AccountID == "" || request.ChatID == "" {
		return errors.New("accountId and conversationId are required")
	}
	if request.Backend == "openclaw" {
		if request.SessionKey == "" && request.ThreadID == "" {
			return errors.New("sessionKey or threadId is required for an OpenClaw binding")
		}
	} else if request.ThreadID == "" {
		return errors.New("threadId is required for a Codex binding")
	}
	if request.ChannelType == "telegram" && request.ConversationType != "default" {
		return errors.New("Telegram conversationType must be default")
	}
	if request.ChannelType == "qqbot" {
		if request.ConversationType != "c2c" && request.ConversationType != "group" {
			return errors.New("QQ Official Bot conversationType must be c2c or group")
		}
		if request.TopicID != "" {
			return errors.New("QQ Official Bot bindings do not support topicId")
		}
	}
	if request.ChannelType == "qq" {
		if request.ConversationType != "private" && request.ConversationType != "group" && request.ConversationType != "legacy" {
			return errors.New("legacy QQ conversationType must be private, group, or legacy")
		}
		if request.TopicID != "" {
			return errors.New("legacy QQ bindings do not support topicId")
		}
	}
	return nil
}

func normalizeBackend(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "codex"
	}
	return value
}

func validateStoredBinding(binding Binding, version int) error {
	if binding.ID == "" || binding.AccountID == "" || binding.ChatID == "" || binding.ThreadID == "" || binding.CreatedAt == "" || binding.UpdatedAt == "" {
		return errors.New("id, accountId, chatId, threadId, createdAt, and updatedAt are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, binding.CreatedAt); err != nil {
		return errors.New("createdAt must be RFC3339")
	}
	if _, err := time.Parse(time.RFC3339Nano, binding.UpdatedAt); err != nil {
		return errors.New("updatedAt must be RFC3339")
	}
	if binding.ChannelType == "qq" && binding.Legacy && !binding.Enabled {
		return nil
	}
	request := CreateRequest{Backend: binding.Backend, TargetID: binding.TargetID, ChannelType: binding.ChannelType, ChannelProfileID: binding.ChannelProfileID, ResourceID: binding.ResourceID, AccountID: binding.AccountID, ConversationType: binding.ConversationType, ConversationID: binding.ConversationID, ChatID: binding.ChatID, TopicID: binding.TopicID, ThreadID: binding.ThreadID, SessionKey: binding.SessionKey}
	return validateRequest(request)
}

// FindProfileAddress resolves a binding against the canonical physical
// resource.  Unlike FindAddress, it intentionally does not use a provider
// bot ID, because one physical bot can serve both Codex and OpenClaw.
func (r *Repository) FindProfileAddress(channelType, resourceID string, address ...string) (Binding, bool) {
	conversationType, chatID, topicID, ok := addressParts(channelType, address)
	if !ok {
		return Binding{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := addressKey(channelType, strings.TrimSpace(resourceID), conversationType, chatID, topicID)
	for _, binding := range r.items {
		if bindingAddressKey(binding) == key {
			return binding, true
		}
	}
	return Binding{}, false
}

func (r *Repository) DeleteProfileAddress(channelType, resourceID string, address ...string) (Binding, error) {
	conversationType, chatID, topicID, ok := addressParts(channelType, address)
	if !ok {
		return Binding{}, ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := addressKey(channelType, strings.TrimSpace(resourceID), conversationType, chatID, topicID)
	for id, binding := range r.items {
		if bindingAddressKey(binding) != key {
			continue
		}
		delete(r.items, id)
		if err := r.saveLocked(); err != nil {
			r.items[id] = binding
			return Binding{}, err
		}
		return binding, nil
	}
	return Binding{}, ErrNotFound
}

func (r *Repository) ListChannelProfile(channelType, resourceID string) []Binding {
	r.mu.RLock()
	defer r.mu.RUnlock()
	channelType, resourceID = strings.ToLower(strings.TrimSpace(channelType)), strings.TrimSpace(resourceID)
	result := []Binding{}
	for _, binding := range r.items {
		if strings.EqualFold(binding.ChannelType, channelType) && bindingAddressOwner(binding) == resourceID {
			result = append(result, binding)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result
}

func (r *Repository) CountChannelProfile(channelType, resourceID string) int {
	return len(r.ListChannelProfile(channelType, resourceID))
}

func addressParts(channelType string, address []string) (conversationType, chatID, topicID string, ok bool) {
	switch len(address) {
	case 2:
		conversationType = "default"
		if !strings.EqualFold(strings.TrimSpace(channelType), "telegram") {
			conversationType = "legacy"
		}
		chatID, topicID = address[0], address[1]
	case 3:
		conversationType, chatID, topicID = address[0], address[1], address[2]
	default:
		return "", "", "", false
	}
	return strings.ToLower(strings.TrimSpace(conversationType)), strings.TrimSpace(chatID), strings.TrimSpace(topicID), true
}

func newID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("binding-%d", time.Now().UnixNano())
	}
	return "binding-" + hex.EncodeToString(buffer)
}

func defaultProfileID(channelType string) string {
	switch strings.ToLower(strings.TrimSpace(channelType)) {
	case "telegram":
		return "telegram-default"
	case "qqbot", "qq":
		return "qq-default"
	default:
		return "channel-default"
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
