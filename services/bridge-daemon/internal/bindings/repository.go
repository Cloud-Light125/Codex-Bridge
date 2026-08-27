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
	ID               string `json:"id"`
	Backend          string `json:"backend,omitempty"`
	ChannelType      string `json:"channelType"`
	AccountID        string `json:"accountId"`
	ConversationType string `json:"conversationType"`
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
	ChannelType      string `json:"channelType"`
	AccountID        string `json:"accountId"`
	ConversationType string `json:"conversationType"`
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
	address := addressKey(request.ChannelType, request.AccountID, request.ConversationType, request.ChatID, request.TopicID)
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
		ID: newID(), Backend: request.Backend, ChannelType: request.ChannelType, AccountID: request.AccountID, ConversationType: request.ConversationType,
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
		if bindingAddressKey(binding) == key {
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
	key := addressKey(request.ChannelType, request.AccountID, request.ConversationType, request.ChatID, request.TopicID)
	var previous *Binding
	for id, existing := range r.items {
		if bindingAddressKey(existing) != key {
			continue
		}
		copy := existing
		previous = &copy
		existing.ThreadID = request.ThreadID
		existing.Backend = request.Backend
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
	binding := Binding{ID: newID(), Backend: request.Backend, ChannelType: request.ChannelType, AccountID: request.AccountID, ConversationType: request.ConversationType, ChatID: request.ChatID, TopicID: request.TopicID, ThreadID: request.ThreadID, SessionKey: request.SessionKey, Enabled: enabled, CreatedAt: now, UpdatedAt: now}
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
	if model.Version != 1 && model.Version != 2 && model.Version != 3 {
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
	if model.Version <= 2 {
		if err := r.saveLocked(); err != nil {
			return fmt.Errorf("migrate bindings to version 3: %w", err)
		}
	}
	return nil
}

func (r *Repository) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	model := diskModel{Version: 3, Bindings: make([]Binding, 0, len(r.items))}
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
	return addressKey(binding.ChannelType, binding.AccountID, binding.ConversationType, binding.ChatID, binding.TopicID)
}

func normalizeRequest(request CreateRequest) CreateRequest {
	request.Backend = normalizeBackend(request.Backend)
	request.ChannelType = strings.ToLower(strings.TrimSpace(request.ChannelType))
	request.AccountID = strings.TrimSpace(request.AccountID)
	request.ConversationType = strings.ToLower(strings.TrimSpace(request.ConversationType))
	if request.ChannelType == "telegram" && request.ConversationType == "" {
		request.ConversationType = "default"
	}
	request.ChatID = strings.TrimSpace(request.ChatID)
	request.TopicID = strings.TrimSpace(request.TopicID)
	request.ThreadID = strings.TrimSpace(request.ThreadID)
	request.SessionKey = strings.TrimSpace(request.SessionKey)
	if request.Backend == "openclaw" && request.SessionKey == "" {
		request.SessionKey = request.ThreadID
	}
	if request.Backend == "openclaw" && request.ThreadID == "" {
		request.ThreadID = request.SessionKey
	}
	return request
}

func normalizeBinding(binding Binding) Binding {
	binding.ID = strings.TrimSpace(binding.ID)
	binding.ChannelType = strings.ToLower(strings.TrimSpace(binding.ChannelType))
	binding.Backend = normalizeBackend(binding.Backend)
	binding.AccountID = strings.TrimSpace(binding.AccountID)
	binding.ConversationType = strings.ToLower(strings.TrimSpace(binding.ConversationType))
	binding.ChatID = strings.TrimSpace(binding.ChatID)
	binding.TopicID = strings.TrimSpace(binding.TopicID)
	binding.ThreadID = strings.TrimSpace(binding.ThreadID)
	binding.SessionKey = strings.TrimSpace(binding.SessionKey)
	if binding.Backend == "openclaw" && binding.SessionKey == "" {
		binding.SessionKey = binding.ThreadID
	}
	if binding.Backend == "openclaw" && binding.ThreadID == "" {
		binding.ThreadID = binding.SessionKey
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
		return errors.New("accountId and chatId are required")
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
	request := CreateRequest{Backend: binding.Backend, ChannelType: binding.ChannelType, AccountID: binding.AccountID, ConversationType: binding.ConversationType, ChatID: binding.ChatID, TopicID: binding.TopicID, ThreadID: binding.ThreadID, SessionKey: binding.SessionKey}
	return validateRequest(request)
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
