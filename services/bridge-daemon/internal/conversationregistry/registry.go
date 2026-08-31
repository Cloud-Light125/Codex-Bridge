package conversationregistry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/numberprefix"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/sessionregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
)

const (
	BackendCodex    = "codex"
	BackendOpenClaw = "openclaw"
)

// Metadata is backend-neutral data used to assign or refresh a stable
// conversation number. TargetID is a Codex Thread ID or an OpenClaw
// SessionKey, depending on Backend.
type Metadata struct {
	Backend    string
	TargetID   string
	Title      string
	Summary    string
	CWD        string
	CreatedAt  string
	LastSeenAt string
	Status     string
}

// Record is the only user-facing number mapping used by the daemon. A number
// is unique across both backends; Backend and TargetID are always stored
// together so a caller never has to infer a backend from an opaque ID.
type Record struct {
	Number     int    `json:"number"`
	Backend    string `json:"backend"`
	TargetID   string `json:"targetId"`
	Title      string `json:"title"`
	Summary    string `json:"summary,omitempty"`
	CWD        string `json:"cwd,omitempty"`
	CreatedAt  string `json:"createdAt"`
	LastSeenAt string `json:"lastSeenAt"`
	Status     string `json:"status,omitempty"`
}

type diskModel struct {
	Version       int      `json:"version"`
	NextNumber    int      `json:"nextNumber"`
	Conversations []Record `json:"conversations"`
}

// Store is the backend-neutral surface consumed by Runtime, channels, query,
// mirror, and the OpenClaw adapter. The legacy adapters below implement the
// same surface only for compatibility with older embedders and tests; the
// daemon itself always passes *Registry.
type Store interface {
	EnsureBackend(string, Metadata) (Record, error)
	EnsureBatchBackend(string, []Metadata) ([]Record, error)
	ByTarget(string, string) (Record, bool)
	ByNumber(int) (Record, bool)
	List() []Record
	ListBackend(string) []Record
	ParsePrefix(string) (Record, string, bool, error)
}

type Registry struct {
	mu         sync.RWMutex
	path       string
	nextNumber int
	byTarget   map[string]Record
	byNumber   map[int]Record
}

// New loads the global registry. When the global file does not yet exist,
// legacyPaths are migrated in order: Codex thread-numbers, then OpenClaw
// session-numbers. Legacy Codex numbers are copied verbatim; OpenClaw entries
// are assigned from the first number after the largest existing Codex number.
// If a global file already exists, missing legacy entries are only backfilled;
// the legacy registries are never used as the active number source.
func New(path string, legacyPaths ...string) (*Registry, error) {
	r := &Registry{
		path:       strings.TrimSpace(path),
		nextNumber: 1,
		byTarget:   map[string]Record{},
		byNumber:   map[int]Record{},
	}

	exists, err := fileExists(r.path)
	if err != nil {
		return nil, err
	}
	if exists {
		if err := r.load(); err != nil {
			return nil, err
		}
	}

	if len(legacyPaths) > 0 {
		codexPath := ""
		if len(legacyPaths) > 0 {
			codexPath = legacyPaths[0]
		}
		openClawPath := ""
		if len(legacyPaths) > 1 {
			openClawPath = legacyPaths[1]
		}
		changed, err := r.migrateLegacy(codexPath, openClawPath)
		if err != nil {
			return nil, err
		}
		if changed {
			r.mu.Lock()
			err = r.saveLocked()
			r.mu.Unlock()
			if err != nil {
				return nil, fmt.Errorf("persist conversation numbers: %w", err)
			}
		}
	}
	return r, nil
}

func NewInMemory() *Registry {
	r, _ := New("")
	return r
}

// NextNumber returns the next number that will be allocated. It is useful for
// diagnostics and migration tests; allocation itself remains locked inside
// EnsureBatch.
func (r *Registry) NextNumber() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextNumber
}

func (r *Registry) Ensure(value Metadata) (Record, error) {
	values, err := r.EnsureBatch([]Metadata{value})
	if err != nil {
		return Record{}, err
	}
	if len(values) == 0 {
		return Record{}, errors.New("backend and targetId are required")
	}
	return values[0], nil
}

func (r *Registry) EnsureBatch(values []Metadata) ([]Record, error) {
	values = append([]Metadata(nil), values...)
	for index := range values {
		values[index].Backend = normalizeBackend(values[index].Backend)
		values[index].TargetID = strings.TrimSpace(values[index].TargetID)
	}
	sort.SliceStable(values, func(i, j int) bool {
		a, b := parseTime(values[i].CreatedAt), parseTime(values[j].CreatedAt)
		if a.Equal(b) {
			left := strings.Join([]string{values[i].Backend, values[i].TargetID}, "\x00")
			right := strings.Join([]string{values[j].Backend, values[j].TargetID}, "\x00")
			return left < right
		}
		if a.IsZero() {
			return false
		}
		if b.IsZero() {
			return true
		}
		return a.Before(b)
	})

	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, value := range values {
		if !validBackend(value.Backend) || value.TargetID == "" {
			continue
		}
		key := targetKey(value.Backend, value.TargetID)
		record, ok := r.byTarget[key]
		if !ok {
			number := r.nextAvailableLocked()
			record = Record{Number: number, Backend: value.Backend, TargetID: value.TargetID, CreatedAt: first(value.CreatedAt, now)}
			r.nextNumber = number + 1
			r.byNumber[record.Number] = record
			changed = true
		}
		updated := record
		updated.Backend = value.Backend
		updated.TargetID = value.TargetID
		updated.Title = strings.TrimSpace(value.Title)
		updated.Summary = strings.TrimSpace(value.Summary)
		updated.CWD = strings.TrimSpace(value.CWD)
		updated.Status = strings.TrimSpace(value.Status)
		updated.LastSeenAt = first(value.LastSeenAt, now)
		if updated.CreatedAt == "" {
			updated.CreatedAt = first(value.CreatedAt, now)
		}
		if updated != record {
			changed = true
			r.byNumber[updated.Number] = updated
		}
		r.byTarget[key] = updated
	}
	if changed && r.path != "" {
		if err := r.saveLocked(); err != nil {
			return nil, err
		}
	}

	result := make([]Record, 0, len(values))
	for _, value := range values {
		key := targetKey(value.Backend, value.TargetID)
		if record, ok := r.byTarget[key]; ok {
			result = append(result, record)
		}
	}
	return result, nil
}

func (r *Registry) EnsureBackend(backend string, value Metadata) (Record, error) {
	value.Backend = backend
	return r.Ensure(value)
}

func (r *Registry) EnsureBatchBackend(backend string, values []Metadata) ([]Record, error) {
	for index := range values {
		values[index].Backend = backend
	}
	return r.EnsureBatch(values)
}

func (r *Registry) ByTarget(backend, targetID string) (Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.byTarget[targetKey(backend, targetID)]
	return record, ok
}

func (r *Registry) ByTargetID(backend, targetID string) (Record, bool) {
	return r.ByTarget(backend, targetID)
}

func (r *Registry) ByNumber(number int) (Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.byNumber[number]
	return record, ok
}

func (r *Registry) List() []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.listLocked()
}

func (r *Registry) listLocked() []Record {
	result := make([]Record, 0, len(r.byNumber))
	for _, record := range r.byNumber {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Number < result[j].Number })
	return result
}

func (r *Registry) ListBackend(backend string) []Record {
	backend = normalizeBackend(backend)
	result := make([]Record, 0)
	for _, record := range r.List() {
		if record.Backend == backend {
			result = append(result, record)
		}
	}
	return result
}

// ParsePrefix resolves #n, [n], and the historical bare n prefix against the
// global registry. Explicit prefixes always report an error for an unknown
// number; an unknown bare number remains ordinary text.
func (r *Registry) ParsePrefix(text string) (record Record, content string, recognized bool, err error) {
	prefix, parsed, parseErr := numberprefix.Parse(text)
	if parseErr != nil {
		return Record{}, text, parsed, parseErr
	}
	if !parsed {
		return Record{}, text, false, nil
	}
	if prefix.Number < 1 {
		if prefix.Explicit {
			return Record{}, text, true, fmt.Errorf("无效的聊天编号")
		}
		return Record{}, text, false, nil
	}
	record, ok := r.ByNumber(prefix.Number)
	if !ok {
		if prefix.Explicit {
			return Record{}, prefix.Content, true, fmt.Errorf("聊天编号 #%d 不存在。发送 /threads 查看可用编号。", prefix.Number)
		}
		return Record{}, text, false, nil
	}
	if strings.TrimSpace(prefix.Content) == "" {
		return record, "", true, fmt.Errorf("请在 #%d 后输入消息或命令。", prefix.Number)
	}
	return record, prefix.Content, true, nil
}

func (r *Registry) nextAvailableLocked() int {
	if r.nextNumber < 1 {
		r.nextNumber = 1
	}
	for {
		if _, exists := r.byNumber[r.nextNumber]; !exists {
			return r.nextNumber
		}
		r.nextNumber++
	}
}

func (r *Registry) load() error {
	if r.path == "" {
		return nil
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		return fmt.Errorf("read conversation numbers: %w", err)
	}
	var model diskModel
	if err := json.Unmarshal(data, &model); err != nil {
		return fmt.Errorf("decode conversation numbers: %w", err)
	}
	if model.Version != 1 {
		return fmt.Errorf("decode conversation numbers: unsupported version %d", model.Version)
	}
	max := 0
	for _, record := range model.Conversations {
		record.Backend = normalizeBackend(record.Backend)
		record.TargetID = strings.TrimSpace(record.TargetID)
		if !validBackend(record.Backend) || record.TargetID == "" || record.Number < 1 {
			return errors.New("decode conversation numbers: invalid record")
		}
		key := targetKey(record.Backend, record.TargetID)
		if _, exists := r.byTarget[key]; exists {
			return fmt.Errorf("decode conversation numbers: duplicate target %q/%q", record.Backend, record.TargetID)
		}
		if _, exists := r.byNumber[record.Number]; exists {
			return fmt.Errorf("decode conversation numbers: duplicate number %d", record.Number)
		}
		r.byTarget[key], r.byNumber[record.Number] = record, record
		if record.Number > max {
			max = record.Number
		}
	}
	r.nextNumber = model.NextNumber
	if r.nextNumber <= max {
		r.nextNumber = max + 1
	}
	if r.nextNumber < 1 {
		r.nextNumber = 1
	}
	return nil
}

func (r *Registry) saveLocked() error {
	if r.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	model := diskModel{Version: 1, NextNumber: r.nextNumber, Conversations: r.listLocked()}
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
		_ = os.Remove(temporary)
		return err
	}
	_ = os.Remove(backup)
	return nil
}

func (r *Registry) migrateLegacy(codexPath, openClawPath string) (bool, error) {
	changed := false
	if strings.TrimSpace(codexPath) != "" {
		legacy, err := threadregistry.New(codexPath)
		if err != nil {
			return false, fmt.Errorf("read legacy Codex numbers: %w", err)
		}
		for _, item := range legacy.List() {
			record := Record{
				Number: item.Number, Backend: BackendCodex, TargetID: item.ThreadID,
				Title: item.Title, CWD: item.CWD, CreatedAt: item.CreatedAt, LastSeenAt: item.LastSeenAt,
			}
			added, err := r.addMigratedRecord(record)
			if err != nil {
				return false, fmt.Errorf("migrate Codex number #%d: %w", item.Number, err)
			}
			changed = changed || added
		}
	}
	if strings.TrimSpace(openClawPath) != "" {
		legacy, err := sessionregistry.New(openClawPath)
		if err != nil {
			return false, fmt.Errorf("read legacy OpenClaw numbers: %w", err)
		}
		items := legacy.List()
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].Number == items[j].Number {
				return items[i].SessionKey < items[j].SessionKey
			}
			return items[i].Number < items[j].Number
		})
		for _, item := range items {
			number := r.nextAvailableLockedForMigration()
			record := Record{
				Number: number, Backend: BackendOpenClaw, TargetID: item.SessionKey,
				Title: item.Title, CreatedAt: item.CreatedAt, LastSeenAt: item.LastSeenAt, Status: item.Status,
			}
			added, err := r.addMigratedRecord(record)
			if err != nil {
				return false, fmt.Errorf("migrate OpenClaw session %q: %w", item.SessionKey, err)
			}
			changed = changed || added
		}
	}
	return changed, nil
}

func (r *Registry) addMigratedRecord(record Record) (bool, error) {
	record.Backend = normalizeBackend(record.Backend)
	record.TargetID = strings.TrimSpace(record.TargetID)
	if !validBackend(record.Backend) || record.TargetID == "" {
		return false, errors.New("backend and targetId are required")
	}
	if record.Number < 1 {
		return false, errors.New("number must be positive")
	}
	key := targetKey(record.Backend, record.TargetID)
	if _, ok := r.byTarget[key]; ok {
		// The global file is authoritative once it exists. This also makes a
		// repeated startup migration tolerant of stale legacy files without
		// changing an already assigned global number.
		return false, nil
	}
	if existing, ok := r.byNumber[record.Number]; ok {
		return false, fmt.Errorf("number #%d is already assigned to %s/%s", record.Number, existing.Backend, existing.TargetID)
	}
	r.byTarget[key], r.byNumber[record.Number] = record, record
	if record.Number >= r.nextNumber {
		r.nextNumber = record.Number + 1
	}
	return true, nil
}

func (r *Registry) nextAvailableLockedForMigration() int {
	if r.nextNumber < 1 {
		r.nextNumber = 1
	}
	for {
		if _, exists := r.byNumber[r.nextNumber]; !exists {
			return r.nextNumber
		}
		r.nextNumber++
	}
}

func fileExists(path string) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, nil
	}
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat conversation numbers: %w", err)
	}
	return true, nil
}

func targetKey(backend, targetID string) string {
	return normalizeBackend(backend) + "\x00" + strings.TrimSpace(targetID)
}

func normalizeBackend(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func validBackend(value string) bool { return value == BackendCodex || value == BackendOpenClaw }

func parseTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	return parsed
}

func first(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// ForCodex and ForOpenClaw adapt the old per-backend registries so existing
// callers can be upgraded independently. Production code should pass the
// global *Registry; these helpers are intentionally the only compatibility
// path that can still see a legacy registry object.
func ForCodex(value any) Store {
	switch typed := value.(type) {
	case *Registry:
		if typed != nil {
			return typed
		}
	case *threadregistry.Registry:
		if typed != nil {
			return legacyCodexStore{registry: typed}
		}
	case Store:
		return typed
	}
	return nil
}

func ForOpenClaw(value any) Store {
	switch typed := value.(type) {
	case *Registry:
		if typed != nil {
			return typed
		}
	case *sessionregistry.Registry:
		if typed != nil {
			return legacyOpenClawStore{registry: typed}
		}
	case Store:
		return typed
	}
	return nil
}

func ForAny(value any) Store {
	switch typed := value.(type) {
	case *Registry:
		if typed != nil {
			return typed
		}
	case *threadregistry.Registry:
		if typed != nil {
			return legacyCodexStore{registry: typed}
		}
	case *sessionregistry.Registry:
		if typed != nil {
			return legacyOpenClawStore{registry: typed}
		}
	case Store:
		return typed
	}
	return nil
}

type legacyCodexStore struct{ registry *threadregistry.Registry }

func (s legacyCodexStore) EnsureBackend(backend string, value Metadata) (Record, error) {
	if normalizeBackend(backend) != BackendCodex {
		return Record{}, errors.New("legacy Codex registry cannot store OpenClaw sessions")
	}
	item, err := s.registry.Ensure(threadregistry.Metadata{ThreadID: value.TargetID, Title: value.Title, CWD: value.CWD, CreatedAt: value.CreatedAt, LastSeenAt: value.LastSeenAt})
	return codexRecord(item), err
}

func (s legacyCodexStore) EnsureBatchBackend(backend string, values []Metadata) ([]Record, error) {
	if normalizeBackend(backend) != BackendCodex {
		return nil, errors.New("legacy Codex registry cannot store OpenClaw sessions")
	}
	items := make([]threadregistry.Metadata, 0, len(values))
	for _, value := range values {
		items = append(items, threadregistry.Metadata{ThreadID: value.TargetID, Title: value.Title, CWD: value.CWD, CreatedAt: value.CreatedAt, LastSeenAt: value.LastSeenAt})
	}
	records, err := s.registry.EnsureBatch(items)
	result := make([]Record, 0, len(records))
	for _, item := range records {
		result = append(result, codexRecord(item))
	}
	return result, err
}

func (s legacyCodexStore) ByTarget(backend, targetID string) (Record, bool) {
	if normalizeBackend(backend) != BackendCodex {
		return Record{}, false
	}
	item, ok := s.registry.ByThreadID(targetID)
	return codexRecord(item), ok
}

func (s legacyCodexStore) ByNumber(number int) (Record, bool) {
	item, ok := s.registry.ByNumber(number)
	return codexRecord(item), ok
}

func (s legacyCodexStore) List() []Record {
	items := s.registry.List()
	result := make([]Record, 0, len(items))
	for _, item := range items {
		result = append(result, codexRecord(item))
	}
	return result
}

func (s legacyCodexStore) ListBackend(backend string) []Record {
	if normalizeBackend(backend) != BackendCodex {
		return nil
	}
	return s.List()
}

func (s legacyCodexStore) ParsePrefix(text string) (Record, string, bool, error) {
	item, content, recognized, err := s.registry.ParsePrefix(text)
	return codexRecord(item), content, recognized, err
}

type legacyOpenClawStore struct{ registry *sessionregistry.Registry }

func (s legacyOpenClawStore) EnsureBackend(backend string, value Metadata) (Record, error) {
	if normalizeBackend(backend) != BackendOpenClaw {
		return Record{}, errors.New("legacy OpenClaw registry cannot store Codex threads")
	}
	item, err := s.registry.Ensure(sessionregistry.Metadata{SessionKey: value.TargetID, Title: value.Title, CreatedAt: value.CreatedAt, LastSeenAt: value.LastSeenAt, Status: value.Status})
	return openClawRecord(item), err
}

func (s legacyOpenClawStore) EnsureBatchBackend(backend string, values []Metadata) ([]Record, error) {
	if normalizeBackend(backend) != BackendOpenClaw {
		return nil, errors.New("legacy OpenClaw registry cannot store Codex threads")
	}
	items := make([]sessionregistry.Metadata, 0, len(values))
	for _, value := range values {
		items = append(items, sessionregistry.Metadata{SessionKey: value.TargetID, Title: value.Title, CreatedAt: value.CreatedAt, LastSeenAt: value.LastSeenAt, Status: value.Status})
	}
	records, err := s.registry.EnsureBatch(items)
	result := make([]Record, 0, len(records))
	for _, item := range records {
		result = append(result, openClawRecord(item))
	}
	return result, err
}

func (s legacyOpenClawStore) ByTarget(backend, targetID string) (Record, bool) {
	if normalizeBackend(backend) != BackendOpenClaw {
		return Record{}, false
	}
	item, ok := s.registry.BySessionKey(targetID)
	return openClawRecord(item), ok
}

func (s legacyOpenClawStore) ByNumber(number int) (Record, bool) {
	item, ok := s.registry.ByNumber(number)
	return openClawRecord(item), ok
}

func (s legacyOpenClawStore) List() []Record {
	items := s.registry.List()
	result := make([]Record, 0, len(items))
	for _, item := range items {
		result = append(result, openClawRecord(item))
	}
	return result
}

func (s legacyOpenClawStore) ListBackend(backend string) []Record {
	if normalizeBackend(backend) != BackendOpenClaw {
		return nil
	}
	return s.List()
}

func (s legacyOpenClawStore) ParsePrefix(text string) (Record, string, bool, error) {
	item, content, recognized, err := s.registry.ParsePrefix(text)
	return openClawRecord(item), content, recognized, err
}

func codexRecord(item threadregistry.Record) Record {
	return Record{Number: item.Number, Backend: BackendCodex, TargetID: item.ThreadID, Title: item.Title, CWD: item.CWD, CreatedAt: item.CreatedAt, LastSeenAt: item.LastSeenAt}
}

func openClawRecord(item sessionregistry.Record) Record {
	return Record{Number: item.Number, Backend: BackendOpenClaw, TargetID: item.SessionKey, Title: item.Title, CreatedAt: item.CreatedAt, LastSeenAt: item.LastSeenAt, Status: item.Status}
}
