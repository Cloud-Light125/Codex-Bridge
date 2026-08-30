package sessionregistry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Metadata is the backend-facing information used to assign or refresh a
// stable number. SessionKey is opaque and must never be interpreted as a
// Codex Thread ID.
type Metadata struct {
	SessionKey string
	Title      string
	CreatedAt  string
	LastSeenAt string
	Status     string
}

type Record struct {
	SessionKey string `json:"sessionKey"`
	Number     int    `json:"number"`
	Title      string `json:"title"`
	CreatedAt  string `json:"createdAt"`
	LastSeenAt string `json:"lastSeenAt"`
	Status     string `json:"status"`
}

type diskModel struct {
	Version    int      `json:"version"`
	NextNumber int      `json:"nextNumber"`
	Sessions   []Record `json:"sessions"`
}

type Registry struct {
	mu         sync.RWMutex
	path       string
	nextNumber int
	byKey      map[string]Record
	byNumber   map[int]string
}

func New(path string) (*Registry, error) {
	r := &Registry{path: strings.TrimSpace(path), nextNumber: 1, byKey: map[string]Record{}, byNumber: map[int]string{}}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func NewInMemory() *Registry {
	r, _ := New("")
	return r
}

func (r *Registry) EnsureBatch(values []Metadata) ([]Record, error) {
	values = append([]Metadata(nil), values...)
	sort.SliceStable(values, func(i, j int) bool {
		a, b := parseTime(values[i].CreatedAt), parseTime(values[j].CreatedAt)
		if a.Equal(b) {
			return strings.TrimSpace(values[i].SessionKey) < strings.TrimSpace(values[j].SessionKey)
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
		value.SessionKey = strings.TrimSpace(value.SessionKey)
		if value.SessionKey == "" {
			continue
		}
		record, ok := r.byKey[value.SessionKey]
		if !ok {
			record = Record{SessionKey: value.SessionKey, Number: r.nextNumber, CreatedAt: first(value.CreatedAt, now)}
			r.nextNumber++
			r.byNumber[record.Number] = record.SessionKey
			changed = true
		}
		lastSeen := first(value.LastSeenAt, now)
		title := strings.TrimSpace(value.Title)
		status := strings.TrimSpace(value.Status)
		if record.Title != title || record.LastSeenAt != lastSeen || record.Status != status {
			record.Title, record.LastSeenAt, record.Status = title, lastSeen, status
			changed = true
		}
		r.byKey[record.SessionKey] = record
	}
	if changed {
		if err := r.saveLocked(); err != nil {
			return nil, err
		}
	}
	result := make([]Record, 0, len(values))
	for _, value := range values {
		if record, ok := r.byKey[strings.TrimSpace(value.SessionKey)]; ok {
			result = append(result, record)
		}
	}
	return result, nil
}

func (r *Registry) Ensure(value Metadata) (Record, error) {
	values, err := r.EnsureBatch([]Metadata{value})
	if err != nil {
		return Record{}, err
	}
	if len(values) == 0 {
		return Record{}, errors.New("sessionKey is required")
	}
	return values[0], nil
}

func (r *Registry) BySessionKey(key string) (Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.byKey[strings.TrimSpace(key)]
	return value, ok
}

func (r *Registry) ByNumber(number int) (Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.byNumber[number]
	if !ok {
		return Record{}, false
	}
	return r.byKey[key], true
}

func (r *Registry) List() []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Record, 0, len(r.byKey))
	for _, value := range r.byKey {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Number < result[j].Number })
	return result
}

// ParsePrefix recognizes #n, [n], and the historical bare n prefix. An
// explicit prefix is considered recognized even when its number is unknown,
// so callers can return a useful routing error instead of treating it as a
// new unnumbered message.
func (r *Registry) ParsePrefix(text string) (record Record, content string, recognized bool, err error) {
	trimmed := strings.TrimSpace(text)
	numberText, rest, explicit := "", "", false
	switch {
	case strings.HasPrefix(trimmed, "#"):
		explicit = true
		numberText, rest = takeNumber(trimmed[1:])
	case strings.HasPrefix(trimmed, "["):
		if close := strings.Index(trimmed, "]"); close > 1 {
			explicit = true
			numberText, rest = strings.TrimSpace(trimmed[1:close]), strings.TrimSpace(trimmed[close+1:])
		}
	default:
		numberText, rest = takeNumber(trimmed)
	}
	if numberText == "" {
		return Record{}, text, explicit, nil
	}
	number, parseErr := strconv.Atoi(numberText)
	if parseErr != nil || number < 1 {
		if explicit {
			return Record{}, text, true, fmt.Errorf("无效的会话编号 #%s", numberText)
		}
		return Record{}, text, false, nil
	}
	value, ok := r.ByNumber(number)
	if !ok {
		if explicit {
			return Record{}, rest, true, fmt.Errorf("OpenClaw 会话编号 #%d 不存在。发送 /threads 查看可用编号。", number)
		}
		return Record{}, text, false, nil
	}
	if strings.TrimSpace(rest) == "" {
		return value, "", true, fmt.Errorf("请在 #%d 后输入消息或命令。", number)
	}
	return value, strings.TrimSpace(rest), true, nil
}

func takeNumber(value string) (string, string) {
	value = strings.TrimLeft(value, " \t")
	index := 0
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
	}
	if index == 0 {
		return "", ""
	}
	if index < len(value) && value[index] != ' ' && value[index] != '\t' && value[index] != '\r' && value[index] != '\n' {
		return "", ""
	}
	return value[:index], strings.TrimSpace(value[index:])
}

func (r *Registry) load() error {
	if r.path == "" {
		return nil
	}
	data, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read OpenClaw session numbers: %w", err)
	}
	var model diskModel
	if err := json.Unmarshal(data, &model); err != nil {
		return fmt.Errorf("decode OpenClaw session numbers: %w", err)
	}
	if model.Version != 1 {
		return fmt.Errorf("decode OpenClaw session numbers: unsupported version %d", model.Version)
	}
	max := 0
	for _, record := range model.Sessions {
		record.SessionKey = strings.TrimSpace(record.SessionKey)
		if record.SessionKey == "" || record.Number < 1 {
			return errors.New("decode OpenClaw session numbers: invalid record")
		}
		if _, exists := r.byKey[record.SessionKey]; exists {
			return fmt.Errorf("decode OpenClaw session numbers: duplicate session %q", record.SessionKey)
		}
		if _, exists := r.byNumber[record.Number]; exists {
			return fmt.Errorf("decode OpenClaw session numbers: duplicate number %d", record.Number)
		}
		r.byKey[record.SessionKey], r.byNumber[record.Number] = record, record.SessionKey
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
	model := diskModel{Version: 1, NextNumber: r.nextNumber, Sessions: make([]Record, 0, len(r.byKey))}
	for _, record := range r.byKey {
		model.Sessions = append(model.Sessions, record)
	}
	sort.Slice(model.Sessions, func(i, j int) bool { return model.Sessions[i].Number < model.Sessions[j].Number })
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
