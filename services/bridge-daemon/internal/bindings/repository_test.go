package bindings

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryRejectsDuplicateChannelAddress(t *testing.T) {
	repository, err := NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	request := CreateRequest{ChannelType: "telegram", AccountID: "bot-1", ChatID: "chat-1", TopicID: "topic-1", ThreadID: "thread-1"}
	if _, err := repository.Create(request); err != nil {
		t.Fatalf("create first binding: %v", err)
	}
	request.ThreadID = "thread-2"
	if _, err := repository.Create(request); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestRepositoryMigratesV1TelegramWithoutLosingFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	original := Binding{
		ID: "binding-old", ChannelType: "telegram", AccountID: "bot-1", ChatID: "chat-1",
		TopicID: "topic-1", ThreadID: "thread-1", Enabled: true,
		CreatedAt: "2026-01-02T03:04:05.123456789Z", UpdatedAt: "2026-02-03T04:05:06.987654321Z",
	}
	writeDiskModel(t, path, diskModel{Version: 1, Bindings: []Binding{original}})
	repository, err := NewRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	found, ok := repository.FindAddress("telegram", "bot-1", "chat-1", "topic-1")
	if !ok {
		t.Fatal("migrated Telegram binding was not found")
	}
	if found.ID != original.ID || found.ThreadID != original.ThreadID || found.CreatedAt != original.CreatedAt || found.UpdatedAt != original.UpdatedAt || found.ConversationType != "default" || !found.Enabled {
		t.Fatalf("migration changed binding: %#v", found)
	}
	var migrated diskModel
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &migrated); err != nil {
		t.Fatal(err)
	}
	if migrated.Version != 4 || len(migrated.Bindings) != 1 || migrated.Bindings[0].ConversationType != "default" ||
		migrated.Bindings[0].ChannelProfileID != "telegram-default" || migrated.Bindings[0].ResourceID != "telegram-default" ||
		migrated.Bindings[0].ConversationID != "chat-1" || migrated.Bindings[0].TargetID != "thread-1" {
		t.Fatalf("unexpected migrated disk model: %#v", migrated)
	}
}

func TestRepositoryMigratesV1QQFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	writeDiskModel(t, path, diskModel{Version: 1, Bindings: []Binding{{
		ID: "binding-qq", ChannelType: "qq", AccountID: "10001", ChatID: "20002",
		ThreadID: "thread-1", Enabled: true, CreatedAt: "2026-01-02T03:04:05Z", UpdatedAt: "2026-01-02T03:04:05Z",
	}}})
	repository, err := NewRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	found, ok := repository.FindAddress("qq", "10001", "20002", "")
	if !ok || found.ConversationType != "legacy" || found.Enabled || !found.Legacy {
		t.Fatalf("legacy QQ binding was not preserved fail-closed: %#v ok=%t", found, ok)
	}
	if _, err := NewRepository(path); err != nil {
		t.Fatalf("migrated v3 legacy record could not be reloaded: %v", err)
	}
}

func TestRepositorySeparatesQQBotC2CAndGroupWithSameOpenID(t *testing.T) {
	repository, err := NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	private, err := repository.Create(CreateRequest{ChannelType: "qqbot", AccountID: "10001", ConversationType: "c2c", ChatID: "openid-shared", ThreadID: "thread-private"})
	if err != nil {
		t.Fatal(err)
	}
	group, err := repository.Create(CreateRequest{ChannelType: "qqbot", AccountID: "10001", ConversationType: "group", ChatID: "openid-shared", ThreadID: "thread-group"})
	if err != nil {
		t.Fatal(err)
	}
	if private.ID == group.ID {
		t.Fatal("private and group bindings received the same ID")
	}
	if found, ok := repository.FindAddress("qqbot", "10001", "c2c", "openid-shared", ""); !ok || found.ThreadID != "thread-private" {
		t.Fatalf("unexpected private binding: %#v ok=%t", found, ok)
	}
	if found, ok := repository.FindAddress("qqbot", "10001", "group", "openid-shared", ""); !ok || found.ThreadID != "thread-group" {
		t.Fatalf("unexpected group binding: %#v ok=%t", found, ok)
	}
}

func TestRepositoryRejectsUnknownDiskVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	writeDiskModel(t, path, diskModel{Version: 99})
	if _, err := NewRepository(path); err == nil {
		t.Fatal("expected unknown disk version to fail")
	}
}

func TestRepositoryRejectsDuplicateStoredAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	base := Binding{
		ChannelType: "qq", AccountID: "10001", ConversationType: "private", ChatID: "20002",
		ThreadID: "thread-1", Enabled: true, CreatedAt: "2026-01-02T03:04:05Z", UpdatedAt: "2026-01-02T03:04:05Z",
	}
	first, second := base, base
	first.ID, second.ID = "binding-1", "binding-2"
	writeDiskModel(t, path, diskModel{Version: 2, Bindings: []Binding{first, second}})
	if _, err := NewRepository(path); err == nil {
		t.Fatal("expected duplicate stored address to fail")
	}
}

func writeDiskModel(t *testing.T, path string, model diskModel) {
	t.Helper()
	data, err := json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryUpsertReplacesAddressAtomically(t *testing.T) {
	repository, err := NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	request := CreateRequest{ChannelType: "telegram", AccountID: "bot-1", ChatID: "chat-1", TopicID: "topic-1", ThreadID: "thread-1"}
	first, previous, err := repository.UpsertAddress(request)
	if err != nil || previous != nil {
		t.Fatalf("first upsert: binding=%#v previous=%#v err=%v", first, previous, err)
	}
	request.ThreadID = "thread-2"
	replaced, previous, err := repository.UpsertAddress(request)
	if err != nil || previous == nil || previous.ThreadID != "thread-1" || replaced.ID != first.ID {
		t.Fatalf("replace: binding=%#v previous=%#v err=%v", replaced, previous, err)
	}
	if found, ok := repository.FindAddress("telegram", "bot-1", "chat-1", "topic-1"); !ok || found.ThreadID != "thread-2" {
		t.Fatalf("unexpected address lookup: %#v ok=%t", found, ok)
	}
}

func TestRepositoryPersistsOpenClawSessionBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	repository, err := NewRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := repository.Create(CreateRequest{
		Backend: "openclaw", ChannelType: "telegram", AccountID: "bot-1", ConversationType: "default",
		ChatID: "chat-1", ThreadID: "agent:main:main", SessionKey: "agent:main:main",
	})
	if err != nil {
		t.Fatalf("create OpenClaw binding: %v", err)
	}
	if created.Backend != "openclaw" || created.SessionKey != "agent:main:main" || created.ThreadID != created.SessionKey {
		t.Fatalf("unexpected OpenClaw binding: %#v", created)
	}
	reloaded, err := NewRepository(path)
	if err != nil {
		t.Fatalf("reload OpenClaw binding: %v", err)
	}
	found, ok := reloaded.FindAddress("telegram", "bot-1", "default", "chat-1", "")
	if !ok || found.Backend != "openclaw" || found.SessionKey != "agent:main:main" {
		t.Fatalf("OpenClaw binding was not persisted: %#v ok=%t", found, ok)
	}
}

func TestRepositoryMigratesV3OpenClawBindingWithoutLosingBackendOrTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	writeDiskModel(t, path, diskModel{Version: 3, Bindings: []Binding{{
		ID: "binding-openclaw-old", Backend: "openclaw", TargetID: "agent:main:main", SessionKey: "agent:main:main",
		ChannelType: "telegram", AccountID: "bot-1", ConversationType: "default", ChatID: "chat-1", Enabled: true,
		CreatedAt: "2026-01-02T03:04:05Z", UpdatedAt: "2026-01-02T03:04:05Z",
	}}})
	repository, err := NewRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	found, ok := repository.FindAddress("telegram", "bot-1", "default", "chat-1", "")
	if !ok || found.Backend != "openclaw" || found.TargetID != "agent:main:main" || found.SessionKey != "agent:main:main" || found.ThreadID != "agent:main:main" {
		t.Fatalf("OpenClaw migration lost explicit backend/target: %#v ok=%t", found, ok)
	}
	if _, err := NewRepository(path); err != nil {
		t.Fatalf("migrated OpenClaw binding could not be reloaded: %v", err)
	}
}

func TestRepositoryKeepsBackendAndTargetExplicitAcrossProfiles(t *testing.T) {
	repository, err := NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}

	// Two physical bot resources may legitimately receive the same platform
	// conversation ID.  Each binding keeps the backend and target explicit;
	// no caller needs to infer a backend from a target string.
	codex, err := repository.Create(CreateRequest{
		Backend: "codex", TargetID: "thread-codex", ChannelType: "telegram", ChannelProfileID: "telegram-1", ResourceID: "telegram-resource-1",
		AccountID: "bot-a", ConversationType: "default", ConversationID: "chat-1", ChatID: "chat-1", ThreadID: "thread-codex",
	})
	if err != nil {
		t.Fatalf("create Codex binding: %v", err)
	}
	openClaw, err := repository.Create(CreateRequest{
		Backend: "openclaw", TargetID: "agent:main:session-1", ChannelType: "telegram", ChannelProfileID: "telegram-2", ResourceID: "telegram-resource-2",
		AccountID: "bot-b", ConversationType: "default", ConversationID: "chat-1", ChatID: "chat-1", ThreadID: "agent:main:session-1", SessionKey: "agent:main:session-1",
	})
	if err != nil {
		t.Fatalf("create OpenClaw binding: %v", err)
	}

	if found, ok := repository.FindProfileAddress("telegram", "telegram-resource-1", "default", "chat-1", ""); !ok || found.ID != codex.ID || found.Backend != "codex" || found.TargetID != "thread-codex" {
		t.Fatalf("Codex lookup crossed into another profile/backend: %#v ok=%t", found, ok)
	}
	if found, ok := repository.FindProfileAddress("telegram", "telegram-resource-2", "default", "chat-1", ""); !ok || found.ID != openClaw.ID || found.Backend != "openclaw" || found.TargetID != "agent:main:session-1" {
		t.Fatalf("OpenClaw lookup crossed into another profile/backend: %#v ok=%t", found, ok)
	}

	// A shared physical bot has one current binding for a conversation.  This
	// prevents a message from being delivered to both backends.
	if _, err := repository.Create(CreateRequest{
		Backend: "openclaw", TargetID: "agent:main:other", ChannelType: "telegram", ChannelProfileID: "telegram-alias", ResourceID: "telegram-resource-1",
		AccountID: "bot-a", ConversationType: "default", ConversationID: "chat-1", ChatID: "chat-1", ThreadID: "agent:main:other", SessionKey: "agent:main:other",
	}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("shared resource must reject a second backend binding for one chat, got %v", err)
	}
}
