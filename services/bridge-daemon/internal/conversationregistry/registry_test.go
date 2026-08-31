package conversationregistry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestMigratesCodexNumbersAndAppendsOpenClaw(t *testing.T) {
	directory := t.TempDir()
	codexPath := filepath.Join(directory, "thread-numbers.json")
	openClawPath := filepath.Join(directory, "openclaw-session-numbers.json")
	globalPath := filepath.Join(directory, "conversation-numbers.json")
	writeJSON(t, codexPath, map[string]any{
		"version": 1, "nextNumber": 193,
		"threads": []map[string]any{
			{"threadId": "thread-191", "number": 191, "title": "Codex 191"},
			{"threadId": "thread-192", "number": 192, "title": "Codex 192"},
		},
	})
	writeJSON(t, openClawPath, map[string]any{
		"version": 1, "nextNumber": 3,
		"sessions": []map[string]any{
			{"sessionKey": "agent:main:main", "number": 1, "title": "main"},
			{"sessionKey": "qqbot:group:1", "number": 2, "title": "group"},
		},
	})

	registry, err := New(globalPath, codexPath, openClawPath)
	if err != nil {
		t.Fatal(err)
	}
	assertRecord(t, registry, 191, BackendCodex, "thread-191")
	assertRecord(t, registry, 192, BackendCodex, "thread-192")
	assertRecord(t, registry, 193, BackendOpenClaw, "agent:main:main")
	assertRecord(t, registry, 194, BackendOpenClaw, "qqbot:group:1")
	if got := registry.NextNumber(); got != 195 {
		t.Fatalf("next number=%d, want 195", got)
	}

	reloaded, err := New(globalPath, codexPath, openClawPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []int{191, 192, 193, 194} {
		if _, ok := reloaded.ByNumber(number); !ok {
			t.Fatalf("number #%d was not persisted", number)
		}
	}
	if got := reloaded.NextNumber(); got != 195 {
		t.Fatalf("reloaded next number=%d, want 195", got)
	}
}

func TestGlobalRegistryAllocatesOneSequenceAcrossBackends(t *testing.T) {
	registry := NewInMemory()
	values := []Metadata{
		{Backend: BackendCodex, TargetID: "codex-1", CreatedAt: "2026-08-31T00:00:01Z"},
		{Backend: BackendOpenClaw, TargetID: "agent:one", CreatedAt: "2026-08-31T00:00:02Z"},
		{Backend: BackendCodex, TargetID: "codex-2", CreatedAt: "2026-08-31T00:00:03Z"},
	}
	if _, err := registry.EnsureBatch(values); err != nil {
		t.Fatal(err)
	}
	assertRecord(t, registry, 1, BackendCodex, "codex-1")
	assertRecord(t, registry, 2, BackendOpenClaw, "agent:one")
	assertRecord(t, registry, 3, BackendCodex, "codex-2")
	seen := map[int]bool{}
	for _, record := range registry.List() {
		if seen[record.Number] {
			t.Fatalf("duplicate number #%d", record.Number)
		}
		seen[record.Number] = true
	}
}

func TestParsePrefixReturnsBackendAndTarget(t *testing.T) {
	registry := NewInMemory()
	if _, err := registry.Ensure(Metadata{Backend: BackendCodex, TargetID: "thread-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Ensure(Metadata{Backend: BackendOpenClaw, TargetID: "agent:main:main"}); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"#2 hello", "[2] hello", "2 hello"} {
		record, content, recognized, err := registry.ParsePrefix(input)
		if err != nil || !recognized || record.Backend != BackendOpenClaw || record.TargetID != "agent:main:main" || content != "hello" {
			t.Fatalf("parse %q: %#v %q %t %v", input, record, content, recognized, err)
		}
	}
}

func TestRejectsDuplicateNumberAcrossBackendsOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conversation-numbers.json")
	writeJSON(t, path, map[string]any{
		"version": 1, "nextNumber": 2,
		"conversations": []map[string]any{
			{"number": 1, "backend": BackendCodex, "targetId": "thread-1"},
			{"number": 1, "backend": BackendOpenClaw, "targetId": "agent:one"},
		},
	})
	if _, err := New(path); err == nil {
		t.Fatal("global registry accepted the same number for Codex and OpenClaw")
	}
}

func assertRecord(t *testing.T, registry *Registry, number int, backend, target string) {
	t.Helper()
	record, ok := registry.ByNumber(number)
	if !ok || record.Backend != backend || record.TargetID != target {
		t.Fatalf("record #%d=%#v ok=%t, want %s/%s", number, record, ok, backend, target)
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
