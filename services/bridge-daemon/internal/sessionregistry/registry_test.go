package sessionregistry

import (
	"path/filepath"
	"testing"
)

func TestRegistryAssignsStableIndependentNumbersAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "openclaw-session-numbers.json")
	registry, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.EnsureBatch([]Metadata{
		{SessionKey: "agent:new", Title: "New", CreatedAt: "2026-08-02T00:00:00Z"},
		{SessionKey: "agent:old", Title: "Old", CreatedAt: "2026-08-01T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	old, ok := registry.BySessionKey("agent:old")
	if !ok || old.Number != 1 {
		t.Fatalf("old session=%#v ok=%t", old, ok)
	}
	newSession, ok := registry.BySessionKey("agent:new")
	if !ok || newSession.Number != 2 {
		t.Fatalf("new session=%#v ok=%t", newSession, ok)
	}
	if _, err := registry.Ensure(Metadata{SessionKey: "agent:old", Title: "Old renamed", Status: "idle"}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if record, ok := reloaded.ByNumber(1); !ok || record.SessionKey != "agent:old" || record.Title != "Old renamed" {
		t.Fatalf("reloaded old session=%#v ok=%t", record, ok)
	}
	added, err := reloaded.Ensure(Metadata{SessionKey: "agent:next"})
	if err != nil || added.Number != 3 {
		t.Fatalf("next session=%#v err=%v", added, err)
	}
}

func TestParsePrefixSupportsHashAndBrackets(t *testing.T) {
	registry := NewInMemory()
	if _, err := registry.Ensure(Metadata{SessionKey: "agent:main", Title: "Main"}); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"#1 hello", "[1] hello", "1 hello"} {
		record, content, recognized, err := registry.ParsePrefix(input)
		if err != nil || !recognized || record.SessionKey != "agent:main" || content != "hello" {
			t.Fatalf("parse %q: %#v %q %t %v", input, record, content, recognized, err)
		}
	}
}
