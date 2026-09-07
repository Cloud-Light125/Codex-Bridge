package control

import (
	"context"
	"errors"
	"testing"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
)

type historyControlReaderFake struct {
	fullReadCalls  int
	historyLimits  []int
	activityCalls  int
	activityLimits []int
	threadList     map[string]any
}

func (f *historyControlReaderFake) ThreadList(context.Context, int, string) (map[string]any, error) {
	if f.threadList != nil {
		return f.threadList, nil
	}
	return map[string]any{"data": []map[string]any{}}, nil
}

func (f *historyControlReaderFake) ThreadRead(_ context.Context, _ string, includeTurns bool) (map[string]any, error) {
	if includeTurns {
		f.fullReadCalls++
		return nil, errors.New("full thread/read must not be used by the paginated fake")
	}
	return map[string]any{"thread": map[string]any{"id": "thread-1", "historyMode": "paginated"}}, nil
}

func (f *historyControlReaderFake) ThreadReadHistory(_ context.Context, _ string, limit int) (map[string]any, error) {
	f.historyLimits = append(f.historyLimits, limit)
	return map[string]any{"thread": map[string]any{
		"id": "thread-1", "historyMode": "paginated",
		"turns": []map[string]any{{"id": "turn-1", "status": "completed", "items": []map[string]any{{"id": "item-1", "type": "agentMessage", "text": "answer"}}}},
	}}, nil
}

func (f *historyControlReaderFake) ThreadReadActivity(_ context.Context, _ string) (map[string]any, error) {
	f.activityCalls++
	return map[string]any{"thread": map[string]any{
		"id": "thread-1", "historyMode": "paginated",
		"turns": []map[string]any{{"id": "turn-latest", "status": "inProgress", "items": []map[string]any{}}},
	}}, nil
}

func (f *historyControlReaderFake) ThreadReadActivityHistory(_ context.Context, _ string, limit int) (map[string]any, error) {
	f.activityLimits = append(f.activityLimits, limit)
	return map[string]any{"thread": map[string]any{
		"id": "thread-1", "historyMode": "paginated",
		"turns": []map[string]any{{"id": "turn-failed", "status": "failed", "error": map[string]any{"message": "boom"}, "items": []map[string]any{}}},
	}}, nil
}

func TestServiceHistoryCompatibilityLayerUsesBoundedOptionalReaders(t *testing.T) {
	reader := &historyControlReaderFake{}
	service := NewService(reader, nil, nil)

	detail, err := service.ReadThread(context.Background(), "thread-1", true)
	if err != nil {
		t.Fatalf("ReadThread returned error: %v", err)
	}
	if reader.fullReadCalls != 0 || len(reader.historyLimits) != 1 || reader.historyLimits[0] != DefaultHistoryTurnLimit {
		t.Fatalf("ReadThread did not use bounded history reader: full=%d history=%#v", reader.fullReadCalls, reader.historyLimits)
	}
	if len(detail.Turns) != 1 || detail.Turns[0].Items[0].Text != "answer" {
		t.Fatalf("history DTO was not reconstructed: %#v", detail)
	}

	activity, err := service.ReadThreadActivity(context.Background(), "thread-1")
	if err != nil {
		t.Fatalf("ReadThreadActivity returned error: %v", err)
	}
	if reader.activityCalls != 1 || len(activity.Turns) != 1 || activity.Turns[0].TurnID != "turn-latest" || len(activity.Turns[0].Items) != 0 {
		t.Fatalf("activity path did not preserve the lightweight DTO: calls=%d detail=%#v", reader.activityCalls, activity)
	}

	failedActivity, err := service.ReadThreadActivityHistory(context.Background(), "thread-1", 10)
	if err != nil {
		t.Fatalf("ReadThreadActivityHistory returned error: %v", err)
	}
	if len(reader.activityLimits) != 1 || reader.activityLimits[0] != 10 || len(failedActivity.Turns) != 1 || failedActivity.Turns[0].Error != "boom" {
		t.Fatalf("status-only history path was not bounded/preserved: limits=%#v detail=%#v", reader.activityLimits, failedActivity)
	}
}

func TestListThreadsReadOnlyDoesNotAllocateConversationNumber(t *testing.T) {
	reader := &historyControlReaderFake{threadList: map[string]any{"data": []map[string]any{{
		"id": "new-thread", "name": "new", "cwd": `D:\work\new`, "createdAt": "2026-09-07T00:00:00Z",
	}}}}
	registry := conversationregistry.NewInMemory()
	service := NewService(reader, nil, registry)
	list, err := service.ListThreadsReadOnly(context.Background(), 20, "")
	if err != nil || len(list.Threads) != 1 {
		t.Fatalf("read-only list failed: list=%#v err=%v", list, err)
	}
	if list.Threads[0].Number != 0 || len(registry.List()) != 0 {
		t.Fatalf("read-only list allocated or exposed a new number: thread=%#v registry=%#v", list.Threads[0], registry.List())
	}
	if _, err := service.ListThreads(context.Background(), 20, ""); err != nil {
		t.Fatal(err)
	}
	if len(registry.List()) != 1 {
		t.Fatalf("normal list did not allocate the discoverable number: %#v", registry.List())
	}
}
