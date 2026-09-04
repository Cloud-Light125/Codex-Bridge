package mirror

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
)

type fakeControl struct {
	mu     sync.Mutex
	detail control.ThreadDetail
}

func (f *fakeControl) ListThreads(context.Context, int, string) (control.ThreadList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return control.ThreadList{Threads: []control.ThreadSummary{f.detail.ThreadSummary}}, nil
}

func (f *fakeControl) ReadThread(context.Context, string, bool) (control.ThreadDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.detail, nil
}

func (f *fakeControl) setTurns(turns ...control.Turn) {
	f.mu.Lock()
	f.detail.Turns = turns
	f.mu.Unlock()
}

func (f *fakeControl) setHistoryMode(mode string) {
	f.mu.Lock()
	f.detail.HistoryMode = mode
	f.mu.Unlock()
}

type fakeRuntime struct{}

func (fakeRuntime) RuntimeState(id string) control.RuntimeState {
	return control.RuntimeState{ThreadID: id, State: "idle"}
}

type sendRecorder struct {
	mu    sync.Mutex
	sent  []string
	fail  bool
	delay time.Duration
}

func (r *sendRecorder) target() Target {
	return Target{
		Status: func() (string, bool) { return "account", true },
		Send: func(_ context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
			if r.delay > 0 {
				time.Sleep(r.delay)
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.fail {
				return channels.OutboundResult{}, errors.New("temporary failure")
			}
			r.sent = append(r.sent, message.Text)
			return channels.OutboundResult{MessageID: "ok"}, nil
		},
	}
}

func TestRolloutFallbackRequiresTaskCompleteAndSkipsCommentary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout-test.jsonl")
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"thread"}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","id":"comment","content":[{"type":"output_text","text":"do not send"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}`,
		`{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","id":"final","content":[{"type":"output_text","text":"LOW-LATENCY-TEST"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := readCompletedRolloutFinal(path); ok {
		t.Fatal("final must not be accepted before task_complete")
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}` + "\n")
	_ = file.Close()
	final, ok := readCompletedRolloutFinal(path)
	if !ok || final.Text != "LOW-LATENCY-TEST" || final.ItemID != "final" {
		t.Fatalf("unexpected fallback: %#v ok=%t", final, ok)
	}
}

func TestRolloutTaskCompleteDoesNotPromoteProgressBeforeFinalFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout-order.jsonl")
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"thread"}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","id":"comment","content":[{"type":"output_text","text":"PROGRESS-1"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}`,
		`{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","id":"wrong","content":[{"type":"output_text","text":"WRONG-PROGRESS"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}`,
		`{"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if final, ok := readCompletedRolloutFinal(path); ok || final.Text != "" {
		t.Fatalf("progress was promoted before final flush: %#v ok=%t", final, ok)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"timestamp":"2026-01-01T00:00:04Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","id":"correct","content":[{"type":"output_text","text":"CORRECT-FINAL"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}` + "\n")
	_ = file.Close()
	final, ok := readCompletedRolloutFinal(path)
	if !ok || final.Text != "CORRECT-FINAL" || final.ItemID != "correct" || final.Phase != "final_answer" {
		t.Fatalf("final flush was not selected: %#v ok=%t", final, ok)
	}
}

func TestAppServerAndRolloutShareFallbackFinalItemID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout-fallback-id.jsonl")
	lines := []string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"thread"}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"CORRECT-FINAL"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}`,
		`{"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rollout, ok := readCompletedRolloutFinal(path)
	if !ok {
		t.Fatal("rollout final without protocol ID was not selected")
	}
	detail := control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread", HistoryMode: "paginated"}, Turns: []control.Turn{{
		TurnID: "turn-1", Status: "completed", Items: []control.Item{{
			Type: "agentMessage", Phase: "final_answer", Text: "CORRECT-FINAL",
		}},
	}}}
	messages := visibleMessages(detail, func(string) string { return "" })
	if len(messages) != 1 || messages[0].ItemID != rollout.ItemID {
		t.Fatalf("App Server/Rollout fallback IDs differ: appserver=%#v rollout=%#v", messages, rollout)
	}
}

func TestRolloutAndAppServerSameFinalAreDeduped(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	service.mu.Lock()
	service.rolloutFinals["thread"] = rolloutFinal{ThreadID: "thread", TurnID: "turn-1", ItemID: "final-1", Phase: "final_answer", Text: "CORRECT-FINAL"}
	service.mu.Unlock()
	service.syncThreadSource("thread", "watcher", "turn-1")
	reader.setHistoryMode("paginated")
	reader.setTurns(control.Turn{TurnID: "turn-1", Status: "completed", Items: []control.Item{{
		Type: "agentMessage", ItemID: "final-1", Phase: "final_answer", Text: "CORRECT-FINAL",
	}}})
	service.syncThread("thread")
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatalf("same final was delivered more than once after source convergence: telegram=%d qq=%d", tg.count(), qq.count())
	}
}

func TestFinalPlatformsSendInParallel(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{delay: 250 * time.Millisecond}, &sendRecorder{delay: 250 * time.Millisecond}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	reader.setTurns(completedTurn("turn-1", "assistant-1", "answer"))
	started := time.Now()
	service.syncThread("thread")
	if elapsed := time.Since(started); elapsed >= 450*time.Millisecond {
		t.Fatalf("platform sends were serialized: %s", elapsed)
	}
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatalf("delivery count telegram=%d qq=%d", tg.count(), qq.count())
	}
}

func (r *sendRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

func (r *sendRecorder) first() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sent) == 0 {
		return ""
	}
	return r.sent[0]
}

func newFixture(t *testing.T, path string, telegram, qq *sendRecorder) (*Service, *fakeControl, *events.Broker, *threadregistry.Registry) {
	t.Helper()
	dir := filepath.Dir(path)
	registry, err := threadregistry.New(filepath.Join(dir, "threads.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = registry.Ensure(threadregistry.Metadata{ThreadID: "thread", Title: "Title", CreatedAt: "2026-01-01T00:00:00Z"})
	reader := &fakeControl{detail: control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread", Number: 1, Title: "Title", CreatedAt: "2026-01-01T00:00:00Z", HistoryMode: "legacy"}}}
	broker := events.NewBroker()
	service, err := New(path, reader, fakeRuntime{}, registry, broker, nil, telegram.target(), qq.target())
	if err != nil {
		t.Fatal(err)
	}
	service.baselineAll()
	_, err = service.Configure(Config{
		Enabled: true, RequireThreadNumber: true,
		Messages: MessageTypes{Assistant: true, RequestUserInput: true, Error: true},
		Telegram: TelegramConfig{Enabled: true, ChatID: "telegram-chat"},
		QQ:       QQConfig{Enabled: true, ConversationType: "c2c", OpenID: "qq-open-id"},
	})
	if err != nil {
		service.Close()
		t.Fatal(err)
	}
	return service, reader, broker, registry
}

func completedTurn(id, itemID, text string) control.Turn {
	return control.Turn{TurnID: id, Status: "completed", Items: []control.Item{
		{ItemID: "user-" + id, Type: "userMessage", Role: "user", Text: "prompt " + id},
		{ItemID: itemID, Type: "agentMessage", Role: "assistant", Text: text},
	}}
}

func TestThread217ProgressIsNeverMirroredAsFinal(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	reader.setHistoryMode("paginated")
	reader.setTurns(control.Turn{TurnID: "turn-217", Status: "completed", Items: []control.Item{
		{ItemID: "user-217", Type: "userMessage", Role: "user", Text: "审查 Canonical Import Audit"},
		{ItemID: "commentary-1", Type: "agentMessage", Role: "assistant", Phase: "commentary", Text: "我会先把 Canonical 观察、schema 迁移..."},
		{ItemID: "progress-217", Type: "agentMessage", Role: "assistant", Text: "5C.3 已提交并确认 clean，commit 为 ca5631...\n现在进入 5D.0：先定向搜索并完整阅读..."},
		{ItemID: "commentary-2", Type: "agentMessage", Role: "assistant", Phase: "commentary", Text: "真实 audit 已启动..."},
		{ItemID: "command-217", Type: "commandExecution", Output: "hidden command output"},
		{ItemID: "final-217", Type: "agentMessage", Role: "assistant", Phase: "final_answer", Text: "已完成，未调用子代理。\n\n### Part A：Phase 5C.3\n\n- 审查通过...\n\n### Part B：Phase 5D.0\n\n..."},
	}})
	service.syncThread("thread")
	for name, recorder := range map[string]*sendRecorder{"telegram": tg, "qq": qq} {
		body := recorder.first()
		if !strings.Contains(body, "已完成，未调用子代理。") || strings.Contains(body, "现在进入 5D.0：先定向搜索") {
			t.Fatalf("%s mirrored the wrong body: %q", name, body)
		}
	}
}

func TestMirrorStaleProgressCursorMigratesToPendingFormalFinal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mirror.json")
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, path, tg, qq)
	defer service.Close()
	reader.setHistoryMode("paginated")
	reader.setTurns(control.Turn{TurnID: "turn-1", Status: "completed", Items: []control.Item{
		{ItemID: "user-1", Type: "userMessage", Role: "user", Text: "prompt"},
		{ItemID: "correct-final-id", Type: "agentMessage", Role: "assistant", Phase: "final_answer", Text: "CORRECT-FINAL"},
	}})
	service.mu.Lock()
	oldKey := "turn-1/old-progress-key"
	service.model.Finals[oldKey] = finalRecord{ThreadID: "thread", TurnID: "turn-1", AssistantMessageID: "old-progress-key", Fingerprint: "old", TelegramMirrored: true, QQMirrored: true, FinalMirrored: true}
	service.model.Cursors["thread"] = Cursor{LastObservedMessage: oldKey, LastTelegramMirrored: oldKey, LastQQMirrored: oldKey}
	_ = service.saveLocked()
	service.mu.Unlock()
	service.baselineAll()
	service.mu.Lock()
	migrated := service.model.Cursors["thread"]
	service.mu.Unlock()
	if migrated.LastTelegramMirrored != "" || migrated.LastQQMirrored != "" || migrated.LastObservedMessage != "" {
		t.Fatalf("stale cursor was not moved before the Turn final: %#v", migrated)
	}
	service.syncThread("thread")
	if tg.count() != 1 || qq.count() != 1 || !strings.Contains(tg.first(), "CORRECT-FINAL") || !strings.Contains(qq.first(), "CORRECT-FINAL") {
		t.Fatalf("stale progress cursor did not release the formal final: telegram=%d/%q qq=%d/%q", tg.count(), tg.first(), qq.count(), qq.first())
	}
	service.syncThread("thread")
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatalf("migrated final was duplicated: telegram=%d qq=%d", tg.count(), qq.count())
	}
}

func TestInitialMirrorBaselineDoesNotReplayExistingFinals(t *testing.T) {
	dir := t.TempDir()
	registry, err := threadregistry.New(filepath.Join(dir, "threads.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = registry.Ensure(threadregistry.Metadata{ThreadID: "thread", Title: "Title", CreatedAt: "2026-01-01T00:00:00Z"})
	tg, qq := &sendRecorder{}, &sendRecorder{}
	reader := &fakeControl{detail: control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread", Number: 1, Title: "Title", HistoryMode: "paginated"}, Turns: []control.Turn{
		{TurnID: "old-1", Status: "completed", Items: []control.Item{{Type: "agentMessage", Role: "assistant", Phase: "final_answer", Text: "OLD-1"}}},
		{TurnID: "old-2", Status: "completed", Items: []control.Item{{Type: "agentMessage", Role: "assistant", Phase: "final_answer", Text: "OLD-2"}}},
	}}}
	service, err := New(filepath.Join(dir, "mirror.json"), reader, fakeRuntime{}, registry, events.NewBroker(), nil, tg.target(), qq.target())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	service.baselineAll()
	if _, err := service.Configure(Config{Enabled: true, Messages: MessageTypes{Assistant: true}, Telegram: TelegramConfig{Enabled: true, ChatID: "telegram-chat"}, QQ: QQConfig{Enabled: true, ConversationType: "c2c", OpenID: "qq-open-id"}}); err != nil {
		t.Fatal(err)
	}
	service.syncThread("thread")
	if tg.count() != 0 || qq.count() != 0 {
		t.Fatalf("initial baseline replayed existing finals: telegram=%d qq=%d", tg.count(), qq.count())
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for mirror delivery")
}

func TestMultipleNotificationsMirrorFinalOnce(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	reader.setTurns(completedTurn("turn-1", "assistant-1", "ONE"))
	for range 5 {
		service.handleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: "thread", TurnID: "turn-1", Payload: map[string]any{"status": "persisted"}})
	}
	waitFor(t, func() bool { return tg.count() == 1 && qq.count() == 1 })
	time.Sleep(50 * time.Millisecond)
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatalf("duplicate final: telegram=%d qq=%d", tg.count(), qq.count())
	}
}

func TestDeltaAndPersistedMirrorFinalOnce(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	service.handleEvent(events.Event{EventType: events.AssistantDelta, ThreadID: "thread", TurnID: "turn-1", ItemID: "assistant-1", Payload: map[string]any{"delta": "streamed"}})
	service.handleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: "thread", TurnID: "turn-1", ItemID: "assistant-1"})
	if tg.count() != 0 || qq.count() != 0 {
		t.Fatal("delta or item completion produced outbound content")
	}
	reader.setTurns(completedTurn("turn-1", "assistant-1", "ONE"))
	service.handleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: "thread", TurnID: "turn-1", Payload: map[string]any{"status": "persisted"}})
	waitFor(t, func() bool { return tg.count() == 1 && qq.count() == 1 })
	service.handleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: "thread", TurnID: "turn-1", ItemID: "assistant-1"})
	service.syncThread("thread")
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatal("persisted final was mirrored more than once")
	}
}

func TestScannerAndNotificationMirrorFinalOnce(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	reader.setTurns(completedTurn("turn-1", "assistant-1", "ONE"))
	done := make(chan struct{}, 2)
	go func() { service.syncThread("thread"); done <- struct{}{} }()
	go func() {
		service.handleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: "thread", TurnID: "turn-1", Payload: map[string]any{"status": "persisted"}})
		done <- struct{}{}
	}()
	<-done
	<-done
	waitFor(t, func() bool { return tg.count() == 1 && qq.count() == 1 })
}

func TestMirrorCursorsAreIndependentAndRestartDoesNotReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mirror.json")
	tg, qq := &sendRecorder{fail: true}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, path, tg, qq)
	reader.setTurns(completedTurn("turn-1", "assistant-1", "answer"))
	service.syncThread("thread")
	if tg.count() != 0 || qq.count() != 1 {
		t.Fatalf("first delivery telegram=%d qq=%d", tg.count(), qq.count())
	}
	tg.mu.Lock()
	tg.fail = false
	tg.mu.Unlock()
	service.syncThread("thread")
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatalf("independent retry telegram=%d qq=%d", tg.count(), qq.count())
	}
	service.Close()

	service2, _, _, _ := newFixture(t, path, tg, qq)
	reader2 := service2.control.(*fakeControl)
	reader2.setTurns(completedTurn("turn-1", "assistant-1", "answer"))
	service2.syncThread("thread")
	service2.Close()
	if tg.count() != 1 || qq.count() != 1 {
		t.Fatalf("restart replayed final telegram=%d qq=%d", tg.count(), qq.count())
	}
}

func TestConsecutiveTurnsAreIndependentMessages(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	reader.setTurns(completedTurn("turn-1", "assistant-1", "ONE"))
	service.syncThread("thread")
	reader.setTurns(completedTurn("turn-1", "assistant-1", "ONE"), completedTurn("turn-2", "assistant-2", "TWO"))
	service.syncThread("thread")
	if tg.count() != 2 || qq.count() != 2 {
		t.Fatalf("turn count telegram=%d qq=%d", tg.count(), qq.count())
	}
	tg.mu.Lock()
	got := append([]string(nil), tg.sent...)
	tg.mu.Unlock()
	if got[0] != "#1 Title\nONE" || got[1] != "#1 Title\nTWO" {
		t.Fatalf("unexpected final format: %#v", got)
	}
}

func TestRequestUserInputStillMirrorsOnce(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, _, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	interaction := interactions.PendingInteraction{
		ID: "input-1", Kind: interactions.KindUserInput, ThreadID: "thread", TurnID: "turn-1",
		Questions: []interactions.Question{{ID: "choice", Text: "选择方案", Options: []interactions.QuestionOption{{Label: "A"}, {Label: "B"}}}},
	}
	event := events.Event{EventType: events.InteractionRequested, ThreadID: "thread", TurnID: "turn-1", Payload: map[string]any{"interaction": interaction}}
	service.handleEvent(event)
	service.handleEvent(event)
	waitFor(t, func() bool { return tg.count() == 1 && qq.count() == 1 })
	if text := tg.first(); !strings.Contains(text, "#1 Title\n需要你选择：") || !strings.Contains(text, "回复：\n#1 1") {
		t.Fatalf("unexpected interaction format: %q", text)
	}
}

func TestFinalOnlySuppressesExtraRemindersButKeepsFormalFinal(t *testing.T) {
	dir := t.TempDir()
	tg, qq := &sendRecorder{}, &sendRecorder{}
	service, reader, _, _ := newFixture(t, filepath.Join(dir, "mirror.json"), tg, qq)
	defer service.Close()
	_, err := service.Configure(Config{
		Enabled: true, FinalOnly: true, RequireThreadNumber: true,
		Messages: MessageTypes{Assistant: true, RequestUserInput: true, Error: true},
		Telegram: TelegramConfig{Enabled: true, ChatID: "telegram-chat"},
		QQ:       QQConfig{Enabled: true, ConversationType: "c2c", OpenID: "qq-open-id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	interaction := interactions.PendingInteraction{
		ID: "input-final-only", Kind: interactions.KindUserInput, ThreadID: "thread", TurnID: "turn-final-only",
		Questions: []interactions.Question{{ID: "choice", Text: "选择方案", Options: []interactions.QuestionOption{{Label: "A"}}}},
	}
	service.handleEvent(events.Event{EventType: events.InteractionRequested, ThreadID: "thread", TurnID: "turn-final-only", Payload: map[string]any{"interaction": interaction}})
	service.handleEvent(events.Event{EventType: events.TurnFailed, ThreadID: "thread", TurnID: "turn-final-only", Payload: map[string]any{"error": "failure detail"}})
	time.Sleep(40 * time.Millisecond)
	if tg.count() != 0 || qq.count() != 0 {
		t.Fatalf("FinalOnly delivered an extra reminder: telegram=%d qq=%d", tg.count(), qq.count())
	}
	reader.setTurns(completedTurn("turn-final-only", "assistant-final-only", "formal final answer"))
	service.syncThread("thread")
	waitFor(t, func() bool { return tg.count() == 1 && qq.count() == 1 })
	if !strings.Contains(tg.first(), "formal final answer") {
		t.Fatalf("formal final answer was not mirrored: %q", tg.first())
	}
}

func TestMirrorFinalOnlyMigratesFromLegacyReminderFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mirror.json")
	data := []byte(`{"version":1,"config":{"enabled":true,"messages":{"assistant":false,"requestUserInput":false,"error":false}}}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	registry, err := threadregistry.New(filepath.Join(filepath.Dir(path), "threads.json"))
	if err != nil {
		t.Fatal(err)
	}
	recorder := &sendRecorder{}
	service, err := New(path, &fakeControl{}, fakeRuntime{}, registry, events.NewBroker(), nil, recorder.target(), recorder.target())
	if err != nil {
		t.Fatal(err)
	}
	if !service.Status().Config.FinalOnly || !service.Status().Config.Messages.Assistant {
		t.Fatal("legacy config with both reminder flags disabled was not migrated to FinalOnly")
	}
	service.Close()

	data = []byte(`{"version":1,"config":{"enabled":true,"messages":{"assistant":true,"requestUserInput":true,"error":false}}}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	service, err = New(path, &fakeControl{}, fakeRuntime{}, registry, events.NewBroker(), nil, recorder.target(), recorder.target())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if service.Status().Config.FinalOnly {
		t.Fatal("legacy config with an enabled reminder was incorrectly migrated to FinalOnly")
	}
}

func TestLongFinalUsesOnlyNecessaryNumberedParts(t *testing.T) {
	parts := splitFinalMessage("#53 Title", strings.Repeat("界", 8000), 3900)
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "#53 Title (1/3)\n") || !strings.HasPrefix(parts[2], "#53 Title (3/3)\n") {
		t.Fatalf("unexpected parts: count=%d first=%q last=%q", len(parts), parts[0][:min(24, len(parts[0]))], parts[len(parts)-1][:min(24, len(parts[len(parts)-1]))])
	}
}

func TestNumericQQAccountCannotBeConfiguredAsMirrorOpenID(t *testing.T) {
	dir := t.TempDir()
	registry, _ := threadregistry.New(filepath.Join(dir, "threads.json"))
	reader := &fakeControl{}
	target := (&sendRecorder{}).target()
	service, err := New(filepath.Join(dir, "mirror.json"), reader, fakeRuntime{}, registry, events.NewBroker(), nil, target, target)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	_, err = service.Configure(Config{Enabled: true, QQ: QQConfig{Enabled: true, ConversationType: "c2c", OpenID: "1234567890"}})
	if err == nil {
		t.Fatal("numeric QQ account must not be accepted as an official OpenID")
	}
}
