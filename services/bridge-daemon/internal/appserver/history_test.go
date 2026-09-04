package appserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type fakeHistoryRPC struct {
	readCalls []bool
	turnCalls []ThreadTurnsListOptions
	itemCalls []ThreadItemsListOptions
	read      func(includeTurns bool) (map[string]any, error)
	turns     func(ThreadTurnsListOptions) (map[string]any, error)
	items     func(ThreadItemsListOptions) (map[string]any, error)
}

func (f *fakeHistoryRPC) ThreadRead(_ context.Context, _ string, includeTurns bool) (map[string]any, error) {
	f.readCalls = append(f.readCalls, includeTurns)
	if f.read != nil {
		return f.read(includeTurns)
	}
	return nil, errors.New("fake ThreadRead is not configured")
}

func (f *fakeHistoryRPC) ThreadTurnsList(_ context.Context, options ThreadTurnsListOptions) (map[string]any, error) {
	f.turnCalls = append(f.turnCalls, options)
	if f.turns != nil {
		return f.turns(options)
	}
	return nil, errors.New("fake ThreadTurnsList is not configured")
}

func (f *fakeHistoryRPC) ThreadItemsList(_ context.Context, options ThreadItemsListOptions) (map[string]any, error) {
	f.itemCalls = append(f.itemCalls, options)
	if f.items != nil {
		return f.items(options)
	}
	return nil, errors.New("fake ThreadItemsList is not configured")
}

func TestHistoryReaderPaginatedUsesTurnsAndItemsWithoutFullRead(t *testing.T) {
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if includeTurns {
				return nil, &RPCError{Code: -32600, Message: "paginated threads do not support thread/read(includeTurns=true)"}
			}
			return paginatedMetadata("thread-1", "paginated"), nil
		},
		turns: func(options ThreadTurnsListOptions) (map[string]any, error) {
			if options.Cursor == "" {
				return map[string]any{
					"data": []map[string]any{
						{"id": "turn-3", "status": "completed", "startedAt": float64(3_000)},
						{"id": "turn-2", "status": "failed", "error": map[string]any{"message": "boom"}, "startedAt": float64(2_000)},
					},
					"nextCursor": "older-1",
				}, nil
			}
			return map[string]any{
				"data": []map[string]any{
					{"id": "turn-2", "status": "failed", "startedAt": float64(2_000)},
					{"id": "turn-1", "status": "completed", "startedAt": float64(1_000)},
				},
				"nextCursor": nil,
			}, nil
		},
		items: func(options ThreadItemsListOptions) (map[string]any, error) {
			if options.TurnID == "turn-1" && options.Cursor == "" {
				return map[string]any{
					"data":       []map[string]any{{"turnId": "turn-1", "item": map[string]any{"id": "user-1", "type": "userMessage"}}},
					"nextCursor": "items-1",
				}, nil
			}
			if options.TurnID == "turn-1" && options.Cursor == "items-1" {
				return map[string]any{
					"data":       []map[string]any{{"turnId": "turn-1", "item": map[string]any{"id": "assistant-1", "type": "agentMessage", "phase": "final_answer"}}},
					"nextCursor": nil,
				}, nil
			}
			return map[string]any{
				"data":       []map[string]any{{"turnId": options.TurnID, "item": map[string]any{"id": options.TurnID + "-item", "type": "agentMessage"}}},
				"nextCursor": nil,
			}, nil
		},
	}

	result, err := NewHistoryReader(fake, nil).ReadThread(context.Background(), "thread-1", 3)
	if err != nil {
		t.Fatalf("ReadThread returned error: %v", err)
	}
	if reflect.DeepEqual(fake.readCalls, []bool{false}) == false {
		t.Fatalf("ThreadRead calls = %#v; want only includeTurns=false", fake.readCalls)
	}
	if len(fake.turnCalls) != 2 {
		t.Fatalf("turn page calls = %d; want 2", len(fake.turnCalls))
	}
	for _, call := range fake.turnCalls {
		if call.ThreadID != "thread-1" || call.SortDirection != "desc" || call.ItemsView != "notLoaded" || call.Limit <= 0 || call.Limit > 3 {
			t.Fatalf("unexpected turns/list options: %#v", call)
		}
	}
	for _, call := range fake.itemCalls {
		if call.ThreadID != "thread-1" || call.TurnID == "" || call.SortDirection != "asc" || call.Limit <= 0 {
			t.Fatalf("unexpected items/list options: %#v", call)
		}
	}

	thread := result["thread"].(map[string]any)
	turns := historyObjects(thread["turns"])
	if got := []string{turnID(turns[0]), turnID(turns[1]), turnID(turns[2])}; !reflect.DeepEqual(got, []string{"turn-1", "turn-2", "turn-3"}) {
		t.Fatalf("turn order = %#v; want chronological order", got)
	}
	items := historyObjects(turns[0]["items"])
	if got := []string{historyString(items[0], "id"), historyString(items[1], "id")}; !reflect.DeepEqual(got, []string{"user-1", "assistant-1"}) {
		t.Fatalf("turn-1 items = %#v; want unwrapped chronological items", got)
	}
}

func TestHistoryReaderDefaultWindowCapsTurnsAndItems(t *testing.T) {
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if includeTurns {
				return nil, &RPCError{Code: -32600, Message: "includeTurns is unsupported"}
			}
			return paginatedMetadata("bounded-thread", "paginated"), nil
		},
		turns: func(options ThreadTurnsListOptions) (map[string]any, error) {
			data := make([]map[string]any, 0, 40)
			for index := 0; index < 40; index++ {
				data = append(data, map[string]any{"id": fmt.Sprintf("turn-%02d", index), "status": "completed"})
			}
			return map[string]any{"data": data}, nil
		},
		items: func(options ThreadItemsListOptions) (map[string]any, error) {
			data := make([]map[string]any, 0, options.Limit)
			for index := 0; index < options.Limit; index++ {
				data = append(data, map[string]any{"id": fmt.Sprintf("%s-item-%03d", options.TurnID, index), "type": "agentMessage"})
			}
			return map[string]any{"data": data}, nil
		},
	}

	result, err := NewHistoryReader(fake, nil).ReadThread(context.Background(), "bounded-thread", 0)
	if err != nil {
		t.Fatalf("ReadThread returned error: %v", err)
	}
	turns := historyObjects(result["thread"].(map[string]any)["turns"])
	if len(turns) != DefaultHistoryTurnLimit {
		t.Fatalf("turn count = %d; want %d", len(turns), DefaultHistoryTurnLimit)
	}
	itemCount := 0
	for _, turn := range turns {
		itemCount += len(historyObjects(turn["items"]))
	}
	if itemCount != maxHistoryItemCount {
		t.Fatalf("item count = %d; want %d", itemCount, maxHistoryItemCount)
	}
	if len(fake.itemCalls) != maxHistoryItemCount/maxHistoryItemsPerTurn {
		t.Fatalf("item page calls = %d; want %d", len(fake.itemCalls), maxHistoryItemCount/maxHistoryItemsPerTurn)
	}
}

func TestHistoryReaderLegacyUsesFullRead(t *testing.T) {
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if !includeTurns {
				return paginatedMetadata("legacy-1", "legacy"), nil
			}
			return map[string]any{"thread": map[string]any{
				"id": "legacy-1", "historyMode": "legacy",
				"turns": []map[string]any{{"id": "legacy-turn", "status": "completed", "items": []map[string]any{{"id": "legacy-item"}}}},
			}}, nil
		},
	}

	result, err := NewHistoryReader(fake, nil).ReadThread(context.Background(), "legacy-1", 3)
	if err != nil {
		t.Fatalf("ReadThread returned error: %v", err)
	}
	if !reflect.DeepEqual(fake.readCalls, []bool{false, true}) {
		t.Fatalf("ThreadRead calls = %#v; want false then true", fake.readCalls)
	}
	if len(fake.turnCalls) != 0 || len(fake.itemCalls) != 0 {
		t.Fatalf("legacy read unexpectedly called paginated RPCs: turns=%d items=%d", len(fake.turnCalls), len(fake.itemCalls))
	}
	if len(historyObjects(result["thread"].(map[string]any)["turns"])) != 1 {
		t.Fatalf("legacy result did not preserve full-read turns: %#v", result)
	}
}

func TestHistoryReaderMarksResolvedHistoryModeForFinalSelection(t *testing.T) {
	legacy := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if !includeTurns {
				return paginatedMetadata("legacy-marker", ""), nil
			}
			return map[string]any{"thread": map[string]any{
				"id": "legacy-marker", "turns": []map[string]any{{"id": "turn-1", "status": "completed"}},
			}}, nil
		},
	}
	legacyResult, err := NewHistoryReader(legacy, nil).ReadThread(context.Background(), "legacy-marker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if mode := historyMode(legacyResult); mode != "legacy" {
		t.Fatalf("legacy compatibility mode = %q; want legacy", mode)
	}

	paginated := &fakeHistoryRPC{
		read: func(bool) (map[string]any, error) {
			return paginatedMetadata("paged-marker", "paginated"), nil
		},
		turns: func(ThreadTurnsListOptions) (map[string]any, error) {
			return map[string]any{"data": []map[string]any{{"id": "turn-1", "status": "completed"}}}, nil
		},
		items: func(ThreadItemsListOptions) (map[string]any, error) {
			return map[string]any{"data": []map[string]any{}}, nil
		},
	}
	paginatedResult, err := NewHistoryReader(paginated, nil).ReadThread(context.Background(), "paged-marker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if mode := historyMode(paginatedResult); mode != "paginated" {
		t.Fatalf("paginated compatibility mode = %q; want paginated", mode)
	}
}

func TestHistoryReaderFallsBackToLegacyWhenPagingIsUnavailable(t *testing.T) {
	probeFailed := false
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if !includeTurns {
				return paginatedMetadata("fallback-1", ""), nil
			}
			if !probeFailed {
				probeFailed = true
				return nil, &RPCError{Code: -32600, Message: "paginated threads do not support thread/read(includeTurns=true)"}
			}
			return map[string]any{"thread": map[string]any{
				"id": "fallback-1", "historyMode": "legacy",
				"turns": []map[string]any{{"id": "legacy-turn", "status": "completed", "items": []map[string]any{{"id": "legacy-item"}}}},
			}}, nil
		},
		turns: func(ThreadTurnsListOptions) (map[string]any, error) {
			return nil, &RPCError{Code: -32601, Message: "Method not found"}
		},
	}

	reader := NewHistoryReader(fake, nil)
	if _, err := reader.ReadThread(context.Background(), "fallback-1", 1); err != nil {
		t.Fatalf("fallback ReadThread returned error: %v", err)
	}
	if !reflect.DeepEqual(fake.readCalls, []bool{false, true, true}) {
		t.Fatalf("fallback calls = %#v; want metadata, paging failure, then legacy full read", fake.readCalls)
	}
	if len(fake.turnCalls) != 1 || reader.Capability() != HistoryCapabilityLegacyOnly {
		t.Fatalf("fallback capability/calls = %v/%d; want legacy-only/one paging call", reader.Capability(), len(fake.turnCalls))
	}
	if _, err := reader.ReadThread(context.Background(), "fallback-1", 1); err != nil {
		t.Fatalf("cached legacy fallback returned error: %v", err)
	}
	if len(fake.turnCalls) != 1 || !reflect.DeepEqual(fake.readCalls, []bool{false, true, true, false, true}) {
		t.Fatalf("cached fallback repeated paging or used the wrong read path: reads=%#v turns=%d", fake.readCalls, len(fake.turnCalls))
	}
}

func TestHistoryReaderExplicitPaginatedNeverFallsBackToFullRead(t *testing.T) {
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if includeTurns {
				return nil, &RPCError{Code: -32600, Message: "paginated threads do not support thread/read(includeTurns=true)"}
			}
			return paginatedMetadata("explicit-paginated", "paginated"), nil
		},
		turns: func(ThreadTurnsListOptions) (map[string]any, error) {
			return nil, &RPCError{Code: -32601, Message: "Method not found"}
		},
	}

	_, err := NewHistoryReader(fake, nil).ReadThread(context.Background(), "explicit-paginated", 1)
	if err == nil || !strings.Contains(err.Error(), "paginated Codex thread history is unavailable") {
		t.Fatalf("explicit paginated unsupported error = %v; want a clear paging error", err)
	}
	if !reflect.DeepEqual(fake.readCalls, []bool{false}) {
		t.Fatalf("explicit paginated path called full read: %#v", fake.readCalls)
	}
}

func TestHistoryReaderUnknownModeRecognizesExactPaginatedReadError(t *testing.T) {
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if includeTurns {
				return nil, &RPCError{Code: -32600, Message: "paginated threads do not support thread/read(includeTurns=true)"}
			}
			return paginatedMetadata("unknown-1", ""), nil
		},
		turns: func(ThreadTurnsListOptions) (map[string]any, error) {
			return map[string]any{
				"data":       []map[string]any{{"id": "turn-1", "status": "completed"}},
				"nextCursor": nil,
			}, nil
		},
		items: func(options ThreadItemsListOptions) (map[string]any, error) {
			return map[string]any{
				"data":       []map[string]any{{"turnId": options.TurnID, "item": map[string]any{"id": "item-1"}}},
				"nextCursor": nil,
			}, nil
		},
	}

	result, err := NewHistoryReader(fake, nil).ReadThread(context.Background(), "unknown-1", 1)
	if err != nil {
		t.Fatalf("exact paginated compatibility error should trigger paging, got %v", err)
	}
	if !reflect.DeepEqual(fake.readCalls, []bool{false, true}) {
		t.Fatalf("unknown-mode probe calls = %#v; want false then one true probe", fake.readCalls)
	}
	if len(historyObjects(result["thread"].(map[string]any)["turns"])) != 1 {
		t.Fatalf("paged fallback did not return a turn: %#v", result)
	}
}

func TestHistoryReaderActivityDoesNotLoadItems(t *testing.T) {
	fake := &fakeHistoryRPC{
		read: func(includeTurns bool) (map[string]any, error) {
			if includeTurns {
				return nil, &RPCError{Code: -32600, Message: "paginated threads do not support thread/read(includeTurns=true)"}
			}
			return paginatedMetadata("activity-1", "paginated"), nil
		},
		turns: func(options ThreadTurnsListOptions) (map[string]any, error) {
			if options.Limit != ActivityHistoryTurnLimit || options.ItemsView != "notLoaded" || options.SortDirection != "desc" {
				t.Fatalf("activity requested unexpected turns/list options: %#v", options)
			}
			return map[string]any{
				"data":       []map[string]any{{"id": "latest", "status": "inProgress"}},
				"nextCursor": "should-not-be-followed",
			}, nil
		},
		items: func(ThreadItemsListOptions) (map[string]any, error) {
			t.Fatalf("activity must not call thread/items/list")
			return nil, nil
		},
	}

	result, err := NewHistoryReader(fake, nil).ReadActivity(context.Background(), "activity-1")
	if err != nil {
		t.Fatalf("ReadActivity returned error: %v", err)
	}
	if len(fake.itemCalls) != 0 {
		t.Fatalf("activity item calls = %d; want 0", len(fake.itemCalls))
	}
	turns := historyObjects(result["thread"].(map[string]any)["turns"])
	if len(turns) != 1 || len(historyObjects(turns[0]["items"])) != 0 {
		t.Fatalf("activity returned unexpected turns/items: %#v", turns)
	}
}

func TestHistoryReaderStopsOnRepeatedCursor(t *testing.T) {
	turnCalls := 0
	fake := &fakeHistoryRPC{
		read: func(bool) (map[string]any, error) {
			return paginatedMetadata("cursor-1", "paginated"), nil
		},
		turns: func(options ThreadTurnsListOptions) (map[string]any, error) {
			turnCalls++
			return map[string]any{
				"data":       []map[string]any{{"id": "turn-1", "status": "completed"}},
				"nextCursor": "same-cursor",
			}, nil
		},
		items: func(ThreadItemsListOptions) (map[string]any, error) {
			return map[string]any{"data": []map[string]any{}, "nextCursor": nil}, nil
		},
	}

	if _, err := NewHistoryReader(fake, nil).ReadThread(context.Background(), "cursor-1", 3); err != nil {
		t.Fatalf("ReadThread returned error: %v", err)
	}
	if turnCalls != 2 {
		t.Fatalf("turn calls = %d; want 2 before repeated cursor stops pagination", turnCalls)
	}
}

func paginatedMetadata(threadID, mode string) map[string]any {
	thread := map[string]any{"id": threadID, "name": "test thread", "turns": []map[string]any{}}
	if strings.TrimSpace(mode) != "" {
		thread["historyMode"] = mode
	}
	return map[string]any{"thread": thread}
}
