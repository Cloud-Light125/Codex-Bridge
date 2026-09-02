package control

import "testing"

func TestNormalizeThreadListProducesStableDTO(t *testing.T) {
	result := normalizeThreadList(map[string]any{
		"data": []any{map[string]any{
			"id":        "thread-1",
			"preview":   "First line\nSecond line",
			"cwd":       `C:\work\demo`,
			"model":     "gpt-5",
			"createdAt": float64(1_700_000_000),
			"updatedAt": float64(1_700_000_100),
			"status": map[string]any{
				"type":        "active",
				"activeFlags": []any{"waitingOnInput"},
			},
		}},
		"nextCursor": "next-page",
	})
	if len(result.Threads) != 1 {
		t.Fatalf("threads = %d, want 1", len(result.Threads))
	}
	thread := result.Threads[0]
	if thread.ThreadID != "thread-1" || thread.Title != "First line" {
		t.Fatalf("unexpected thread identity: %#v", thread)
	}
	if thread.Status != "active[waitingOnInput]" {
		t.Fatalf("status = %q", thread.Status)
	}
	if thread.Archived != nil {
		t.Fatalf("archived should be unknown, got %#v", thread.Archived)
	}
	if result.NextCursor != "next-page" {
		t.Fatalf("next cursor = %q", result.NextCursor)
	}
}

func TestNormalizeThreadDetailParsesMessagesAndTools(t *testing.T) {
	detail := normalizeThreadDetail(map[string]any{"thread": map[string]any{
		"id": "thread-1",
		"turns": []any{map[string]any{
			"id":     "turn-1",
			"status": "completed",
			"items": []any{
				map[string]any{"id": "user-1", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "你好"}}},
				map[string]any{"id": "tool-1", "type": "commandExecution", "command": "git status", "status": "completed", "aggregatedOutput": "clean"},
			},
		}},
	}})
	if len(detail.Turns) != 1 || len(detail.Turns[0].Items) != 2 {
		t.Fatalf("unexpected detail shape: %#v", detail)
	}
	if detail.Turns[0].Items[0].Text != "你好" || detail.Turns[0].Items[1].Label != "git status" {
		t.Fatalf("unexpected normalized items: %#v", detail.Turns[0].Items)
	}
}

func TestNormalizePaginatedThreadShapesPreservesTurnStateAndPhases(t *testing.T) {
	detail := normalizeThreadDetail(map[string]any{"thread": map[string]any{
		"id": "thread-paged", "historyMode": "paginated",
		"turns": []map[string]any{{
			"id":          "turn-paged",
			"status":      "failed",
			"startedAt":   float64(1_700_000_000),
			"completedAt": float64(1_700_000_010),
			"error":       map[string]any{"message": "Codex failed"},
			"items": []map[string]any{
				{"id": "user-paged", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "请求"}}},
				{"id": "phase-paged", "type": "agentMessage", "phase": "commentary", "text": "过程"},
				{"id": "answer-paged", "type": "agentMessage", "phase": "final_answer", "text": "最终回答"},
			},
		}},
	}})

	if detail.HistoryMode != "paginated" {
		t.Fatalf("history mode = %q; want paginated", detail.HistoryMode)
	}
	if len(detail.Turns) != 1 {
		t.Fatalf("turn count = %d; want 1", len(detail.Turns))
	}
	turn := detail.Turns[0]
	if turn.TurnID != "turn-paged" || turn.Status != "failed" || turn.Error != "Codex failed" {
		t.Fatalf("turn state was not preserved: %#v", turn)
	}
	if turn.CreatedAt == "" || turn.UpdatedAt == "" {
		t.Fatalf("paginated started/completed timestamps were lost: %#v", turn)
	}
	if len(turn.Items) != 3 || turn.Items[0].Text != "请求" || turn.Items[1].Phase != "commentary" || turn.Items[2].Text != "最终回答" {
		t.Fatalf("message phases/items were not preserved: %#v", turn.Items)
	}
}
