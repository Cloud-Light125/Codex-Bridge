package control

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

func normalizeThreadList(raw map[string]any) ThreadList {
	items := mapSlice(firstValue(raw, "data", "items", "threads", "results"))
	threads := make([]ThreadSummary, 0, len(items))
	for _, item := range items {
		threads = append(threads, normalizeThreadSummary(item))
	}
	return ThreadList{Threads: threads, NextCursor: stringValue(firstValue(raw, "nextCursor", "next_cursor"))}
}

func NormalizeThreadList(raw map[string]any) ThreadList { return normalizeThreadList(raw) }

func normalizeThreadDetail(raw map[string]any) ThreadDetail {
	payload := raw
	if nested, ok := raw["thread"].(map[string]any); ok {
		payload = nested
	}
	detail := ThreadDetail{ThreadSummary: normalizeThreadSummary(payload), Turns: []Turn{}}
	for _, turn := range mapSlice(payload["turns"]) {
		detail.Turns = append(detail.Turns, normalizeTurn(turn))
	}
	// The paginated app-server returns pages in descending order.  Keep the
	// public DTO contract chronological regardless of whether the source was a
	// paginated response, a legacy full read, or a test double.  Callers may
	// therefore safely use Turns[len(Turns)-1] as the newest Turn.
	sort.SliceStable(detail.Turns, func(left, right int) bool {
		leftAt, leftOK := turnTime(detail.Turns[left])
		rightAt, rightOK := turnTime(detail.Turns[right])
		if leftOK && rightOK && !leftAt.Equal(rightAt) {
			return leftAt.Before(rightAt)
		}
		return false
	})
	return detail
}

func turnTime(turn Turn) (time.Time, bool) {
	for _, value := range []string{turn.CreatedAt, turn.UpdatedAt} {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func NormalizeThreadDetail(raw map[string]any) ThreadDetail { return normalizeThreadDetail(raw) }

func normalizeThreadSummary(raw map[string]any) ThreadSummary {
	payload := raw
	if nested, ok := raw["thread"].(map[string]any); ok {
		payload = nested
	}
	id := stringValue(firstValue(payload, "id", "threadId", "thread_id"))
	preview := stringValue(firstValue(payload, "preview", "summary"))
	title := stringValue(firstValue(payload, "name", "title"))
	if title == "" {
		title = firstNonEmpty(firstLine(preview), id)
	}
	var archived *bool
	if value, ok := boolValue(payload["archived"]); ok {
		archived = &value
	}
	return ThreadSummary{
		ThreadID: id, Title: title, Summary: preview, CWD: stringValue(payload["cwd"]),
		Model:       stringValue(firstValue(payload, "model", "modelId", "model_id")),
		CreatedAt:   timeValue(firstValue(payload, "createdAt", "created_at")),
		UpdatedAt:   timeValue(firstValue(payload, "updatedAt", "updated_at")),
		HistoryMode: stringValue(firstValue(payload, "historyMode", "history_mode")),
		Archived:    archived, Status: statusValue(payload["status"]),
		SessionID:   stringValue(firstValue(payload, "sessionId", "session_id")),
		SourceKind:  sourceKindValue(firstValue(payload, "source", "threadSource", "thread_source")),
		RolloutPath: stringValue(firstValue(payload, "path", "rolloutPath", "rollout_path")),
		Ephemeral:   booleanValue(payload["ephemeral"]),
	}
}

func normalizeTurn(raw map[string]any) Turn {
	turn := Turn{
		TurnID: stringValue(firstValue(raw, "id", "turnId", "turn_id")), Status: statusValue(raw["status"]),
		CreatedAt: timeValue(firstValue(raw, "createdAt", "created_at", "startedAt", "started_at")),
		UpdatedAt: timeValue(firstValue(raw, "updatedAt", "updated_at", "completedAt", "completed_at")),
		Error:     turnErrorText(raw["error"]),
		Items:     []Item{},
	}
	for _, rawItem := range mapSlice(raw["items"]) {
		if strings.Contains(strings.ToLower(stringValue(rawItem["type"])), "reasoning") {
			continue
		}
		turn.Items = append(turn.Items, normalizeItem(rawItem))
	}
	return turn
}

func turnErrorText(value any) string {
	if text := stringValue(value); text != "" {
		return text
	}
	object, _ := value.(map[string]any)
	if object == nil {
		return ""
	}
	return stringValue(object["message"])
}

func normalizeItem(raw map[string]any) Item {
	itemType := stringValue(raw["type"])
	item := Item{
		ItemID: stringValue(firstValue(raw, "id", "itemId", "item_id")), Type: itemType,
		Role: stringValue(raw["role"]), Phase: stringValue(raw["phase"]), Status: optionalStatusValue(raw["status"]),
	}
	switch itemType {
	case "userMessage", "agentMessage":
		item.Text = messageText(raw)
	case "plan":
		item.Text = firstNonEmpty(stringValue(raw["text"]), stringValue(raw["plan"]))
	case "commandExecution":
		item.Label = commandText(raw["command"])
		item.Output = stringValue(firstValue(raw, "aggregatedOutput", "output"))
	case "fileChange":
		item.Label, item.Output = fileChangeText(raw)
	case "dynamicToolCall", "mcpToolCall", "webSearch", "collabAgentToolCall":
		item.Label = firstNonEmpty(stringValue(firstValue(raw, "tool", "name", "query")), itemType)
		item.Output = stringValue(firstValue(raw, "output", "result"))
	default:
		item.Text = stringValue(raw["text"])
	}
	return item
}

func optionalStatusValue(value any) string {
	if value == nil {
		return ""
	}
	return statusValue(value)
}

func messageText(raw map[string]any) string {
	if direct := stringValue(raw["text"]); direct != "" {
		return direct
	}
	content, ok := raw["content"].([]any)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(content))
	for _, value := range content {
		part, _ := value.(map[string]any)
		if text := stringValue(firstValue(part, "text", "value")); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func commandText(value any) string {
	if text := stringValue(value); text != "" {
		return text
	}
	values, _ := value.([]any)
	parts := make([]string, 0, len(values))
	for _, raw := range values {
		if text := stringValue(raw); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

func fileChangeText(raw map[string]any) (string, string) {
	if path := stringValue(raw["path"]); path != "" {
		return path, stringValue(firstValue(raw, "diff", "output"))
	}
	paths := []string{}
	diffs := []string{}
	for _, change := range mapSlice(raw["changes"]) {
		if path := stringValue(change["path"]); path != "" {
			paths = append(paths, path)
		}
		if diff := stringValue(change["diff"]); diff != "" {
			diffs = append(diffs, diff)
		}
	}
	return firstNonEmpty(strings.Join(paths, ", "), "文件修改"), strings.Join(diffs, "\n")
}

func statusValue(value any) string {
	if text := stringValue(value); text != "" {
		return text
	}
	object, ok := value.(map[string]any)
	if !ok {
		return "unknown"
	}
	statusType := stringValue(object["type"])
	flags, _ := object["activeFlags"].([]any)
	flagText := make([]string, 0, len(flags))
	for _, flag := range flags {
		if text := stringValue(flag); text != "" {
			flagText = append(flagText, text)
		}
	}
	if len(flagText) > 0 {
		return fmt.Sprintf("%s[%s]", firstNonEmpty(statusType, "active"), strings.Join(flagText, ","))
	}
	return firstNonEmpty(statusType, "unknown")
}

func timeValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return unixTime(int64(typed))
	case json.Number:
		integer, _ := typed.Int64()
		return unixTime(integer)
	case int64:
		return unixTime(typed)
	case int:
		return unixTime(int64(typed))
	default:
		return ""
	}
}

func unixTime(value int64) string {
	if value <= 0 {
		return ""
	}
	if value > 10_000_000_000 {
		value /= 1000
	}
	return time.Unix(value, 0).UTC().Format(time.RFC3339)
}

func mapSlice(value any) []map[string]any {
	result := []map[string]any{}
	switch raw := value.(type) {
	case []any:
		result = make([]map[string]any, 0, len(raw))
		for _, item := range raw {
			if object, ok := item.(map[string]any); ok {
				result = append(result, object)
			}
		}
	case []map[string]any:
		result = append(result, raw...)
	}
	return result
}

func firstValue(raw map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := raw[key]; ok && value != nil {
			return value
		}
	}
	return nil
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func boolValue(value any) (bool, bool) {
	typed, ok := value.(bool)
	return typed, ok
}

func booleanValue(value any) bool {
	typed, _ := boolValue(value)
	return typed
}

func sourceKindValue(value any) string {
	if text := stringValue(value); text != "" {
		return text
	}
	object, _ := value.(map[string]any)
	if object == nil {
		return ""
	}
	return stringValue(firstValue(object, "kind", "type", "source"))
}

func firstLine(value string) string {
	if index := strings.IndexAny(value, "\r\n"); index >= 0 {
		value = value[:index]
	}
	return strings.TrimSpace(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
