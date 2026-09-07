package control

import "strings"

func ActivityFromThreadRead(raw map[string]any) ThreadActivity {
	payload := raw
	if nested, ok := raw["thread"].(map[string]any); ok {
		payload = nested
	}
	activity := ThreadActivity{
		ThreadID:    stringValue(firstValue(payload, "id", "threadId", "thread_id")),
		Status:      statusValue(payload["status"]),
		HistoryMode: strings.ToLower(strings.TrimSpace(stringValue(firstValue(payload, "historyMode", "history_mode")))),
	}
	if archived, ok := boolValue(payload["archived"]); ok {
		activity.Archived = archived
	}
	statusLower := strings.ToLower(activity.Status)
	activity.WaitingApproval = strings.Contains(statusLower, "waitingonapproval")
	activity.WaitingInput = strings.Contains(statusLower, "waitingonuserinput") || strings.Contains(statusLower, "waitingoninput")
	activity.Active = strings.HasPrefix(statusLower, "active") || strings.Contains(statusLower, "inprogress") || activity.WaitingApproval || activity.WaitingInput

	turns := mapSlice(payload["turns"])
	if len(turns) == 0 {
		return activity
	}
	last := turns[len(turns)-1]
	activity.TurnID = stringValue(firstValue(last, "id", "turnId", "turn_id"))
	activity.LatestTurnID = activity.TurnID
	turnStatus := strings.ToLower(statusValue(last["status"]))
	activity.LatestTurnStatus = statusValue(last["status"])
	if strings.Contains(turnStatus, "inprogress") || strings.Contains(turnStatus, "running") {
		activity.Active = true
	}
	if strings.Contains(turnStatus, "waitingonapproval") {
		activity.WaitingApproval = true
	}
	if strings.Contains(turnStatus, "waitingonuserinput") || strings.Contains(turnStatus, "waitingoninput") {
		activity.WaitingInput = true
	}
	if turnStatus == "completed" || turnStatus == "failed" || turnStatus == "interrupted" || turnStatus == "cancelled" || turnStatus == "canceled" {
		if !activity.WaitingApproval && !activity.WaitingInput && !strings.HasPrefix(statusLower, "active") {
			activity.Active = false
		}
	}
	return activity
}
