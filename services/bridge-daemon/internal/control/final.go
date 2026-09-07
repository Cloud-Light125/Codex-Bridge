package control

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const FinalNotAvailableYet = "FINAL_NOT_AVAILABLE_YET"

// FinalSelectionMode is the protocol/history compatibility mode that governs
// whether an assistant item without a phase can be used as a legacy answer.
// Unknown is intentionally strict: an absent mode is not evidence that a
// server is legacy.
type FinalSelectionMode uint8

const (
	FinalSelectionModeUnknown FinalSelectionMode = iota
	FinalSelectionModePaginated
	FinalSelectionModeLegacy
)

// FinalAssistantSelection is the diagnostic form of final selection. Callers
// that only need the item can use SelectFinalAssistantItem.
type FinalAssistantSelection struct {
	Item     Item
	Found    bool
	Explicit bool
	Result   string
}

// FinalSelectionModeForHistory converts the app-server compatibility marker
// into the selector mode. Unknown values remain strict.
func FinalSelectionModeForHistory(historyMode string) FinalSelectionMode {
	switch strings.ToLower(strings.TrimSpace(historyMode)) {
	case "paginated":
		return FinalSelectionModePaginated
	case "legacy":
		return FinalSelectionModeLegacy
	default:
		return FinalSelectionModeUnknown
	}
}

// IsExplicitFinalPhase reports whether phase is one of the protocol's
// terminal assistant phases. Matching is case-insensitive and ignores
// surrounding whitespace.
func IsExplicitFinalPhase(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "final_answer", "final", "answer":
		return true
	default:
		return false
	}
}

// IsAssistantMessageItem identifies an assistant message candidate without
// deciding whether that candidate is a final answer.
func IsAssistantMessageItem(item Item) bool {
	return strings.EqualFold(strings.TrimSpace(item.Type), "agentMessage") ||
		strings.EqualFold(strings.TrimSpace(item.Role), "assistant")
}

// FinalAssistantItemID returns the protocol item ID when present and the
// stable final-item fallback ID used by both App Server history and rollout
// discovery when a source omitted that ID. It must only be called after final
// selection has accepted the item.
func FinalAssistantItemID(turnID string, item Item) string {
	if id := strings.TrimSpace(item.ItemID); id != "" {
		return id
	}
	hash := sha256.Sum256([]byte(strings.Join([]string{strings.TrimSpace(turnID), "assistant", strings.TrimSpace(item.Text)}, "\x00")))
	return hex.EncodeToString(hash[:12])
}

// SelectFinalAssistantItem returns the formal final assistant item for a
// completed Turn. Explicit terminal phases always win, and the last explicit
// item in protocol order is selected when more than one is present. A legacy
// unphased fallback is allowed only for a confirmed Legacy history mode.
func SelectFinalAssistantItem(turn Turn, mode FinalSelectionMode) (Item, bool) {
	selection := ResolveFinalAssistantItem(turn, mode)
	return selection.Item, selection.Found
}

// FindPersistedAssistantEvidence is intentionally separate from the mirror
// selector. Persistence verification needs evidence that a completed Turn has
// an assistant message recorded, while remote delivery must continue to use
// SelectFinalAssistantItem and its strict phase rules. In particular, this
// helper never authorizes a commentary/progress item for delivery.
func FindPersistedAssistantEvidence(turn Turn, mode FinalSelectionMode) (Item, bool) {
	return SelectFinalAssistantItem(turn, mode)
}

// ResolveFinalAssistantItem is the explanatory form used by diagnostics and
// candidate-selection logs. It deliberately does not inspect message text
// beyond requiring a non-empty body for a usable item.
func ResolveFinalAssistantItem(turn Turn, mode FinalSelectionMode) FinalAssistantSelection {
	if !strings.EqualFold(strings.TrimSpace(turn.Status), "completed") {
		return FinalAssistantSelection{Result: "turn_not_completed"}
	}

	var explicit Item
	explicitFound := false
	var legacy Item
	legacyFound := false
	for _, item := range turn.Items {
		if !IsAssistantMessageItem(item) || strings.TrimSpace(item.Text) == "" {
			continue
		}
		if IsExplicitFinalPhase(item.Phase) {
			explicit = item
			explicitFound = true
			continue
		}
		if strings.TrimSpace(item.Phase) == "" {
			legacy = item
			legacyFound = true
		}
	}
	if explicitFound {
		return FinalAssistantSelection{Item: explicit, Found: true, Explicit: true, Result: "selected"}
	}
	if mode == FinalSelectionModeLegacy && legacyFound {
		return FinalAssistantSelection{Item: legacy, Found: true, Result: "selected"}
	}
	return FinalAssistantSelection{Result: FinalNotAvailableYet}
}
