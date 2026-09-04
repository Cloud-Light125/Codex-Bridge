package control

import "testing"

func TestSelectFinalAssistantItemUsesExplicitPhaseAndProtocolOrder(t *testing.T) {
	turn := Turn{Status: "completed", Items: []Item{
		{ItemID: "commentary", Type: "agentMessage", Phase: "commentary", Text: "PROGRESS-1"},
		{ItemID: "unphased", Type: "agentMessage", Text: "WRONG-PROGRESS"},
		{ItemID: "analysis", Role: "assistant", Phase: "analysis", Text: "ANALYSIS"},
		{ItemID: "final-1", Type: "agentMessage", Phase: "final", Text: "FINAL-1"},
		{ItemID: "final-2", Role: "assistant", Phase: "FINAL_ANSWER", Text: "FINAL-2"},
	}}

	item, ok := SelectFinalAssistantItem(turn, FinalSelectionModePaginated)
	if !ok || item.ItemID != "final-2" || item.Text != "FINAL-2" {
		t.Fatalf("selection=%#v ok=%t; want final-2", item, ok)
	}
}

func TestSelectFinalAssistantItemRequiresExplicitFinalForPaginatedHistory(t *testing.T) {
	turn := Turn{Status: "completed", Items: []Item{{
		ItemID: "progress", Type: "agentMessage", Text: "UNPHASED-PROGRESS",
	}}}
	if item, ok := SelectFinalAssistantItem(turn, FinalSelectionModePaginated); ok || item.ItemID != "" {
		t.Fatalf("paginated selection=%#v ok=%t; want no final", item, ok)
	}
	if item, ok := SelectFinalAssistantItem(turn, FinalSelectionModeUnknown); ok || item.ItemID != "" {
		t.Fatalf("unknown selection=%#v ok=%t; want no final", item, ok)
	}
	selection := ResolveFinalAssistantItem(turn, FinalSelectionModePaginated)
	if selection.Found || selection.Result != FinalNotAvailableYet {
		t.Fatalf("paginated unavailable selection=%#v; want %q", selection, FinalNotAvailableYet)
	}
}

func TestSelectFinalAssistantItemAllowsConfirmedLegacyFallback(t *testing.T) {
	turn := Turn{Status: "completed", Items: []Item{{
		ItemID: "legacy", Type: "agentMessage", Text: "LEGACY-FINAL",
	}}}
	item, ok := SelectFinalAssistantItem(turn, FinalSelectionModeLegacy)
	if !ok || item.ItemID != "legacy" || item.Text != "LEGACY-FINAL" {
		t.Fatalf("legacy selection=%#v ok=%t", item, ok)
	}

	turn.Items = append(turn.Items, Item{ItemID: "explicit", Type: "agentMessage", Phase: "answer", Text: "EXPLICIT-FINAL"})
	item, ok = SelectFinalAssistantItem(turn, FinalSelectionModeLegacy)
	if !ok || item.ItemID != "explicit" || item.Text != "EXPLICIT-FINAL" {
		t.Fatalf("explicit selection=%#v ok=%t; explicit final must win", item, ok)
	}
}

func TestSelectFinalAssistantItemRequiresCompletedTurn(t *testing.T) {
	turn := Turn{Status: "inProgress", Items: []Item{{ItemID: "final", Type: "agentMessage", Phase: "final_answer", Text: "not yet"}}}
	if item, ok := SelectFinalAssistantItem(turn, FinalSelectionModePaginated); ok || item.ItemID != "" {
		t.Fatalf("in-progress selection=%#v ok=%t; want no final", item, ok)
	}
}

func TestFinalSelectionHelpersNormalizePhaseAndMode(t *testing.T) {
	for _, phase := range []string{"final_answer", "FINAL", " answer "} {
		if !IsExplicitFinalPhase(phase) {
			t.Fatalf("phase %q was not recognized as explicit final", phase)
		}
	}
	for _, phase := range []string{"", "commentary", "analysis", "reasoning", "progress", "unknown"} {
		if IsExplicitFinalPhase(phase) {
			t.Fatalf("phase %q was incorrectly recognized as explicit final", phase)
		}
	}
	if FinalSelectionModeForHistory("") != FinalSelectionModeUnknown || FinalSelectionModeForHistory("legacy") != FinalSelectionModeLegacy || FinalSelectionModeForHistory("PAGINATED") != FinalSelectionModePaginated {
		t.Fatal("history mode normalization is incorrect")
	}
}

func TestFinalAssistantItemIDIsStableWhenProtocolOmitsItemID(t *testing.T) {
	item := Item{Text: "CORRECT-FINAL"}
	first := FinalAssistantItemID("turn-1", item)
	second := FinalAssistantItemID("turn-1", item)
	if first == "" || first != second {
		t.Fatalf("fallback final item id is not stable: %q vs %q", first, second)
	}
	if FinalAssistantItemID("turn-1", Item{ItemID: "protocol-id", Text: item.Text}) != "protocol-id" {
		t.Fatal("protocol item ID must take precedence over fallback")
	}
}
