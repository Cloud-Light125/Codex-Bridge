package appserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

// ThreadTurnsListOptions and ThreadItemsListOptions intentionally mirror the
// current app-server request fields. Cursor values are opaque and must be
// passed back verbatim to the server.
type ThreadTurnsListOptions struct {
	ThreadID      string
	Cursor        string
	Limit         int
	SortDirection string
	ItemsView     string
}

type ThreadItemsListOptions struct {
	ThreadID      string
	TurnID        string
	Cursor        string
	Limit         int
	SortDirection string
}

// ThreadHistoryRPC is the read-only protocol surface required by the
// paginated history reader. Client implements it directly; the interface also
// makes the protocol chain independently testable with a fake app-server.
type ThreadHistoryRPC interface {
	ThreadRead(context.Context, string, bool) (map[string]any, error)
	ThreadTurnsList(context.Context, ThreadTurnsListOptions) (map[string]any, error)
	ThreadItemsList(context.Context, ThreadItemsListOptions) (map[string]any, error)
}

type HistoryCapability uint8

const (
	HistoryCapabilityUnknown HistoryCapability = iota
	HistoryCapabilityPaginatedSupported
	HistoryCapabilityLegacyOnly
)

const (
	// The desktop initially shows a bounded recent window. Callers that need a
	// smaller window (for example /history 3) pass that limit explicitly.
	DefaultHistoryTurnLimit  = 30
	ActivityHistoryTurnLimit = 1
	maxHistoryTurnLimit      = 50
	maxHistoryPageCount      = 100
	maxHistoryItemCount      = 300
	maxHistoryItemsPerTurn   = 100
	historyTurnPageLimit     = 50
	historyItemPageLimit     = 100
)

var ErrPaginatedHistoryUnavailable = errors.New("paginated Codex thread history is unavailable")

// HistoryReader implements the compatibility policy once per app-server
// session. It reads metadata first, then chooses either the legacy full read
// or the paginated turns/items chain. It never probes includeTurns=true after
// the server has successfully demonstrated paginated support.
type HistoryReader struct {
	rpc    ThreadHistoryRPC
	logger *bridgelog.SafeLogger

	mu                sync.RWMutex
	probeMu           sync.Mutex
	capability        HistoryCapability
	capabilityError   error
	loggedModes       map[string]bool
	unsupportedLogged bool
}

func NewHistoryReader(rpc ThreadHistoryRPC, logger *bridgelog.SafeLogger) *HistoryReader {
	return &HistoryReader{rpc: rpc, logger: logger, loggedModes: make(map[string]bool)}
}

func (r *HistoryReader) Capability() HistoryCapability {
	if r == nil {
		return HistoryCapabilityUnknown
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.capability
}

// Reset starts a fresh capability cache for a newly started app-server
// process. Capabilities are session-scoped: a restarted Codex binary may be a
// different version or may expose a different experimental API surface.
func (r *HistoryReader) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.capability = HistoryCapabilityUnknown
	r.capabilityError = nil
	r.loggedModes = make(map[string]bool)
	r.unsupportedLogged = false
	r.mu.Unlock()
}

// ReadThread loads at most limit recent Turns and their persisted Items.
// limit <= 0 uses DefaultHistoryTurnLimit.
func (r *HistoryReader) ReadThread(ctx context.Context, threadID string, limit int) (map[string]any, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, errors.New("thread ID is required")
	}
	metadata, err := r.rpc.ThreadRead(ctx, threadID, false)
	if err != nil {
		return nil, err
	}
	return r.readWithMetadata(ctx, metadata, threadID, normalizeHistoryTurnLimit(limit))
}

// ReadActivity reads only metadata and, when the thread is paginated, the
// newest Turn with itemsView=notLoaded. Legacy servers have no equivalent
// lightweight turn RPC, so their metadata-only status is returned as-is.
func (r *HistoryReader) ReadActivity(ctx context.Context, threadID string) (map[string]any, error) {
	return r.readActivity(ctx, threadID, ActivityHistoryTurnLimit, false)
}

// ReadActivityHistory returns metadata and a bounded set of recent Turns
// without loading any Items. It is useful for status surfaces such as
// /failed, which need Turn status/error fields but never message bodies.
func (r *HistoryReader) ReadActivityHistory(ctx context.Context, threadID string, limit int) (map[string]any, error) {
	return r.readActivity(ctx, threadID, normalizeHistoryTurnLimit(limit), true)
}

func (r *HistoryReader) readActivity(ctx context.Context, threadID string, limit int, legacyNeedsTurns bool) (map[string]any, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, errors.New("thread ID is required")
	}
	metadata, err := r.rpc.ThreadRead(ctx, threadID, false)
	if err != nil {
		return nil, err
	}
	mode := historyMode(metadata)
	if mode == "legacy" && legacyNeedsTurns {
		return r.readLegacy(ctx, threadID)
	}
	if r.Capability() == HistoryCapabilityLegacyOnly && legacyNeedsTurns {
		if mode == "paginated" {
			return nil, r.paginatedUnavailable(threadID)
		}
		return r.readLegacy(ctx, threadID)
	}
	if mode != "legacy" && r.Capability() != HistoryCapabilityLegacyOnly {
		if r.Capability() == HistoryCapabilityUnknown {
			r.probeMu.Lock()
			defer r.probeMu.Unlock()
		}
		if r.Capability() == HistoryCapabilityLegacyOnly {
			if mode == "paginated" {
				return nil, r.paginatedUnavailable(threadID)
			}
			if legacyNeedsTurns {
				return r.readLegacy(ctx, threadID)
			}
			return metadata, nil
		}
		history, pageErr := r.readPaginated(ctx, metadata, threadID, limit, false)
		if pageErr == nil {
			if mode == "" {
				r.noteMode("paginated")
			}
			return history, nil
		}
		if isHistoryUnsupportedError(pageErr) {
			if mode == "paginated" {
				r.setCapability(HistoryCapabilityLegacyOnly, pageErr)
				r.noteUnsupported(threadID, pageErr)
				if legacyNeedsTurns {
					return nil, r.paginatedUnavailable(threadID)
				}
				return metadata, nil
			}
			if legacyNeedsTurns {
				if legacy, legacyErr := r.readLegacy(ctx, threadID); legacyErr == nil {
					r.setCapability(HistoryCapabilityLegacyOnly, pageErr)
					r.noteMode("legacy")
					return legacy, nil
				}
			}
			// Activity is deliberately best effort when an old server exposes
			// paginated metadata but has not implemented the experimental RPCs.
			// The metadata remains authoritative for the list/status surface.
			r.setCapability(HistoryCapabilityLegacyOnly, pageErr)
			r.noteUnsupported(threadID, pageErr)
			return metadata, nil
		}
		return nil, pageErr
	}
	return metadata, nil
}

func (r *HistoryReader) readWithMetadata(ctx context.Context, metadata map[string]any, threadID string, limit int) (map[string]any, error) {
	mode := historyMode(metadata)
	if mode != "" {
		r.noteMode(mode)
	}

	switch mode {
	case "legacy":
		return r.readLegacy(ctx, threadID)
	case "paginated":
		return r.readPaginatedMode(ctx, metadata, threadID, limit)
	}

	// Older servers did not return historyMode. Use the cached server
	// capability when available; only an unknown session performs the one
	// compatibility probe that may call includeTurns=true.
	return r.readUnknownMode(ctx, metadata, threadID, limit)
}

func (r *HistoryReader) readPaginatedMode(ctx context.Context, metadata map[string]any, threadID string, limit int) (map[string]any, error) {
	if r.Capability() == HistoryCapabilityUnknown {
		r.probeMu.Lock()
		defer r.probeMu.Unlock()
	}
	switch r.Capability() {
	case HistoryCapabilityPaginatedSupported:
		return r.readPaginated(ctx, metadata, threadID, limit, true)
	case HistoryCapabilityLegacyOnly:
		return nil, r.paginatedUnavailable(threadID)
	}
	history, err := r.readPaginated(ctx, metadata, threadID, limit, true)
	if err == nil {
		return history, nil
	}
	if isHistoryUnsupportedError(err) {
		r.setCapability(HistoryCapabilityLegacyOnly, err)
		return nil, r.paginatedUnavailable(threadID)
	}
	return nil, err
}

func (r *HistoryReader) readUnknownMode(ctx context.Context, metadata map[string]any, threadID string, limit int) (map[string]any, error) {
	if r.Capability() == HistoryCapabilityUnknown {
		r.probeMu.Lock()
		defer r.probeMu.Unlock()
	}
	// Another concurrent reader may have established the capability while
	// this call was waiting for the probe lock.
	switch r.Capability() {
	case HistoryCapabilityPaginatedSupported:
		return r.readPaginated(ctx, metadata, threadID, limit, true)
	case HistoryCapabilityLegacyOnly:
		return r.readLegacy(ctx, threadID)
	}

	legacy, err := r.rpc.ThreadRead(ctx, threadID, true)
	if err == nil {
		r.setCapability(HistoryCapabilityLegacyOnly, nil)
		r.noteMode("legacy")
		return legacy, nil
	}
	if !isPaginatedReadUnsupportedError(err) {
		return nil, err
	}

	// The exact paginated includeTurns error is a mode probe, not a terminal
	// history failure. Continue with the real paging chain.
	history, pageErr := r.readPaginated(ctx, metadata, threadID, limit, true)
	if pageErr == nil {
		return history, nil
	}
	if !isHistoryUnsupportedError(pageErr) {
		return nil, pageErr
	}
	return r.readLegacyAfterUnsupported(ctx, threadID, pageErr)
}

func (r *HistoryReader) readLegacy(ctx context.Context, threadID string) (map[string]any, error) {
	return r.rpc.ThreadRead(ctx, threadID, true)
}

func (r *HistoryReader) paginatedUnavailable(threadID string) error {
	return fmt.Errorf("%w: thread %s reports paginated history but the server does not expose turns/items paging", ErrPaginatedHistoryUnavailable, shortHistoryID(threadID))
}

func (r *HistoryReader) readLegacyAfterUnsupported(ctx context.Context, threadID string, pagingErr error) (map[string]any, error) {
	r.setCapability(HistoryCapabilityLegacyOnly, pagingErr)
	legacy, err := r.readLegacy(ctx, threadID)
	if err != nil {
		// Preserve the actual legacy RPC error. In particular, a server that
		// advertises a paginated record but implements neither history path
		// must not look like a successful empty history.
		return nil, err
	}
	r.noteMode("legacy")
	return legacy, nil
}

func (r *HistoryReader) readPaginated(ctx context.Context, metadata map[string]any, threadID string, limit int, includeItems bool) (map[string]any, error) {
	turns, err := r.readTurns(ctx, threadID, limit)
	if err != nil {
		return nil, err
	}
	remainingItems := maxHistoryItemCount
	for index := range turns {
		if includeItems {
			itemLimit := minInt(maxHistoryItemsPerTurn, remainingItems)
			items, itemErr := r.readItems(ctx, threadID, turnID(turns[index]), itemLimit)
			if itemErr != nil {
				return nil, itemErr
			}
			turns[index]["items"] = items
			remainingItems -= len(items)
		} else {
			// Preserve the existing DTO shape while making it explicit that
			// Activity intentionally did not load any Items.
			turns[index]["items"] = []map[string]any{}
		}
	}
	r.setCapability(HistoryCapabilityPaginatedSupported, nil)
	return withTurns(metadata, turns), nil
}

func (r *HistoryReader) readTurns(ctx context.Context, threadID string, limit int) ([]map[string]any, error) {
	limit = normalizeHistoryTurnLimit(limit)
	turns := make([]map[string]any, 0, limit)
	seenTurns := make(map[string]bool)
	seenCursors := make(map[string]bool)
	cursor := ""
	pageCount := 0
	for pageCount < maxHistoryPageCount && len(turns) < limit {
		pageCount++
		page, err := r.rpc.ThreadTurnsList(ctx, ThreadTurnsListOptions{
			ThreadID: threadID, Cursor: cursor, Limit: minInt(historyTurnPageLimit, limit-len(turns)),
			SortDirection: "desc", ItemsView: "notLoaded",
		})
		if err != nil {
			if isHistoryUnsupportedError(err) {
				return nil, &historyUnsupportedError{err: err}
			}
			return nil, err
		}
		for _, rawTurn := range historyObjects(firstHistoryValue(page, "data", "items")) {
			id := turnID(rawTurn)
			if id != "" && seenTurns[id] {
				continue
			}
			if id != "" {
				seenTurns[id] = true
			}
			turns = append(turns, cloneHistoryMap(rawTurn))
			if len(turns) >= limit {
				break
			}
		}
		next := historyString(page, "nextCursor", "next_cursor")
		if next == "" {
			break
		}
		if next == cursor || seenCursors[next] {
			r.warnCursor("turns", threadID, cursor, next)
			break
		}
		seenCursors[next] = true
		cursor = next
	}
	if pageCount >= maxHistoryPageCount && cursor != "" && len(turns) < limit {
		r.warnPageLimit("turns", threadID)
	}

	// The protocol returns desc pages. The DTO contract remains chronological.
	for left, right := 0, len(turns)-1; left < right; left, right = left+1, right-1 {
		turns[left], turns[right] = turns[right], turns[left]
	}
	return turns, nil
}

func (r *HistoryReader) readItems(ctx context.Context, threadID, turnID string, limit int) ([]map[string]any, error) {
	if strings.TrimSpace(turnID) == "" || limit <= 0 {
		return []map[string]any{}, nil
	}
	items := make([]map[string]any, 0)
	seenItems := make(map[string]bool)
	seenCursors := make(map[string]bool)
	cursor := ""
	pageCount := 0
	for pageCount < maxHistoryPageCount && len(items) < limit {
		pageCount++
		page, err := r.rpc.ThreadItemsList(ctx, ThreadItemsListOptions{
			ThreadID: threadID, TurnID: turnID, Cursor: cursor,
			Limit: minInt(historyItemPageLimit, limit-len(items)), SortDirection: "asc",
		})
		if err != nil {
			if isHistoryUnsupportedError(err) {
				return nil, &historyUnsupportedError{err: err}
			}
			return nil, err
		}
		for _, entry := range historyObjects(firstHistoryValue(page, "data", "items")) {
			item := historyItem(entry)
			id := historyString(item, "id", "itemId", "item_id")
			if id != "" && seenItems[id] {
				continue
			}
			if id != "" {
				seenItems[id] = true
			}
			items = append(items, item)
			if len(items) >= limit {
				break
			}
		}
		next := historyString(page, "nextCursor", "next_cursor")
		if next == "" {
			break
		}
		if next == cursor || seenCursors[next] {
			r.warnCursor("items", threadID, cursor, next)
			break
		}
		seenCursors[next] = true
		cursor = next
	}
	if pageCount >= maxHistoryPageCount && cursor != "" {
		r.warnPageLimit("items", threadID)
	}
	return items, nil
}

type historyUnsupportedError struct{ err error }

func (e *historyUnsupportedError) Error() string { return e.err.Error() }
func (e *historyUnsupportedError) Unwrap() error { return e.err }

func isHistoryUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		if rpcErr.Code == -32601 {
			return true
		}
		message := strings.ToLower(rpcErr.Message)
		return strings.Contains(message, "not supported") || strings.Contains(message, "unsupported")
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not supported") || strings.Contains(message, "unsupported")
}

func isPaginatedReadUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		message := strings.ToLower(rpcErr.Message)
		message = strings.ReplaceAll(message, " ", "")
		return (rpcErr.Code == -32600 && strings.Contains(message, "paginatedthreadsdonotsupportthread/read(includeturns=true)")) ||
			(rpcErr.Code == -32601 && strings.Contains(message, "paginated"))
	}
	message := strings.ToLower(err.Error())
	message = strings.ReplaceAll(message, " ", "")
	return strings.Contains(message, "paginatedthreadsdonotsupportthread/read(includeturns=true)")
}

func (r *HistoryReader) setCapability(value HistoryCapability, cause error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capability = value
	if value == HistoryCapabilityLegacyOnly && cause != nil {
		r.capabilityError = fmt.Errorf("%w: %v", ErrPaginatedHistoryUnavailable, cause)
	}
}

func (r *HistoryReader) cachedUnavailable() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.capabilityError != nil {
		return r.capabilityError
	}
	return ErrPaginatedHistoryUnavailable
}

func (r *HistoryReader) noteMode(mode string) {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" || r == nil {
		return
	}
	r.mu.Lock()
	if r.loggedModes[mode] {
		r.mu.Unlock()
		return
	}
	r.loggedModes[mode] = true
	r.mu.Unlock()
	if r.logger != nil {
		r.logger.Printf("Codex history mode: %s", mode)
	}
}

func (r *HistoryReader) noteUnsupported(threadID string, err error) {
	// Keep this diagnostic bounded to one message per server session. The
	// thread ID is included only as a short safe diagnostic, never message data.
	r.mu.Lock()
	if r.unsupportedLogged {
		r.mu.Unlock()
		return
	}
	r.unsupportedLogged = true
	if r.capabilityError == nil {
		r.capabilityError = fmt.Errorf("%w: %v", ErrPaginatedHistoryUnavailable, err)
	}
	r.mu.Unlock()
	if r.logger != nil {
		r.logger.Printf("Codex paginated history unavailable threadId=%s error=%s", shortHistoryID(threadID), err)
	}
}

func (r *HistoryReader) warnCursor(kind, threadID, current, next string) {
	if r.logger != nil {
		r.logger.Printf("Codex history warning: repeated %s cursor threadId=%s currentCursor=%s nextCursor=%s", kind, shortHistoryID(threadID), cursorDiagnostic(current), cursorDiagnostic(next))
	}
}

func (r *HistoryReader) warnPageLimit(kind, threadID string) {
	if r.logger != nil {
		r.logger.Printf("Codex history warning: %s page limit reached threadId=%s", kind, shortHistoryID(threadID))
	}
}

func historyMode(raw map[string]any) string {
	payload := raw
	if nested, ok := raw["thread"].(map[string]any); ok {
		payload = nested
	}
	return strings.ToLower(strings.TrimSpace(historyString(payload, "historyMode", "history_mode")))
}

func withTurns(raw map[string]any, turns []map[string]any) map[string]any {
	result := cloneHistoryMap(raw)
	if nested, ok := raw["thread"].(map[string]any); ok {
		result["thread"] = cloneHistoryMap(nested)
		result["thread"].(map[string]any)["turns"] = turns
		return result
	}
	result["turns"] = turns
	return result
}

func historyItem(entry map[string]any) map[string]any {
	if item, ok := entry["item"].(map[string]any); ok {
		return cloneHistoryMap(item)
	}
	return cloneHistoryMap(entry)
}

func turnID(raw map[string]any) string {
	return historyString(raw, "id", "turnId", "turn_id")
}

func firstHistoryValue(raw map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := raw[key]; ok && value != nil {
			return value
		}
	}
	return nil
}

func historyString(raw map[string]any, keys ...string) string {
	value := firstHistoryValue(raw, keys...)
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func historyObjects(value any) []map[string]any {
	result := []map[string]any{}
	switch values := value.(type) {
	case []any:
		for _, value := range values {
			if object, ok := value.(map[string]any); ok {
				result = append(result, object)
			}
		}
	case []map[string]any:
		result = append(result, values...)
	}
	return result
}

func cloneHistoryMap(raw map[string]any) map[string]any {
	result := make(map[string]any, len(raw))
	for key, value := range raw {
		result[key] = value
	}
	return result
}

func normalizeHistoryTurnLimit(limit int) int {
	if limit <= 0 {
		return DefaultHistoryTurnLimit
	}
	if limit > maxHistoryTurnLimit {
		return maxHistoryTurnLimit
	}
	return limit
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func shortHistoryID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 12 {
		return value
	}
	return value[:12] + "…"
}

func cursorDiagnostic(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "<empty>"
	}
	if len(value) > 16 {
		return value[:16] + "…"
	}
	return value
}
