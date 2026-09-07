package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
)

var ErrUnavailable = errors.New("Codex app-server is unavailable")

const DefaultHistoryTurnLimit = 50

type ThreadReader interface {
	ThreadList(ctx context.Context, limit int, cursor string) (map[string]any, error)
	ThreadRead(ctx context.Context, threadID string, includeTurns bool) (map[string]any, error)
}

// ThreadHistoryReader is implemented by the runtime's app-server adapter.
// Keeping it optional preserves source compatibility for legacy test doubles
// and non-Codex readers while ensuring the real app-server path never asks a
// paginated thread for thread/read(includeTurns=true).
type ThreadHistoryReader interface {
	ThreadReadHistory(ctx context.Context, threadID string, limit int) (map[string]any, error)
}

// ThreadTurnReader is the single-Turn, full-item read path used by explicit
// output queries. The optional interface keeps older test doubles usable.
type ThreadTurnReader interface {
	ThreadReadTurn(ctx context.Context, threadID, turnID string) (map[string]any, error)
}

type ThreadActivityReader interface {
	ThreadReadActivity(ctx context.Context, threadID string) (map[string]any, error)
}

type ThreadActivityHistoryReader interface {
	ThreadReadActivityHistory(ctx context.Context, threadID string, limit int) (map[string]any, error)
}

type DetailReader interface {
	ReadThread(ctx context.Context, threadID string, includeTurns bool) (ThreadDetail, error)
}

type DetailHistoryReader interface {
	ReadThreadHistory(ctx context.Context, threadID string, limit int) (ThreadDetail, error)
}

type DetailTurnReader interface {
	ReadThreadTurnOutput(ctx context.Context, threadID, turnID string) (ThreadDetail, error)
}

type DetailActivityReader interface {
	ReadThreadActivity(ctx context.Context, threadID string) (ThreadDetail, error)
}

type DetailActivityHistoryReader interface {
	ReadThreadActivityHistory(ctx context.Context, threadID string, limit int) (ThreadDetail, error)
}

// ReadThreadHistory keeps the compatibility decision at the control boundary
// so channels, query services, Mirror, and Task Center do not implement RPC
// pagination independently.
func ReadThreadHistory(ctx context.Context, reader DetailReader, threadID string, limit int) (ThreadDetail, error) {
	if history, ok := reader.(DetailHistoryReader); ok {
		return history.ReadThreadHistory(ctx, threadID, limit)
	}
	return reader.ReadThread(ctx, threadID, true)
}

// ReadThreadActivity is the bounded status path. It never requests message
// Items and falls back to metadata-only ReadThread(false) for old readers.
func ReadThreadActivity(ctx context.Context, reader DetailReader, threadID string) (ThreadDetail, error) {
	if activity, ok := reader.(DetailActivityReader); ok {
		return activity.ReadThreadActivity(ctx, threadID)
	}
	return reader.ReadThread(ctx, threadID, false)
}

// ReadThreadActivityHistory is the bounded status-only multi-Turn path. Its
// fallback preserves compatibility with old readers that only expose the
// original detail method.
func ReadThreadActivityHistory(ctx context.Context, reader DetailReader, threadID string, limit int) (ThreadDetail, error) {
	if activity, ok := reader.(DetailActivityHistoryReader); ok {
		return activity.ReadThreadActivityHistory(ctx, threadID, limit)
	}
	return reader.ReadThread(ctx, threadID, false)
}

type RuntimeStateProvider interface {
	RuntimeState(threadID string) RuntimeState
}

type Service struct {
	reader   ThreadReader
	states   RuntimeStateProvider
	registry any
}

func NewService(reader ThreadReader, states RuntimeStateProvider, registry any) *Service {
	return &Service{reader: reader, states: states, registry: registry}
}

func (s *Service) ListThreads(ctx context.Context, limit int, cursor string) (ThreadList, error) {
	raw, err := s.reader.ThreadList(ctx, limit, cursor)
	if err != nil {
		return ThreadList{}, err
	}
	result := normalizeThreadList(raw)
	store := conversationregistry.ForCodex(s.registry)
	if store != nil {
		metadata := make([]conversationregistry.Metadata, 0, len(result.Threads))
		for _, thread := range result.Threads {
			metadata = append(metadata, threadMetadata(thread))
		}
		if _, err := store.EnsureBatchBackend(conversationregistry.BackendCodex, metadata); err != nil {
			return ThreadList{}, err
		}
	}
	for index := range result.Threads {
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendCodex, result.Threads[index].ThreadID); ok {
				result.Threads[index].Number = record.Number
			}
		}
		if s.states != nil {
			result.Threads[index].Status = s.states.RuntimeState(result.Threads[index].ThreadID).State
		}
	}
	return result, nil
}

// ListThreadsReadOnly returns the same normalized list view without allocating
// a new global conversation number. Remote status/project/output queries use
// this path; binding/list surfaces that intentionally discover sessions may
// continue using ListThreads.
func (s *Service) ListThreadsReadOnly(ctx context.Context, limit int, cursor string) (ThreadList, error) {
	raw, err := s.reader.ThreadList(ctx, limit, cursor)
	if err != nil {
		return ThreadList{}, err
	}
	result := normalizeThreadList(raw)
	store := conversationregistry.ForCodex(s.registry)
	for index := range result.Threads {
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendCodex, result.Threads[index].ThreadID); ok {
				result.Threads[index].Number = record.Number
			}
		}
		if s.states != nil {
			result.Threads[index].Status = s.states.RuntimeState(result.Threads[index].ThreadID).State
		}
	}
	return result, nil
}

func (s *Service) ReadThread(ctx context.Context, threadID string, includeTurns bool) (ThreadDetail, error) {
	var (
		raw map[string]any
		err error
	)
	if includeTurns {
		raw, err = s.readThreadHistoryRaw(ctx, threadID, DefaultHistoryTurnLimit)
	} else {
		raw, err = s.reader.ThreadRead(ctx, threadID, false)
	}
	return s.decorateThread(raw, threadID, err)
}

// ReadThreadHistory is the bounded history entry point used by /history and
// other features that know how many recent Turns they need.
func (s *Service) ReadThreadHistory(ctx context.Context, threadID string, limit int) (ThreadDetail, error) {
	raw, err := s.readThreadHistoryRaw(ctx, threadID, limit)
	return s.decorateThread(raw, threadID, err)
}

// ReadThreadTurnOutput hydrates only the requested Turn when the backend
// exposes the paginated single-Turn path. Legacy readers fall back to the
// existing bounded history surface and select the requested Turn from it.
func (s *Service) ReadThreadTurnOutput(ctx context.Context, threadID, turnID string) (ThreadDetail, error) {
	var (
		raw map[string]any
		err error
	)
	if reader, ok := s.reader.(ThreadTurnReader); ok {
		raw, err = reader.ThreadReadTurn(ctx, threadID, turnID)
	} else {
		raw, err = s.readThreadHistoryRaw(ctx, threadID, DefaultHistoryTurnLimit)
	}
	detail, decorateErr := s.decorateThread(raw, threadID, err)
	if decorateErr != nil {
		return ThreadDetail{}, decorateErr
	}
	if strings.TrimSpace(turnID) != "" {
		filtered := make([]Turn, 0, 1)
		for _, turn := range detail.Turns {
			if turn.TurnID == strings.TrimSpace(turnID) {
				filtered = append(filtered, turn)
				break
			}
		}
		detail.Turns = filtered
	}
	return detail, nil
}

// ReadThreadActivity intentionally does not load Items. For the real
// app-server client it performs thread/read(false) plus one paginated Turn;
// legacy readers remain metadata-only when no lightweight RPC exists.
func (s *Service) ReadThreadActivity(ctx context.Context, threadID string) (ThreadDetail, error) {
	var (
		raw map[string]any
		err error
	)
	if reader, ok := s.reader.(ThreadActivityReader); ok {
		raw, err = reader.ThreadReadActivity(ctx, threadID)
	} else {
		raw, err = s.reader.ThreadRead(ctx, threadID, false)
	}
	return s.decorateThread(raw, threadID, err)
}

// ReadThreadActivityHistory intentionally omits Items while retaining a
// bounded number of recent Turns for status/error queries.
func (s *Service) ReadThreadActivityHistory(ctx context.Context, threadID string, limit int) (ThreadDetail, error) {
	var (
		raw map[string]any
		err error
	)
	if reader, ok := s.reader.(ThreadActivityHistoryReader); ok {
		raw, err = reader.ThreadReadActivityHistory(ctx, threadID, limit)
	} else {
		raw, err = s.reader.ThreadRead(ctx, threadID, false)
	}
	return s.decorateThread(raw, threadID, err)
}

func (s *Service) readThreadHistoryRaw(ctx context.Context, threadID string, limit int) (map[string]any, error) {
	if reader, ok := s.reader.(ThreadHistoryReader); ok {
		return reader.ThreadReadHistory(ctx, threadID, limit)
	}
	// Older injected readers only expose the original full-read method. This
	// fallback is reachable for legacy servers/test doubles; the production
	// Manager implements ThreadHistoryReader and takes the safe path above.
	return s.reader.ThreadRead(ctx, threadID, true)
}

func (s *Service) decorateThread(raw map[string]any, threadID string, err error) (ThreadDetail, error) {
	if err != nil {
		return ThreadDetail{}, err
	}
	detail := normalizeThreadDetail(raw)
	if store := conversationregistry.ForCodex(s.registry); store != nil && detail.ThreadID != "" {
		record, err := store.EnsureBackend(conversationregistry.BackendCodex, threadMetadata(detail.ThreadSummary))
		if err != nil {
			return ThreadDetail{}, err
		}
		detail.Number = record.Number
	}
	if s.states != nil {
		detail.Runtime = s.states.RuntimeState(threadID)
	}
	if detail.Archived != nil && *detail.Archived {
		detail.Runtime.CanSend = false
	}
	return detail, nil
}

func threadMetadata(thread ThreadSummary) conversationregistry.Metadata {
	return conversationregistry.Metadata{Backend: conversationregistry.BackendCodex, TargetID: thread.ThreadID, Title: thread.Title, CWD: thread.CWD, CreatedAt: thread.CreatedAt, LastSeenAt: firstSeen(thread.UpdatedAt)}
}

func firstSeen(value string) string {
	if value != "" {
		return value
	}
	return time.Now().UTC().Format(time.RFC3339Nano)
}
