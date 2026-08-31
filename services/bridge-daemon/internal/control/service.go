package control

import (
	"context"
	"errors"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
)

var ErrUnavailable = errors.New("Codex app-server is unavailable")

type ThreadReader interface {
	ThreadList(ctx context.Context, limit int, cursor string) (map[string]any, error)
	ThreadRead(ctx context.Context, threadID string, includeTurns bool) (map[string]any, error)
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

func (s *Service) ReadThread(ctx context.Context, threadID string, includeTurns bool) (ThreadDetail, error) {
	raw, err := s.reader.ThreadRead(ctx, threadID, includeTurns)
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
