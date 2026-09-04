package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

type MessageTypes struct {
	User             bool `json:"user"`
	Assistant        bool `json:"assistant"`
	Status           bool `json:"status"`
	RequestUserInput bool `json:"requestUserInput"`
	Error            bool `json:"error"`
}
type TelegramConfig struct {
	Enabled bool   `json:"enabled"`
	ChatID  string `json:"chatId"`
}
type QQConfig struct {
	Enabled          bool   `json:"enabled"`
	ConversationType string `json:"conversationType"`
	OpenID           string `json:"openId"`
}
type Config struct {
	Enabled             bool           `json:"enabled"`
	FinalOnly           bool           `json:"finalOnly"`
	RequireThreadNumber bool           `json:"requireThreadNumber"`
	Messages            MessageTypes   `json:"messages"`
	Telegram            TelegramConfig `json:"telegram"`
	QQ                  QQConfig       `json:"qq"`
}
type Status struct {
	Config             Config `json:"config"`
	TelegramState      string `json:"telegramState"`
	QQState            string `json:"qqState"`
	QQCapabilityNotice string `json:"qqCapabilityNotice"`
	LastTelegramError  string `json:"lastTelegramError,omitempty"`
	LastQQError        string `json:"lastQQError,omitempty"`
	LastQQErrorCode    string `json:"lastQQErrorCode,omitempty"`
}
type Cursor struct {
	LastObservedMessage  string `json:"lastObservedMessage"`
	LastTelegramMirrored string `json:"lastTelegramMirrored"`
	LastQQMirrored       string `json:"lastQQMirrored"`
}
type liveCursor struct {
	Telegram bool `json:"telegram,omitempty"`
	QQ       bool `json:"qq,omitempty"`
}
type finalRecord struct {
	ThreadID           string `json:"threadId"`
	TurnID             string `json:"turnId"`
	AssistantMessageID string `json:"assistantMessageId"`
	Fingerprint        string `json:"fingerprint"`
	TelegramMirrored   bool   `json:"telegramMirrored"`
	QQMirrored         bool   `json:"qqMirrored"`
	FinalMirrored      bool   `json:"finalMirrored"`
}
type diskModel struct {
	Version       int                    `json:"version"`
	Config        Config                 `json:"config"`
	Cursors       map[string]Cursor      `json:"cursors"`
	LiveDelivered map[string]liveCursor  `json:"liveDelivered,omitempty"`
	Finals        map[string]finalRecord `json:"finals,omitempty"`
}

type Control interface {
	ListThreads(context.Context, int, string) (control.ThreadList, error)
	ReadThread(context.Context, string, bool) (control.ThreadDetail, error)
}

const (
	mirrorHistoryTurnLimit = 50
	maxMirrorDedupeKeys    = 1024
	maxRolloutFinals       = 1024
	maxTransientTurns      = 512
)

type Runtime interface {
	RuntimeState(string) control.RuntimeState
}
type Target struct {
	Status func() (accountID string, ready bool)
	Send   func(context.Context, channels.OutboundMessage) (channels.OutboundResult, error)
}

type Service struct {
	mu                sync.Mutex
	path              string
	model             diskModel
	control           Control
	registry          any
	broker            *events.Broker
	logger            *bridgelog.SafeLogger
	telegram          Target
	qq                Target
	ctx               context.Context
	cancel            context.CancelFunc
	done              chan struct{}
	workers           sync.WaitGroup
	baselineOnce      sync.Once
	baselineComplete  bool
	turnOrigins       map[string]turnOrigin
	retry             map[string]bool
	syncLocks         map[string]*keyedSyncLock
	pendingSync       map[string]string
	rolloutFinals     map[string]rolloutFinal
	observedFinals    map[string]bool
	resolvedFinals    map[string]bool
	completedFinals   map[string]bool
	finalCandidateLog map[string]bool
	lastTelegramError string
	lastQQError       string
	lastQQErrorCode   string
}
type visibleMessage struct{ Key, TurnID, ItemID, Kind, Text, Origin, Phase string }
type turnOrigin struct {
	Origin string
	At     time.Time
}
type keyedSyncLock struct {
	mu   sync.Mutex
	refs int
}
type rolloutFinal struct{ ThreadID, TurnID, ItemID, Phase, Text, CompletedAt string }
type mirrorCursorMigration struct {
	Platform string
	OldKey   string
	Key      string
	TurnID   string
	Result   string
	Changed  bool
}

type mirrorTargetError struct {
	message string
}

func (e *mirrorTargetError) Error() string           { return e.message }
func (e *mirrorTargetError) BridgeErrorCode() string { return "invalid_openid" }

func DefaultConfig() Config {
	return Config{Enabled: false, RequireThreadNumber: true, Messages: MessageTypes{Assistant: true, RequestUserInput: true, Error: true}, QQ: QQConfig{ConversationType: "c2c"}}
}

func New(path string, controlService Control, _ Runtime, registry any, broker *events.Broker, logger *bridgelog.SafeLogger, telegram, qq Target) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{path: path, control: controlService, registry: registry, broker: broker, logger: logger, telegram: telegram, qq: qq, ctx: ctx, cancel: cancel, done: make(chan struct{}), turnOrigins: map[string]turnOrigin{}, retry: map[string]bool{}, syncLocks: map[string]*keyedSyncLock{}, pendingSync: map[string]string{}, rolloutFinals: map[string]rolloutFinal{}, observedFinals: map[string]bool{}, resolvedFinals: map[string]bool{}, completedFinals: map[string]bool{}, finalCandidateLog: map[string]bool{}, model: diskModel{Version: 1, Config: DefaultConfig(), Cursors: map[string]Cursor{}, LiveDelivered: map[string]liveCursor{}, Finals: map[string]finalRecord{}}}
	if err := s.load(); err != nil {
		cancel()
		return nil, err
	}
	go s.eventLoop()
	s.goRun(s.watchRollouts)
	return s, nil
}

func (s *Service) Close() {
	s.cancel()
	<-s.done
	s.workers.Wait()
}

func (s *Service) goRun(run func()) {
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		run()
	}()
}
func (s *Service) Status() Status {
	s.mu.Lock()
	cfg := s.model.Config
	tgError, qqError, qqErrorCode := s.lastTelegramError, s.lastQQError, s.lastQQErrorCode
	s.mu.Unlock()
	_, tgReady := s.telegram.Status()
	_, qqReady := s.qq.Status()
	tgState := "disabled"
	if cfg.Enabled && cfg.Telegram.Enabled {
		if tgReady {
			tgState = "ready"
		} else {
			tgState = "channel-unavailable"
		}
	}
	qqState := "disabled"
	if cfg.Enabled && cfg.QQ.Enabled {
		if qqReady {
			qqState = "ready-limited"
		} else {
			qqState = "channel-unavailable"
		}
	}
	if tgError != "" {
		tgState = "error"
	}
	if qqError != "" {
		qqState = "platform-rejected"
	}
	if qqErrorCode == "platform_capability_limited" || qqErrorCode == "permission_not_granted" {
		qqState = "platform-capability-limited"
	}
	if cfg.Enabled && cfg.QQ.Enabled && looksLikeNumericQQAccount(cfg.QQ.OpenID) {
		qqState = "target-invalid"
		qqErrorCode = "invalid_openid"
		qqError = "C2C 镜像目标必须是 User OpenID，Group 镜像目标必须是 Group OpenID；不能填写数字 QQ 号。"
	}
	return Status{Config: cfg, TelegramState: tgState, QQState: qqState, QQCapabilityNotice: "QQ 官方机器人主动消息受平台主动消息权限、场景窗口和额度限制；Bridge 使用官方 API，平台拒绝时会记录失败且不会回退 NapCat。", LastTelegramError: tgError, LastQQError: qqError, LastQQErrorCode: qqErrorCode}
}
func (s *Service) Configure(cfg Config) (Status, error) {
	// 0.6.2 intentionally removes event-stream mirroring. Keep the legacy JSON
	// fields for API/state compatibility, but never allow them to re-enable the
	// noisy User or Status paths. Formal final answers are always enabled by the
	// new contract, even when an older file had assistant=false.
	cfg.Messages.User = false
	cfg.Messages.Status = false
	cfg.Messages.Assistant = true
	cfg.Telegram.ChatID = strings.TrimSpace(cfg.Telegram.ChatID)
	cfg.QQ.OpenID = strings.TrimSpace(cfg.QQ.OpenID)
	cfg.QQ.ConversationType = strings.ToLower(strings.TrimSpace(cfg.QQ.ConversationType))
	if cfg.QQ.ConversationType == "" {
		cfg.QQ.ConversationType = "c2c"
	}
	if cfg.QQ.ConversationType != "c2c" && cfg.QQ.ConversationType != "group" {
		return Status{}, errors.New("qq.conversationType must be c2c or group")
	}
	if cfg.Telegram.Enabled && cfg.Telegram.ChatID == "" {
		return Status{}, errors.New("telegram.chatId is required when Telegram mirror is enabled")
	}
	if cfg.QQ.Enabled && cfg.QQ.OpenID == "" {
		return Status{}, errors.New("qq.openId is required when QQ mirror is enabled")
	}
	if cfg.QQ.Enabled && looksLikeNumericQQAccount(cfg.QQ.OpenID) {
		return Status{}, errors.New("qq.openId must be an official User OpenID or Group OpenID, not a numeric QQ account")
	}
	s.mu.Lock()
	s.model.Config = cfg
	s.lastTelegramError, s.lastQQError, s.lastQQErrorCode = "", "", ""
	err := s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		return Status{}, err
	}
	s.baselineAll()
	s.goRun(s.retryAll)
	return s.Status(), nil
}

func (s *Service) eventLoop() {
	stream, unsubscribe := s.broker.Subscribe()
	defer unsubscribe()
	defer close(s.done)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.retryFailed()
		case event, ok := <-stream:
			if !ok {
				return
			}
			s.handleEvent(event)
		}
	}
}
func (s *Service) handleEvent(event events.Event) {
	switch event.EventType {
	case events.CodexConnected:
		s.baselineOnce.Do(func() { s.goRun(s.baselineAll) })
	case events.ChannelConnected:
		s.goRun(s.retryAll)
	case events.ThreadUpdated:
		if event.ThreadID != "" {
			source, _ := event.Payload["source"].(string)
			s.triggerSync(event.ThreadID, firstNonEmpty(source, "appserver"), event.TurnID)
			if event.TurnID != "" && source == "appserver" {
				s.scheduleQuickChecks(event.ThreadID, event.TurnID)
			}
		}
	case events.TurnStarted:
		origin, _ := event.Payload["source"].(string)
		if origin == "local" {
			origin = "bridge"
		}
		s.mu.Lock()
		s.turnOrigins[event.TurnID] = turnOrigin{Origin: origin, At: time.Now().UTC()}
		s.pruneTurnOriginsLocked(time.Now().UTC())
		s.mu.Unlock()
		s.triggerSync(event.ThreadID, "appserver", event.TurnID)
	case events.InteractionRequested:
		if s.enabledType("input") {
			origin := s.originForTurn(event.TurnID)
			if origin != "telegram" && origin != "qqbot" {
				s.sendInteraction(event)
			}
		}
	case events.TurnFailed:
		s.forgetTurnOrigin(event.TurnID)
		if s.enabledType("error") {
			message := "任务失败"
			if detail, _ := event.Payload["error"].(string); strings.TrimSpace(detail) != "" {
				message += "：" + safeError(errors.New(detail))
			} else {
				message += "。"
			}
			s.sendExceptional("failure", event.ThreadID, event.TurnID, message)
		}
	case events.TurnInterrupted:
		s.forgetTurnOrigin(event.TurnID)
		if s.enabledType("error") {
			s.sendExceptional("stop", event.ThreadID, event.TurnID, "已停止。")
		}
	case events.TurnCompleted:
		s.forgetTurnOrigin(event.TurnID)
		status, _ := event.Payload["status"].(string)
		if strings.EqualFold(strings.TrimSpace(status), "persisted") {
			s.triggerSync(event.ThreadID, "appserver", event.TurnID)
		}
	}
}

func (s *Service) baselineAll() {
	store := conversationregistry.ForCodex(s.registry)
	if store == nil {
		return
	}
	cursor := ""
	summaries := []control.ThreadSummary{}
	for page := 0; page < 100; page++ {
		ctx, cancel := context.WithTimeout(s.ctx, 8*time.Second)
		list, err := s.control.ListThreads(ctx, 100, cursor)
		cancel()
		if err != nil {
			return
		}
		summaries = append(summaries, list.Threads...)
		if list.NextCursor == "" || list.NextCursor == cursor {
			break
		}
		cursor = list.NextCursor
	}
	metadata := make([]conversationregistry.Metadata, 0, len(summaries))
	for _, thread := range summaries {
		metadata = append(metadata, conversationregistry.Metadata{Backend: conversationregistry.BackendCodex, TargetID: thread.ThreadID, Title: thread.Title, CWD: thread.CWD, CreatedAt: thread.CreatedAt, LastSeenAt: thread.UpdatedAt})
	}
	_, _ = store.EnsureBatchBackend(conversationregistry.BackendCodex, metadata)
	for _, thread := range summaries {
		ctx, cancel := context.WithTimeout(s.ctx, 8*time.Second)
		detail, err := control.ReadThreadHistory(ctx, s.control, thread.ThreadID, mirrorHistoryTurnLimit)
		cancel()
		if err != nil {
			continue
		}
		messages := visibleMessages(detail, s.originForTurn)
		last := ""
		if len(messages) > 0 {
			last = messages[len(messages)-1].Key
		}
		var migrations []mirrorCursorMigration
		s.mu.Lock()
		c, exists := s.model.Cursors[thread.ThreadID]
		if !exists {
			for _, message := range messages {
				s.setBaselineFinalLocked(thread.ThreadID, message, true, true)
			}
			c = Cursor{LastObservedMessage: last, LastTelegramMirrored: last, LastQQMirrored: last}
			s.model.Cursors[thread.ThreadID] = c
		} else {
			// Migrate cursors from older final semantics. A cursor that points at
			// an invalid progress item for a Turn is moved just before that Turn's
			// current formal final so the final can be delivered once. An unknown
			// cursor is normalized to the current safe baseline to avoid replaying
			// the entire bounded history.
			for _, migration := range []struct {
				name  string
				value *string
			}{
				{name: "observed", value: &c.LastObservedMessage},
				{name: "telegram", value: &c.LastTelegramMirrored},
				{name: "qq", value: &c.LastQQMirrored},
			} {
				result := normalizeMirrorCursor(messages, *migration.value, s.model.Finals)
				if result.Changed {
					result.Platform = migration.name
					migrations = append(migrations, result)
					*migration.value = result.Key
				}
			}
			for _, message := range messages {
				s.ensureFinalRecordLocked(thread.ThreadID, message)
			}
			for _, migration := range migrations {
				if migration.Result != "safe_baseline" {
					continue
				}
				for _, message := range messages {
					s.setPlatformMirroredLocked(message.Key, migration.Platform, true)
				}
			}
			s.model.Cursors[thread.ThreadID] = c
		}
		for _, message := range messages {
			if record, ok := s.model.Finals[message.Key]; ok {
				if record.TelegramMirrored && record.QQMirrored {
					record.FinalMirrored = true
					s.model.Finals[message.Key] = record
				}
			}
		}
		_ = s.saveLocked()
		s.mu.Unlock()
		for _, migration := range migrations {
			s.logCursorMigration(thread.ThreadID, migration)
		}
	}
	s.mu.Lock()
	s.baselineComplete = true
	s.mu.Unlock()
}

func containsMessageKey(messages []visibleMessage, key string) bool {
	for _, message := range messages {
		if message.Key == key {
			return true
		}
	}
	return false
}

func cursorNeedsNormalization(messages []visibleMessage, key string) bool {
	return key != "" && !containsMessageKey(messages, key)
}

func normalizeMirrorCursor(messages []visibleMessage, cursor string, finals map[string]finalRecord) mirrorCursorMigration {
	result := mirrorCursorMigration{OldKey: cursor, Key: cursor}
	if !cursorNeedsNormalization(messages, cursor) {
		return result
	}
	if record, ok := finals[cursor]; ok && strings.TrimSpace(record.TurnID) != "" {
		for index, message := range messages {
			if message.TurnID != record.TurnID {
				continue
			}
			previous := ""
			if index > 0 {
				previous = messages[index-1].Key
			}
			return mirrorCursorMigration{OldKey: cursor, Key: previous, TurnID: record.TurnID, Result: "same_turn_final", Changed: true}
		}
	}
	last := ""
	if len(messages) > 0 {
		last = messages[len(messages)-1].Key
	}
	return mirrorCursorMigration{OldKey: cursor, Key: last, Result: "safe_baseline", Changed: true}
}

func (s *Service) ensureFinalRecordLocked(threadID string, message visibleMessage) {
	if _, found := s.model.Finals[message.Key]; found {
		return
	}
	s.model.Finals[message.Key] = finalRecord{
		ThreadID: threadID, TurnID: message.TurnID, AssistantMessageID: message.ItemID,
		Fingerprint: fingerprint(message.Text),
	}
}

func (s *Service) setBaselineFinalLocked(threadID string, message visibleMessage, telegramMirrored, qqMirrored bool) {
	s.ensureFinalRecordLocked(threadID, message)
	record := s.model.Finals[message.Key]
	record.ThreadID = firstNonEmpty(record.ThreadID, threadID)
	record.TurnID = message.TurnID
	record.AssistantMessageID = message.ItemID
	record.Fingerprint = fingerprint(message.Text)
	if telegramMirrored {
		record.TelegramMirrored = true
	}
	if qqMirrored {
		record.QQMirrored = true
	}
	record.FinalMirrored = record.TelegramMirrored && record.QQMirrored
	s.model.Finals[message.Key] = record
}

func (s *Service) setPlatformMirroredLocked(key, platform string, mirrored bool) {
	record, ok := s.model.Finals[key]
	if !ok {
		return
	}
	if platform == "telegram" {
		record.TelegramMirrored = mirrored
	} else if platform == "qq" || platform == "qqbot" {
		record.QQMirrored = mirrored
	}
	record.FinalMirrored = record.TelegramMirrored && record.QQMirrored
	s.model.Finals[key] = record
}

func (s *Service) logCursorMigration(threadID string, migration mirrorCursorMigration) {
	if s.logger == nil {
		return
	}
	s.logger.Printf("mirrorCursorMigration threadId=%s platform=%s result=%s turnId=%s oldCursor=%s newCursor=%s", threadID, migration.Platform, migration.Result, migration.TurnID, diagnosticKey(migration.OldKey), diagnosticKey(migration.Key))
}

func diagnosticKey(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 96 {
		return value[:96]
	}
	return value
}

func (s *Service) triggerSync(threadID, source, turnID string) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return
	}
	s.mu.Lock()
	if _, running := s.pendingSync[threadID]; running {
		s.pendingSync[threadID] = source
		s.mu.Unlock()
		return
	}
	s.pendingSync[threadID] = ""
	s.mu.Unlock()
	s.goRun(func() {
		current := source
		for {
			s.syncThreadSource(threadID, current, turnID)
			s.mu.Lock()
			next := s.pendingSync[threadID]
			if next == "" {
				delete(s.pendingSync, threadID)
			} else {
				s.pendingSync[threadID] = ""
			}
			s.mu.Unlock()
			if next == "" {
				return
			}
			current = next
		}
	})
}

func (s *Service) scheduleQuickChecks(threadID, turnID string) {
	s.goRun(func() {
		previous := time.Duration(0)
		for _, elapsed := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 5 * time.Second, 10 * time.Second} {
			timer := time.NewTimer(elapsed - previous)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			s.triggerSync(threadID, "retry", turnID)
			previous = elapsed
		}
	})
}

func (s *Service) syncThread(threadID string) { s.syncThreadSource(threadID, "scanner", "") }

func (s *Service) syncThreadSource(threadID, source, expectedTurnID string) {
	store := conversationregistry.ForCodex(s.registry)
	if store == nil {
		return
	}
	unlock := s.lockSync(threadID)
	defer unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	detail, err := control.ReadThreadHistory(ctx, s.control, threadID, mirrorHistoryTurnLimit)
	if err != nil {
		s.mu.Lock()
		fallback := s.rolloutFinals[threadID]
		s.mu.Unlock()
		if fallback.TurnID == "" {
			return
		}
		detail.ThreadID = threadID
		if record, ok := store.ByTarget(conversationregistry.BackendCodex, threadID); ok {
			detail.Title, detail.Number = record.Title, record.Number
		}
	}
	_, _ = store.EnsureBackend(conversationregistry.BackendCodex, conversationregistry.Metadata{Backend: conversationregistry.BackendCodex, TargetID: detail.ThreadID, Title: detail.Title, CWD: detail.CWD, CreatedAt: detail.CreatedAt, LastSeenAt: detail.UpdatedAt})
	s.logFinalCandidates(detail, "appserver")
	messages := visibleMessages(detail, s.originForTurn)
	if expectedTurnID != "" {
		for _, message := range messages {
			if message.TurnID == expectedTurnID {
				s.logFinalMilestone("final_first_observed", threadID, expectedTurnID, source)
				break
			}
		}
	}
	s.mu.Lock()
	rollout := s.rolloutFinals[threadID]
	s.mu.Unlock()
	useRollout := rollout.TurnID != "" && !containsTurn(messages, rollout.TurnID) &&
		((expectedTurnID != "" && rollout.TurnID == expectedTurnID) || source == "watcher")
	if useRollout {
		s.logRolloutFinalCandidate(rollout)
		messages = append(messages, visibleMessage{Key: rollout.TurnID + "/" + rollout.ItemID, TurnID: rollout.TurnID, ItemID: rollout.ItemID, Kind: "assistant", Phase: rollout.Phase, Text: rollout.Text})
	}
	s.mu.Lock()
	cursor, exists := s.model.Cursors[threadID]
	cfg := s.model.Config
	baselineComplete := s.baselineComplete
	for _, message := range messages {
		s.ensureFinalRecordLocked(threadID, message)
	}
	s.mu.Unlock()
	if !exists {
		last := ""
		// During the initial baseline, every pre-existing final is considered
		// already observed. A Thread first seen after that baseline starts empty,
		// so its first Desktop/CLI final is mirrored.
		if !baselineComplete && len(messages) > 0 {
			last = messages[len(messages)-1].Key
		}
		s.mu.Lock()
		s.model.Cursors[threadID] = Cursor{LastObservedMessage: last, LastTelegramMirrored: last, LastQQMirrored: last}
		_ = s.saveLocked()
		s.mu.Unlock()
		cursor = Cursor{LastObservedMessage: last, LastTelegramMirrored: last, LastQQMirrored: last}
	}
	if len(messages) == 0 {
		return
	}
	var migrations []mirrorCursorMigration
	if exists {
		s.mu.Lock()
		for _, migration := range []struct {
			name  string
			value *string
		}{
			{name: "observed", value: &cursor.LastObservedMessage},
			{name: "telegram", value: &cursor.LastTelegramMirrored},
			{name: "qq", value: &cursor.LastQQMirrored},
		} {
			result := normalizeMirrorCursor(messages, *migration.value, s.model.Finals)
			if result.Changed {
				result.Platform = migration.name
				migrations = append(migrations, result)
				*migration.value = result.Key
			}
		}
		for _, migration := range migrations {
			if migration.Result != "safe_baseline" {
				continue
			}
			for _, message := range messages {
				s.setPlatformMirroredLocked(message.Key, migration.Platform, true)
			}
		}
		_ = s.saveLocked()
		s.mu.Unlock()
		for _, migration := range migrations {
			s.logCursorMigration(threadID, migration)
		}
	}
	if expectedTurnID != "" {
		for _, message := range messages {
			if message.TurnID == expectedTurnID {
				s.logFinalMilestone("final_resolved", threadID, message.TurnID, source)
				break
			}
		}
	}
	var deliveries sync.WaitGroup
	deliver := func(platform string, last *string) {
		defer deliveries.Done()
		pending, cursorKnown := afterCursor(messages, *last)
		if !cursorKnown {
			return
		}
		for _, message := range pending {
			if !cfg.Enabled || (platform == "telegram" && !cfg.Telegram.Enabled) || (platform == "qqbot" && !cfg.QQ.Enabled) || !messageEnabled(cfg.Messages, message.Kind) {
				*last = message.Key
				continue
			}
			if s.isFinalMirrored(message.Key, platform) {
				*last = message.Key
				continue
			}
			if err := s.sendVisible(ctx, detail, message, platform); err != nil {
				break
			}
			s.markFinalMirrored(message.Key, platform, cfg)
			*last = message.Key
			stage := platform
			if stage == "qqbot" {
				stage = "qq"
			}
			if s.logger != nil {
				s.logger.Printf("latency stage=%s_sent threadId=%s turnId=%s at=%s source=%s", stage, threadID, message.TurnID, time.Now().UTC().Format(time.RFC3339Nano), source)
			}
		}
	}
	deliveries.Add(2)
	go deliver("telegram", &cursor.LastTelegramMirrored)
	go deliver("qqbot", &cursor.LastQQMirrored)
	deliveries.Wait()
	cursor.LastObservedMessage = messages[len(messages)-1].Key
	s.mu.Lock()
	s.model.Cursors[threadID] = cursor
	if cfg.Enabled && ((cfg.Telegram.Enabled && cursor.LastTelegramMirrored != cursor.LastObservedMessage) || (cfg.QQ.Enabled && cursor.LastQQMirrored != cursor.LastObservedMessage)) {
		s.retry[threadID] = true
	} else {
		delete(s.retry, threadID)
	}
	if rollout.TurnID != "" && containsTurn(messages, rollout.TurnID) {
		delete(s.rolloutFinals, threadID)
	}
	_ = s.saveLocked()
	s.mu.Unlock()
}

func (s *Service) logFinalMilestone(stage, threadID, turnID, source string) {
	key := threadID + "/" + turnID
	s.mu.Lock()
	seen := s.observedFinals
	if stage == "final_resolved" {
		seen = s.resolvedFinals
	} else if stage == "turn_completed" {
		seen = s.completedFinals
	}
	if seen[key] {
		s.mu.Unlock()
		return
	}
	seen[key] = true
	trimMirrorDedupe(seen)
	s.mu.Unlock()
	if s.logger != nil {
		s.logger.Printf("latency stage=%s threadId=%s turnId=%s at=%s source=%s", stage, threadID, turnID, time.Now().UTC().Format(time.RFC3339Nano), source)
	}
}

func (s *Service) logFinalCandidates(detail control.ThreadDetail, source string) {
	mode := control.FinalSelectionModeForHistory(detail.HistoryMode)
	for _, turn := range detail.Turns {
		if !strings.EqualFold(strings.TrimSpace(turn.Status), "completed") {
			continue
		}
		selection := control.ResolveFinalAssistantItem(turn, mode)
		if selection.Found {
			selectedID := control.FinalAssistantItemID(turn.TurnID, selection.Item)
			s.logFinalCandidate(detail.ThreadID, turn.TurnID, selectedID, selection.Item.Phase, source, "selected")
		}
		for _, item := range turn.Items {
			if !control.IsAssistantMessageItem(item) || strings.TrimSpace(item.Text) == "" || control.IsExplicitFinalPhase(item.Phase) {
				continue
			}
			if selection.Found && item.ItemID == selection.Item.ItemID && strings.TrimSpace(item.Text) == strings.TrimSpace(selection.Item.Text) {
				continue
			}
			s.logFinalCandidate(detail.ThreadID, turn.TurnID, item.ItemID, item.Phase, source, "rejected_non_final_phase")
		}
	}
}

func (s *Service) logRolloutFinalCandidate(final rolloutFinal) {
	s.logFinalCandidate(final.ThreadID, final.TurnID, final.ItemID, final.Phase, "rollout", "selected")
}

func (s *Service) logFinalCandidate(threadID, turnID, itemID, phase, source, result string) {
	if s.logger == nil {
		return
	}
	key := strings.Join([]string{source, threadID, turnID, itemID, phase, result}, "\x00")
	s.mu.Lock()
	if s.finalCandidateLog[key] {
		s.mu.Unlock()
		return
	}
	s.finalCandidateLog[key] = true
	trimMirrorDedupe(s.finalCandidateLog)
	s.mu.Unlock()
	s.logger.Printf("finalCandidate source=%s threadId=%s turnId=%s itemId=%s phase=%s result=%s", source, threadID, turnID, itemID, strings.TrimSpace(phase), result)
}

func trimMirrorDedupe(values map[string]bool) {
	for len(values) > maxMirrorDedupeKeys {
		for key := range values {
			delete(values, key)
			break
		}
	}
}

func (s *Service) forgetTurnOrigin(turnID string) {
	if strings.TrimSpace(turnID) == "" {
		return
	}
	s.mu.Lock()
	delete(s.turnOrigins, turnID)
	s.mu.Unlock()
}

func (s *Service) pruneTurnOriginsLocked(now time.Time) {
	for turnID, value := range s.turnOrigins {
		if !value.At.IsZero() && now.Sub(value.At) >= 30*time.Minute {
			delete(s.turnOrigins, turnID)
		}
	}
	for len(s.turnOrigins) > maxTransientTurns {
		oldestID := ""
		var oldestAt time.Time
		for turnID, value := range s.turnOrigins {
			if oldestID == "" || value.At.Before(oldestAt) {
				oldestID, oldestAt = turnID, value.At
			}
		}
		if oldestID == "" {
			return
		}
		delete(s.turnOrigins, oldestID)
	}
}

func (s *Service) isFinalMirrored(key, platform string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.model.Finals[key]
	if platform == "telegram" {
		return record.TelegramMirrored
	}
	return record.QQMirrored
}

func (s *Service) markFinalMirrored(key, platform string, cfg Config) {
	s.mu.Lock()
	record := s.model.Finals[key]
	if platform == "telegram" {
		record.TelegramMirrored = true
	} else {
		record.QQMirrored = true
	}
	record.FinalMirrored = (!cfg.Telegram.Enabled || record.TelegramMirrored) && (!cfg.QQ.Enabled || record.QQMirrored)
	s.model.Finals[key] = record
	_ = s.saveLocked()
	s.mu.Unlock()
}

func (s *Service) lockSync(key string) func() {
	s.mu.Lock()
	lock, ok := s.syncLocks[key]
	if !ok {
		lock = &keyedSyncLock{}
		s.syncLocks[key] = lock
	}
	lock.refs++
	s.mu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.mu.Lock()
		lock.refs--
		if lock.refs == 0 && s.syncLocks[key] == lock {
			delete(s.syncLocks, key)
		}
		s.mu.Unlock()
	}
}

func (s *Service) retryFailed() {
	s.mu.Lock()
	ids := []string{}
	for id, pending := range s.retry {
		if pending {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	for _, id := range ids {
		threadID := id
		s.triggerSync(threadID, "retry", "")
	}
}
func (s *Service) retryAll() {
	store := conversationregistry.ForCodex(s.registry)
	if store == nil {
		return
	}
	for _, record := range store.ListBackend(conversationregistry.BackendCodex) {
		threadID := record.TargetID
		s.triggerSync(threadID, "scanner", "")
	}
}

func visibleMessages(detail control.ThreadDetail, origin func(string) string) []visibleMessage {
	result := []visibleMessage{}
	mode := control.FinalSelectionModeForHistory(detail.HistoryMode)
	for _, turn := range detail.Turns {
		item, ok := control.SelectFinalAssistantItem(turn, mode)
		if !ok {
			continue
		}
		id := control.FinalAssistantItemID(turn.TurnID, item)
		result = append(result, visibleMessage{Key: turn.TurnID + "/" + id, TurnID: turn.TurnID, ItemID: id, Kind: "assistant", Phase: item.Phase, Text: strings.TrimSpace(item.Text), Origin: origin(turn.TurnID)})
	}
	return result
}
func afterCursor(messages []visibleMessage, cursor string) ([]visibleMessage, bool) {
	if cursor == "" {
		return messages, true
	}
	for i, m := range messages {
		if m.Key == cursor {
			return messages[i+1:], true
		}
	}
	return nil, false
}
func messageEnabled(types MessageTypes, kind string) bool {
	return kind == "assistant" && types.Assistant
}

func (s *Service) sendVisible(ctx context.Context, detail control.ThreadDetail, message visibleMessage, platform string) error {
	header := s.header(detail.ThreadID, detail.Title)
	return s.sendFinalPlatform(ctx, platform, header, message.Text)
}
func (s *Service) header(threadID, title string) string {
	store := conversationregistry.ForCodex(s.registry)
	if store == nil {
		return ""
	}
	record, ok := store.ByTarget(conversationregistry.BackendCodex, threadID)
	if !ok {
		return ""
	}
	title = strings.TrimSpace(title)
	if title == "" {
		title = record.Title
	}
	return strings.TrimSpace(fmt.Sprintf("#%d %s", record.Number, title))
}
func (s *Service) sendPlatform(ctx context.Context, platform, text string) error {
	s.mu.Lock()
	cfg := s.model.Config
	s.mu.Unlock()
	switch platform {
	case "telegram":
		account, ready := s.telegram.Status()
		if !ready {
			return errors.New("telegram unavailable")
		}
		_, err := s.telegram.Send(ctx, channels.OutboundMessage{Address: channels.ChannelAddress{ChannelType: "telegram", AccountID: account, ConversationType: "default", ChatID: cfg.Telegram.ChatID}, Text: text})
		if err != nil {
			s.logFailure(platform, err)
		} else {
			s.mu.Lock()
			s.lastTelegramError = ""
			s.mu.Unlock()
		}
		return err
	case "qqbot":
		if looksLikeNumericQQAccount(cfg.QQ.OpenID) {
			err := &mirrorTargetError{message: "QQ mirror target is a numeric QQ account, not an official OpenID"}
			s.logFailure(platform, err)
			return err
		}
		account, ready := s.qq.Status()
		if !ready {
			return errors.New("qq unavailable")
		}
		_, err := s.qq.Send(ctx, channels.OutboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: account, ConversationType: cfg.QQ.ConversationType, ChatID: cfg.QQ.OpenID}, Text: text})
		if err != nil {
			s.logFailure(platform, err)
		} else {
			s.mu.Lock()
			s.lastQQError = ""
			s.lastQQErrorCode = ""
			s.mu.Unlock()
		}
		return err
	}
	return nil
}

func (s *Service) sendFinalPlatform(ctx context.Context, platform, header, text string) error {
	limit := 3900
	if platform == "qqbot" {
		limit = 5000
	}
	for _, part := range splitFinalMessage(header, text, limit) {
		if err := s.sendPlatform(ctx, platform, part); err != nil {
			return err
		}
	}
	return nil
}

func splitFinalMessage(header, text string, limit int) []string {
	header, text = strings.TrimSpace(header), strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if header == "" {
		header = "Codex"
	}
	whole := header + "\n" + text
	if len([]rune(whole)) <= limit {
		return []string{whole}
	}
	contentLimit := limit - len([]rune(header)) - 24
	if contentLimit < 64 {
		contentLimit = 64
	}
	content := splitRunes(text, contentLimit)
	result := make([]string, len(content))
	for index, part := range content {
		result[index] = fmt.Sprintf("%s (%d/%d)\n%s", header, index+1, len(content), part)
	}
	return result
}

func splitRunes(text string, limit int) []string {
	runes := []rune(strings.TrimSpace(text))
	result := []string{}
	for len(runes) > limit {
		cut := limit
		for index := limit; index > limit/2; index-- {
			if runes[index-1] == '\n' || runes[index-1] == ' ' {
				cut = index
				break
			}
		}
		result = append(result, strings.TrimSpace(string(runes[:cut])))
		runes = runes[cut:]
	}
	if value := strings.TrimSpace(string(runes)); value != "" {
		result = append(result, value)
	}
	return result
}
func (s *Service) logFailure(platform string, err error) {
	s.mu.Lock()
	if platform == "telegram" {
		s.lastTelegramError = safeError(err)
	} else {
		s.lastQQError = safeError(err)
		s.lastQQErrorCode = bridgeErrorCode(err)
	}
	s.mu.Unlock()
	if s.logger != nil {
		s.logger.Printf("%s mirror send failed: %v", platform, err)
	}
	s.broker.Publish(events.ChannelError, map[string]any{"channelType": platform, "code": "mirror_send_failed"})
}

func bridgeErrorCode(err error) string {
	for err != nil {
		if coded, ok := err.(interface{ BridgeErrorCode() string }); ok {
			return strings.TrimSpace(coded.BridgeErrorCode())
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = unwrapper.Unwrap()
	}
	return "message_send_failed"
}

func looksLikeNumericQQAccount(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 5 || len(value) > 15 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	value := strings.TrimSpace(err.Error())
	if len(value) > 160 {
		value = value[:160]
	}
	return value
}

func (s *Service) markLive(key, platform string) {
	s.mu.Lock()
	value := s.model.LiveDelivered[key]
	if platform == "telegram" {
		value.Telegram = true
	} else {
		value.QQ = true
	}
	s.model.LiveDelivered[key] = value
	_ = s.saveLocked()
	s.mu.Unlock()
}
func (s *Service) originForTurn(turnID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnOrigins[turnID].Origin
}

func (s *Service) enabledType(kind string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.model.Config.Enabled {
		return false
	}
	switch kind {
	case "assistant":
		return s.model.Config.Messages.Assistant
	case "input":
		return s.model.Config.Messages.RequestUserInput
	case "error":
		return s.model.Config.Messages.Error
	}
	return false
}
func (s *Service) sendInteraction(event events.Event) {
	raw, ok := event.Payload["interaction"].(interactions.PendingInteraction)
	if !ok {
		return
	}
	store := conversationregistry.ForCodex(s.registry)
	if store == nil {
		return
	}
	record, ok := store.ByTarget(conversationregistry.BackendCodex, event.ThreadID)
	if !ok {
		return
	}
	var body strings.Builder
	if raw.Kind != interactions.KindUserInput {
		body.WriteString("Codex 正在等待桌面端审批。")
	} else {
		body.WriteString("需要你选择：\n\n")
		if raw.Description != "" {
			body.WriteString(strings.TrimSpace(raw.Description) + "\n\n")
		}
		for qi, q := range raw.Questions {
			if q.Text != "" {
				body.WriteString(strings.TrimSpace(q.Text) + "\n")
			}
			for oi, o := range q.Options {
				fmt.Fprintf(&body, "%d. %s\n", oi+1, o.Label)
			}
			if qi+1 < len(raw.Questions) {
				body.WriteByte('\n')
			}
		}
		fmt.Fprintf(&body, "\n回复：\n#%d 1", record.Number)
	}
	key := "interaction/" + event.ThreadID + "/" + event.TurnID + "/" + firstNonEmpty(raw.ID, fingerprint(body.String()))
	s.sendOnce(key, event.ThreadID, record.Title, body.String())
}

func (s *Service) sendExceptional(kind, threadID, turnID, text string) {
	store := conversationregistry.ForCodex(s.registry)
	if store == nil {
		return
	}
	record, ok := store.ByTarget(conversationregistry.BackendCodex, threadID)
	if !ok {
		return
	}
	s.sendOnce(kind+"/"+threadID+"/"+turnID, threadID, record.Title, text)
}

func (s *Service) sendOnce(key, threadID, title, text string) {
	s.goRun(func() {
		unlock := s.lockSync("event/" + key)
		defer unlock()
		s.mu.Lock()
		cfg := s.model.Config
		delivered := s.model.LiveDelivered[key]
		s.mu.Unlock()
		// FinalOnly is intentionally enforced here as a second line of
		// defence.  Final answers use the formal-final delivery path below;
		// this path is reserved for optional input/failure/stop reminders.
		if cfg.FinalOnly {
			return
		}
		payload := strings.TrimSpace(s.header(threadID, title) + "\n" + strings.TrimSpace(text))
		if cfg.Enabled && cfg.Telegram.Enabled && !delivered.Telegram {
			ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
			err := s.sendPlatform(ctx, "telegram", payload)
			cancel()
			if err == nil {
				s.markLive(key, "telegram")
			}
		}
		if cfg.Enabled && cfg.QQ.Enabled && !delivered.QQ {
			ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
			err := s.sendPlatform(ctx, "qqbot", payload)
			cancel()
			if err == nil {
				s.markLive(key, "qqbot")
			}
		}
	})
}
func (s *Service) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var model diskModel
	if err := json.Unmarshal(data, &model); err != nil {
		return fmt.Errorf("decode mirror state: %w", err)
	}
	// Version 1 mirror files predate the independent FinalOnly switch.  Keep
	// their behaviour: old input/error flags become the opt-in extra-reminder
	// settings, while both disabled means strict final-only delivery.
	var raw struct {
		Config struct {
			FinalOnly *bool `json:"finalOnly"`
		} `json:"config"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode mirror compatibility state: %w", err)
	}
	if raw.Config.FinalOnly == nil {
		model.Config.FinalOnly = !model.Config.Messages.RequestUserInput && !model.Config.Messages.Error
	}
	if model.Version != 1 {
		return fmt.Errorf("unsupported mirror state version %d", model.Version)
	}
	if model.Cursors == nil {
		model.Cursors = map[string]Cursor{}
	}
	if model.LiveDelivered == nil {
		model.LiveDelivered = map[string]liveCursor{}
	}
	if model.Finals == nil {
		model.Finals = map[string]finalRecord{}
	}
	model.Config.Messages.User = false
	model.Config.Messages.Status = false
	model.Config.Messages.Assistant = true
	s.model = model
	return nil
}
func (s *Service) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.model, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	bak := s.path + ".bak"
	_ = os.Remove(bak)
	if _, err := os.Stat(s.path); err == nil {
		if err := os.Rename(s.path, bak); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Rename(bak, s.path)
		return err
	}
	_ = os.Remove(bak)
	return nil
}
func fingerprint(values ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return hex.EncodeToString(hash[:12])
}
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func SortedCursors(value map[string]Cursor) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
