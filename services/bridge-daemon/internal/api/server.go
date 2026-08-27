package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/mirror"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/openclaw"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/qqbot"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/telegram"
)

type Server struct {
	token    string
	runtime  *bridgeruntime.Manager
	control  *control.Service
	bindings *bindings.Repository
	broker   *events.Broker
	logger   *bridgelog.SafeLogger
	telegram *telegram.Service
	qqbot    *qqbot.Service
	openclaw *openclaw.Service
	mirror   *mirror.Service
	commands *commandregistry.Registry
	http     *http.Server
}

func New(token string, runtimeManager *bridgeruntime.Manager, controlService *control.Service, bindingRepository *bindings.Repository, broker *events.Broker, logger *bridgelog.SafeLogger, telegramService *telegram.Service, qqbotService *qqbot.Service, mirrorService *mirror.Service, registries ...*commandregistry.Registry) *Server {
	server := &Server{token: token, runtime: runtimeManager, control: controlService, bindings: bindingRepository, broker: broker, logger: logger, telegram: telegramService, qqbot: qqbotService, mirror: mirrorService}
	if len(registries) > 0 {
		server.commands = registries[0]
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("GET /api/v1/status", server.authorized(server.status))
	mux.HandleFunc("GET /api/v1/threads", server.authorized(server.threads))
	mux.HandleFunc("GET /api/v1/threads/{threadId}", server.authorized(server.thread))
	mux.HandleFunc("POST /api/v1/threads/{threadId}/turns", server.authorized(server.startTurn))
	mux.HandleFunc("POST /api/v1/threads/{threadId}/persistence/verify", server.authorized(server.verifyThreadPersistence))
	mux.HandleFunc("POST /api/v1/threads/{threadId}/turns/{turnId}/interrupt", server.authorized(server.interruptTurn))
	mux.HandleFunc("GET /api/v1/interactions", server.authorized(server.interactionList))
	mux.HandleFunc("GET /api/v1/interactions/{interactionId}", server.authorized(server.interaction))
	mux.HandleFunc("POST /api/v1/interactions/{interactionId}/respond", server.authorized(server.respondInteraction))
	mux.HandleFunc("GET /api/v1/bindings", server.authorized(server.bindingList))
	mux.HandleFunc("POST /api/v1/bindings", server.authorized(server.createBinding))
	mux.HandleFunc("DELETE /api/v1/bindings/{bindingId}", server.authorized(server.deleteBinding))
	mux.HandleFunc("PUT /api/v1/settings/security", server.authorized(server.updateSecurity))
	mux.HandleFunc("PUT /api/v1/settings/codex", server.authorized(server.updateCodex))
	mux.HandleFunc("PUT /api/v1/settings/openclaw", server.authorized(server.openclawConfigure))
	mux.HandleFunc("GET /api/v1/openclaw/status", server.authorized(server.openclawStatus))
	mux.HandleFunc("POST /api/v1/openclaw/test", server.authorized(server.openclawTest))
	mux.HandleFunc("GET /api/v1/openclaw/sessions", server.authorized(server.openclawSessions))
	mux.HandleFunc("GET /api/v1/openclaw/sessions/{sessionKey}", server.authorized(server.openclawSession))
	mux.HandleFunc("POST /api/v1/openclaw/sessions/{sessionKey}/messages", server.authorized(server.openclawSend))
	mux.HandleFunc("POST /api/v1/openclaw/sessions/{sessionKey}/abort", server.authorized(server.openclawAbort))
	mux.HandleFunc("GET /api/v1/commands", server.authorized(server.commandList))
	mux.HandleFunc("POST /api/v1/commands", server.authorized(server.commandCreate))
	mux.HandleFunc("PUT /api/v1/commands/{id}", server.authorized(server.commandUpdate))
	mux.HandleFunc("DELETE /api/v1/commands/{id}", server.authorized(server.commandDelete))
	mux.HandleFunc("POST /api/v1/commands/{id}/lock", server.authorized(server.commandLock))
	mux.HandleFunc("POST /api/v1/commands/{id}/unlock", server.authorized(server.commandUnlock))
	mux.HandleFunc("POST /api/v1/commands/{id}/restore", server.authorized(server.commandRestore))
	mux.HandleFunc("GET /api/v1/mirror", server.authorized(server.mirrorStatus))
	mux.HandleFunc("PUT /api/v1/mirror", server.authorized(server.mirrorConfigure))
	mux.HandleFunc("GET /api/v1/events", server.authorized(server.eventStream))
	mux.HandleFunc("GET /api/v1/channels", server.authorized(server.channelList))
	mux.HandleFunc("GET /api/v1/channels/telegram/status", server.authorized(server.telegramStatus))
	mux.HandleFunc("POST /api/v1/channels/telegram/configure", server.authorized(server.telegramConfigure))
	mux.HandleFunc("POST /api/v1/channels/telegram/test", server.authorized(server.telegramTest))
	mux.HandleFunc("POST /api/v1/channels/telegram/test-proxy", server.authorized(server.telegramTestProxy))
	mux.HandleFunc("POST /api/v1/channels/telegram/start", server.authorized(server.telegramStart))
	mux.HandleFunc("POST /api/v1/channels/telegram/stop", server.authorized(server.telegramStop))
	mux.HandleFunc("DELETE /api/v1/channels/telegram/token", server.authorized(server.telegramDeleteToken))
	mux.HandleFunc("GET /api/v1/channels/qqbot/status", server.authorized(server.qqbotStatus))
	mux.HandleFunc("POST /api/v1/channels/qqbot/configure", server.authorized(server.qqbotConfigure))
	mux.HandleFunc("POST /api/v1/channels/qqbot/secret", server.authorized(server.qqbotSecret))
	mux.HandleFunc("DELETE /api/v1/channels/qqbot/secret", server.authorized(server.qqbotDeleteSecret))
	mux.HandleFunc("POST /api/v1/channels/qqbot/test", server.authorized(server.qqbotTest))
	mux.HandleFunc("POST /api/v1/channels/qqbot/network-test", server.authorized(server.qqbotNetworkTest))
	mux.HandleFunc("POST /api/v1/channels/qqbot/start", server.authorized(server.qqbotStart))
	mux.HandleFunc("POST /api/v1/channels/qqbot/stop", server.authorized(server.qqbotStop))
	mux.HandleFunc("GET /api/v1/channels/qqbot/discovered-identities", server.authorized(server.qqbotDiscoveredIdentities))
	server.http = &http.Server{
		Handler: server.securityHeaders(mux), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	return server
}

// SetOpenClawBackend attaches the optional OpenClaw transport after all
// existing Codex/channel services have been constructed. This keeps the
// current api.New call shape and makes OpenClaw additive.
func (s *Server) SetOpenClawBackend(backend *openclaw.Service) {
	s.openclaw = backend
}

func (s *Server) commandList(response http.ResponseWriter, _ *http.Request) {
	if s.commands == nil {
		writeError(response, http.StatusServiceUnavailable, "commands_unavailable", "指令服务尚未初始化")
		return
	}
	writeJSON(response, http.StatusOK, s.commands.List())
}

func (s *Server) commandCreate(response http.ResponseWriter, request *http.Request) {
	if !s.requireCommands(response) {
		return
	}
	var input commandregistry.Mutation
	if !decodeBody(response, request, 64*1024, &input) {
		return
	}
	result, err := s.commands.Create(input)
	if err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, result)
}

func (s *Server) commandUpdate(response http.ResponseWriter, request *http.Request) {
	if !s.requireCommands(response) {
		return
	}
	var input commandregistry.Mutation
	if !decodeBody(response, request, 64*1024, &input) {
		return
	}
	result, err := s.commands.Update(request.PathValue("id"), input)
	if err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) commandDelete(response http.ResponseWriter, request *http.Request) {
	if !s.requireCommands(response) {
		return
	}
	if err := s.commands.Delete(request.PathValue("id")); err != nil {
		writeCommandError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (s *Server) commandLock(response http.ResponseWriter, request *http.Request) {
	s.commandSetLocked(response, request, true)
}

func (s *Server) commandUnlock(response http.ResponseWriter, request *http.Request) {
	s.commandSetLocked(response, request, false)
}

func (s *Server) commandSetLocked(response http.ResponseWriter, request *http.Request, locked bool) {
	if !s.requireCommands(response) {
		return
	}
	result, err := s.commands.SetLocked(request.PathValue("id"), locked)
	if err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) commandRestore(response http.ResponseWriter, request *http.Request) {
	if !s.requireCommands(response) {
		return
	}
	result, err := s.commands.Restore(request.PathValue("id"))
	if err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) requireCommands(response http.ResponseWriter) bool {
	if s.commands != nil {
		return true
	}
	writeError(response, http.StatusServiceUnavailable, "commands_unavailable", "指令服务尚未初始化")
	return false
}

func writeCommandError(response http.ResponseWriter, err error) {
	status, code := http.StatusBadRequest, "command_invalid"
	if errors.Is(err, commandregistry.ErrNotFound) {
		status, code = http.StatusNotFound, "command_not_found"
	} else if errors.Is(err, commandregistry.ErrLocked) {
		status, code = http.StatusConflict, "command_locked"
	}
	writeError(response, status, code, err.Error())
}

func (s *Server) mirrorStatus(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, s.mirror.Status())
}

func (s *Server) mirrorConfigure(response http.ResponseWriter, request *http.Request) {
	var input mirror.Config
	if !decodeBody(response, request, 64*1024, &input) {
		return
	}
	status, err := s.mirror.Configure(input)
	if err != nil {
		writeError(response, http.StatusBadRequest, "mirror_config_invalid", err.Error())
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) Serve(listener net.Listener) error {
	err := s.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	var failures []error
	if s.telegram != nil {
		if err := s.telegram.Stop(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if s.qqbot != nil {
		if err := s.qqbot.Stop(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if s.openclaw != nil {
		if err := s.openclaw.Stop(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if err := s.http.Shutdown(ctx); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(response, request)
	})
}

func (s *Server) authorized(next http.HandlerFunc) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		header := strings.TrimSpace(request.Header.Get("Authorization"))
		if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
			writeError(response, http.StatusUnauthorized, "unauthorized", "需要有效的本地 Bearer Token")
			return
		}
		provided := strings.TrimSpace(header[len("Bearer "):])
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) != 1 {
			writeError(response, http.StatusUnauthorized, "unauthorized", "需要有效的本地 Bearer Token")
			return
		}
		next(response, request)
	}
}

func (s *Server) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]any{"ok": true, "service": "bridge-daemon"})
}

func (s *Server) status(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, s.runtime.Status())
}

func (s *Server) threads(response http.ResponseWriter, request *http.Request) {
	limit := 50
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 200 {
			writeError(response, http.StatusBadRequest, "invalid_limit", "limit 必须是 1 到 200 之间的整数")
			return
		}
		limit = parsed
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	result, err := s.control.ListThreads(ctx, limit, request.URL.Query().Get("cursor"))
	if err != nil {
		s.writeCodexError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) thread(response http.ResponseWriter, request *http.Request) {
	threadID, ok := pathID(response, request.PathValue("threadId"), "Thread")
	if !ok {
		return
	}
	includeTurns := strings.EqualFold(request.URL.Query().Get("includeTurns"), "true")
	ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
	defer cancel()
	result, err := s.control.ReadThread(ctx, threadID, includeTurns)
	if err != nil {
		s.writeCodexError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) startTurn(response http.ResponseWriter, request *http.Request) {
	threadID, ok := pathID(response, request.PathValue("threadId"), "Thread")
	if !ok {
		return
	}
	var input control.StartTurnRequest
	if !decodeBody(response, request, 64*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	result, err := s.runtime.StartTurn(ctx, threadID, input)
	if err != nil {
		s.writeRuntimeError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, result)
}

func (s *Server) verifyThreadPersistence(response http.ResponseWriter, request *http.Request) {
	threadID, ok := pathID(response, request.PathValue("threadId"), "Thread")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 45*time.Second)
	defer cancel()
	result, err := s.runtime.VerifyThreadPersistence(ctx, threadID)
	if err != nil && result.ThreadID == "" {
		s.writeRuntimeError(response, err)
		return
	}
	// A negative persistence result is a completed diagnostic operation, not a
	// transport failure. Return it to the UI with status and evidence intact.
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) interruptTurn(response http.ResponseWriter, request *http.Request) {
	threadID, ok := pathID(response, request.PathValue("threadId"), "Thread")
	if !ok {
		return
	}
	turnID, ok := pathID(response, request.PathValue("turnId"), "Turn")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	result, err := s.runtime.InterruptTurn(ctx, threadID, turnID)
	if err != nil {
		s.writeRuntimeError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, result)
}

func (s *Server) interactionList(response http.ResponseWriter, request *http.Request) {
	status := strings.TrimSpace(request.URL.Query().Get("status"))
	if status != "" && status != "pending" && status != "resolved" && status != "allowed" && status != "denied" && status != "submitted" && status != "expired" && status != "cancelled" {
		writeError(response, http.StatusBadRequest, "invalid_status", "无效的交互状态筛选")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"interactions": s.runtime.ListInteractions(status)})
}

func (s *Server) interaction(response http.ResponseWriter, request *http.Request) {
	id, ok := pathID(response, request.PathValue("interactionId"), "Interaction")
	if !ok {
		return
	}
	result, found := s.runtime.GetInteraction(id)
	if !found {
		writeError(response, http.StatusNotFound, "interaction_not_found", "交互请求不存在")
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) respondInteraction(response http.ResponseWriter, request *http.Request) {
	id, ok := pathID(response, request.PathValue("interactionId"), "Interaction")
	if !ok {
		return
	}
	var input interactions.ResponseRequest
	if !decodeBody(response, request, 128*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	result, err := s.runtime.RespondInteraction(ctx, id, input)
	if err != nil {
		s.writeRuntimeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) bindingList(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]any{"bindings": s.bindings.List()})
}

func (s *Server) createBinding(response http.ResponseWriter, request *http.Request) {
	var input bindings.CreateRequest
	if !decodeBody(response, request, 32*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	backend := strings.ToLower(strings.TrimSpace(input.Backend))
	if backend == "" {
		backend = conversation.BackendCodex
	}
	if backend != conversation.BackendCodex && backend != conversation.BackendOpenClaw {
		writeError(response, http.StatusBadRequest, "invalid_backend", "不支持的会话后端")
		return
	}
	input.Backend = backend
	if backend == conversation.BackendOpenClaw {
		if s.openclaw == nil {
			writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
			return
		}
		target := strings.TrimSpace(input.SessionKey)
		if target == "" {
			target = strings.TrimSpace(input.ThreadID)
		}
		detail, err := s.openclaw.ReadSession(ctx, target)
		if err != nil || detail.Key == "" {
			writeError(response, http.StatusBadRequest, "session_not_found", "绑定目标 OpenClaw Session 不存在")
			return
		}
		if detail.Archived != nil && *detail.Archived {
			writeError(response, http.StatusConflict, "session_archived", "绑定目标 OpenClaw Session 已归档")
			return
		}
		input.SessionKey, input.ThreadID = detail.Key, detail.Key
	} else {
		thread, err := s.control.ReadThread(ctx, strings.TrimSpace(input.ThreadID), false)
		if err != nil || thread.ThreadID == "" {
			writeError(response, http.StatusBadRequest, "thread_not_found", "绑定目标 Thread 不存在")
			return
		}
		if thread.Archived != nil && *thread.Archived {
			writeError(response, http.StatusConflict, "thread_archived", "绑定目标 Thread 已归档")
			return
		}
	}
	created, err := s.bindings.Create(input)
	if errors.Is(err, bindings.ErrDuplicate) {
		writeError(response, http.StatusConflict, "binding_conflict", "同一渠道地址最多只能绑定一个 Thread")
		return
	}
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_binding", err.Error())
		return
	}
	s.broker.Publish(events.BindingCreated, safeAPIBindingPayload(created))
	if s.telegram != nil {
		s.telegram.BindingCreated(created)
	}
	if s.qqbot != nil {
		s.qqbot.BindingCreated(created)
	}
	writeJSON(response, http.StatusCreated, created)
}

func (s *Server) deleteBinding(response http.ResponseWriter, request *http.Request) {
	id, ok := pathID(response, request.PathValue("bindingId"), "Binding")
	if !ok {
		return
	}
	var deleted bindings.Binding
	for _, binding := range s.bindings.List() {
		if binding.ID == id {
			deleted = binding
			break
		}
	}
	if s.telegram != nil && deleted.ID != "" {
		s.telegram.BindingDeleted(deleted)
	}
	if s.qqbot != nil && deleted.ID != "" {
		s.qqbot.BindingDeleted(deleted)
	}
	err := s.bindings.Delete(id)
	if errors.Is(err, bindings.ErrNotFound) {
		writeError(response, http.StatusNotFound, "binding_not_found", "绑定不存在")
		return
	}
	if err != nil {
		writeError(response, http.StatusInternalServerError, "binding_delete_failed", "删除绑定失败")
		return
	}
	// The pre-delete notification above revokes delivery immediately. Notify
	// again after persistence so both channel summaries observe the new count.
	if s.telegram != nil && deleted.ID != "" {
		s.telegram.BindingDeleted(deleted)
	}
	if s.qqbot != nil && deleted.ID != "" {
		s.qqbot.BindingDeleted(deleted)
	}
	s.broker.Publish(events.BindingDeleted, safeAPIBindingPayload(deleted))
	response.WriteHeader(http.StatusNoContent)
}

func safeAPIBindingPayload(binding bindings.Binding) map[string]any {
	return map[string]any{
		"bindingId": binding.ID, "backend": binding.Backend, "sessionKey": shortAPIID(binding.SessionKey), "channelType": binding.ChannelType,
		"conversationType": binding.ConversationType,
		"account":          maskedAPIID(binding.AccountID), "chat": maskedAPIID(binding.ChatID),
		"topic": maskedAPIID(binding.TopicID), "threadId": shortAPIID(binding.ThreadID),
	}
}

func maskedAPIID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 4 {
		return "***"
	}
	return "***" + value[len(value)-4:]
}

func shortAPIID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 8 {
		return value
	}
	return value[:8] + "…"
}

func (s *Server) updateSecurity(response http.ResponseWriter, request *http.Request) {
	var input struct {
		SandboxMode string `json:"sandboxMode"`
	}
	if !decodeBody(response, request, 8*1024, &input) {
		return
	}
	status, err := s.runtime.SetSandboxMode(input.SandboxMode)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_sandbox", "沙盒模式只允许 read-only 或 workspace-write")
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) updateCodex(response http.ResponseWriter, request *http.Request) {
	var input struct {
		Path   string `json:"path"`
		Source string `json:"source"`
	}
	if !decodeBody(response, request, 16*1024, &input) {
		return
	}
	if strings.TrimSpace(input.Path) == "" {
		writeError(response, http.StatusBadRequest, "invalid_codex_path", "Codex path must not be empty")
		return
	}
	status, err := s.runtime.ApplyCodexPath(input.Path, input.Source)
	if err != nil {
		writeError(response, http.StatusUnprocessableEntity, "invalid_codex_path", bridgelog.Redact(err.Error()))
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) openclawStatus(response http.ResponseWriter, _ *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	writeJSON(response, http.StatusOK, s.openclaw.ConnectionStatus())
}

func (s *Server) openclawConfigure(response http.ResponseWriter, request *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	var input openclaw.ConfigureRequest
	if !decodeBody(response, request, 32*1024, &input) {
		return
	}
	status, err := s.openclaw.Configure(input)
	if err != nil {
		writeError(response, http.StatusBadRequest, "openclaw_configuration_invalid", bridgelog.Redact(err.Error()))
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) openclawTest(response http.ResponseWriter, request *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	var input openclaw.ConfigureRequest
	if !decodeOptionalBody(response, request, 32*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
	defer cancel()
	status, err := s.openclaw.Test(ctx, input)
	if err != nil {
		status.LastError = bridgelog.Redact(err.Error())
		writeJSON(response, http.StatusOK, status)
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) openclawSessions(response http.ResponseWriter, request *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 200 {
			writeError(response, http.StatusBadRequest, "invalid_limit", "limit 必须是 1 到 200 之间的整数")
			return
		}
		limit = parsed
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	sessions, err := s.openclaw.ListSessions(ctx, limit)
	if err != nil {
		s.writeOpenClawError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) openclawSession(response http.ResponseWriter, request *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	key, ok := pathID(response, request.PathValue("sessionKey"), "Session")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
	defer cancel()
	detail, err := s.openclaw.ReadSession(ctx, key)
	if err != nil {
		s.writeOpenClawError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, detail)
}

func (s *Server) openclawSend(response http.ResponseWriter, request *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	key, ok := pathID(response, request.PathValue("sessionKey"), "Session")
	if !ok {
		return
	}
	var input struct {
		Message string `json:"message"`
	}
	if !decodeBody(response, request, 128*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	result, err := s.openclaw.SendMessage(ctx, key, input.Message)
	if err != nil {
		s.writeOpenClawError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, result)
}

func (s *Server) openclawAbort(response http.ResponseWriter, request *http.Request) {
	if s.openclaw == nil {
		writeError(response, http.StatusServiceUnavailable, "openclaw_unavailable", "OpenClaw 后端尚未初始化")
		return
	}
	key, ok := pathID(response, request.PathValue("sessionKey"), "Session")
	if !ok {
		return
	}
	var input struct {
		RunID string `json:"runId"`
	}
	if !decodeOptionalBody(response, request, 16*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	result, err := s.openclaw.Abort(ctx, key, input.RunID)
	if err != nil {
		s.writeOpenClawError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, result)
}

func (s *Server) writeOpenClawError(response http.ResponseWriter, err error) {
	message := bridgelog.Redact(err.Error())
	s.logger.Printf("local API OpenClaw request failed: %s", message)
	status, code := http.StatusServiceUnavailable, "openclaw_unavailable"
	if errors.Is(err, openclaw.ErrSessionNotFound) {
		status, code = http.StatusNotFound, "session_not_found"
	} else if errors.Is(err, openclaw.ErrNotConnected) {
		status, code = http.StatusServiceUnavailable, "openclaw_disconnected"
	}
	writeError(response, status, code, message)
}

func (s *Server) channelList(response http.ResponseWriter, _ *http.Request) {
	channels := []any{}
	if s.telegram != nil {
		channels = append(channels, s.telegram.Adapter().TelegramStatus())
	}
	if s.qqbot != nil {
		channels = append(channels, s.qqbot.Adapter().QQBotStatus())
	}
	writeJSON(response, http.StatusOK, map[string]any{"channels": channels})
}

func (s *Server) telegramStatus(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, s.telegram.Adapter().TelegramStatus())
}

func (s *Server) telegramConfigure(response http.ResponseWriter, request *http.Request) {
	var input telegram.ConfigureRequest
	if !decodeBody(response, request, 32*1024, &input) {
		return
	}
	status, err := s.telegram.Configure(input)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_telegram_configuration", err.Error())
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) telegramTest(response http.ResponseWriter, request *http.Request) {
	var input telegram.TestRequest
	if !decodeOptionalBody(response, request, 16*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	result := s.telegram.Adapter().Test(ctx, input)
	s.broker.Publish(events.TelegramTested, map[string]any{"ok": result.OK, "category": result.Category})
	// A completed diagnostic is transported as 200 even when reachability is
	// false; the typed category is what the desktop client presents.
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) telegramTestProxy(response http.ResponseWriter, request *http.Request) {
	var input telegram.ProxyTestRequest
	if !decodeOptionalBody(response, request, 16*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	result := s.telegram.Adapter().TestProxy(ctx, input)
	s.broker.Publish(events.TelegramTested, map[string]any{
		"ok": result.OK, "category": result.Category, "networkStage": "proxy-test",
		"effectiveProxyMode": result.EffectiveProxyMode, "maskedProxyAddress": result.MaskedProxyAddress,
	})
	// A completed diagnostic request always returns its typed result. Network
	// reachability is represented by result.OK/category, not by the local API
	// transport status, so desktop clients can show the precise safe category.
	writeJSON(response, http.StatusOK, result)
}

func (s *Server) telegramStart(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
	defer cancel()
	if err := s.telegram.Start(ctx); err != nil {
		writeError(response, http.StatusConflict, "telegram_start_failed", telegramSafeAPIMessage(err))
		return
	}
	writeJSON(response, http.StatusOK, s.telegram.Adapter().TelegramStatus())
}

func (s *Server) telegramStop(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := s.telegram.Stop(ctx); err != nil {
		writeError(response, http.StatusGatewayTimeout, "telegram_stop_failed", "Telegram polling did not stop in time")
		return
	}
	writeJSON(response, http.StatusOK, s.telegram.Adapter().TelegramStatus())
}

func (s *Server) telegramDeleteToken(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := s.telegram.DeleteToken(ctx); err != nil {
		writeError(response, http.StatusGatewayTimeout, "telegram_token_delete_failed", "Telegram polling did not stop in time")
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (s *Server) qqbotStatus(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, s.qqbot.Adapter().QQBotStatus())
}

func (s *Server) qqbotConfigure(response http.ResponseWriter, request *http.Request) {
	var input qqbot.ConfigureRequest
	if !decodeBody(response, request, 64*1024, &input) {
		return
	}
	status, err := s.qqbot.Configure(input)
	if err != nil {
		writeError(response, http.StatusBadRequest, qqbot.ClassifyError(err), qqbotSafeAPIMessage(err))
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (s *Server) qqbotSecret(response http.ResponseWriter, request *http.Request) {
	var input qqbot.SecretRequest
	if !decodeBody(response, request, 16*1024, &input) {
		return
	}
	status, err := s.qqbot.Adapter().SetSecret(input.AppSecret)
	input.AppSecret = ""
	if err != nil {
		writeError(response, http.StatusBadRequest, qqbot.ClassifyError(err), qqbotSafeAPIMessage(err))
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"secretConfigured": status.SecretConfigured})
}

func (s *Server) qqbotDeleteSecret(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := s.qqbot.DeleteSecret(ctx); err != nil {
		writeError(response, http.StatusGatewayTimeout, "secret_delete_failed", "停止 QQ 官方机器人或清除 AppSecret 失败。")
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (s *Server) qqbotTest(response http.ResponseWriter, request *http.Request) {
	var input qqbot.TestRequest
	if !decodeOptionalBody(response, request, 8*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
	defer cancel()
	writeJSON(response, http.StatusOK, s.qqbot.Test(ctx, input))
}

func (s *Server) qqbotNetworkTest(response http.ResponseWriter, request *http.Request) {
	var input struct{}
	if !decodeOptionalBody(response, request, 8*1024, &input) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 20*time.Second)
	defer cancel()
	writeJSON(response, http.StatusOK, s.qqbot.Adapter().TestNetwork(ctx))
}

func (s *Server) qqbotStart(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	if err := s.qqbot.Start(ctx); err != nil {
		writeError(response, http.StatusConflict, qqbot.ClassifyError(err), qqbotSafeAPIMessage(err))
		return
	}
	writeJSON(response, http.StatusOK, s.qqbot.Adapter().QQBotStatus())
}

func (s *Server) qqbotStop(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := s.qqbot.Stop(ctx); err != nil {
		writeError(response, http.StatusGatewayTimeout, "qqbot_stop_failed", "QQ 官方机器人未能在限定时间内停止。")
		return
	}
	writeJSON(response, http.StatusOK, s.qqbot.Adapter().QQBotStatus())
}

func (s *Server) qqbotDiscoveredIdentities(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]any{"identities": s.qqbot.Adapter().DiscoveredIdentities()})
}

func qqbotSafeAPIMessage(err error) string {
	return qqbot.SafeErrorMessage(err)
}

func telegramSafeAPIMessage(err error) string {
	if errors.Is(err, telegram.ErrInvalidToken) {
		return "Telegram 拒绝了机器人 Token，请检查后重试。"
	}
	if errors.Is(err, telegram.ErrConflict) {
		return "另一个 Telegram getUpdates 客户端正在使用此机器人，请先停止它。"
	}
	switch telegramErrorCategory(err) {
	case "invalid-proxy":
		return "Telegram 代理配置无效，请检查代理模式和地址。"
	case "proxy-refused":
		return "Telegram 代理连接被拒绝，请检查代理地址和端口。"
	case "timeout":
		return "连接 Telegram 超时，请检查代理或网络。"
	case "tls":
		return "Telegram TLS 握手失败，请检查代理证书或系统时间。"
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "token is required") {
		return "请先配置 Telegram 机器人 Token。"
	}
	if strings.Contains(message, "allowed telegram user id") {
		return "请至少配置一个允许访问的 Telegram 用户 ID。"
	}
	return "无法启动 Telegram，请检查代理、网络和机器人配置。"
}

func telegramErrorCategory(err error) string {
	var apiErr *telegram.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Kind
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return ""
}

func (s *Server) eventStream(response http.ResponseWriter, request *http.Request) {
	flusher, ok := response.(http.Flusher)
	if !ok {
		writeError(response, http.StatusInternalServerError, "streaming_unavailable", "当前 HTTP 响应不支持 SSE")
		return
	}
	response.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
	channel, unsubscribe := s.broker.Subscribe()
	defer unsubscribe()
	_, _ = fmt.Fprint(response, ": connected\n\n")
	flusher.Flush()
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case event, open := <-channel:
			if !open {
				return
			}
			payload, _ := json.Marshal(event)
			_, _ = fmt.Fprintf(response, "id: %s\nevent: %s\ndata: %s\n\n", event.EventID, event.EventType, payload)
			flusher.Flush()
		case <-keepAlive.C:
			_, _ = fmt.Fprint(response, ": keep-alive\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) writeRuntimeError(response http.ResponseWriter, err error) {
	var compatibility *bridgeruntime.ProtocolCompatibilityError
	if errors.As(err, &compatibility) {
		s.logger.Printf("Codex protocol compatibility failure: %s", bridgelog.Redact(compatibility.Detail))
		writeError(response, http.StatusBadGateway, "codex_protocol_incompatible", compatibility.Message)
		return
	}
	var conflict *bridgeruntime.ConflictError
	if errors.As(err, &conflict) {
		writeJSON(response, http.StatusConflict, map[string]any{
			"code": conflict.Code, "message": conflict.Message,
			"threadId": conflict.ThreadID, "currentState": conflict.CurrentState,
		})
		return
	}
	var validation *bridgeruntime.ValidationError
	if errors.As(err, &validation) {
		status := http.StatusBadRequest
		if validation.Code == "thread_not_found" {
			status = http.StatusNotFound
		}
		writeError(response, status, validation.Code, validation.Message)
		return
	}
	s.writeCodexError(response, err)
}

func (s *Server) writeCodexError(response http.ResponseWriter, err error) {
	message := bridgelog.Redact(err.Error())
	s.logger.Printf("local API Codex request failed: %s", message)
	status := http.StatusServiceUnavailable
	code := "codex_unavailable"
	if strings.Contains(strings.ToLower(message), "not found") {
		status, code = http.StatusNotFound, "thread_not_found"
	}
	writeError(response, status, code, message)
}

func decodeBody(response http.ResponseWriter, request *http.Request, maxBytes int64, target any) bool {
	request.Body = http.MaxBytesReader(response, request.Body, maxBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			writeError(response, http.StatusBadRequest, "empty_body", "请求体不能为空")
		} else {
			writeError(response, http.StatusBadRequest, "invalid_json", "请求 JSON 无效或超过大小限制")
		}
		return false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeError(response, http.StatusBadRequest, "invalid_json", "请求体只能包含一个 JSON 对象")
		return false
	}
	return true
}

func decodeOptionalBody(response http.ResponseWriter, request *http.Request, maxBytes int64, target any) bool {
	request.Body = http.MaxBytesReader(response, request.Body, maxBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		writeError(response, http.StatusBadRequest, "invalid_json", "请求 JSON 无效或超过大小限制")
		return false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeError(response, http.StatusBadRequest, "invalid_json", "请求体只能包含一个 JSON 对象")
		return false
	}
	return true
}

func pathID(response http.ResponseWriter, raw, name string) (string, bool) {
	value, err := url.PathUnescape(raw)
	value = strings.TrimSpace(value)
	if err != nil || value == "" || len(value) > 256 || strings.ContainsAny(value, "/\\") {
		writeError(response, http.StatusBadRequest, "invalid_id", name+" ID 无效")
		return "", false
	}
	return value, true
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]any{"code": code, "message": message})
}
